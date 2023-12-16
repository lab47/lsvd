package lsvd

import (
	"fmt"
	"strings"

	"github.com/hashicorp/go-hclog"
	"github.com/lab47/lsvd/pkg/treemap"
	"github.com/lab47/mode"
)

type ExtentMap struct {
	log hclog.Logger
	m   *treemap.TreeMap[LBA, *RangedOPBA]

	coverBlocks int
}

func NewExtentMap(log hclog.Logger) *ExtentMap {
	return &ExtentMap{
		log: log,
		m:   treemap.New[LBA, *RangedOPBA](),
	}
}

func (e *ExtentMap) Len() int {
	return e.m.Len()
}

func (e *ExtentMap) find(lba LBA) treemap.ForwardIterator[LBA, *RangedOPBA] {
	i := e.m.Floor(lba)
	if i.Valid() {
		return i
	}

	return e.m.LowerBound(lba)
}

func (e *ExtentMap) update(rng Extent, pba OPBA) error {
	var (
		toDelete []LBA
		toAdd    []*RangedOPBA
	)

loop:
	for i := e.m.Floor(rng.LBA); i.Valid(); i.Next() {
		e.log.Trace("found bound", "lba", i.Key(), "from", rng.LBA)

		cur := i.Value()

		coverage := cur.Range.Cover(rng)

		e.log.Trace("considering",
			"a", cur.Range, "b", rng,
			"a-sub-b", coverage,
		)

		/*

			if i.Key() == rng.LBA {
				if cur.Range.Blocks > rng.Blocks {
					toDelete = append(toDelete, i.Key())

					// The current range is larger than our new one,
					// so we need to change the start of the current range
					// to just past the new range.
					pivot := rng.LBA + LBA(rng.Blocks)
					cur.Range.LBA = pivot
					cur.Range.Blocks -= rng.Blocks

					toAdd = append(toAdd, cur)
				}

				// if the new rng is larger than the current one
				// we can safely discard it when we do the Set() below.

				break
			}
		*/

		switch coverage {
		case CoverNone:
			// ok, disjoint, easy and done.
			break loop

		case CoverCompletely:
			// The new range is a complete subrange (ie a hole)
			// into the existing range, so we need to adjust
			// and add a new range before and after the hole.

			prefix := ExtentFrom(cur.Range.LBA, rng.LBA-1)
			suffix := ExtentFrom(rng.Last()+1, cur.Range.Last())

			if suffix.Blocks > 0 {
				dup := *cur
				dup.Range = suffix
				toAdd = append(toAdd, &dup)
			}

			if prefix.Blocks > 0 {
				cur.Range = prefix
			} else {
				toDelete = append(toDelete, prefix.LBA)
			}
		case CoverPartly:
			if rng.Cover(cur.Range) == CoverCompletely {
				// The new range completely covers the current one
				// so we can clobber it.
			} else {
				// We need to shrink the range of cur down to not overlap
				// with the new range.
				cur.Range = ExtentFrom(cur.Range.LBA, rng.LBA-1)
			}
			/*
						} else if cur.Range.LBA < rng.LBA {
							if cur.Range.Blocks > rng.Blocks {
								// New range is making a hole in the current range, so
								// we need to dup and put a new range afterward

								dup := *cur
								dup.Range = ExtentFrom(rng.Last()+1, cur.Range.Last())

								toAdd = append(toAdd, &dup)
							} else {
								old := cur.Range
								cur.Range = ExtentFrom(cur.Range.LBA, rng.LBA-1)
								e.log.Trace("shrunk range", "old", old, "new", cur.Range)
							}
				} else if rng.Contains(cur.Range.Last()) {
					// The new range is a superrange of the current one, so
					// just nuke the current one.
					toDelete = append(toDelete, i.Key())
				} else {
					// ok, they overlap with rng first and cur.Range second
					// so we need to adjust where cur.Range starts.
					pivot := rng.LBA + LBA(rng.Blocks)
					cur.Range = ExtentFrom(pivot, cur.Range.Last())

					toDelete = append(toDelete, i.Key())
					toAdd = append(toAdd, cur)
			*/
		}
	}

loop2:
	// Also check for ranges that start higher to be considered
	for i := e.m.LowerBound(rng.LBA); i.Valid(); i.Next() {
		cur := i.Value()
		coverage := rng.Cover(cur.Range)

		switch coverage {
		case CoverNone:
			break loop2

		case CoverCompletely:
			// our new range completely covers the exist one, so we delete it.
			toDelete = append(toDelete, i.Key())
		case CoverPartly:
			old := cur.Range
			pivot := rng.Last() + 1
			cur.Range = ExtentFrom(pivot, cur.Range.Last())

			toDelete = append(toDelete, i.Key())
			toAdd = append(toAdd, cur)
			e.log.Trace("pivoting range", "pivot", pivot, "from", old, "to", cur.Range)
		}
	}

	for _, lba := range toDelete {
		e.m.Del(lba)
	}

	for _, pba := range toAdd {
		e.m.Set(pba.Range.LBA, pba)
	}

	e.log.Trace("adding read range", "range", rng)
	e.m.Set(rng.LBA, &RangedOPBA{
		OPBA: pba,
		Full: rng,

		// This value is updated as the range is shrunk
		// when new, overlapping ranges are added. Full does not
		// change size though.
		Range: rng,
	})

	if mode.Debug() {
		e.log.Debug("validating map post update")
		return e.Validate()
	}

	return nil
}

func (e *ExtentMap) Validate() error {
	var prev *RangedOPBA

	for i := e.m.Iterator(); i.Valid(); i.Next() {
		lba := i.Key()
		pba := i.Value()

		if lba != pba.Range.LBA {
			return fmt.Errorf("key didn't match pba: %d != %d", lba, pba.Range.LBA)
		}

		if prev != nil {
			if prev.Range.Last() >= lba {
				return fmt.Errorf("overlapping ranges detected: %s <=> %s",
					prev.Range, pba.Range)
			}
		}

		prev = pba
	}

	return nil
}

func (e *ExtentMap) Render() string {
	var parts []string
	for i := e.m.Iterator(); i.Valid(); i.Next() {
		pba := i.Value()

		parts = append(parts, fmt.Sprintf("%d-%d", pba.Range.LBA, pba.Range.Last()))
	}

	return strings.Join(parts, " ")
}

func (e *ExtentMap) Resolve(rng Extent) ([]*RangedOPBA, error) {
	var ret []*RangedOPBA

loop:
	for i := e.m.Floor(rng.LBA); i.Valid(); i.Next() {
		cur := i.Value()

		e.log.Trace("consider for resolve", "cur", cur.Range, "against", rng)

		switch cur.Range.Cover(rng) {
		case CoverPartly:
			ret = append(ret, cur)
		case CoverCompletely:
			ret = append(ret, cur)
			break loop
		case CoverNone:
			break loop
		}
	}

	return ret, nil
}
