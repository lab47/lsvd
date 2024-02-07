package lsvd

import (
	"fmt"
	"strings"
	"sync"

	"github.com/hashicorp/go-hclog"
	"github.com/lab47/lsvd/pkg/treemap"
	"github.com/lab47/mode"
)

type ExtentMap struct {
	log hclog.Logger
	mu  sync.Mutex
	m   *treemap.TreeMap[LBA, *PartialExtent]

	coverBlocks int
}

func NewExtentMap(log hclog.Logger) *ExtentMap {
	return &ExtentMap{
		log: log,
		m:   treemap.New[LBA, *PartialExtent](),
	}
}

func (e *ExtentMap) Len() int {
	return e.m.Len()
}

func (e *ExtentMap) find(lba LBA) treemap.ForwardIterator[LBA, *PartialExtent] {
	i := e.m.Floor(lba)
	if i.Valid() {
		return i
	}

	return e.m.LowerBound(lba)
}

func (m *ExtentMap) checkExtent(e Extent) Extent {
	if mode.Debug() {
		if e.Blocks == 0 {
			panic(fmt.Sprintf("empty range detected: %s", e))
		}

		if e.Blocks > 1_000_000_000 {
			panic(fmt.Sprintf("extremely large range detected: %s", e))
		}
	}

	return e
}

func (e *ExtentMap) UpdateBatch(log hclog.Logger, entries []ExtentLocation, segId SegmentId, s *Segments) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, ent := range entries {
		if mode.Debug() {
			log.Trace("updating read map", "extent", ent.Extent)
		}
		affected, err := e.update(ent)
		if err != nil {
			log.Error("error updating read map", "error", err)
		}

		s.UpdateUsage(log, segId, affected)
	}

	return nil
}

func (e *ExtentMap) Update(pba ExtentLocation) ([]PartialExtent, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.update(pba)
}

func (e *ExtentMap) update(pba ExtentLocation) ([]PartialExtent, error) {
	var (
		toDelete []LBA
		toAdd    []*PartialExtent
		affected []PartialExtent

		rng = pba.Extent
	)

	if pba.Flags == 1 && pba.RawSize == 0 {
		panic("bad opba")
	}

	e.checkExtent(rng)

	e.log.Trace("triggered update", "extent", rng)

loop:
	for i := e.m.Floor(rng.LBA); i.Valid(); i.Next() {
		// If we advance past our start position, stop immediately.
		// We'll pick up these entries in the lowerbound loop below.
		if i.Key() >= rng.LBA {
			break
		}

		e.log.Trace("found bound", "key", i.Key(), "match", i.Value().Partial, "from", rng.LBA)

		cur := i.Value()

		orig := cur.Partial

		coverage := cur.Partial.Cover(rng)

		e.log.Trace("considering",
			"a", orig, "b", rng,
			"a-sub-b", coverage,
		)

		switch coverage {
		case CoverNone:
			// ok, disjoint, easy and done.
			break loop

		case CoverExact:
			// The ranges are exactly the same, so we don't
			// need to adjust anything.

			affected = append(affected, *cur)
		case CoverSuperRange:
			// The new range is a complete subrange (ie a hole)
			// into the existing range, so we need to adjust
			// and add a new range before and after the hole.

			suffix, ok := ExtentFrom(rng.Last()+1, cur.Partial.Last())
			if ok {
				dup := *cur
				dup.Partial = suffix
				toAdd = append(toAdd, &dup)
			}

			if rng.LBA > 0 {
				prefix, ok := ExtentFrom(cur.Partial.LBA, rng.LBA-1)
				if ok {
					cur.Partial = prefix
				}
			}

			rem := *cur
			rem.Partial = rng
			affected = append(affected, rem)

			e.checkExtent(cur.Partial)
		case CoverPartly:
			var masked Extent

			if rng.Cover(cur.Partial) == CoverSuperRange {
				// The new range completely covers the current one
				// so we can clobber it.
				masked = rng
			} else {
				// We need to shrink the range of cur down to not overlap
				// with the new range.
				update, ok := ExtentFrom(cur.Partial.LBA, rng.LBA-1)
				if !ok {
					e.log.Error("error calculate updated range", "orig", cur.Partial, "target", rng.LBA-1)
					return nil, fmt.Errorf("error calculating new range")
				}

				masked, ok = ExtentFrom(rng.LBA, cur.Partial.Last())
				if !ok {
					e.log.Error("error calculate masked range", "orig", cur.Partial, "target", rng.LBA-1)
					return nil, fmt.Errorf("error calculating new range")
				}

				cur.Partial = update
			}

			rem := *cur
			rem.Partial = masked
			affected = append(affected, rem)

			e.checkExtent(cur.Partial)
		default:
			return nil, fmt.Errorf("invalid coverage value: %s", coverage)
		}
	}

loop2:
	// Also check for ranges that start higher to be considered
	for i := e.m.LowerBound(rng.LBA); i.Valid(); i.Next() {
		cur := i.Value()
		coverage := rng.Cover(cur.Partial)

		orig := cur.Partial

		e.log.Trace("considering",
			"a", rng, "b", orig,
			"a-sub-b", coverage,
		)

		switch coverage {
		case CoverNone:
			break loop2

		case CoverSuperRange, CoverExact:
			// our new range completely covers the exist one, so we delete it.
			affected = append(affected, *cur)
			toDelete = append(toDelete, i.Key())
		case CoverPartly:
			old := cur.Partial
			pivot := rng.Last() + 1
			update, ok := ExtentFrom(pivot, old.Last())
			if !ok {
				e.log.Error("error calculating new extent", "pivot", pivot, "old", old)
				return nil, fmt.Errorf("error calculating new extent")
			}

			rem := *cur

			rem.Partial, ok = ExtentFrom(old.LBA, rng.Last())
			if !ok {
				e.log.Error("error calculating masked extent", "pivot", pivot, "old", old)
				return nil, fmt.Errorf("error calculating new extent")
			}
			affected = append(affected, rem)

			cur.Partial = update

			toDelete = append(toDelete, i.Key())
			toAdd = append(toAdd, cur)
			e.log.Trace("pivoting range", "pivot", pivot, "from", old, "to", cur.Partial)
			e.checkExtent(cur.Partial)
		default:
			return nil, fmt.Errorf("invalid coverage value: %s", coverage)
		}
	}

	for _, lba := range toDelete {
		e.log.Trace("deleting range", "lba", lba)
		e.m.Del(lba)
	}

	for _, pba := range toAdd {
		e.checkExtent(pba.Partial)
		e.log.Trace("adding range", "rng", pba.Partial)
		e.m.Set(pba.Partial.LBA, pba)
	}

	e.checkExtent(rng)

	e.log.Trace("adding read range", "range", rng)
	e.m.Set(rng.LBA, &PartialExtent{
		ExtentLocation: pba,

		// This value is updated as the range is shrunk
		// when new, overlapping ranges are added. Full does not
		// change size though.
		Partial: rng,
	})

	if false { // mode.Debug() {
		e.log.Debug("validating map post update")
		return nil, e.Validate()
	}

	return affected, nil
}

func (e *ExtentMap) Validate() error {
	var prev *PartialExtent

	for i := e.m.Iterator(); i.Valid(); i.Next() {
		lba := i.Key()
		pba := i.Value()

		if pba.Partial.Blocks == 0 || pba.Blocks == 0 {
			return fmt.Errorf("invalid zero length range at %v: %v", lba, pba.Partial.LBA)
		}

		if pba.Partial.Blocks >= 1_000_000_000 {
			e.log.Error("extremely large block range detected", "range", pba.Partial)
			return fmt.Errorf("extremly large block range detected: %d: %s", lba, pba.Partial)
		}

		if lba != pba.Partial.LBA {
			return fmt.Errorf("key didn't match pba: %d != %d", lba, pba.Partial.LBA)
		}

		if prev != nil {
			if prev.Partial.Last() >= lba {
				return fmt.Errorf("overlapping ranges detected: %s <=> %s",
					prev.Partial, pba.Partial)
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

		if pba.Partial.Blocks == 1 {
			parts = append(parts, fmt.Sprintf("%d", pba.Partial.LBA))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", pba.Partial.LBA, pba.Partial.Last()))
		}
	}

	return strings.Join(parts, " ")
}

func (e *ExtentMap) Resolve(rng Extent) ([]*PartialExtent, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var ret []*PartialExtent

loop:
	for i := e.m.Floor(rng.LBA); i.Valid(); i.Next() {
		// Only consider ranges that start before the requested one
		if i.Key() >= rng.LBA {
			break
		}

		cur := i.Value()

		e.log.Trace("consider for resolve", "cur", cur.Partial, "against", rng)

		switch cur.Partial.Cover(rng) {
		case CoverPartly:
			ret = append(ret, cur)
		case CoverSuperRange, CoverExact:
			ret = append(ret, cur)
			break loop
		case CoverNone:
			break loop
		}
	}

loop2:
	// Also check for ranges that start higher to be considered
	for i := e.m.LowerBound(rng.LBA); i.Valid(); i.Next() {
		cur := i.Value()
		coverage := cur.Partial.Cover(rng)

		orig := cur.Partial

		e.log.Trace("considering",
			"a", rng, "b", orig,
			"a-sub-b", coverage,
		)

		switch coverage {
		case CoverNone:
			break loop2
		case CoverSuperRange, CoverExact:
			ret = append(ret, cur)
			break loop2
		case CoverPartly:
			ret = append(ret, cur)
		default:
			return nil, fmt.Errorf("invalid coverage value: %s", coverage)
		}

	}

	return ret, nil
}
