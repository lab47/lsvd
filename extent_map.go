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
	mu sync.Mutex
	m  *treemap.TreeMap[LBA, *PartialExtent]

	coverBlocks int
}

func NewExtentMap() *ExtentMap {
	return &ExtentMap{
		m: treemap.New[LBA, *PartialExtent](),
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
		affected, err := e.update(log, ent)
		if err != nil {
			log.Error("error updating read map", "error", err)
		}

		s.UpdateUsage(log, segId, affected)
	}

	return nil
}

func (e *ExtentMap) Update(log hclog.Logger, pba ExtentLocation) ([]PartialExtent, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.update(log, pba)
}

func (e *ExtentMap) update(log hclog.Logger, pba ExtentLocation) ([]PartialExtent, error) {
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

	log.Trace("triggered update", "extent", rng)

loop:
	for i := e.m.Floor(rng.LBA); i.Valid(); i.Next() {
		// If we advance past our start position, stop immediately.
		// We'll pick up these entries in the lowerbound loop below.
		if i.Key() >= rng.LBA {
			break
		}

		log.Trace("found bound", "key", i.Key(), "match", i.Value().Live, "from", rng.LBA)

		cur := i.Value()

		orig := cur.Live

		coverage := cur.Live.Cover(rng)

		log.Trace("considering",
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

			suffix, ok := ExtentFrom(rng.Last()+1, cur.Live.Last())
			if ok {
				dup := *cur
				dup.Live = suffix
				toAdd = append(toAdd, &dup)
			}

			if rng.LBA > 0 {
				prefix, ok := ExtentFrom(cur.Live.LBA, rng.LBA-1)
				if ok {
					cur.Live = prefix
				}
			}

			rem := *cur
			rem.Live = rng
			affected = append(affected, rem)

			e.checkExtent(cur.Live)
		case CoverPartly:
			var masked Extent

			if rng.Cover(cur.Live) == CoverSuperRange {
				// The new range completely covers the current one
				// so we can clobber it.
				masked = rng
			} else {
				// We need to shrink the range of cur down to not overlap
				// with the new range.
				update, ok := ExtentFrom(cur.Live.LBA, rng.LBA-1)
				if !ok {
					log.Error("error calculate updated range", "orig", cur.Live, "target", rng.LBA-1)
					return nil, fmt.Errorf("error calculating new range")
				}

				masked, ok = ExtentFrom(rng.LBA, cur.Live.Last())
				if !ok {
					log.Error("error calculate masked range", "orig", cur.Live, "target", rng.LBA-1)
					return nil, fmt.Errorf("error calculating new range")
				}

				cur.Live = update
			}

			rem := *cur
			rem.Live = masked
			affected = append(affected, rem)

			e.checkExtent(cur.Live)
		default:
			return nil, fmt.Errorf("invalid coverage value: %s", coverage)
		}
	}

loop2:
	// Also check for ranges that start higher to be considered
	for i := e.m.LowerBound(rng.LBA); i.Valid(); i.Next() {
		cur := i.Value()
		coverage := rng.Cover(cur.Live)

		orig := cur.Live

		log.Trace("considering",
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
			old := cur.Live
			pivot := rng.Last() + 1
			update, ok := ExtentFrom(pivot, old.Last())
			if !ok {
				log.Error("error calculating new extent", "pivot", pivot, "old", old)
				return nil, fmt.Errorf("error calculating new extent")
			}

			rem := *cur

			rem.Live, ok = ExtentFrom(old.LBA, rng.Last())
			if !ok {
				log.Error("error calculating masked extent", "pivot", pivot, "old", old)
				return nil, fmt.Errorf("error calculating new extent")
			}
			affected = append(affected, rem)

			cur.Live = update

			toDelete = append(toDelete, i.Key())
			toAdd = append(toAdd, cur)
			log.Trace("pivoting range", "pivot", pivot, "from", old, "to", cur.Live)
			e.checkExtent(cur.Live)
		default:
			return nil, fmt.Errorf("invalid coverage value: %s", coverage)
		}
	}

	for _, lba := range toDelete {
		log.Trace("deleting range", "lba", lba)
		e.m.Del(lba)
	}

	for _, pba := range toAdd {
		e.checkExtent(pba.Live)
		log.Trace("adding range", "rng", pba.Live)
		e.m.Set(pba.Live.LBA, pba)
	}

	e.checkExtent(rng)

	log.Trace("adding read range", "range", rng)
	e.m.Set(rng.LBA, &PartialExtent{
		ExtentLocation: pba,

		// This value is updated as the range is shrunk
		// when new, overlapping ranges are added. Full does not
		// change size though.
		Live: rng,
	})

	if false { // mode.Debug() {
		log.Debug("validating map post update")
		return nil, e.Validate(log)
	}

	return affected, nil
}

func (e *ExtentMap) Validate(log hclog.Logger) error {
	var prev *PartialExtent

	for i := e.m.Iterator(); i.Valid(); i.Next() {
		lba := i.Key()
		pba := i.Value()

		if pba.Live.Blocks == 0 || pba.Blocks == 0 {
			return fmt.Errorf("invalid zero length range at %v: %v", lba, pba.Live.LBA)
		}

		if pba.Live.Blocks >= 1_000_000_000 {
			log.Error("extremely large block range detected", "range", pba.Live)
			return fmt.Errorf("extremly large block range detected: %d: %s", lba, pba.Live)
		}

		if lba != pba.Live.LBA {
			return fmt.Errorf("key didn't match pba: %d != %d", lba, pba.Live.LBA)
		}

		if prev != nil {
			if prev.Live.Last() >= lba {
				return fmt.Errorf("overlapping ranges detected: %s <=> %s",
					prev.Live, pba.Live)
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

		if pba.Live.Blocks == 1 {
			parts = append(parts, fmt.Sprintf("%d", pba.Live.LBA))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", pba.Live.LBA, pba.Live.Last()))
		}
	}

	return strings.Join(parts, " ")
}

func (e *ExtentMap) Resolve(log hclog.Logger, rng Extent) ([]PartialExtent, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var ret []PartialExtent

loop:
	for i := e.m.Floor(rng.LBA); i.Valid(); i.Next() {
		// Only consider ranges that start before the requested one
		if i.Key() >= rng.LBA {
			break
		}

		cur := i.Value()

		log.Trace("consider for resolve", "cur", cur.Live, "against", rng)

		switch cur.Live.Cover(rng) {
		case CoverPartly:
			ret = append(ret, *cur)
		case CoverSuperRange, CoverExact:
			ret = append(ret, *cur)
			break loop
		case CoverNone:
			break loop
		}
	}

loop2:
	// Also check for ranges that start higher to be considered
	for i := e.m.LowerBound(rng.LBA); i.Valid(); i.Next() {
		cur := i.Value()
		coverage := cur.Live.Cover(rng)

		orig := cur.Live

		log.Trace("considering",
			"a", rng, "b", orig,
			"a-sub-b", coverage,
		)

		switch coverage {
		case CoverNone:
			break loop2
		case CoverSuperRange, CoverExact:
			ret = append(ret, *cur)
			break loop2
		case CoverPartly:
			ret = append(ret, *cur)
		default:
			return nil, fmt.Errorf("invalid coverage value: %s", coverage)
		}
	}

	for _, r := range ret {
		if r.Live.Cover(rng) == CoverNone {
			log.Error("we fucked up, we included a covernone range", "rng", r, "req", rng)
		}
	}

	return ret, nil
}
