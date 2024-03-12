package lsvd

import (
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/lab47/lsvd/logger"
	"github.com/lab47/lsvd/pkg/treemap"
	"github.com/lab47/mode"
)

type segLocations struct {
	seg  SegmentId
	disk uint16
}

type compactPE struct {
	physLBA       LBA
	physBlocks    uint32
	liveLBADiff   uint16
	liveBlockDiff uint16

	segIdx   uint32
	byteSize uint32
	offset   uint32
	rawSize  uint32
}

func (c compactPE) Extent() Extent {
	return Extent{
		LBA:    c.physLBA,
		Blocks: c.physBlocks,
	}
}

func (c compactPE) Live() Extent {
	return Extent{
		LBA:    LBA(c.physLBA) + LBA(c.liveLBADiff),
		Blocks: uint32(c.physBlocks) - uint32(c.liveBlockDiff),
	}
}

func (c compactPE) LiveLBA() LBA {
	return LBA(c.physLBA) + LBA(c.liveLBADiff)
}

func (c compactPE) LiveLast() LBA {
	return LBA(c.physLBA) + LBA(c.liveLBADiff) + LBA(c.LiveBlocks()) - 1
}

func (c compactPE) LiveBlocks() uint32 {
	return uint32(c.physBlocks) - uint32(c.liveBlockDiff)
}

func (c *compactPE) SetLive(ext Extent) {
	ld := ext.LBA - c.physLBA
	if ld > math.MaxUint16 {
		panic("compact PE failure, live diff too large")
	}
	c.liveLBADiff = uint16(ld)

	bd := c.physBlocks - ext.Blocks
	if bd > math.MaxUint16 {
		panic("compact PE failure, live block diff too large")
	}

	c.liveBlockDiff = uint16(bd)
}

func (m *ExtentMap) ToPE(c compactPE) PartialExtent {
	sl := m.segmentByIdx[c.segIdx]

	return PartialExtent{
		Live: c.Live(),
		ExtentLocation: ExtentLocation{
			ExtentHeader: ExtentHeader{
				Extent:  c.Extent(),
				Size:    c.byteSize,
				Offset:  c.offset,
				RawSize: c.rawSize,
			},
			Segment: sl.seg,
			Disk:    sl.disk,
		},
	}
}

type ExtentMap struct {
	mu sync.Mutex
	m  *treemap.TreeMap[LBA, compactPE]

	coverBlocks int

	segmentByDesc map[segLocations]uint32
	segmentByIdx  map[uint32]segLocations

	affected   []PartialExtent
	addScratch []compactPE
	delScratch []LBA
}

func NewExtentMap() *ExtentMap {
	return &ExtentMap{
		m:             treemap.New[LBA, compactPE](),
		segmentByDesc: make(map[segLocations]uint32),
		segmentByIdx:  make(map[uint32]segLocations),
	}
}

func (e *ExtentMap) Len() int {
	return e.m.Len()
}

type Iterator struct {
	e *ExtentMap
	treemap.ForwardIterator[LBA, compactPE]
}

func (e *ExtentMap) Iterator() *Iterator {
	return &Iterator{
		e:               e,
		ForwardIterator: e.m.Iterator(),
	}
}

func (e *Iterator) Value() PartialExtent {
	return e.e.ToPE(e.ForwardIterator.Value())
}

func (e *Iterator) CompactValue() compactPE {
	return e.ForwardIterator.Value()
}

func (e *Iterator) CompactValuePtr() *compactPE {
	return e.ForwardIterator.ValuePtr()
}

func (e *ExtentMap) Populate(log logger.Logger, o *ExtentMap, diskId uint16) error {
	for i := e.Iterator(); i.Valid(); i.Next() {
		loc := i.Value().ExtentLocation
		loc.Disk = diskId

		_, err := o.Update(log, loc)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
func (e *ExtentMap) find(lba LBA) treemap.ForwardIterator[LBA, PartialExtent] {
	i := e.m.Floor(lba)
	if i.Valid() {
		return i
	}

	return e.m.LowerBound(lba)
}
*/

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

func (e *ExtentMap) LockToPatch(fn func() error) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return fn()
}

func (e *ExtentMap) UpdateBatch(log logger.Logger, entries []ExtentLocation, segId SegmentId, s *Segments) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	var (
		affected = e.affected
		err      error
	)

	for _, ent := range entries {
		if mode.Debug() {
			log.Trace("updating read map", "extent", ent.Extent)
		}
		affected = affected[:0]
		affected, err = e.update(log, ent, affected)
		if err != nil {
			log.Error("error updating read map", "error", err)
		}

		s.UpdateUsage(log, segId, affected)
	}

	e.affected = affected

	return nil
}

func (e *ExtentMap) Update(log logger.Logger, pba ExtentLocation) ([]PartialExtent, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.update(log, pba, nil)
}

func (e *ExtentMap) update(log logger.Logger, pba ExtentLocation, affected []PartialExtent) ([]PartialExtent, error) {
	var (
		toDelete = e.delScratch[:0]
		toAdd    = e.addScratch[:0]

		rng = pba.Extent
	)

	defer func() {
		extentUpdates.Inc()
		e.addScratch = toAdd[:0]
		e.delScratch = toDelete[:0]
	}()

	e.checkExtent(rng)

	isTrace := log.IsTrace()

	if isTrace {
		log.Trace("triggered update", "extent", rng)
	}

loop:
	for i := e.m.Floor(rng.LBA); i.Valid(); i.Next() {
		// If we advance past our start position, stop immediately.
		// We'll pick up these entries in the lowerbound loop below.
		if i.Key() >= rng.LBA {
			break
		}

		if isTrace {
			log.Trace("found bound", "key", i.Key(), "match", i.Value().Live(), "from", rng.LBA)
		}

		cur := i.ValuePtr()

		coverage := cur.Live().Cover(rng)

		if isTrace {
			log.Trace("considering",
				"a", cur.Live(), "b", rng,
				"a-sub-b", coverage,
			)
		}

		switch coverage {
		case CoverNone:
			// ok, disjoint, easy and done.
			break loop

		case CoverExact:
			// The ranges are exactly the same, so we don't
			// need to adjust anything.

			affected = append(affected, e.ToPE(*cur))
		case CoverSuperRange:
			// The new range is a complete subrange (ie a hole)
			// into the existing range, so we need to adjust
			// and add a new range before and after the hole.

			suffix, ok := ExtentFrom(rng.Last()+1, cur.Live().Last())
			if ok {
				dup := *cur
				dup.SetLive(suffix)
				toAdd = append(toAdd, dup)
			}

			if rng.LBA > 0 {
				prefix, ok := ExtentFrom(cur.LiveLBA(), rng.LBA-1)
				if ok {
					cur.SetLive(prefix)
				}
			}

			rem := *cur
			rem.SetLive(rng)
			affected = append(affected, e.ToPE(rem))

			e.checkExtent(cur.Live())
		case CoverPartly:
			var masked Extent

			if rng.Cover(cur.Live()) == CoverSuperRange {
				// The new range completely covers the current one
				// so we can clobber it.
				masked = rng
			} else {
				// We need to shrink the range of cur down to not overlap
				// with the new range.
				update, ok := ExtentFrom(cur.LiveLBA(), rng.LBA-1)
				if !ok {
					log.Error("error calculate updated range", "orig", cur.Live, "target", rng.LBA-1)
					return nil, fmt.Errorf("error calculating new range")
				}

				masked, ok = ExtentFrom(rng.LBA, cur.Live().Last())
				if !ok {
					log.Error("error calculate masked range", "orig", cur.Live, "target", rng.LBA-1)
					return nil, fmt.Errorf("error calculating new range")
				}

				cur.SetLive(update)
			}

			rem := *cur
			rem.SetLive(masked)
			affected = append(affected, e.ToPE(rem))

			e.checkExtent(cur.Live())
		default:
			return nil, fmt.Errorf("invalid coverage value: %s", coverage)
		}
	}

loop2:
	// Also check for ranges that start higher to be considered
	for i := e.m.LowerBound(rng.LBA); i.Valid(); i.Next() {
		cur := i.ValuePtr()
		coverage := rng.Cover(cur.Live())

		if isTrace {
			log.Trace("considering",
				"a", rng, "b", cur.Live(),
				"a-sub-b", coverage,
			)
		}

		switch coverage {
		case CoverNone:
			break loop2

		case CoverSuperRange, CoverExact:
			// our new range completely covers the exist one, so we delete it.
			affected = append(affected, e.ToPE(*cur))
			toDelete = append(toDelete, i.Key())
		case CoverPartly:
			old := cur.Live()
			pivot := rng.Last() + 1
			update, ok := ExtentFrom(pivot, old.Last())
			if !ok {
				log.Error("error calculating new extent", "pivot", pivot, "old", old)
				return nil, fmt.Errorf("error calculating new extent")
			}

			rem := cur

			remLive, ok := ExtentFrom(old.LBA, rng.Last())
			if !ok {
				log.Error("error calculating masked extent", "pivot", pivot, "old", old)
				return nil, fmt.Errorf("error calculating new extent")
			}
			rem.SetLive(remLive)
			affected = append(affected, e.ToPE(*rem))

			cur.SetLive(update)

			toDelete = append(toDelete, i.Key())
			toAdd = append(toAdd, *cur)

			if isTrace {
				log.Trace("pivoting range", "pivot", pivot, "from", old, "to", cur.Live)
			}
			e.checkExtent(cur.Live())
		default:
			return nil, fmt.Errorf("invalid coverage value: %s", coverage)
		}
	}

	for _, lba := range toDelete {
		if isTrace {
			log.Trace("deleting range", "lba", lba)
		}
		e.m.Del(lba)
	}

	for _, pba := range toAdd {
		e.checkExtent(pba.Live())
		if isTrace {
			log.Trace("adding range", "rng", pba.Live)
		}
		e.m.Set(pba.LiveLBA(), pba)
	}

	e.checkExtent(rng)

	if isTrace {
		log.Trace("adding read range", "range", rng)
	}

	e.set(PartialExtent{
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

func (e *ExtentMap) segmentIdx(loc ExtentLocation) uint32 {
	key := segLocations{
		seg:  loc.Segment,
		disk: loc.Disk,
	}
	idx, ok := e.segmentByDesc[key]
	if !ok {
		idx = uint32(len(e.segmentByDesc))
		e.segmentByDesc[key] = idx
		e.segmentByIdx[idx] = key
	}

	return idx
}

func (e *ExtentMap) segment(ce compactPE) SegmentId {
	return e.segmentByIdx[ce.segIdx].seg
}

func (ce *compactPE) SetFromHeader(eh ExtentHeader, seg uint32) {
	curLive := ce.Live()
	// Read live and reset it later sicne the diffs will change

	*ce = compactPE{
		physLBA:    eh.LBA,
		physBlocks: eh.Blocks,
		segIdx:     seg,
		byteSize:   eh.Size,
		offset:     eh.Offset,
		rawSize:    eh.RawSize,
	}

	ce.SetLive(curLive)
}

func (e *ExtentMap) set(pe PartialExtent) {
	ce := compactPE{
		physLBA:    pe.LBA,
		physBlocks: pe.Blocks,
		segIdx:     e.segmentIdx(pe.ExtentLocation),
		byteSize:   pe.Size,
		offset:     pe.Offset,
		rawSize:    pe.RawSize,
	}

	ce.SetLive(pe.Live)

	e.m.Set(ce.LiveLBA(), ce)
}

func (e *ExtentMap) Validate(log logger.Logger) error {
	var prev compactPE

	for i := e.m.Iterator(); i.Valid(); i.Next() {
		lba := i.Key()
		pba := i.Value()

		if pba.LiveBlocks() == 0 || pba.physBlocks == 0 {
			return fmt.Errorf("invalid zero length range at %v: %v", lba, pba.LiveLBA())
		}

		if pba.LiveBlocks() >= 1_000_000_000 {
			log.Error("extremely large block range detected", "range", pba.Live)
			return fmt.Errorf("extremly large block range detected: %d: %s", lba, pba.Live())
		}

		if lba != pba.LiveLBA() {
			return fmt.Errorf("key didn't match pba: %d != %d", lba, pba.LiveLBA())
		}

		if prev.physBlocks != 0 {
			if prev.Live().Last() >= lba {
				return fmt.Errorf("overlapping ranges detected: %s <=> %s",
					prev.Live(), pba.Live())
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

		if pba.LiveBlocks() == 1 {
			parts = append(parts, fmt.Sprintf("%d", pba.LiveLBA()))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", pba.LiveLBA(), pba.LiveLast()))
		}
	}

	return strings.Join(parts, " ")
}

func (e *ExtentMap) RenderExpanded() string {
	var parts []string
	for i := e.m.Iterator(); i.Valid(); i.Next() {
		pba := i.ValuePtr()

		if pba.LiveBlocks() == 1 {
			parts = append(parts, fmt.Sprintf("%p %d => %s [%s]", pba, pba.LiveLBA(), e.segment(*pba).String(), pba.Extent()))
		} else {
			parts = append(parts, fmt.Sprintf("%p %d-%d => %s [%s]", pba, pba.LiveLBA(), pba.LiveLast(), e.segment(*pba).String(), pba.Extent()))
		}
	}

	return strings.Join(parts, "\n")
}

func (e *ExtentMap) Resolve(log logger.Logger, rng Extent) ([]PartialExtent, error) {
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

		if log.IsTrace() {
			log.Trace("consider for resolve", "cur", cur.Live, "against", rng)
		}

		switch cur.Live().Cover(rng) {
		case CoverPartly:
			ret = append(ret, e.ToPE(cur))
		case CoverSuperRange, CoverExact:
			ret = append(ret, e.ToPE(cur))
			break loop
		case CoverNone:
			break loop
		}
	}

loop2:
	// Also check for ranges that start higher to be considered
	for i := e.m.LowerBound(rng.LBA); i.Valid(); i.Next() {
		cur := i.Value()
		coverage := cur.Live().Cover(rng)

		orig := cur.Live

		// Guard for performance. Logging any variable data causes allocations as they're
		// promoted to interfaces.
		if log.IsTrace() {
			log.Trace("considering",
				"a", rng, "b", orig,
				"a-sub-b", coverage,
			)
		}

		switch coverage {
		case CoverNone:
			break loop2
		case CoverSuperRange, CoverExact:
			ret = append(ret, e.ToPE(cur))
			break loop2
		case CoverPartly:
			ret = append(ret, e.ToPE(cur))
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
