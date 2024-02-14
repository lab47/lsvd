package lsvd

import (
	"sort"
	"sync"

	"github.com/hashicorp/go-hclog"
)

type Segments struct {
	segmentsMu sync.Mutex
	segments   map[SegmentId]*Segment
}

func NewSegments() *Segments {
	return &Segments{
		segments: make(map[SegmentId]*Segment),
	}
}

func (s *Segments) SegmentIds() []SegmentId {
	var ret []SegmentId

	for k := range s.segments {
		ret = append(ret, k)
	}

	return ret
}

func (s *Segments) LiveSegments() []SegmentId {
	var ret []SegmentId

	for k, s := range s.segments {
		if s.deleted {
			continue
		}
		ret = append(ret, k)
	}

	return ret

}

func (s *Segments) Create(segId SegmentId, stats *SegmentStats) {
	s.segmentsMu.Lock()
	defer s.segmentsMu.Unlock()

	s.segments[segId] = &Segment{
		Size: stats.Blocks,
		Used: stats.Blocks,
	}
}

func (s *Segments) CreateOrUpdate(segId SegmentId, usedBytes, usedBlocks uint64) {
	s.segmentsMu.Lock()
	defer s.segmentsMu.Unlock()

	// TODO where is the Size coming from in the create case??
	seg, ok := s.segments[segId]
	if ok {
		seg.UsedBytes += usedBytes
		seg.Used += usedBlocks
	} else {
		s.segments[segId] = &Segment{
			UsedBytes: usedBytes,
			Used:      usedBlocks,
		}
	}
}

func (s *Segments) UpdateUsage(log hclog.Logger, self SegmentId, affected []PartialExtent) {
	s.segmentsMu.Lock()
	defer s.segmentsMu.Unlock()

	for _, r := range affected {
		if r.Segment != self {
			rng := r.Live

			if seg, ok := s.segments[r.Segment]; ok {
				if seg.deleted {
					continue
				}

				if o, ok := seg.detectedCleared(rng); ok {
					log.Warn("detected clearing overlapping extent", "orig", o, "cur", r)
				}
				seg.cleared = append(seg.cleared, rng)
				seg.Used -= uint64(rng.Blocks)
			} else {
				log.Warn("missing segment during usage update", "id", r.Segment.String())
			}
		}
	}
}

func (s *Segments) LogSegmentInfo(log hclog.Logger) {
	s.segmentsMu.Lock()
	defer s.segmentsMu.Unlock()

	type ent struct {
		seg     SegmentId
		density float64
		stats   *Segment
	}

	var entries []ent

	for segId, stats := range s.segments {
		entries = append(entries, ent{segId, stats.Density(), stats})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].density < entries[j].density
	})

	for _, e := range entries {
		log.Info("segment density", "segment", e.seg,
			"density", e.density,
			"total", e.stats.Size,
			"used", e.stats.Used,
		)
	}
}

func (s *Segments) SetDeleted(segId SegmentId) {
	s.segmentsMu.Lock()
	defer s.segmentsMu.Unlock()

	s.segments[segId].deleted = true
}

func (s *Segments) FindDeleted() []SegmentId {
	s.segmentsMu.Lock()
	defer s.segmentsMu.Unlock()

	var toDelete []SegmentId

	for i, s := range s.segments {
		if s.deleted {
			toDelete = append(toDelete, i)
		}
	}

	for _, i := range toDelete {
		delete(s.segments, i)
	}

	return toDelete
}

func (d *Segments) AllDeadSegments() ([]SegmentId, error) {
	d.segmentsMu.Lock()
	defer d.segmentsMu.Unlock()

	var ret []SegmentId

	for segId, stats := range d.segments {
		if stats.deleted {
			continue
		}

		if stats.Used == 0 {
			ret = append(ret, segId)
		}
	}

	return ret, nil
}

func (d *Segments) PickSegmentToGC(min float64) (SegmentId, bool, error) {
	d.segmentsMu.Lock()
	defer d.segmentsMu.Unlock()

	var (
		smallestId    SegmentId
		smallestStats *Segment
	)

	for segId, stats := range d.segments {
		if stats.deleted {
			continue
		}

		d := stats.Density()
		if d > min {
			continue
		}

		if smallestStats == nil || d < smallestStats.Density() {
			smallestStats = stats
			smallestId = segId
		}
	}

	if smallestStats == nil {
		return SegmentId{}, false, nil
	}

	return smallestId, true, nil
}
