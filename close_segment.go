package lsvd

import (
	"context"
	"strings"
	"time"

	"github.com/lab47/mode"
)

func (d *Disk) CloseSegment(ctx context.Context) error {
	ch, err := d.closeSegmentAsync(ctx)
	if ch == nil || err != nil {
		return err
	}

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *Disk) closeSegmentAsync(ctx context.Context) (chan struct{}, error) {
	segId := SegmentId(d.curSeq)

	oc := d.curOC

	var err error
	d.curOC, err = d.newSegmentCreator()
	if err != nil {
		return nil, err
	}

	d.log.Debug("starting goroutine to close segment")

	d.prevCacheMu.Lock()
	for d.prevCache != nil {
		d.prevCacheCond.Wait()
	}
	d.prevCache = oc
	d.prevCacheMu.Unlock()

	done := make(chan struct{})

	go func() {
		defer d.log.Debug("finished goroutine to close segment")
		defer close(done)
		defer segmentsWritten.Inc()

		var (
			entries []ExtentLocation
			stats   *SegmentStats
			err     error
		)

		start := time.Now()
		for {
			entries, stats, err = oc.Flush(ctx, d.sa, segId)
			if err != nil {
				d.log.Error("error flushing data to segment, retrying", "error", err)
				time.Sleep(5 * time.Second)
				continue
			}

			break
		}

		flushDur := time.Since(start)

		d.log.Debug("segment published, resetting write cache")

		sums := map[Extent]string{}
		resi := map[Extent][]*PartialExtent{}

		if mode.Debug() {
			sums = map[Extent]string{}

			var data RangeData

			for _, ent := range entries {
				data.Reset(ent.Extent)

				_, err := oc.FillExtent(data)
				if err != nil {
					d.log.Error("error reading extent for validation", "error", err)
				}
				sum := rangeSum(data.data)
				sums[ent.Extent] = sum

				ranges, err := d.lba2pba.Resolve(ent.Extent)
				if err != nil {
					d.log.Error("error performing resolution for block read check")
				} else {
					resi[ent.Extent] = ranges
				}
			}
		}

		mapStart := time.Now()

		d.segmentsMu.Lock()

		d.segments[segId] = &Segment{
			Size: stats.Blocks,
			Used: stats.Blocks,
		}

		d.segmentsMu.Unlock()

		d.lbaMu.Lock()
		for _, ent := range entries {
			if mode.Debug() {
				d.log.Trace("updating read map", "extent", ent.Extent)
			}
			affected, err := d.lba2pba.Update(ent)
			if err != nil {
				d.log.Error("error updating read map", "error", err)
			}

			d.updateUsage(segId, affected)
		}
		d.lbaMu.Unlock()

		d.prevCacheMu.Lock()
		d.prevCache = nil
		d.prevCacheCond.Signal()
		d.prevCacheMu.Unlock()

		mapDur := time.Since(mapStart)

		if mode.Debug() {
			d.log.Info("performing extent validation")
			passed := 0
			for _, ent := range entries {
				data, err := d.ReadExtent(ctx, ent.Extent)
				if err != nil {
					d.log.Error("error reading extent for validation", "error", err)
				}
				sum := rangeSum(data.data)

				if sum != sums[ent.Extent] {
					d.log.Error("block read validation failed", "extent", ent.Extent,
						"sum", sum, "expected", sums[ent.Extent])
					ranges, err := d.lba2pba.Resolve(ent.Extent)
					if err != nil {
						d.log.Error("unable to resolve for check", "error", err)
					} else {
						var before []string
						for _, r := range resi[ent.Extent] {
							before = append(before, r.String())
						}

						var after []string
						for _, r := range ranges {
							after = append(after, r.String())
						}

						d.log.Error("block read validation ranges",
							"before", strings.Join(before, " "),
							"after", strings.Join(after, " "))
					}
				} else {
					passed++
				}
			}

			d.log.Warn("finished block read validation", "passed", passed)
		}

		if d.afterNS != nil {
			d.afterNS(segId)
		}

		finDur := time.Since(start)

		d.log.Info("uploaded new segment", "segment", segId, "flush-dur", flushDur, "map-dur", mapDur, "dur", finDur)

		err = d.cleanupDeletedSegments(ctx)
		if err != nil {
			d.log.Error("error cleaning up deleted segments", "error", err)
		}

		d.log.Debug("finished background segment flush")
	}()

	return done, nil
}

func (d *Disk) updateUsage(self SegmentId, affected []PartialExtent) {
	d.segmentsMu.Lock()
	defer d.segmentsMu.Unlock()

	for _, r := range affected {
		if r.Segment != self {
			rng := r.Partial

			if seg, ok := d.segments[r.Segment]; ok {
				if seg.deleted {
					continue
				}

				if o, ok := seg.detectedCleared(rng); ok {
					d.log.Warn("detected clearing overlapping extent", "orig", o, "cur", r)
				}
				seg.cleared = append(seg.cleared, rng)
				seg.Used -= uint64(rng.Blocks)
			} else {
				d.log.Warn("missing segment during usage update", "id", r.Segment.String())
			}
		}
	}
}

func (d *Disk) cleanupDeletedSegments(ctx context.Context) error {
	d.segmentsMu.Lock()
	defer d.segmentsMu.Unlock()

	var toDelete []SegmentId

	for i, s := range d.segments {
		if s.deleted {
			toDelete = append(toDelete, i)
		}
	}

	for _, i := range toDelete {
		delete(d.segments, i)
		d.log.Debug("removing segment from volume", "volume", d.volName, "segment", i)
		err := d.sa.RemoveSegmentFromVolume(ctx, d.volName, i)
		if err != nil {
			return err
		}

		err = d.removeSegmentIfPossible(ctx, i)
		if err != nil {
			return err
		}
	}

	return nil
}
