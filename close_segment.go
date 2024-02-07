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

type extentValidator struct {
	sums    map[Extent]string
	resi    map[Extent][]*PartialExtent
	entries []ExtentLocation
}

func (e *extentValidator) populate(d *Disk, oc *SegmentCreator, entries []ExtentLocation) {
	e.sums = map[Extent]string{}
	e.resi = map[Extent][]*PartialExtent{}
	e.entries = entries

	var data RangeData

	for _, ent := range entries {
		data.Reset(ent.Extent)

		_, err := oc.FillExtent(data)
		if err != nil {
			d.log.Error("error reading extent for validation", "error", err)
		}
		sum := rangeSum(data.data)
		e.sums[ent.Extent] = sum

		ranges, err := d.lba2pba.Resolve(ent.Extent)
		if err != nil {
			d.log.Error("error performing resolution for block read check")
		} else {
			e.resi[ent.Extent] = ranges
		}
	}
}

func (e *extentValidator) validate(ctx context.Context, d *Disk) {
	entries := e.entries

	d.log.Info("performing extent validation")
	passed := 0
	for _, ent := range entries {
		data, err := d.ReadExtent(ctx, ent.Extent)
		if err != nil {
			d.log.Error("error reading extent for validation", "error", err)
		}
		sum := rangeSum(data.data)

		if sum != e.sums[ent.Extent] {
			d.log.Error("block read validation failed", "extent", ent.Extent,
				"sum", sum, "expected", e.sums[ent.Extent])
			ranges, err := d.lba2pba.Resolve(ent.Extent)
			if err != nil {
				d.log.Error("unable to resolve for check", "error", err)
			} else {
				var before []string
				for _, r := range e.resi[ent.Extent] {
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

func (d *Disk) closeSegmentAsync(ctx context.Context) (chan struct{}, error) {
	segId := SegmentId(d.curSeq)

	oc := d.curOC

	var err error
	d.curOC, err = d.newSegmentCreator()
	if err != nil {
		return nil, err
	}

	d.log.Debug("starting goroutine to close segment")

	d.prevCache.SetWhenClear(oc)

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

		// We retry because flush does network calls and we want to just keep trying
		// forever.
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

		var validator *extentValidator

		if mode.Debug() {
			validator = &extentValidator{}
			validator.populate(d, oc, entries)
		}

		mapStart := time.Now()

		d.s.Create(segId, stats)

		d.lba2pba.UpdateBatch(d.log, entries, segId, d.s)

		d.prevCache.Clear()

		mapDur := time.Since(mapStart)

		if validator != nil {
			validator.validate(ctx, d)
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

func (d *Disk) cleanupDeletedSegments(ctx context.Context) error {
	for _, i := range d.s.FindDeleted() {
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
