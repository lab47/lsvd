package lsvd

import (
	"context"
	"time"

	"github.com/lab47/mode"
)

// CloseSegment synchronously closes the current segment, as well as giving
// any background GC process to finish up first.
func (d *Disk) CloseSegment(ctx context.Context) error {
	d.flushGC(ctx, true)

	if d.curOC == nil || d.curOC.EmptyP() {
		err := d.cleanupDeletedSegments(ctx)
		if err != nil {
			d.log.Error("error cleaning up deleted segments", "error", err)
		}
		return nil
	}

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
	segId := d.curSeq

	s := time.Now()
	oc := d.curOC

	var err error
	d.curOC, err = d.newSegmentCreator()
	if err != nil {
		return nil, err
	}

	d.log.Info("flushing segment to storage in background", "segment", segId)

	d.prevCache.SetWhenClear(oc)

	done := make(chan struct{})

	go func() {
		defer d.log.Debug("finished goroutine to close segment")
		defer close(done)
		defer segmentsWritten.Inc()

		defer func() {
			segmentTotalTime.Add(time.Since(s).Seconds())
		}()

		var (
			entries []ExtentLocation
			stats   *SegmentStats
			err     error
		)

		// We flush the GC again here because we want any current GC segment to
		// be added before we're about to upload this one, so that the lba map
		// stays correct. If we don't do this then any live extents that were shrunk
		// while we were GC'ing will be written to the lba map AFTER the update that shrank
		// the extent, corrupting the disk.
		d.flushGC(ctx, true)

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
			validator.populate(d.log, d, oc, entries)
		}

		mapStart := time.Now()

		d.s.Create(segId, stats)

		err = d.lba2pba.UpdateBatch(d.log, entries, segId, d.s)
		if err != nil {
			d.log.Error("error updating lba map", "error", err)
		}

		d.prevCache.Clear()

		mapDur := time.Since(mapStart)

		if validator != nil {
			validator.validate(ctx, d.log, d)
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
		d.log.Info("removing segment from volume", "volume", d.volName, "segment", i)
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
