package lsvd

import (
	"context"
	"fmt"
	"time"

	"github.com/lab47/lsvd/logger"
	"github.com/lab47/mode"
	"github.com/pkg/errors"
)

type EventKind int

const (
	CloseSegment EventKind = iota
	CleanupSegments
	StartGC
)

type Event struct {
	Kind      EventKind
	Value     any
	SegmentId SegmentId
	Done      chan EventResult
}

type EventResult struct {
	Segment SegmentId
	Error   error
}

type Controller struct {
	log      logger.Logger
	d        *Disk
	events   chan Event
	internal []Event
}

func NewController(ctx context.Context, d *Disk) (*Controller, error) {
	c := &Controller{
		log:    d.log,
		d:      d,
		events: make(chan Event, 20),
	}

	return c, nil
}

func (c *Controller) Run(ctx context.Context) {
	c.handleControl(ctx)
}

func (c *Controller) EventsCh() chan Event {
	return c.events
}

func (c *Controller) queueInternal(ev Event) {
	c.internal = append(c.internal, ev)
}

func (c *Controller) handleControl(gctx context.Context) {
	ctx := NewContext(gctx)

	for {

		for _, ev := range c.internal {
			ctx.Reset()
			err := c.handleEvent(ctx, ev)
			if err != nil {
				c.log.Error("error handling event", "error", err, "event-kind", ev.Kind)
			}
		}

		c.internal = c.internal[:0]

		ctx.Reset()
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-c.events:
			if !ok {
				return
			}

			err := c.handleEvent(ctx, ev)
			if err != nil {
				c.log.Error("error handling event", "error", err, "event-kind", ev.Kind)
			}
		}
	}
}

func (c *Controller) handleEvent(ctx *Context, ev Event) error {
	switch ev.Kind {
	case CloseSegment:
		return c.closeSegment(ctx, ev)
	case CleanupSegments:
		return c.d.cleanupDeletedSegments(ctx)
	case StartGC:
		return c.startGC(ctx, ev)
	default:
		return fmt.Errorf("unknown kind: %d", ev.Kind)
	}
}

func (c *Controller) closeSegment(ctx *Context, ev Event) error {
	oc := ev.Value.(*SegmentCreator)
	done := ev.Done
	segId := ev.SegmentId

	s := time.Now()

	d := c.d

	defer c.log.Debug("finished goroutine to close segment")
	defer func() {
		defer close(done)
		done <- EventResult{
			Segment: segId,
		}
	}()
	defer segmentsWritten.Inc()
	defer oc.Close()

	defer func() {
		segmentTotalTime.Add(time.Since(s).Seconds())
	}()

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
			c.log.Error("error flushing data to segment, retrying", "error", err)
			time.Sleep(5 * time.Second)
			continue
		}

		break
	}

	flushDur := time.Since(start)

	c.log.Debug("segment published, resetting write cache")

	var validator *extentValidator

	if mode.Debug() {
		validator = &extentValidator{}
		validator.populate(c.log, ctx, d, oc, entries)
	}

	mapStart := time.Now()

	d.s.Create(segId, stats)

	err = d.lba2pba.UpdateBatch(c.log, entries, segId, d.s)
	if err != nil {
		c.log.Error("error updating lba map", "error", err)
	}

	extents.Set(float64(d.lba2pba.m.Len()))

	d.prevCache.Clear()

	mapDur := time.Since(mapStart)

	if validator != nil {
		validator.validate(ctx, c.log, d)
	}

	if d.afterNS != nil {
		d.afterNS(segId)
	}

	finDur := time.Since(start)

	c.log.Info("uploaded new segment", "segment", segId, "flush-dur", flushDur, "map-dur", mapDur, "dur", finDur)

	c.queueInternal(Event{
		Kind: CleanupSegments,
	})

	density := d.s.Usage()
	dataDensity.Set(density)

	c.log.Info("finished background segment flush", "total-density", density)

	if d.autoGC && d.s.TotalBytes() > GCTotalThreshold && density < GCDensityThreshold {
		c.log.Info("data density dropped below GC threshold, starting GC",
			"density", density,
			"theshold", GCDensityThreshold,
		)

		c.queueInternal(Event{
			Kind: StartGC,
		})
	}

	return nil
}

func (c *Controller) returnError(ev Event, err error) error {
	if ev.Done != nil {
		go func() {
			defer close(ev.Done)
			ev.Done <- EventResult{
				Error: err,
			}
		}()
	}

	return err
}

func (c *Controller) startGC(ctx *Context, ev Event) error {
	d := c.d

	dead, newDensity := c.d.s.PruneDeadSegments()
	if dead > 0 {
		c.queueInternal(Event{
			Kind: CleanupSegments,
		})

		d.log.Info("detected and pruned dead segments", "segments", dead, "new-density", newDensity)
		if newDensity > GCDensityThreshold {
			if ev.Done != nil {
				go func() {
					defer close(ev.Done)
					ev.Done <- EventResult{}
				}()
			}
		}
	}

	if density := d.s.Usage(); density > GCDensityThreshold {
		d.log.Debug("skipping GC has usage has raised since request", "density", density)
		return nil
	}

	toGC, ok, err := d.s.LeastDenseSegment(d.log)
	if !ok {
		d.log.Warn("GC was requested, but no least dense segment available")
		return nil
	}

	if err != nil {
		return errors.Wrapf(err, "error picking segment to GC")
	}

	ci, err := d.CopyIterator(ctx, toGC)
	if err != nil {
		d.log.Error("error creating copy iterator segment to GC",
			"error", err,
			"segment", toGC,
		)
		return c.returnError(ev, err)
	}

	if ci == nil {
		d.log.Info("copied found a dead segment and deleted it directly, gc skipped")
	} else {
		d.log.Info("beginning GC of segment", "segment", toGC)

		err = ci.ProcessFromExtents(ctx, d.log)
		if err != nil {
			d.log.Error("error processing segment for gc", "error", err, "segment", toGC)
			return c.returnError(ev, err)
		}

		err = ci.Close(ctx)
		if err != nil {
			d.log.Error("error closing segment after gc", "error", err, "segment", toGC)
			return c.returnError(ev, err)
		}
	}

	density := d.s.Usage()

	d.log.Info("GC cycle complete", "updated-density", density)

	dataDensity.Set(density)

	if ev.Done != nil {
		go func() {
			defer close(ev.Done)
			ev.Done <- EventResult{
				Segment: toGC,
			}
		}()
	}

	c.queueInternal(Event{
		Kind: CleanupSegments,
	})

	return nil
}
