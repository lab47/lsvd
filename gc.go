package lsvd

import (
	"bufio"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/lab47/lsvd/logger"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
)

const defaultGCRatio = 0.3

func (d *Disk) GCOnce(ctx context.Context) (SegmentId, error) {
	segId, ci, err := d.StartGC(ctx, 1.0)
	if err != nil {
		return SegmentId{}, err
	}

	if ci == nil {
		return segId, nil
	}

	defer ci.Close()

	err = ci.ProcessFromExtents(ctx, d.log)
	if err != nil {
		return segId, err
	}

	err = ci.updateDisk(ctx)
	if err != nil {
		return segId, nil
	}

	return segId, nil
}

func (d *Disk) StartGC(ctx context.Context, min float64) (SegmentId, *CopyIterator, error) {
	toGC, ok, err := d.s.PickSegmentToGC(d.log, min, nil)
	if !ok {
		return SegmentId{}, nil, nil
	}

	if err != nil {
		return SegmentId{}, nil, err
	}

	d.log.Debug("copying live data from segment", "seg", toGC)

	ci, err := d.CopyIterator(ctx, toGC)
	if err != nil {
		return SegmentId{}, nil, err
	}

	return toGC, ci, nil
}

func (d *Disk) GCInBackground(ctx context.Context, min float64) (SegmentId, bool, error) {
	toGC, ok, err := d.s.PickSegmentToGC(d.log, min, nil)
	if !ok {
		return SegmentId{}, false, nil
	}

	if err != nil {
		return SegmentId{}, false, err
	}

	d.log.Info("starting background GC process", "segment", toGC)

	ci, err := d.CopyIterator(ctx, toGC)
	if err != nil {
		return SegmentId{}, false, err
	}

	d.bgCopy = ci
	d.bgDone = make(chan struct{})

	go func() {
		defer d.doneWithCopy(ci)

		err := ci.ProcessFromExtents(ctx, d.log)
		if err != nil {
			ci.err = err
			return
		}
	}()

	return toGC, true, nil
}

type gcExtent struct {
	*PartialExtent
	Live Extent
}

type CopyIterator struct {
	seg SegmentId
	d   *Disk
	or  SegmentReader
	br  *bufio.Reader

	err error

	hdr SegmentHeader

	left uint32

	totalBlocks   uint64
	copiedExtents int
	copiedBlocks  uint64

	newSegment SegmentId
	builder    SegmentBuilder

	segmentsProcessed []SegmentId
	extents           []gcExtent
}

func (c *CopyIterator) gatherExtents() {
	c.segmentsProcessed = append(c.segmentsProcessed, c.seg)
	c.extents = c.extents[:0]

	for i := c.d.lba2pba.Iterator(); i.Valid(); i.Next() {
		pe := i.Value()
		if pe.Segment == c.seg {
			c.extents = append(c.extents, gcExtent{
				PartialExtent: pe,
				Live:          pe.Live,
			})
		}
	}
}

func (d *CopyIterator) fetchExtent(
	_ context.Context,
	_ logger.Logger,
	addr ExtentLocation,
) (RangeData, error) {
	startFetch := time.Now()

	rawData := buffers.Get(int(addr.Size))

	_, err := d.or.ReadAt(rawData, int64(addr.Offset))
	if err != nil {
		return RangeData{}, err
	}

	var rangeData []byte

	switch addr.Flags {
	case Uncompressed:
		rangeData = rawData
	case Compressed:
		startDecomp := time.Now()
		sz := addr.RawSize

		uncomp := buffers.Get(int(sz))

		n, err := lz4.UncompressBlock(rawData, uncomp)
		if err != nil {
			return RangeData{}, errors.Wrapf(err, "error uncompressing data (rawsize: %d, compdata: %d)", len(rawData), len(uncomp))
		}

		if n != int(sz) {
			return RangeData{}, fmt.Errorf("failed to uncompress correctly, %d != %d", n, sz)
		}

		// We're finished with the raw extent data.
		buffers.Return(rawData)

		rangeData = uncomp
		compressionOverhead.Add(time.Since(startDecomp).Seconds())
	case ZstdCompressed:
		startDecomp := time.Now()
		sz := addr.RawSize

		uncomp := buffers.Get(int(sz))

		d, err := zstd.NewReader(nil)
		if err != nil {
			return RangeData{}, err
		}

		res, err := d.DecodeAll(rawData, uncomp[:0])
		if err != nil {
			return RangeData{}, errors.Wrapf(err, "error uncompressing data (rawsize: %d, compdata: %d)", len(rawData), len(uncomp))
		}

		n := len(res)

		if n != int(sz) {
			return RangeData{}, fmt.Errorf("failed to uncompress correctly, %d != %d", n, sz)
		}

		// We're finished with the raw extent data.
		buffers.Return(rawData)

		rangeData = uncomp
		compressionOverhead.Add(time.Since(startDecomp).Seconds())
	default:
		return RangeData{}, fmt.Errorf("unknown flags value: %d", addr.Flags)
	}

	src := MapRangeData(addr.Extent, rangeData)

	readProcessing.Add(time.Since(startFetch).Seconds())
	return src, nil
}

func (c *CopyIterator) ProcessFromExtents(ctx context.Context, log logger.Logger) error {
	log.Debug("copying extents for segment", "extents", len(c.extents), "segment", c.seg, "new-segment", c.newSegment)

	for _, rng := range c.extents {
		if rng.Size == 0 {
			c.builder.ZeroBlocks(rng.Live)
			continue
		}

		data, err := c.fetchExtent(ctx, c.d.log, rng.ExtentLocation)
		if err != nil {
			return err
		}

		// Collapse data down to the actual live range

		view, ok := data.SubRange(rng.Live)
		if !ok {
			return fmt.Errorf("error calculating sub-range from %s to %s", rng.Extent, rng.Live)
		}

		_, eh, err := c.builder.WriteExtent(c.d.log, view)
		if err != nil {
			return err
		}

		c.copiedBlocks += uint64(eh.Blocks)
		c.copiedExtents++

		buffers.Return(data.data)
	}

	return nil
}

func (d *Disk) doneWithCopy(_ *CopyIterator) {
	close(d.bgDone)
}

func (d *Disk) flushGC(ctx context.Context, wait bool) (bool, error) {
	ci := d.bgCopy
	if ci == nil {
		return false, nil
	}

	if wait {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-d.bgDone:
			d.bgCopy = nil

			d.log.Debug("flushing background gc data")

			ci.updateDisk(ctx)
			return false, ci.Close()
		}
	}

	// Since the caller isn't waitiing, then if the background is done,
	// we'll attempt keep it the process running by spawning a new goroutine
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-d.bgDone:
		// ok
	default:
		return true, nil
	}

	more, err := ci.attemptAnotherSegment(ctx, d, defaultGCRatio)
	if err != nil {
		return false, err
	}

	if more {
		d.log.Debug("found another segment to gc")
		return true, nil
	}

	// TODO: future feature: when there are no more segments, don't
	// immediately close and upload the segment. Instead we can keep
	// this iterator open and wait for the next GC cycle to create
	// a larger object. In the end, it doesn't matter that much if we
	// upload a new, smaller object since the pre-caching we were doing
	// on the older one didn't help anyway since it a lot of dead extents.

	d.bgCopy = nil

	d.log.Debug("flushing background gc data")

	ci.updateDisk(ctx)
	err = ci.Close()

	return false, err
}

func (d *Disk) CheckpointGC(ctx context.Context) (bool, error) {
	more, err := d.flushGC(ctx, false)
	if err != nil {
		return false, err
	}

	d.cleanupDeletedSegments(ctx)

	return more, nil
}

func (ci *CopyIterator) extentsByteSize() int {
	var bs int

	for _, e := range ci.extents {
		bs += e.ByteSize()
	}

	return bs
}

func (ci *CopyIterator) attemptAnotherSegment(ctx context.Context, d *Disk, min float64) (bool, error) {
	toGC, ok, err := d.s.PickSegmentToGC(d.log, min, ci.segmentsProcessed)
	if err != nil {
		return false, err
	}

	if !ok {
		d.log.Trace("attempting to GC another segment, couldn't find one")
		return false, nil
	}

	// See if we can/should fit this new segment into the open iterator
	if ci.extentsByteSize()+ci.builder.BodySize() > FlushThreshHold {
		d.log.Trace("next segment too large for existing copy iterator, not continuing")
		return false, nil
	}

	f, err := d.sa.OpenSegment(ctx, toGC)
	if err != nil {
		return false, errors.Wrapf(err, "opening segment %s", toGC)
	}

	ci.or = f
	ci.seg = toGC

	d.log.Debug("continuing gc in existing segment", "from", toGC, "to", ci.newSegment)

	ci.gatherExtents()

	ci.d.bgDone = make(chan struct{})

	go func() {
		defer d.doneWithCopy(ci)

		err := ci.ProcessFromExtents(ctx, d.log)
		if err != nil {
			ci.err = err
			return
		}
	}()

	return true, nil
}

func (c *CopyIterator) updateDisk(ctx context.Context) error {
	c.d.log.Trace("uploading post-gc segment", "segment", c.newSegment)
	locs, stats, err := c.builder.Flush(ctx, c.d.log, c.d.sa, c.newSegment, c.d.volName)
	if err != nil {
		return err
	}

	c.d.log.Trace("updating disk with data from post-gc segment", "segment", c.newSegment)
	c.d.s.Create(c.newSegment, stats)

	return c.d.lba2pba.UpdateBatch(c.d.log, locs, c.newSegment, c.d.s)
}

func (c *CopyIterator) Close() error {
	c.d.s.SetDeleted(c.seg)

	c.d.log.Info("gc cycle complete",
		"segment", c.seg.String(),
		"extents", c.copiedExtents,
		"blocks", c.copiedBlocks,
		"percent", float64(c.copiedBlocks)/float64(c.hdr.ExtentCount),
	)
	return c.or.Close()
}

func (d *Disk) CopyIterator(ctx context.Context, seg SegmentId) (*CopyIterator, error) {
	f, err := d.sa.OpenSegment(ctx, seg)
	if err != nil {
		return nil, errors.Wrapf(err, "opening segment %s", seg)
	}

	br := bufio.NewReader(ToReader(f))

	newSeg, err := d.nextSeq()
	if err != nil {
		return nil, err
	}

	ci := &CopyIterator{
		d:   d,
		or:  f,
		br:  br,
		seg: seg,

		newSegment: newSeg,
	}

	err = ci.hdr.Read(br)
	if err != nil {
		return nil, err
	}

	ci.totalBlocks = uint64(ci.hdr.ExtentCount)
	ci.left = ci.hdr.ExtentCount

	ci.gatherExtents()

	return ci, nil
}

func (d *Disk) removeSegmentIfPossible(ctx context.Context, seg SegmentId) error {
	volumes, err := d.sa.ListVolumes(ctx)
	if err != nil {
		return err
	}

	for _, vol := range volumes {
		segments, err := d.sa.ListSegments(ctx, vol)
		if err != nil {
			return err
		}

		if slices.Index(segments, seg) != -1 {
			// ok, someone holding on to it, return early
			return nil
		}
	}

	d.log.Info("removing segment", "segment", seg)
	// ok, no volume has it, we can remove it.
	return d.sa.RemoveSegment(ctx, seg)
}
