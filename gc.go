package lsvd

import (
	"bufio"
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/lab47/lsvd/logger"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
)

type GCRequest int

const (
	NormalGC GCRequest = iota
)

func (d *Disk) startGCRoutine(gctx context.Context, trigger chan GCRequest) {
	ctx := NewContext(gctx)

	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-trigger:
			if !ok {
				return
			}

			ctx.Reset()

			if req == NormalGC {
				toGC, ok, err := d.s.LeastDenseSegment(d.log)
				if !ok {
					d.log.Warn("GC was requested, but no least dense segment available")
					continue
				}

				if err != nil {
					d.log.Error("error picking segment to GC", "error", err)
					continue
				}

				ci, err := d.CopyIterator(ctx, toGC)
				if err != nil {
					d.log.Error("error creating copy iterator segment to GC",
						"error", err,
						"segment", toGC,
					)
					continue
				}

				d.log.Info("beginning GC of segment", "segment", toGC)

				err = ci.ProcessFromExtents(ctx, d.log)
				if err != nil {
					d.log.Error("error processing segment for gc", "error", err, "segment", toGC)
				}

				err = ci.Close(ctx)
				if err != nil {
					d.log.Error("error closing segment after gc", "error", err, "segment", toGC)
				}

				continue
			}

			d.log.Error("unknown GC request", "request", req)
		}
	}
}

func (d *Disk) TriggerGC(ctx context.Context, req GCRequest) {
	// Means that auto GC is diisabled
	if d.gcTrigger == nil {
		return
	}

	select {
	case <-ctx.Done():
		return
	case d.gcTrigger <- req:
		// ok
	}
}

const defaultGCRatio = 0.3

func (d *Disk) GCOnce(ctx *Context) (SegmentId, error) {
	segId, ci, err := d.StartGC(ctx, 1.0)
	if err != nil {
		return SegmentId{}, err
	}

	if ci == nil {
		return segId, nil
	}

	defer ci.Close(ctx)

	err = ci.ProcessFromExtents(ctx, d.log)
	if err != nil {
		return segId, err
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

func (d *Disk) GCInBackground(gctx context.Context, min float64) (SegmentId, bool, error) {
	toGC, ci, err := d.StartGC(gctx, min)
	if err != nil {
		return SegmentId{}, false, err
	}

	if ci == nil {
		return SegmentId{}, false, nil
	}

	go func() {
		ctx := NewContext(gctx)

		err := ci.ProcessFromExtents(ctx, d.log)
		if err != nil {
			ci.err = err
			return
		}

		ci.err = ci.Close(ctx)
	}()

	return toGC, true, nil
}

type gcExtent struct {
	CE   *compactPE
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

	expectedBlocks uint64
	totalBlocks    uint64
	copiedExtents  int
	copiedBlocks   uint64

	newSegment SegmentId
	builder    SegmentBuilder

	segmentsProcessed []SegmentId
	extents           []gcExtent
	results           []ExtentHeader
}

func (c *CopyIterator) gatherExtents() {
	c.expectedBlocks = c.d.s.SegmentTotalBlocks(c.seg)

	c.segmentsProcessed = append(c.segmentsProcessed, c.seg)
	c.extents = c.extents[:0]

	idx := c.d.lba2pba.segmentIdx(ExtentLocation{
		Segment: c.seg,
		Disk:    0,
	})

	for i := c.d.lba2pba.LockedIterator(); i.Valid(); i.Next() {
		pe := i.CompactValuePtr()
		if pe.segIdx == idx {
			c.extents = append(c.extents, gcExtent{
				CE:   pe,
				Live: pe.Live(),
			})
			c.d.log.Trace("captured extent to update", "addr", spew.Sdump(pe))
		}
	}
}

func (d *CopyIterator) fetchExtent(
	ctx *Context,
	_ logger.Logger,
	addr ExtentLocation,
) (RangeData, error) {
	startFetch := time.Now()

	rawData := ctx.Allocate(int(addr.Size))

	_, err := d.or.ReadAt(rawData, int64(addr.Offset))
	if err != nil {
		return RangeData{}, err
	}

	var rangeData []byte

	switch addr.Flags() {
	case Uncompressed:
		rangeData = rawData
	case Compressed:
		startDecomp := time.Now()
		sz := addr.RawSize

		uncomp := ctx.Allocate(int(sz))

		n, err := lz4.UncompressBlock(rawData, uncomp)
		if err != nil {
			return RangeData{}, errors.Wrapf(err, "error uncompressing data (rawsize: %d, compdata: %d)", len(rawData), len(uncomp))
		}

		if n != int(sz) {
			return RangeData{}, fmt.Errorf("failed to uncompress correctly, %d != %d", n, sz)
		}

		rangeData = uncomp
		compressionOverhead.Add(time.Since(startDecomp).Seconds())
	default:
		return RangeData{}, fmt.Errorf("unknown flags value: %d", addr.Flags())
	}

	src := MapRangeData(addr.Extent, rangeData)

	readProcessing.Add(time.Since(startFetch).Seconds())
	return src, nil
}

func (c *CopyIterator) ProcessFromExtents(ctx *Context, log logger.Logger) error {
	log.Debug("copying extents for segment", "extents", len(c.extents), "segment", c.seg, "new-segment", c.newSegment)

	marker := ctx.Marker()

	for _, ce := range c.extents {
		ctx.ResetTo(marker)

		rng := c.d.lba2pba.ToPE(*ce.CE)

		if rng.Size == 0 {
			c.builder.ZeroBlocks(rng.Live)
			c.results = append(c.results, rng.ExtentHeader)

			c.copiedBlocks += uint64(rng.Blocks)
			c.copiedExtents++
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

		c.results = append(c.results, eh)
	}

	return nil
}

func (d *Disk) CheckpointGC(ctx context.Context) error {
	return d.cleanupDeletedSegments(ctx)
}

func (ci *CopyIterator) extentsByteSize() int {
	var bs int

	for _, e := range ci.extents {
		bs += int(e.CE.byteSize)
	}

	return bs
}

func (c *CopyIterator) updateDisk(ctx context.Context) error {
	c.d.log.Trace("uploading post-gc segment", "segment", c.newSegment)
	_, stats, err := c.builder.Flush(ctx, c.d.log, c.d.sa, c.newSegment, c.d.volName)
	if err != nil {
		return err
	}

	c.d.log.Trace("patching block map from post-gc segment", "segment", c.newSegment)
	c.d.s.Create(c.newSegment, stats)

	idx := c.d.lba2pba.segmentIdx(ExtentLocation{
		Segment: c.seg,
		Disk:    0,
	})

	newIdx := c.d.lba2pba.segmentIdx(ExtentLocation{
		Segment: c.newSegment,
		Disk:    0,
	})

	return c.d.lba2pba.LockToPatch(func() error {
		for i, pe := range c.extents {
			// it's possible that the extent has been deleted and reused by the time
			// we're returning here. So if the segment is different, then we know it's been
			// reused. It's not possible to reuse an extent in the same segment because of how
			// we manage extents in segments.
			if pe.CE.segIdx != idx {
				continue
			}

			// Double check that we're patching for the same live extent. Otherwise bail!
			if pe.CE.Live() != pe.Live {
				continue
			}

			eh := c.results[i]
			if eh.Size != 0 {
				eh.Offset += stats.DataOffset
			}

			pe.CE.SetFromHeader(eh, newIdx)
		}

		return nil
	})
}

func (c *CopyIterator) Close(ctx context.Context) error {
	err := c.updateDisk(ctx)
	if err != nil {
		return err
	}

	c.d.s.SetDeleted(c.seg, c.d.log)

	c.d.log.Info("gc cycle complete",
		"segment", c.seg.String(),
		"extents", c.copiedExtents,
		"blocks", c.copiedBlocks,
		"expected-blocks", c.expectedBlocks,
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

	ci.builder.em = NewExtentMap()

	path := filepath.Join(d.path, "writecache."+seg.String())
	err = ci.builder.OpenWrite(path, d.log)
	if err != nil {
		return nil, err
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
