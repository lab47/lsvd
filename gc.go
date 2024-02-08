package lsvd

import (
	"bufio"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/lab47/lz4decode"
	"github.com/pkg/errors"
)

func (d *Disk) GCOnce(ctx context.Context) (SegmentId, error) {
	segId, ci, err := d.StartGC(ctx, 1.0)
	if err != nil {
		return SegmentId{}, err
	}

	if ci == nil {
		return segId, nil
	}

	d.log.Trace("copying live data from segment", "seg", segId)

	defer ci.Close()

	var done bool
	for !done {
		done, err = ci.Process(ctx, 5*time.Minute)
		if err != nil {
			return SegmentId{}, err
		}
	}

	return segId, nil
}

func (d *Disk) StartGC(ctx context.Context, min float64) (SegmentId, *CopyIterator, error) {
	toGC, ok, err := d.s.PickSegmentToGC(min)
	if !ok {
		return SegmentId{}, nil, nil
	}

	if err != nil {
		return SegmentId{}, nil, err
	}

	d.log.Trace("copying live data from segment", "seg", toGC)

	ci, err := d.CopyIterator(ctx, toGC)
	if err != nil {
		return SegmentId{}, nil, err
	}

	return toGC, ci, nil
}

type CopyIterator struct {
	seg SegmentId
	d   *Disk
	or  SegmentReader
	br  *bufio.Reader

	hdr SegmentHeader

	left uint32

	totalBlocks   uint64
	copiedExtents int
	copiedBlocks  uint64
}

func (c *CopyIterator) Process(ctx context.Context, dur time.Duration) (bool, error) {
	view := make([]byte, BlockSize*10)

	br := c.br

	s := time.Now()

loop:
	for c.left > 0 {
		c.left--

		var eh ExtentHeader

		err := eh.Read(br)
		if err != nil {
			return false, err
		}

		if len(view) < int(eh.Size) {
			view = make([]byte, eh.Size)
		}

		extent := eh.Extent

		c.d.log.Trace("considering for copy", "extent", extent, "size", eh.Size, "offset", eh.Offset)

		pes, err := c.d.lba2pba.Resolve(c.d.log, extent)
		if err != nil {
			return false, err
		}

		for _, pe := range pes {
			if pe.Segment != c.seg {
				c.d.log.Trace("discarding segment", "extent", extent, "target", pe.Extent, "segment", pe.Segment, "seg", c.seg)
				continue loop
			}
		}

		c.copiedBlocks += uint64(eh.Blocks)
		c.copiedExtents++

		// This it's an extent of empty blocks, zero.
		if eh.Size == 0 {
			c.d.ZeroBlocks(ctx, extent)
		} else {
			view := view[:eh.Size]

			offset := int64(c.hdr.DataOffset + uint32(eh.Offset))

			_, err = c.or.ReadAt(view, offset)
			if err != nil {
				return false, err
			}

			if eh.Flags == 1 {
				sz := eh.RawSize // binary.BigEndian.Uint32(view)

				if eh.RawSize == 0 {
					panic("missing rawsize 3")
				}

				uncomp := buffers.Get(int(sz))
				defer buffers.Return(uncomp)

				n, err := lz4decode.UncompressBlock(view, uncomp, nil)
				if err != nil {
					return false, err
				}

				if n != int(sz) {
					return false, fmt.Errorf("failed to uncompress correctly")
				}

				view = uncomp
			}

			c.d.log.Trace("copying extent", "extent", extent, "view", len(view))

			err = c.d.WriteExtent(ctx, BlockDataView(view).MapTo(eh.LBA))
			if err != nil {
				return false, err
			}
		}

		if time.Since(s) >= dur {
			return false, nil
		}
	}

	return true, nil
}

func (c *CopyIterator) Close() error {
	c.d.s.SetDeleted(c.seg)

	c.d.log.Info("gc cycle complete",
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

	ci := &CopyIterator{
		d:   d,
		or:  f,
		br:  br,
		seg: seg,
	}

	err = ci.hdr.Read(br)
	if err != nil {
		return nil, err
	}

	ci.totalBlocks = uint64(ci.hdr.ExtentCount)
	ci.left = ci.hdr.ExtentCount

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
