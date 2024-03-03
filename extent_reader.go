package lsvd

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/go-hclog"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
)

type ExtentReader struct {
	openSegments *lru.Cache[SegmentId, SegmentReader]
	sa           SegmentAccess
	rangeCache   *RangeCache
}

func NewExtentReader(log hclog.Logger, path string, sa SegmentAccess) (*ExtentReader, error) {
	openSegments, err := lru.NewWithEvict[SegmentId, SegmentReader](
		256, func(key SegmentId, value SegmentReader) {
			openSegments.Dec()
			value.Close()
		})
	if err != nil {
		return nil, err
	}

	er := &ExtentReader{
		openSegments: openSegments,
		sa:           sa,
	}

	rc, err := NewRangeCache(RangeCacheOptions{
		Path:      path,
		ChunkSize: 1024 * 1024,
		MaxSize:   1024 * 1024 * 1024,
		Fetch:     er.fetchData,
	})
	if err != nil {
		return nil, err
	}

	er.rangeCache = rc

	return er, nil
}

func (d *ExtentReader) Close() error {
	d.rangeCache.Close()
	d.openSegments.Purge()

	return nil
}

func (d *ExtentReader) returnData(data RangeData) {
	buffers.Return(data.data)
}

func (d *ExtentReader) fetchData(ctx context.Context, seg SegmentId, data []byte, off int64) error {
	ci, ok := d.openSegments.Get(seg)
	if !ok {
		lf, err := d.sa.OpenSegment(ctx, seg)
		if err != nil {
			return err
		}

		ci = lf

		d.openSegments.Add(seg, ci)
		openSegments.Inc()
	}

	_, err := ci.ReadAt(data, off)
	if err != nil {
		return nil
	}

	// We don't check the size because the last chunk might not be a full chunk.

	return nil
}

func (d *ExtentReader) fetchExtent(
	ctx context.Context,
	log hclog.Logger,
	pe *PartialExtent,
) (RangeData, error) {
	startFetch := time.Now()

	addr := pe.ExtentLocation

	rawData := buffers.Get(int(addr.Size))

	n, err := d.rangeCache.ReadAt(ctx, addr.Segment, rawData, int64(addr.Offset))
	if err != nil {
		return RangeData{}, err
	}

	if n != len(rawData) {
		log.Error("didn't read full data", "read", n, "expected", len(rawData), "size", addr.Size)
		return RangeData{}, fmt.Errorf("short read detected")
	}

	var rangeData []byte

	switch pe.Flags {
	case Uncompressed:
		rangeData = rawData
	case Compressed:
		startDecomp := time.Now()
		sz := pe.RawSize

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
		sz := pe.RawSize

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
		return RangeData{}, fmt.Errorf("unknown flags value: %d", pe.Flags)
	}

	src := MapRangeData(pe.Extent, rangeData)

	readProcessing.Add(time.Since(startFetch).Seconds())
	return src, nil
}
