package lsvd

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-hclog"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
)

type ExtentReader struct {
	extentCache  *ExtentCache
	openSegments *lru.Cache[SegmentId, SegmentReader]
	sa           SegmentAccess
}

func NewExtentReader(log hclog.Logger, path string, sa SegmentAccess) (*ExtentReader, error) {
	ec, err := NewExtentCache(log, path)
	if err != nil {
		return nil, err
	}

	openSegments, err := lru.NewWithEvict[SegmentId, SegmentReader](
		256, func(key SegmentId, value SegmentReader) {
			openSegments.Dec()
			value.Close()
		})
	if err != nil {
		return nil, err
	}

	er := &ExtentReader{
		extentCache:  ec,
		openSegments: openSegments,
		sa:           sa,
	}

	return er, nil
}

func (d *ExtentReader) Close() error {
	d.openSegments.Purge()
	d.extentCache.Close()

	return nil
}

func (d *ExtentReader) returnData(data RangeData) {
	buffers.Return(data.data)
}

func (d *ExtentReader) fetchExtent(
	ctx context.Context,
	log hclog.Logger,
	pe *PartialExtent,
) (RangeData, error) {
	addr := pe.ExtentLocation

	rawData := buffers.Get(int(addr.Size))

	found, err := d.extentCache.ReadExtent(pe, rawData)
	if err != nil {
		log.Error("error reading extent from read cache", "error", err)
	}

	if found {
		extentCacheHits.Inc()
	} else {
		extentCacheMiss.Inc()

		ci, ok := d.openSegments.Get(addr.Segment)
		if !ok {
			lf, err := d.sa.OpenSegment(ctx, addr.Segment)
			if err != nil {
				return RangeData{}, err
			}

			ci = lf

			d.openSegments.Add(addr.Segment, ci)
			openSegments.Inc()
		}

		n, err := ci.ReadAt(rawData, int64(addr.Offset))
		if err != nil {
			return RangeData{}, nil
		}

		if n != len(rawData) {
			log.Error("didn't read full data", "read", n, "expected", len(rawData), "size", addr.Size)
			return RangeData{}, fmt.Errorf("short read detected")
		}

		d.extentCache.WriteExtent(pe, rawData)
	}

	var rangeData []byte

	switch pe.Flags {
	case Uncompressed:
		rangeData = rawData
	case Compressed:
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
	case ZstdCompressed:
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
	default:
		return RangeData{}, fmt.Errorf("unknown flags value: %d", pe.Flags)
	}

	src := MapRangeData(pe.Extent, rangeData)

	return src, nil
}
