package lsvd

import (
	"context"
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/lab47/lsvd/logger"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
)

type ExtentReader struct {
	log          logger.Logger
	openSegments *lru.Cache[SegmentId, SegmentReader]
	sa           SegmentAccess
	rangeCache   *RangeCache
}

func NewExtentReader(log logger.Logger, path string, sa SegmentAccess) (*ExtentReader, error) {
	openSegments, err := lru.NewWithEvict[SegmentId, SegmentReader](
		256, func(key SegmentId, value SegmentReader) {
			openSegments.Dec()
			value.Close()
		})
	if err != nil {
		return nil, err
	}

	er := &ExtentReader{
		log:          log,
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

	d.log.Trace("reading data from segment in storage", "segment", seg, "offset", off)

	_, err := ci.ReadAt(data, off)
	if err != nil {
		return nil
	}

	// We don't check the size because the last chunk might not be a full chunk.

	return nil
}

func FillFromeCache(d []byte, cps []CachePosition) error {
	for _, c := range cps {
		_, err := c.fd.ReadAt(d[:c.size], c.off)
		if err != nil {
			return err
		}

		d = d[c.size:]
	}

	return nil
}

func (d *ExtentReader) fetchUncompressedExtent(
	ctx context.Context,
	log logger.Logger,
	pe *PartialExtent,
	cps []CachePosition,
) (RangeData, []CachePosition, error) {
	startFetch := time.Now()

	addr := pe.ExtentLocation

	cp, err := d.rangeCache.CachePositions(ctx, addr.Segment, int64(addr.Size), int64(addr.Offset), cps)
	if err != nil {
		return RangeData{}, nil, err
	}

	if log.IsTrace() {
		log.Trace("reading uncompressed extent", "offset", addr.Offset, "size", addr.Size, "cache-offset", cp[0].off)
	}

	readProcessing.Add(time.Since(startFetch).Seconds())
	return RangeData{}, cp, nil
}

func (d *ExtentReader) fetchExtent(
	ctx context.Context,
	log logger.Logger,
	pe *PartialExtent,
	cps []CachePosition,
) (RangeData, []CachePosition, error) {
	if cap(cps) > 0 && pe.Flags() == Uncompressed {
		return d.fetchUncompressedExtent(ctx, log, pe, cps)
	}

	startFetch := time.Now()

	addr := pe.ExtentLocation

	rawData := buffers.Get(int(addr.Size))

	n, err := d.rangeCache.ReadAt(ctx, addr.Segment, rawData, int64(addr.Offset))
	if err != nil {
		return RangeData{}, nil, err
	}

	if n != len(rawData) {
		log.Error("didn't read full data", "read", n, "expected", len(rawData), "size", addr.Size)
		return RangeData{}, nil, fmt.Errorf("short read detected")
	}

	var rangeData []byte

	switch pe.Flags() {
	case Uncompressed:
		rangeData = rawData
	case Compressed:
		startDecomp := time.Now()
		sz := pe.RawSize

		uncomp := buffers.Get(int(sz))

		n, err := lz4.UncompressBlock(rawData, uncomp)
		if err != nil {
			return RangeData{}, nil, errors.Wrapf(err, "error uncompressing data (rawsize: %d, compdata: %d)", len(rawData), len(uncomp))
		}

		if n != int(sz) {
			return RangeData{}, nil, fmt.Errorf("failed to uncompress correctly, %d != %d", n, sz)
		}

		// We're finished with the raw extent data.
		buffers.Return(rawData)

		rangeData = uncomp
		compressionOverhead.Add(time.Since(startDecomp).Seconds())
	default:
		return RangeData{}, nil, fmt.Errorf("unknown flags value: %d", pe.Flags())
	}

	src := MapRangeData(pe.Extent, rangeData)

	readProcessing.Add(time.Since(startFetch).Seconds())
	return src, nil, nil
}

func (d *ExtentReader) fetchExtentUncached(
	ctx context.Context,
	log logger.Logger,
	pe *PartialExtent,
	cps []CachePosition,
) (RangeData, []CachePosition, error) {
	if cap(cps) > 0 && pe.Flags() == Uncompressed {
		return d.fetchUncompressedExtent(ctx, log, pe, cps)
	}

	startFetch := time.Now()

	addr := pe.ExtentLocation

	rawData := buffers.Get(int(addr.Size))

	err := d.fetchData(ctx, addr.Segment, rawData, int64(addr.Offset))
	if err != nil {
		return RangeData{}, nil, err
	}

	var rangeData []byte

	switch pe.Flags() {
	case Uncompressed:
		rangeData = rawData
	case Compressed:
		startDecomp := time.Now()
		sz := pe.RawSize

		uncomp := buffers.Get(int(sz))

		n, err := lz4.UncompressBlock(rawData, uncomp)
		if err != nil {
			return RangeData{}, nil, errors.Wrapf(err, "error uncompressing data (rawsize: %d, compdata: %d)", len(rawData), len(uncomp))
		}

		if n != int(sz) {
			return RangeData{}, nil, fmt.Errorf("failed to uncompress correctly, %d != %d", n, sz)
		}

		// We're finished with the raw extent data.
		buffers.Return(rawData)

		rangeData = uncomp
		compressionOverhead.Add(time.Since(startDecomp).Seconds())
		return RangeData{}, nil, fmt.Errorf("unknown flags value: %d", pe.Flags())
	}

	src := MapRangeData(pe.Extent, rangeData)

	readProcessing.Add(time.Since(startFetch).Seconds())
	return src, nil, nil
}
