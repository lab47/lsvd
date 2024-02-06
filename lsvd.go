package lsvd

import (
	"context"
	"crypto/rand"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/lab47/lz4decode"
	"github.com/lab47/mode"

	"github.com/hashicorp/go-hclog"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
)

const (
	// The size of all blocks in bytes
	BlockSize = 4 * 1024

	// How big the segment gets before we flush it to S3
	FlushThreshHold = 15 * 1024 * 1024
)

type Disk struct {
	SeqGen func() ulid.ULID
	log    hclog.Logger
	path   string

	size    int64
	volName string

	prevCacheMu   sync.Mutex
	prevCacheCond *sync.Cond
	prevCache     *SegmentCreator

	curSeq ulid.ULID

	lbaMu   sync.Mutex
	lba2pba *ExtentMap

	extentCache  *ExtentCache
	openSegments *lru.Cache[SegmentId, SegmentReader]

	sa    SegmentAccess
	curOC *SegmentCreator

	segmentsMu sync.Mutex
	segments   map[SegmentId]*Segment

	afterNS func(SegmentId)
}

func NewDisk(ctx context.Context, log hclog.Logger, path string, options ...Option) (*Disk, error) {
	ec, err := NewExtentCache(log, filepath.Join(path, "readcache"))
	if err != nil {
		return nil, err
	}

	var o opts
	o.autoCreate = true

	for _, opt := range options {
		opt(&o)
	}

	if o.sa == nil {
		o.sa = &LocalFileAccess{Dir: path}
	}

	if o.volName == "" {
		o.volName = "default"
	}

	err = o.sa.InitContainer(ctx)
	if err != nil {
		return nil, err
	}

	var sz int64

	vi, err := o.sa.GetVolumeInfo(ctx, o.volName)
	if err != nil || vi.Name == "" {
		if !o.autoCreate {
			return nil, fmt.Errorf("unknown volume: %s", o.volName)
		}

		err = o.sa.InitVolume(ctx, &VolumeInfo{Name: o.volName})
		if err != nil {
			return nil, err
		}
	} else {
		sz = vi.Size
	}

	log.Info("attaching to volume", "name", o.volName, "size", sz)

	d := &Disk{
		log:         log,
		path:        path,
		size:        sz,
		lba2pba:     NewExtentMap(log),
		extentCache: ec,
		sa:          o.sa,
		volName:     o.volName,
		SeqGen:      o.seqGen,
		segments:    make(map[SegmentId]*Segment),
		afterNS:     o.afterNS,
	}

	d.prevCacheCond = sync.NewCond(&d.prevCacheMu)

	openSegments, err := lru.NewWithEvict[SegmentId, SegmentReader](
		256, func(key SegmentId, value SegmentReader) {
			openSegments.Dec()
			value.Close()
		})
	if err != nil {
		return nil, err
	}

	d.openSegments = openSegments

	err = d.restoreWriteCache(ctx)
	if err != nil {
		return nil, err
	}

	if d.curOC == nil {
		d.curOC, err = d.newSegmentCreator()
		if err != nil {
			return nil, err
		}
	}

	goodMap, err := d.loadLBAMap(ctx)
	if err != nil {
		return nil, err
	}

	if goodMap {
		log.Info("reusing serialized LBA map", "blocks", d.lba2pba.Len())
	} else {
		err = d.rebuildFromSegments(ctx)
		if err != nil {
			return nil, err
		}
	}

	return d, nil
}

func (r *Disk) SetAfterNS(f func(SegmentId)) {
	r.afterNS = f
}

type ExtentLocation struct {
	ExtentHeader
	Segment SegmentId
}

type PartialExtent struct {
	Partial Extent
	ExtentLocation
}

func (r *PartialExtent) String() string {
	return fmt.Sprintf("%s (%s): %s %d:%d", r.Partial, r.Extent, r.Segment, r.Offset, r.Size)
}

var monoRead = ulid.Monotonic(rand.Reader, 2)

func (d *Disk) nextSeq() (ulid.ULID, error) {
	if d.SeqGen != nil {
		return d.SeqGen(), nil
	}

	ul, err := ulid.New(ulid.Now(), monoRead)
	if err != nil {
		return ulid.ULID{}, err
	}

	return ul, nil
}

func (d *Disk) newSegmentCreator() (*SegmentCreator, error) {
	seq, err := d.nextSeq()
	if err != nil {
		return nil, errors.Wrapf(err, "error generating sequence number")
	}

	d.curSeq = seq

	path := filepath.Join(d.path, "writecache."+seq.String())
	return NewSegmentCreator(d.log, d.volName, path)
}

func (d *Disk) ReadExtent(ctx context.Context, rng Extent) (RangeData, error) {
	start := time.Now()

	defer func() {
		blocksReadLatency.Observe(time.Since(start).Seconds())
	}()

	iops.Inc()

	data := NewRangeData(rng)

	d.log.Trace("attempting to fill request from write cache", "extent", rng)

	remaining, err := d.fillFromWriteCache(ctx, data)
	if err != nil {
		return RangeData{}, err
	}

	// Completely filled range from the write cache
	if len(remaining) == 0 {
		d.log.Trace("extent filled entirely from write cache")
		return data, nil
	}

	d.log.Trace("remaining extents needed", "total", len(remaining))

	if d.log.IsTrace() {
		for _, r := range remaining {
			d.log.Trace("remaining", "extent", r)
		}
	}

	type readRequest struct {
		pe      *PartialExtent
		extents []Extent
	}

	var (
		reqs []*readRequest
		last *PartialExtent
	)

	for _, h := range remaining {
		pes, err := d.resolvePartialExtents(h)
		if err != nil {
			d.log.Error("error computing opbas", "error", err, "rng", h)
			return RangeData{}, err
		}

		if len(pes) == 0 {
			d.log.Trace("no partial extents found")
			// nothing for range, and since the data is pre-zero'd, we
			// don't need to clear anything here.
		} else {
			for _, pe := range pes {
				if pe.Size == 0 {
					// it's empty! cool cool, we don't need to fill the hole
					// since the slice we're filling inside data has already been
					// cleared when it's created.
					continue
				}

				// Because the holes can be smaller than the read ranges,
				// 2 or more holes in sequence might be served by the same
				// segment range.
				if last == pe {
					reqs[len(reqs)-1].extents = append(reqs[len(reqs)-1].extents, h)
				} else {
					r := &readRequest{
						pe:      pe,
						extents: []Extent{h},
					}

					reqs = append(reqs, r)
					last = pe
				}
			}
		}
	}

	if d.log.IsTrace() {
		d.log.Trace("pes needed", "total", len(reqs))

		for _, o := range reqs {
			d.log.Trace("partial-extent needed",
				"segment", o.pe.Segment, "offset", o.pe.Offset, "size", o.pe.Size,
				"usable", o.pe.Partial, "full", o.pe.Extent)
		}
	}

	// With our set of segments and partial extents in hand, go reach each one
	// and populate data. This could be parallelized as each touches a different
	// range of data.
	for _, o := range reqs {
		err := d.readPartialExtent(ctx, o.pe, o.extents, rng, data)
		if err != nil {
			return RangeData{}, err
		}
	}

	return data, nil
}

func (d *Disk) fillFromWriteCache(ctx context.Context, data RangeData) ([]Extent, error) {
	used, err := d.curOC.FillExtent(data)
	if err != nil {
		return nil, err
	}

	var remaining []Extent

	d.log.Trace("write cache used", "request", data.Extent, "used", used)

	if len(used) == 0 {
		remaining = []Extent{data.Extent}
	} else {
		var ok bool
		remaining, ok = data.SubMany(used)
		if !ok {
			return nil, fmt.Errorf("internal error calculating remaining extents")
		}
	}

	d.log.Trace("requesting reads from prev cache", "used", used, "remaining", remaining)

	return d.fillingFromPrevWriteCache(ctx, data, remaining)
}

func (d *Disk) fillingFromPrevWriteCache(ctx context.Context, data RangeData, holes []Extent) ([]Extent, error) {
	// This is broken out from fillFromWriteCache because we need to hold prevCacheMu since
	// the segment closing goroutine is running elsewhere and touches it.
	d.prevCacheMu.Lock()
	defer d.prevCacheMu.Unlock()

	oc := d.prevCache

	// If there is no previous cache, bail.
	if oc == nil {
		return holes, nil
	}

	var remaining []Extent

	for _, sub := range holes {
		sr, ok := data.SubRange(sub)
		if !ok {
			return nil, fmt.Errorf("error calculating subrange")
		}

		used, err := oc.FillExtent(sr)
		if err != nil {
			return nil, err
		}

		if len(used) == 0 {
			remaining = append(remaining, sub)
		} else {
			res, ok := sub.SubMany(used)
			if !ok {
				return nil, fmt.Errorf("error subtracting partial holes")
			}

			remaining = append(remaining, res...)
		}
	}

	d.log.Trace("write cache didn't find", "input", holes, "holes", remaining)

	return remaining, nil
}

func (d *Disk) resolvePartialExtents(rng Extent) ([]*PartialExtent, error) {
	d.lbaMu.Lock()
	defer d.lbaMu.Unlock()

	return d.lba2pba.Resolve(rng)
}

func (d *Disk) readPartialExtent(
	ctx context.Context,
	pe *PartialExtent,
	rngs []Extent,
	dataRange Extent,
	dest RangeData,
) error {
	addr := pe.ExtentLocation

	rawData := buffers.Get(int(addr.Size))
	defer buffers.Return(rawData)

	found, err := d.extentCache.ReadExtent(pe, rawData)
	if err != nil {
		d.log.Error("error reading extent from read cache", "error", err)
	}

	if found {
		extentCacheHits.Inc()
	} else {
		extentCacheMiss.Inc()

		ci, ok := d.openSegments.Get(addr.Segment)
		if !ok {
			lf, err := d.sa.OpenSegment(ctx, addr.Segment)
			if err != nil {
				return err
			}

			ci = lf

			d.openSegments.Add(addr.Segment, ci)
			openSegments.Inc()
		}

		n, err := ci.ReadAt(rawData, int64(addr.Offset))
		if err != nil {
			return nil
		}

		if n != len(rawData) {
			d.log.Error("didn't read full data", "read", n, "expected", len(rawData), "size", addr.Size)
			return fmt.Errorf("short read detected")
		}

		d.extentCache.WriteExtent(pe, rawData)
	}

	if pe.Flags == Compressed {
		sz := pe.RawSize

		uncomp := buffers.Get(int(sz))
		defer buffers.Return(uncomp)

		n, err := lz4decode.UncompressBlock(rawData, uncomp, nil)
		if err != nil {
			return errors.Wrapf(err, "error uncompressing data (rawsize: %d, compdata: %d)", len(rawData), len(uncomp))
		}

		if n != int(sz) {
			return fmt.Errorf("failed to uncompress correctly")
		}

		rawData = uncomp
	} else if pe.Flags != Uncompressed {
		return fmt.Errorf("unknown flags value: %d", pe.Flags)
	}

	src := RangeData{
		Extent: pe.Extent, // be sure not to use pe.Partial as srcData is the full extent
		BlockData: BlockData{
			blocks: int(pe.Partial.Blocks),
			data:   rawData,
		},
	}

	// the bytes at the beginning of data are for LBA dataBegin.LBA.
	// the bytes at the beginning of rawData are for LBA full.LBA.
	// we want to compute the 2 byte ranges:
	//   1. the byte range for rng within data
	//   2. the byte range for rng within rawData
	// Then we copy the bytes from 2 to 1.
	for _, x := range rngs {
		overlap, ok := pe.Partial.Clamp(x)
		if !ok {
			d.log.Error("error clamping required range to usable range")
			return fmt.Errorf("error clamping range")
		}

		d.log.Trace("preparing to copy data from segment", "request", x, "clamped", overlap)

		// Compute our source range and destination range against overlap

		subDest, ok := dest.SubRange(overlap)
		if !ok {
			d.log.Error("error clamping range", "full", pe.Partial, "sub", overlap)
			return fmt.Errorf("error clamping range: %s => %s", pe.Partial, overlap)
		}

		subSrc, ok := src.SubRange(overlap)
		if !ok {
			d.log.Error("error calculate source subrange",
				"input", src.Extent, "sub", overlap,
				"request", x, "usable", pe.Partial,
				"full", pe.Extent,
			)
			return fmt.Errorf("error calculate source subrange")
		}

		d.log.Trace("copying segment data",
			"src", src.Extent,
			"dest", dest.Extent,
			"sub-source", subSrc.Extent, "sub-dest", subDest.Extent,
		)
		n := copy(subDest.data, subSrc.data)
		if n != len(subDest.data) {
			d.log.Error("error copying data from partial extent", "expected", len(subDest.data), "was", n)
		}
	}

	return nil
}

func (d *Disk) ZeroBlocks(ctx context.Context, rng Extent) error {
	iops.Inc()
	blocksWritten.Add(float64(rng.Blocks))

	return d.curOC.ZeroBlocks(rng)
}

func (d *Disk) WriteExtent(ctx context.Context, data RangeData) error {
	start := time.Now()

	defer func() {
		blocksWriteLatency.Observe(time.Since(start).Seconds())
	}()

	iops.Inc()

	err := d.curOC.WriteExtent(data)
	if err != nil {
		d.log.Error("error write extents to segment creator", "error", err)
		return err
	}

	if d.curOC.BodySize() >= FlushThreshHold {
		d.log.Info("flushing new segment",
			"body-size", d.curOC.BodySize(),
			"extents", d.curOC.Entries(),
			"blocks", d.curOC.TotalBlocks(),
			"storage-ratio", d.curOC.AvgStorageRatio(),
		)
		ch, err := d.closeSegmentAsync(ctx)
		if err != nil {
			return err
		}

		if mode.Debug() {
			select {
			case <-ch:
				d.log.Debug("segment has been flushed")
			case <-ctx.Done():
			}
		}
	}

	return nil
}

func (d *Disk) SyncWriteCache() error {
	iops.Inc()

	if d.curOC != nil {
		return d.curOC.Sync()
	}

	return nil
}

func (d *Disk) Close(ctx context.Context) error {
	err := d.CloseSegment(ctx)
	if err != nil {
		return errors.Wrapf(err, "error closing segment")
	}

	err = d.saveLBAMap(ctx)
	if err != nil {
		d.log.Error("error saving LBA cached map", "error", err)
		err = errors.Wrapf(err, "error saving lba map")
	}

	d.openSegments.Purge()

	d.extentCache.Close()

	return err
}

func (d *Disk) LogSegmentInfo() {
	d.segmentsMu.Lock()
	defer d.segmentsMu.Unlock()

	type ent struct {
		seg     SegmentId
		density float64
		stats   *Segment
	}

	var entries []ent

	for segId, stats := range d.segments {
		entries = append(entries, ent{segId, stats.Density(), stats})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].density < entries[j].density
	})

	for _, e := range entries {
		d.log.Info("segment density", "segment", e.seg,
			"density", e.density,
			"total", e.stats.Size,
			"used", e.stats.Used,
		)
	}
}

func (d *Disk) Size() int64 {
	return d.size
}
