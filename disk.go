package lsvd

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/lab47/lsvd/logger"
	"github.com/lab47/mode"

	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
)

const (
	// The size of all blocks in bytes
	BlockSize = 4 * 1024

	// How big the segment gets before we flush it to S3
	FlushThreshHold = 32 * 1024 * 1024
)

type Disk struct {
	SeqGen func() ulid.ULID
	log    logger.Logger
	path   string

	size     int64
	volName  string
	readOnly bool
	useZstd  bool

	prevCache *PreviousCache

	curSeq SegmentId

	lba2pba *ExtentMap
	er      *ExtentReader

	sa    SegmentAccess
	curOC *SegmentCreator

	s *Segments

	afterNS func(SegmentId)

	readDisks []*Disk

	bgmu sync.Mutex
}

func NewDisk(ctx context.Context, log logger.Logger, path string, options ...Option) (*Disk, error) {
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

	err := o.sa.InitContainer(ctx)
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

	for _, ld := range o.lowers {
		if !ld.readOnly {
			return nil, fmt.Errorf("lower disk not open'd read-only")
		}
	}

	log.Info("attaching to volume", "name", o.volName, "size", sz)

	er, err := NewExtentReader(log, filepath.Join(path, "readcache"), o.sa)
	if err != nil {
		return nil, err
	}
	d := &Disk{
		log:       log,
		path:      path,
		size:      sz,
		lba2pba:   NewExtentMap(),
		sa:        o.sa,
		volName:   o.volName,
		SeqGen:    o.seqGen,
		afterNS:   o.afterNS,
		readOnly:  o.ro,
		useZstd:   o.useZstd,
		er:        er,
		prevCache: NewPreviousCache(),
		s:         NewSegments(),
	}

	d.readDisks = append(d.readDisks, d)
	d.readDisks = append(d.readDisks, o.lowers...)

	if !d.readOnly {
		err = d.restoreWriteCache(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "restoring write cache")
		}

		if d.curOC == nil {
			d.curOC, err = d.newSegmentCreator()
			if err != nil {
				return nil, errors.Wrapf(err, "creating segment creator")
			}
		}

		log.Info("starting sequence", "seq", d.curSeq)
	}

	/*
		goodMap, err := d.loadLBAMap(ctx)
		if err != nil {
			return nil, err
		}
	*/

	if false {
		log.Info("reusing serialized LBA map", "blocks", d.lba2pba.Len())
	} else {
		err = d.rebuildFromSegments(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "rebuilding segments")
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
	Disk    uint16
}

type PartialExtent struct {
	Live Extent
	ExtentLocation
}

func (r *PartialExtent) String() string {
	return fmt.Sprintf("%s (%s): %s %d:%d", r.Live, r.Extent, r.Segment, r.Offset, r.Size)
}

func (d *Disk) nextSeq() (SegmentId, error) {
	if d.SeqGen != nil {
		return SegmentId(d.SeqGen()), nil
	}

	ul, err := ulid.New(ulid.Now(), ulid.DefaultEntropy())
	if err != nil {
		return SegmentId{}, err
	}

	return SegmentId(ul), nil
}

func (d *Disk) newSegmentCreator() (*SegmentCreator, error) {
	seq, err := d.nextSeq()
	if err != nil {
		return nil, errors.Wrapf(err, "error generating sequence number")
	}

	d.curSeq = seq

	path := filepath.Join(d.path, "writecache."+seq.String())
	sc, err := NewSegmentCreator(d.log, d.volName, path)
	if err != nil {
		return nil, err
	}

	d.log.Trace("creating new segment creator", "segment", seq, "oc", fmt.Sprintf("%p", sc))
	return sc, nil
}

// Used to test things are setup the way we expect
func (d *Disk) resolveSegmentAccess(ext Extent) ([]PartialExtent, error) {
	return d.lba2pba.Resolve(d.log, ext)
}

func (d *Disk) ReadExtent(ctx context.Context, rng Extent) (RangeData, error) {
	b := B(ctx)

	data := b.NewRangeData(rng)

	cp, err := d.ReadExtentInto(ctx, data)
	if cp.fd != nil {
		err = FillFromeCache(data.WriteData(), []CachePosition{cp})
		if err != nil {
			return RangeData{}, err
		}
	}

	return data, err
}

func (d *Disk) ReadExtentInto(ctx context.Context, data RangeData) (CachePosition, error) {
	start := time.Now()

	defer func() {
		blocksReadLatency.Observe(time.Since(start).Seconds())
	}()

	rng := data.Extent

	blocksRead.Add(float64(rng.Blocks))

	iops.Inc()

	log := d.log

	log.Debug("attempting to fill request from write cache", "extent", rng)

	remaining, err := d.fillFromWriteCache(ctx, log, data)
	if err != nil {
		return CachePosition{}, err
	}

	// Completely filled range from the write cache
	if len(remaining) == 0 {
		d.log.Debug("extent filled entirely from write cache")
		return CachePosition{}, nil
	}

	log.Trace("remaining extents needed", "total", len(remaining))

	type readRequest struct {
		pe      PartialExtent
		extents []Extent
	}

	var (
		reqs []*readRequest
		last *readRequest
	)

	// remaining is the extents that we still need to fill.
	for _, h := range remaining {

		// We resolve each one into a set of partial extents which have
		// information about which segment the partials are in.
		//
		// Invariant: each of the pes.Partial extents must be a part of +h+.
		pes, err := d.lba2pba.Resolve(log, h)
		if err != nil {
			log.Error("error computing opbas", "error", err, "rng", h)
			return CachePosition{}, err
		}

		if len(pes) == 0 {
			log.Debug("no partial extents found")
			if v, ok := data.SubRange(h); ok {
				clear(v.WriteData())
			}
			// nothing for range, and since the data is pre-zero'd, we
			// don't need to clear anything here.
		} else {
			// Pure read from one extent, optimize!
			if len(remaining) == 1 && remaining[0] == rng && len(pes) == 1 && pes[0].Flags() == Uncompressed {
				log.Trace("reading single, uncompressed extent via fast path")
				// Invariants: remaining[0] == rng == data.Extent
				// Invariants: pes[0].Live fully covers remaining[0]
				pe := pes[0]
				ld := d.readDisks[pe.Disk]
				cps, err := ld.readOneExtent(ctx, &pe, rng, data)
				if err != nil {
					return CachePosition{}, err
				}

				return cps, nil
			}

			for _, pe := range pes {
				if pe.Size == 0 {
					if v, ok := data.SubRange(pe.Live); ok {
						clear(v.WriteData())
					}
					// it's empty! cool cool, we don't need to fill the hole
					// since the slice we're filling inside data has already been
					// cleared when it's created.
					continue
				}

				if mode.Debug() && pe.Live.Cover(h) == CoverNone {
					log.Error("resolve returned extent that doesn't cover", "hole", h, "pe", pe.Live)
				}

				// Because the holes can be smaller than the read ranges,
				// 2 or more holes in sequence might be served by the same
				// segment range.
				if last != nil && last.pe == pe {
					last.extents = append(last.extents, h)
				} else {
					r := &readRequest{
						pe:      pe,
						extents: []Extent{h},
					}

					reqs = append(reqs, r)
					last = r
				}
			}
		}
	}

	if log.Enabled(ctx, slog.LevelDebug) {
		log.Debug("pes needed", "total", len(reqs))

		for _, o := range reqs {
			log.Debug("partial-extent needed",
				"segment", o.pe.Segment, "offset", o.pe.Offset, "size", o.pe.Size,
				"usable", o.pe.Live, "full", o.pe.Extent,
				"disk-id", o.pe.Disk,
			)
		}
	}

	// With our set of segments and partial extents in hand, go reach each one
	// and populate data. This could be parallelized as each touches a different
	// range of data.
	for _, o := range reqs {
		ld := d.readDisks[o.pe.Disk]
		err := ld.readPartialExtent(ctx, &o.pe, o.extents, rng, data)
		if err != nil {
			return CachePosition{}, err
		}
	}

	return CachePosition{}, nil
}

func (d *Disk) fillFromWriteCache(ctx context.Context, log logger.Logger, data RangeData) ([]Extent, error) {
	if d.curOC == nil {
		return []Extent{data.Extent}, nil
	}

	log.Trace("consulting oc for extent", "oc", fmt.Sprintf("%p", d.curOC))
	used, err := d.curOC.FillExtent(data.View())
	if err != nil {
		return nil, err
	}

	var remaining []Extent

	log.Trace("write cache used", "request", data.Extent, "used", used)

	if len(used) == 0 {
		remaining = []Extent{data.Extent}
	} else {
		var ok bool
		remaining, ok = data.SubMany(used)
		if !ok {
			return nil, fmt.Errorf("internal error calculating remaining extents")
		}
	}

	log.Trace("requesting reads from prev cache", "used", used, "remaining", remaining)

	return d.fillingFromPrevWriteCache(ctx, log, data, remaining)
}

func (d *Disk) fillingFromPrevWriteCache(ctx context.Context, log logger.Logger, data RangeData, holes []Extent) ([]Extent, error) {
	oc := d.prevCache.Load()

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

	log.Debug("write cache didn't find", "input", holes, "holes", remaining)

	return remaining, nil
}

func (d *Disk) readOneExtent(
	ctx context.Context,
	pe *PartialExtent,
	x Extent,
	dest RangeData,
) (CachePosition, error) {
	src, cps, err := d.er.fetchExtent(ctx, d.log, pe, true)
	if err != nil {
		return CachePosition{}, err
	}

	if len(cps) == 1 {
		d.log.Trace("single extent found directly in read cache")
		// There are a few elements, let's write them out so we keep them straight:
		// pe.Extent is the data covered by cps[0]
		// pe.Live is sub-range of pe.Extent that is only the data to consider
		// x is the data the user requests, and it's contained fully within pe.Live

		adjusted := cps[0]

		// go from extent to live
		adjusted.off += (int64(pe.Live.LBA-pe.LBA) * BlockSize)
		adjusted.size = int64(pe.Live.ByteSize())

		// go from live to x
		adjusted.off += (int64(x.LBA-pe.Live.LBA) * BlockSize)
		adjusted.size = int64(x.ByteSize())

		return adjusted, nil
	}

	d.log.Trace("single extent not found in cache", "cps", len(cps))

	inflateCache.Inc()

	rawData := buffers.Get(int(pe.Size))

	err = FillFromeCache(rawData, cps)
	if err != nil {
		return CachePosition{}, err
	}

	src = MapRangeData(pe.Extent, rawData)
	defer d.er.returnData(src)

	// the bytes at the beginning of data are for LBA dataBegin.LBA.
	// the bytes at the beginning of rawData are for LBA full.LBA.
	// we want to compute the 2 byte ranges:
	//   1. the byte range for rng within data
	//   2. the byte range for rng within rawData
	// Then we copy the bytes from 2 to 1.
	overlap, ok := pe.Live.Clamp(x)
	if !ok {
		d.log.Error("error clamping required range to usable range", "request", x, "partial", pe.Live)
		return CachePosition{}, fmt.Errorf("error clamping range")
	}

	d.log.Debug("preparing to copy data from segment", "request", x, "clamped", overlap)

	// Compute our source range and destination range against overlap

	subDest, ok := dest.SubRange(overlap)
	if !ok {
		d.log.Error("error clamping range", "full", pe.Live, "sub", overlap)
		return CachePosition{}, fmt.Errorf("error clamping range: %s => %s", pe.Live, overlap)
	}

	subSrc, ok := src.SubRange(overlap)
	if !ok {
		d.log.Error("error calculate source subrange",
			"input", src.Extent, "sub", overlap,
			"request", x, "usable", pe.Live,
			"full", pe.Extent,
		)
		return CachePosition{}, fmt.Errorf("error calculate source subrange")
	}

	d.log.Debug("copying segment data",
		"src", src.Extent,
		"dest", dest.Extent,
		"sub-source", subSrc.Extent, "sub-dest", subDest.Extent,
	)
	n := subDest.Copy(subSrc)
	if n != subDest.ByteSize() {
		d.log.Error("error copying data from partial extent", "expected", subDest.ByteSize(), "was", n)
	}

	return CachePosition{}, nil
}

func (d *Disk) readPartialExtent(
	ctx context.Context,
	pe *PartialExtent,
	rngs []Extent,
	dataRange Extent,
	dest RangeData,
) error {
	src, _, err := d.er.fetchExtent(ctx, d.log, pe, false)
	if err != nil {
		return err
	}

	defer d.er.returnData(src)

	// the bytes at the beginning of data are for LBA dataBegin.LBA.
	// the bytes at the beginning of rawData are for LBA full.LBA.
	// we want to compute the 2 byte ranges:
	//   1. the byte range for rng within data
	//   2. the byte range for rng within rawData
	// Then we copy the bytes from 2 to 1.
	for _, x := range rngs {
		overlap, ok := pe.Live.Clamp(x)
		if !ok {
			d.log.Error("error clamping required range to usable range", "request", x, "partial", pe.Live)
			return fmt.Errorf("error clamping range")
		}

		d.log.Debug("preparing to copy data from segment", "request", x, "clamped", overlap)

		// Compute our source range and destination range against overlap

		subDest, ok := dest.SubRange(overlap)
		if !ok {
			d.log.Error("error clamping range", "full", pe.Live, "sub", overlap)
			return fmt.Errorf("error clamping range: %s => %s", pe.Live, overlap)
		}

		subSrc, ok := src.SubRange(overlap)
		if !ok {
			d.log.Error("error calculate source subrange",
				"input", src.Extent, "sub", overlap,
				"request", x, "usable", pe.Live,
				"full", pe.Extent,
			)
			return fmt.Errorf("error calculate source subrange")
		}

		d.log.Debug("copying segment data",
			"src", src.Extent,
			"dest", dest.Extent,
			"sub-source", subSrc.Extent, "sub-dest", subDest.Extent,
		)
		n := subDest.Copy(subSrc)
		if n != subDest.ByteSize() {
			d.log.Error("error copying data from partial extent", "expected", subDest.ByteSize(), "was", n)
		}
	}

	return nil
}

func (d *Disk) ZeroBlocks(ctx context.Context, rng Extent) error {
	if d.readOnly {
		return nil
	}

	iops.Inc()
	blocksWritten.Add(float64(rng.Blocks))

	return d.curOC.ZeroBlocks(rng)
}

func (d *Disk) checkFlush(ctx context.Context) error {
	if d.curOC.ShouldFlush(FlushThreshHold) {
		d.log.Info("flushing new segment",
			"body-size", d.curOC.BodySize(),
			"extents", d.curOC.Entries(),
			"blocks", d.curOC.TotalBlocks(),
			"input-bytes", d.curOC.InputBytes(),
			"empty-blocks", d.curOC.EmptyBlocks(),
			"single-bes", d.curOC.builder.singleBEs,
			"compression-rate", d.curOC.CompressionRate(),
			"storage-ratio", d.curOC.StorageRatio(),
			"comp-rate-histo", d.curOC.CompressionRateHistogram(),
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

var ErrReadOnly = errors.New("disk open'd read-only")

func (d *Disk) WriteExtent(ctx context.Context, data RangeData) error {
	if d.readOnly {
		return ErrReadOnly
	}

	start := time.Now()

	defer func() {
		blocksWriteLatency.Observe(time.Since(start).Seconds())
	}()

	blocksWritten.Add(float64(data.Blocks))

	iops.Inc()

	err := d.curOC.WriteExtent(data)
	if err != nil {
		d.log.Error("error write extents to segment creator", "error", err)
		return err
	}

	return d.checkFlush(ctx)
}

// WriteExtents writes multiple extents without performing any segment
// flush checking between them, thusly making sure that all of them end
// up in the same segment.
func (d *Disk) WriteExtents(ctx context.Context, ranges []RangeData) error {
	if d.readOnly {
		return ErrReadOnly
	}

	start := time.Now()

	defer func() {
		blocksWriteLatency.Observe(time.Since(start).Seconds())
	}()

	iops.Add(float64(len(ranges)))

	for _, data := range ranges {
		err := d.curOC.WriteExtent(data)
		if err != nil {
			d.log.Error("error write extents to segment creator", "error", err)
			return err
		}
	}

	return d.checkFlush(ctx)
}

func (d *Disk) SyncWriteCache() error {
	if d.readOnly {
		return nil
	}

	iops.Inc()

	if d.curOC != nil {
		return d.curOC.builder.Sync()
	}

	return nil
}

func (d *Disk) Close(ctx context.Context) error {
	d.CheckpointGC(ctx)

	err := d.CloseSegment(ctx)
	if err != nil {
		return errors.Wrapf(err, "error closing segment")
	}

	err = d.saveLBAMap(ctx)
	if err != nil {
		d.log.Error("error saving LBA cached map", "error", err)
		err = errors.Wrapf(err, "error saving lba map")
	}

	d.er.Close()

	return err
}

func (d *Disk) Size() int64 {
	return d.size
}
