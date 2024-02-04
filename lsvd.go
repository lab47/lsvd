package lsvd

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lab47/lz4decode"
	"github.com/lab47/mode"

	"github.com/edsrzf/mmap-go"
	"github.com/hashicorp/go-hclog"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
)

type (
	SegmentId ulid.ULID
	LBA       uint64

	OPBA struct {
		Segment SegmentId
		Offset  uint32
		Size    uint32
		Flag    byte
	}

	RangedOPBA struct {
		Range Extent
		Full  Extent
		OPBA
	}
)

type (
	segmentInfo struct {
		f ObjectReader
		m mmap.MMap
	}
)

var (
	headerSize = binary.Size(SegmentHeader{})
	empty      SegmentId
)

func (s SegmentId) String() string {
	return ulid.ULID(s).String()
}

const SegmentIdSize = 16

const BlockSize = 4 * 1024

const (
	flushThreshHold = 15 * 1024 * 1024
	maxWriteCache   = 100 * 1024 * 1024
)

type Disk struct {
	SeqGen func() ulid.ULID
	log    hclog.Logger
	path   string

	size    int64
	volName string

	prevCacheMu   sync.Mutex
	prevCacheCond *sync.Cond
	prevCache     *ObjectCreator

	curSeq ulid.ULID

	lbaMu   sync.Mutex
	lba2pba *ExtentMap

	extentCache  *ExtentCache
	openSegments *lru.Cache[SegmentId, ObjectReader]

	sa    SegmentAccess
	curOC *ObjectCreator

	segmentsMu sync.Mutex
	segments   map[SegmentId]*Segment

	afterNS func(SegmentId)
}

func RoundToBlockSize(sz int64) int64 {
	diff := sz % BlockSize
	if diff == 0 {
		return sz
	}

	return sz - diff
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

	openSegments, err := lru.NewWithEvict[SegmentId, ObjectReader](
		256, func(key SegmentId, value ObjectReader) {
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
		d.curOC, err = d.newObjectCreator()
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
		err = d.rebuildFromObjects(ctx)
		if err != nil {
			return nil, err
		}
	}

	return d, nil
}

func (r *Disk) SetAfterNS(f func(SegmentId)) {
	r.afterNS = f
}

func (r *RangedOPBA) String() string {
	return fmt.Sprintf("%s (%s): %s %d:%d", r.Range, r.Full, r.Segment, r.Offset, r.Size)
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

func (d *Disk) newObjectCreator() (*ObjectCreator, error) {
	seq, err := d.nextSeq()
	if err != nil {
		return nil, err
	}

	d.curSeq = seq

	path := filepath.Join(d.path, "writecache."+seq.String())
	return NewObjectCreator(d.log, d.volName, path)
}

func (d *Disk) closeSegmentAsync(ctx context.Context) (chan struct{}, error) {
	segId := SegmentId(d.curSeq)

	oc := d.curOC

	var err error
	d.curOC, err = d.newObjectCreator()
	if err != nil {
		return nil, err
	}

	d.prevCacheMu.Lock()
	for d.prevCache != nil {
		d.prevCacheCond.Wait()
	}
	d.prevCache = oc
	d.prevCacheMu.Unlock()

	done := make(chan struct{})

	go func() {
		defer close(done)
		defer segmentsWritten.Inc()

		var (
			entries []objectEntry
			stats   *SegmentStats
			err     error
		)

		start := time.Now()
		for {
			entries, stats, err = oc.Flush(ctx, d.sa, segId)
			if err != nil {
				d.log.Error("error flushing data to object, retrying", "error", err)
				time.Sleep(5 * time.Second)
				continue
			}

			break
		}

		flushDur := time.Since(start)

		d.log.Debug("object published, resetting write cache")

		sums := map[Extent]string{}
		resi := map[Extent][]*RangedOPBA{}

		if mode.Debug() {
			sums = map[Extent]string{}

			var data RangeData

			for _, ent := range entries {
				data.Reset(ent.extent)

				_, err := oc.FillExtent(data)
				if err != nil {
					d.log.Error("error reading extent for validation", "error", err)
				}
				sum := rangeSum(data.data)
				sums[ent.extent] = sum

				ranges, err := d.lba2pba.Resolve(ent.extent)
				if err != nil {
					d.log.Error("error performing resolution for block read check")
				} else {
					resi[ent.extent] = ranges
				}

				//d.log.Info("extent sum", "extent", ent.extent, "sum", sum)
			}
		}

		mapStart := time.Now()

		d.segmentsMu.Lock()

		d.segments[segId] = &Segment{
			Size: stats.Blocks,
			Used: stats.Blocks,
		}

		d.segmentsMu.Unlock()

		d.lbaMu.Lock()
		for _, ent := range entries {
			if mode.Debug() {
				d.log.Trace("updating read map", "extent", ent.extent)
			}
			affected, err := d.lba2pba.Update(ent.extent, ent.opba)
			if err != nil {
				d.log.Error("error updating read map", "error", err)
			}

			d.updateUsage(segId, affected)
		}
		d.lbaMu.Unlock()

		d.prevCacheMu.Lock()
		d.prevCache = nil
		d.prevCacheCond.Signal()
		d.prevCacheMu.Unlock()

		mapDur := time.Since(mapStart)

		if mode.Debug() {
			passed := 0
			for _, ent := range entries {
				data, err := d.ReadExtent(ctx, ent.extent)
				if err != nil {
					d.log.Error("error reading extent for validation", "error", err)
				}
				sum := rangeSum(data.data)

				if sum != sums[ent.extent] {
					d.log.Error("block read validation failed", "extent", ent.extent,
						"sum", sum, "expected", sums[ent.extent])
					ranges, err := d.lba2pba.Resolve(ent.extent)
					if err != nil {
						d.log.Error("unable to resolve for check", "error", err)
					} else {
						var before []string
						for _, r := range resi[ent.extent] {
							before = append(before, r.String())
						}

						var after []string
						for _, r := range ranges {
							after = append(after, r.String())
						}

						d.log.Error("block read validation ranges",
							"before", strings.Join(before, " "),
							"after", strings.Join(after, " "))
					}
				} else {
					passed++
				}
			}

			d.log.Warn("finished block read validation", "passed", passed)
		}

		if d.afterNS != nil {
			d.afterNS(segId)
		}

		finDur := time.Since(start)

		d.log.Info("uploaded new object", "segment", segId, "flush-dur", flushDur, "map-dur", mapDur, "dur", finDur)

		err = d.cleanupDeletedSegments(ctx)
		if err != nil {
			d.log.Error("error cleaning up deleted segments", "error", err)
		}

		d.log.Debug("finished background object flush")
	}()

	return done, nil
}

func (d *Disk) updateUsage(self SegmentId, affected []RangedOPBA) {
	d.segmentsMu.Lock()
	defer d.segmentsMu.Unlock()

	for _, r := range affected {
		if r.Segment != self {
			rng := r.Range

			if seg, ok := d.segments[r.Segment]; ok {
				if seg.deleted {
					continue
				}

				if o, ok := seg.detectedCleared(rng); ok {
					d.log.Warn("detected clearing overlapping extent", "orig", o, "cur", r)
				}
				seg.cleared = append(seg.cleared, rng)
				seg.Used -= uint64(rng.Blocks)
			} else {
				d.log.Warn("missing segment during usage update", "id", r.Segment.String())
			}
		}
	}
}

func (d *Disk) CloseSegment(ctx context.Context) error {
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

const perBlockHeader = 16

var emptyBlock = make([]byte, BlockSize)

type Cover int

const (
	CoverSuperRange Cover = iota
	CoverExact
	CoverPartly
	CoverNone
)

func (c Cover) String() string {
	switch c {
	case CoverSuperRange:
		return "cover-super-range"
	case CoverExact:
		return "cover-exact"
	case CoverPartly:
		return "cover-partly"
	case CoverNone:
		return "cover-none"
	default:
		return "bad-cover"
	}
}

func (d *Disk) computeOPBAs(rng Extent) ([]*RangedOPBA, error) {
	d.lbaMu.Lock()
	defer d.lbaMu.Unlock()

	return d.lba2pba.Resolve(rng)
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
	d.prevCacheMu.Lock()
	defer d.prevCacheMu.Unlock()

	oc := d.prevCache

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

type readRequest struct {
	obpa    *RangedOPBA
	extents []Extent
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

	var (
		opbas []*readRequest
		last  *RangedOPBA
	)

	for _, h := range remaining {
		ropba, err := d.computeOPBAs(h)
		if err != nil {
			d.log.Error("error computing opbas", "error", err, "rng", h)
			return RangeData{}, err
		}

		if len(ropba) == 0 {
			d.log.Trace("no ranged opbas found")
			// nothing for range, and since the data is pre-zero'd, we
			// don't need to clear anything here.
		} else {
			for _, opba := range ropba {
				if opba.Size == 0 {
					// it's empty! cool cool, we don't need to fill the hole
					continue
				}

				// Because the holes can be smaller than the read ranges,
				// 2 or more holes in sequence might be served by the same
				// object range.
				if last == opba {
					opbas[len(opbas)-1].extents = append(opbas[len(opbas)-1].extents, h)
				} else {
					r := &readRequest{
						obpa:    opba,
						extents: []Extent{h},
					}

					opbas = append(opbas, r)
					last = opba
				}
			}
		}
	}

	if d.log.IsTrace() {
		d.log.Trace("opbas needed", "total", len(opbas))

		for _, o := range opbas {
			d.log.Trace("opba needed",
				"segment", o.obpa.Segment, "offset", o.obpa.Offset, "size", o.obpa.Size,
				"usable", o.obpa.Range, "full", o.obpa.Full)
		}
	}

	for _, o := range opbas {
		err := d.readOPBA(ctx, o.obpa, o.extents, rng, data)
		if err != nil {
			return RangeData{}, err
		}
	}

	return data, nil
}

func (d *Disk) readOPBA(
	ctx context.Context,
	ranged *RangedOPBA,
	rngs []Extent,
	dataRange Extent,
	dest RangeData,
) error {
	addr := ranged.OPBA

	rawData := buffers.Get(int(addr.Size))
	defer buffers.Return(rawData)

	found, err := d.extentCache.ReadExtent(ranged, rawData)
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

		d.extentCache.WriteExtent(ranged, rawData)
	}

	if ranged.Flag == 1 {
		sz := binary.BigEndian.Uint32(rawData)

		uncomp := buffers.Get(int(sz))
		defer buffers.Return(uncomp)

		n, err := lz4decode.UncompressBlock(rawData[4:], uncomp, nil)
		if err != nil {
			return err
		}

		if n != int(sz) {
			return fmt.Errorf("failed to uncompress correctly")
		}

		rawData = uncomp
	} else if ranged.Flag != 0 {
		return fmt.Errorf("unknown type byte: %x", rawData[0])
	}

	src := RangeData{
		Extent: ranged.Full, // got to be full, that's what rawData has.
		BlockData: BlockData{
			blocks: int(ranged.Range.Blocks),
			data:   rawData,
		},
	}

	// the bytes at the beginning of data are for LBA dataBegin.LBA.
	// the bytes at the beginning of rawData are for LBA full.LBA.
	// we want to compute the 2 byte ranges:
	//   1. the byte range for rng within data
	//   2. the byte range for rng within rawData
	// Then we copy the bytes from 2 to 1.
	for _, rrng := range rngs {
		rng, ok := ranged.Range.Clamp(rrng)
		if !ok {
			d.log.Error("error clamping required range to usable range")
			return fmt.Errorf("error clamping range")
		}

		d.log.Trace("preparing to copy data from object", "request", rrng, "clamped", rng)

		subDest, ok := dest.SubRange(rng)
		if !ok {
			d.log.Error("error clamping range", "full", ranged.Range, "sub", rng)
			return fmt.Errorf("error clamping range: %s => %s", ranged.Range, rng)
		}

		subSrc, ok := src.SubRange(rng)
		if !ok {
			d.log.Error("error calculate source subrange",
				"input", src.Extent, "sub", rng,
				"request", rrng, "usable", ranged.Range,
				"full", ranged.Full,
			)
			return fmt.Errorf("error calculate source subrange")
		}

		d.log.Trace("copying object data",
			"src", src.Extent,
			"dest", dest.Extent,
			"sub-source", subSrc.Extent, "sub-dest", subDest.Extent,
		)
		n := copy(subDest.data, subSrc.data)
		if n != len(dest.data) {
			d.log.Trace("copied data into buffer", "size", n,
				"src", subSrc.Extent.Blocks,
				"dest", subDest.Extent.Blocks,
			)
		}
	}

	return nil
}

var crcTable = crc64.MakeTable(crc64.ECMA)

func crcLBA(crc uint64, lba LBA) uint64 {
	x := uint64(lba)

	a := [8]byte{
		byte(x >> 56),
		byte(x >> 48),
		byte(x >> 40),
		byte(x >> 32),
		byte(x >> 24),
		byte(x >> 16),
		byte(x >> 8),
		byte(x),
	}

	return crc64.Update(crc, crcTable, a[:])
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
		d.log.Error("error write extents to object creator", "error", err)
		return err
	}

	if d.curOC.BodySize() >= flushThreshHold {
		d.log.Info("flushing new object",
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
				d.log.Debug("object has been flushed")
			case <-ctx.Done():
			}
		}
	}

	return nil
}

func (d *Disk) rebuildFromObjects(ctx context.Context) error {
	entries, err := d.sa.ListSegments(ctx, d.volName)
	if err != nil {
		return err
	}

	for _, ent := range entries {
		err := d.rebuildFromObject(ctx, ent)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Disk) rebuildFromObject(ctx context.Context, seg SegmentId) error {
	d.log.Info("rebuilding mappings from object", "id", seg)

	f, err := d.sa.OpenSegment(ctx, seg)
	if err != nil {
		return err
	}

	defer f.Close()

	br := bufio.NewReader(ToReader(f))

	var hdr SegmentHeader

	err = hdr.Read(br)
	if err != nil {
		return err
	}

	d.log.Debug("extent header info", "count", hdr.ExtentCount, "data-begin", hdr.DataOffset)

	segTrack := &Segment{}

	d.segments[seg] = segTrack

	for i := uint32(0); i < hdr.ExtentCount; i++ {
		var eh ExtentHeader

		err := eh.Read(br)
		if err != nil {
			return err
		}

		segTrack.Size += uint64(eh.Blocks)
		segTrack.Used += uint64(eh.Blocks)

		segTrack.TotalBytes += eh.Size
		segTrack.UsedBytes += eh.Size

		affected, err := d.lba2pba.Update(eh.Extent, OPBA{
			Segment: seg,
			Offset:  hdr.DataOffset + uint32(eh.Offset),
			Size:    uint32(eh.Size),
			Flag:    eh.Flags,
		})
		if err != nil {
			return err
		}

		d.updateUsage(seg, affected)
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

func (d *Disk) restoreWriteCache(ctx context.Context) error {
	entries, err := filepath.Glob(filepath.Join(d.path, "writecache.*"))
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		return nil
	}

	d.log.Info("restoring write cache")

	for _, ent := range entries {
		err := d.restoreWriteCacheFile(ctx, ent)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Disk) restoreWriteCacheFile(ctx context.Context, path string) error {
	oc, err := NewObjectCreator(d.log, d.volName, path)
	if err != nil {
		return err
	}

	d.curOC = oc

	return nil
}

func (d *Disk) Close(ctx context.Context) error {
	err := d.CloseSegment(ctx)
	if err != nil {
		return err
	}

	err = d.saveLBAMap(ctx)
	if err != nil {
		d.log.Error("error saving LBA cached map", "error", err)
	}

	d.openSegments.Purge()

	d.extentCache.Close()

	return err
}

func (d *Disk) saveLBAMap(ctx context.Context) error {
	f, err := os.Create(filepath.Join(d.path, "head.map"))
	if err != nil {
		return err
	}

	defer f.Close()

	return saveLBAMap(d.lba2pba, f)
}

func (d *Disk) loadLBAMap(ctx context.Context) (bool, error) {
	f, err := os.Open(filepath.Join(d.path, "head.map"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}

		return false, err
	}

	defer f.Close()

	d.log.Trace("reloading lba map from head.map")

	m, err := processLBAMap(d.log, f)
	if err != nil {
		return false, err
	}

	for i := m.m.Iterator(); i.Valid(); i.Next() {
		ro := i.Value()

		seg := d.segments[ro.Segment]
		if seg == nil {
			seg = &Segment{}
			d.segments[ro.Segment] = seg
		}

		seg.UsedBytes += uint64(ro.Size)
		seg.Used += uint64(ro.Range.Blocks)
	}

	d.lba2pba = m

	return true, nil
}

func saveLBAMap(m *ExtentMap, f io.Writer) error {
	for it := m.m.Iterator(); it.Valid(); it.Next() {
		cur := it.Value()

		hclog.L().Error("write to lba map", "extent", cur.Range, "flag", cur.Flag)

		err := binary.Write(f, binary.BigEndian, cur)
		if err != nil {
			return err
		}
	}

	return nil
}

func processLBAMap(log hclog.Logger, f io.Reader) (*ExtentMap, error) {
	m := NewExtentMap(log)

	for {
		var (
			pba RangedOPBA
		)

		err := binary.Read(f, binary.BigEndian, &pba)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		log.Trace("read from lba map", "extent", pba.Range, "flag", pba.Flag)

		m.m.Set(pba.Range.LBA, &pba)
	}

	return m, nil
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

func (d *Disk) pickSegmentToGC(ctx context.Context, min float64) (SegmentId, bool, error) {
	d.segmentsMu.Lock()
	defer d.segmentsMu.Unlock()

	var (
		smallestId    SegmentId
		smallestStats *Segment
	)

	for segId, stats := range d.segments {
		if stats.deleted {
			continue
		}

		d := stats.Density()
		if d > min {
			continue
		}

		if smallestStats == nil || d < smallestStats.Density() {
			smallestStats = stats
			smallestId = segId
		}
	}

	if smallestStats == nil {
		return SegmentId{}, false, nil
	}

	return smallestId, true, nil
}

func (d *Disk) GCOnce(ctx context.Context) (SegmentId, error) {
	segId, ci, err := d.StartGC(ctx, 1.0)
	if err != nil {
		return SegmentId{}, err
	}

	if ci == nil {
		return segId, nil
	}

	d.log.Trace("copying live data from object", "seg", segId)

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

func (d *Disk) cleanupDeletedSegments(ctx context.Context) error {
	d.segmentsMu.Lock()
	defer d.segmentsMu.Unlock()

	var toDelete []SegmentId

	for i, s := range d.segments {
		if s.deleted {
			toDelete = append(toDelete, i)
		}
	}

	for _, i := range toDelete {
		delete(d.segments, i)
		d.log.Debug("removing segment from volume", "volume", d.volName, "segment", i)
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

func (d *Disk) StartGC(ctx context.Context, min float64) (SegmentId, *CopyIterator, error) {
	toGC, ok, err := d.pickSegmentToGC(ctx, min)
	if !ok {
		return SegmentId{}, nil, nil
	}

	if err != nil {
		return SegmentId{}, nil, err
	}

	d.log.Trace("copying live data from object", "seg", toGC)

	ci, err := d.CopyIterator(ctx, toGC)
	if err != nil {
		return SegmentId{}, nil, err
	}

	return toGC, ci, nil
}

type CopyIterator struct {
	seg SegmentId
	d   *Disk
	or  ObjectReader
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

		opbas, err := c.d.computeOPBAs(extent)
		if err != nil {
			return false, err
		}

		for _, opba := range opbas {
			if opba.Segment != c.seg {
				c.d.log.Trace("discarding segment", "extent", extent, "target", opba.Full, "segment", opba.Segment, "seg", c.seg)
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
				sz := binary.BigEndian.Uint32(view)

				uncomp := buffers.Get(int(sz))
				defer buffers.Return(uncomp)

				n, err := lz4decode.UncompressBlock(view[4:], uncomp, nil)
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
	c.d.segmentsMu.Lock()
	defer c.d.segmentsMu.Unlock()

	c.d.segments[c.seg].deleted = true

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

func (d *Disk) Size() int64 {
	return d.size
}
