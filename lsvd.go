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
	BlockData struct {
		blocks int
		data   []byte
	}

	SegmentId ulid.ULID
	LBA       uint64

	OPBA struct {
		Segment SegmentId
		Offset  uint32
		Size    uint32
	}

	RangedOPBA struct {
		Range Extent
		Full  Extent
		OPBA
	}

	SegmentHeader struct {
		CreatedAt uint64
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

func (e BlockData) Blocks() int {
	return e.blocks
}

func (e *BlockData) CopyTo(data []byte) error {
	copy(data, e.data)
	return nil
}

func (e BlockData) MapTo(lba LBA) RangeData {
	return RangeData{
		Extent:    Extent{lba, uint32(e.Blocks())},
		BlockData: e,
	}
}

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
			err     error
		)

		start := time.Now()
		for {
			entries, err = oc.Flush(ctx, d.sa, segId)
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

		d.lbaMu.Lock()
		for _, ent := range entries {
			if mode.Debug() {
				d.log.Trace("updating read map", "extent", ent.extent)
			}
			err = d.lba2pba.Update(ent.extent, ent.opba)
			if err != nil {
				d.log.Error("error updating read map", "error", err)
			}
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

		finDur := time.Since(start)

		d.log.Info("uploaded new object", "segment", segId, "flush-dur", flushDur, "map-dur", mapDur, "dur", finDur)

		d.log.Debug("finished background object flush")
	}()

	return done, nil
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

func NewBlockData(sz int) BlockData {
	return BlockData{
		blocks: sz,
		data:   make([]byte, BlockSize*sz),
	}
}

func BlockDataView(blk []byte) BlockData {
	cnt := len(blk) / BlockSize
	if cnt < 0 || len(blk)%BlockSize != 0 {
		panic("invalid block data size for extent")
	}

	return BlockData{
		blocks: cnt,
		data:   slices.Clone(blk),
	}
}

func BlockDataOverlay(blk []byte) (BlockData, error) {
	cnt := len(blk) / BlockSize
	if cnt < 0 || len(blk)%BlockSize != 0 {
		return BlockData{}, fmt.Errorf("invalid extent length, not block sized: %d", len(blk))
	}

	return BlockData{
		blocks: cnt,
		data:   blk,
	}, nil
}

func (e BlockData) BlockView(cnt int) []byte {
	return e.data[BlockSize*cnt : (BlockSize*cnt)+BlockSize]
}

func (e BlockData) SetBlock(blk int, data []byte) {
	if len(data) != BlockSize {
		panic("invalid data length, not block size")
	}

	copy(e.data[BlockSize*blk:], data)
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

func (d *Disk) ReadExtent(ctx context.Context, rng Extent) (BlockData, error) {
	start := time.Now()

	defer func() {
		blocksReadLatency.Observe(time.Since(start).Seconds())
	}()

	iops.Inc()

	data := NewRangeData(rng)

	d.log.Trace("attempting to fill request from write cache", "extent", rng)

	remaining, err := d.fillFromWriteCache(ctx, data)
	if err != nil {
		return BlockData{}, err
	}

	// Completely filled range from the write cache
	if len(remaining) == 0 {
		d.log.Trace("extent filled entirely from write cache")
		return data.BlockData, nil
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
			return BlockData{}, err
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
			return BlockData{}, err
		}
	}

	return data.BlockData, nil
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

	switch rawData[0] {
	case 0:
		rawData = rawData[1:]
	case 1:
		sz := binary.BigEndian.Uint32(rawData[1:])

		uncomp := buffers.Get(int(sz))
		defer buffers.Return(uncomp)

		n, err := lz4decode.UncompressBlock(rawData[5:], uncomp, nil)
		if err != nil {
			return err
		}

		if n != int(sz) {
			return fmt.Errorf("failed to uncompress correctly")
		}

		rawData = uncomp
	default:
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

	var cnt, dataBegin uint32

	err = binary.Read(br, binary.BigEndian, &cnt)
	if err != nil {
		return err
	}

	err = binary.Read(br, binary.BigEndian, &dataBegin)
	if err != nil {
		return err
	}

	for i := uint32(0); i < cnt; i++ {
		lba, err := binary.ReadUvarint(br)
		if err != nil {
			return err
		}

		count, err := binary.ReadUvarint(br)
		if err != nil {
			return err
		}

		_, err = br.ReadByte()
		if err != nil {
			return err
		}

		blkSize, err := binary.ReadUvarint(br)
		if err != nil {
			return err
		}

		blkOffset, err := binary.ReadUvarint(br)
		if err != nil {
			return err
		}

		err = d.lba2pba.Update(Extent{LBA: LBA(lba), Blocks: uint32(count)}, OPBA{
			Segment: seg,
			Offset:  dataBegin + uint32(blkOffset),
			Size:    uint32(blkSize),
		})
		if err != nil {
			return err
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

	d.lba2pba = m

	return true, nil
}

func saveLBAMap(m *ExtentMap, f io.Writer) error {
	for it := m.m.Iterator(); it.Valid(); it.Next() {
		cur := it.Value()

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

		m.m.Set(pba.Range.LBA, &pba)
	}

	return m, nil
}

func (d *Disk) pickSegmentToGC(segments []SegmentId) (SegmentId, bool) {
	if len(segments) == 0 {
		return SegmentId{}, false
	}

	return segments[0], true
}

func (d *Disk) GCOnce(ctx context.Context) (SegmentId, error) {
	segments, err := d.sa.ListSegments(ctx, d.volName)
	if err != nil {
		return SegmentId{}, nil
	}

	toGC, ok := d.pickSegmentToGC(segments)
	if !ok {
		return SegmentId{}, nil
	}

	d.log.Trace("copying live data from object", "seg", toGC)

	err = d.copyLive(ctx, toGC)
	if err != nil {
		return toGC, err
	}

	return toGC, nil
}

func (d *Disk) copyLive(ctx context.Context, seg SegmentId) error {
	f, err := d.sa.OpenSegment(ctx, seg)
	if err != nil {
		return errors.Wrapf(err, "opening local live object")
	}

	defer f.Close()

	br := bufio.NewReader(ToReader(f))

	var cnt, dataBegin uint32

	err = binary.Read(br, binary.BigEndian, &cnt)
	if err != nil {
		return err
	}

	err = binary.Read(br, binary.BigEndian, &dataBegin)
	if err != nil {
		return err
	}

	view := make([]byte, BlockSize*10)

loop:
	for i := uint32(0); i < cnt; i++ {
		lba, err := binary.ReadUvarint(br)
		if err != nil {
			return err
		}

		blocks, err := binary.ReadUvarint(br)
		if err != nil {
			return err
		}

		_, err = br.ReadByte()
		if err != nil {
			return err
		}

		blkSize, err := binary.ReadUvarint(br)
		if err != nil {
			return err
		}

		if len(view) < int(blkSize) {
			view = make([]byte, blkSize)
		}

		blkOffset, err := binary.ReadUvarint(br)
		if err != nil {
			return err
		}

		extent := Extent{LBA: LBA(lba), Blocks: uint32(blocks)}

		d.log.Trace("considering for copy", "extent", extent, "size", blkSize, "offset", blkOffset)

		opbas, err := d.computeOPBAs(extent)
		if err != nil {
			return err
		}

		for _, opba := range opbas {
			if opba.Segment != seg {
				d.log.Trace("discarding segment", "extent", extent, "target", opba.Full, "segment", opba.Segment, "seg", seg)
				continue loop
			}
		}

		view := view[:blkSize]

		_, err = f.ReadAt(view, int64(dataBegin+uint32(blkOffset)))
		if err != nil {
			return err
		}

		if view[0] == 1 {
			sz := binary.BigEndian.Uint32(view[1:])

			uncomp := buffers.Get(int(sz))
			defer buffers.Return(uncomp)

			n, err := lz4decode.UncompressBlock(view[5:], uncomp, nil)
			if err != nil {
				return err
			}

			if n != int(sz) {
				return fmt.Errorf("failed to uncompress correctly")
			}

			view = uncomp
		} else {
			view = view[1:]
		}

		d.log.Trace("copying extent", "extent", extent, "view", len(view))

		err = d.WriteExtent(ctx, BlockDataView(view).MapTo(LBA(lba)))
		if err != nil {
			return err
		}
	}

	d.log.Debug("removing segment from volume", "volume", d.volName, "segment", seg)
	err = d.sa.RemoveSegmentFromVolume(ctx, d.volName, seg)
	if err != nil {
		return err
	}

	return d.removeSegmentIfPossible(ctx, seg)
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

	// ok, no volume has it, we can remove it.
	return d.sa.RemoveSegment(ctx, seg)
}

func (d *Disk) Size() int64 {
	return d.size
}
