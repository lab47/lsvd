package lsvd

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/lab47/lsvd/pkg/list"

	"github.com/edsrzf/mmap-go"
	"github.com/hashicorp/go-hclog"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/igrmk/treemap/v2"
	"github.com/oklog/ulid/v2"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
)

type (
	BlockData struct {
		blocks int
		data   []byte
	}

	Extent struct {
		LBA    LBA
		Blocks uint32
	}

	SegmentId ulid.ULID
)

func (s SegmentId) String() string {
	return ulid.ULID(s).String()
}

const BlockSize = 4 * 1024

func (e BlockData) Blocks() int {
	return e.blocks
}

func (e *BlockData) CopyTo(data []byte) error {
	copy(data, e.data)
	return nil
}

type segmentInfo struct {
	f ObjectReader
	m mmap.MMap
}

type objPBA struct {
	PBA

	Flags byte
	Size  uint32
}

const flushThreshHold = 15 * 1024 * 1024

type writeCache struct {
	f *os.File
	m map[LBA]uint32

	next *writeCache
}

type OPBA struct {
	Segment SegmentId
	Offset  uint32
	Size    uint32
}

type Disk struct {
	SeqGen func() ulid.ULID
	log    hclog.Logger
	path   string

	size    int64
	volName string

	prevCacheMu sync.Mutex
	prevCache   list.List[*writeCache]

	curSeq     ulid.ULID
	writeCache *os.File
	wcOffsets  map[LBA]uint32
	curOffset  uint32
	wcBufWrite *bufio.Writer

	lbaMu   sync.Mutex
	ext2pba *treemap.TreeMap[Extent, OPBA]

	readCache    *DiskCache
	openSegments *lru.Cache[SegmentId, ObjectReader]

	sa    SegmentAccess
	curOC *ObjectCreator
}

const diskCacheSize = 4096 // 16MB read cache

func RoundToBlockSize(sz int64) int64 {
	diff := sz % BlockSize
	if diff == 0 {
		return sz
	}

	return sz - diff
}

func (d *Disk) resolve(ext BlockData) []BlockData {
	return nil
}

func NewDisk(ctx context.Context, log hclog.Logger, path string, options ...Option) (*Disk, error) {
	dc, err := NewDiskCache(filepath.Join(path, "readcache"), diskCacheSize)
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

	em := treemap.NewWithKeyCompare[Extent, OPBA](CompareExtent)

	d := &Disk{
		log:       log,
		path:      path,
		size:      sz,
		ext2pba:   em,
		readCache: dc,
		wcOffsets: make(map[LBA]uint32),
		sa:        o.sa,
		volName:   o.volName,
		curOC:     &ObjectCreator{log: log, volName: o.volName},
	}

	openSegments, err := lru.NewWithEvict[SegmentId, ObjectReader](
		256, func(key SegmentId, value ObjectReader) {
			openSegments.Dec()
			value.Close()
		})
	if err != nil {
		return nil, err
	}

	d.openSegments = openSegments

	goodMap, err := d.loadLBAMap(ctx)
	if err != nil {
		return nil, err
	}

	if goodMap {
		log.Info("reusing serialized LBA map", "blocks", d.ext2pba.Len())
	} else {
		err = d.rebuildFromObjects(ctx)
		if err != nil {
			return nil, err
		}
	}

	err = d.restoreWriteCache(ctx)
	if err != nil {
		return nil, err
	}

	return d, nil
}

func CompareExtent(a, b Extent) bool {
	as, af := a.Range()
	bs, _ := b.Range()

	// if a ends before be begins, then yeah, it's less.
	// otherwise, no.
	return as <= bs && af < bs
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

type SegmentHeader struct {
	CreatedAt uint64
}

var (
	headerSize = binary.Size(SegmentHeader{})
	empty      SegmentId
)

func (d *Disk) nextLog() error {
	seq, err := d.nextSeq()
	if err != nil {
		return err
	}

	d.curSeq = seq

	f, err := os.Create(filepath.Join(d.path, "writecache."+seq.String()))
	if err != nil {
		return err
	}

	d.writeCache = f
	d.wcBufWrite = bufio.NewWriter(f)

	ts := uint64(time.Now().Unix())

	err = binary.Write(f, binary.BigEndian, SegmentHeader{
		CreatedAt: ts,
	})
	if err != nil {
		return err
	}

	d.curOffset = uint32(headerSize)

	return nil
}

func (d *Disk) closeSegmentAsync(ctx context.Context) (chan struct{}, error) {
	if d.writeCache == nil {
		return nil, nil
	}

	segId := SegmentId(d.curSeq)

	oc := d.curOC

	d.curOC = &ObjectCreator{log: d.log, volName: d.volName}

	d.writeCache.Sync()

	wc := &writeCache{
		f: d.writeCache,
		m: d.wcOffsets,
	}

	d.prevCacheMu.Lock()
	elem := d.prevCache.PushFront(wc)
	d.prevCacheMu.Unlock()

	d.curOffset = 0
	d.wcOffsets = make(map[LBA]uint32)
	d.writeCache = nil

	done := make(chan struct{})

	go func() {
		defer close(done)
		defer segmentsWritten.Inc()

		var (
			entries []objectEntry
			err     error
		)

		for {
			entries, err = oc.Flush(ctx, d.sa, segId)
			if err != nil {
				d.log.Error("error flushing data to object, retrying", "error", err)
				time.Sleep(5 * time.Second)
				continue
			}

			break
		}

		d.log.Debug("object published, resetting write cache")

		d.lbaMu.Lock()
		for _, ent := range entries {
			d.ext2pba.Set(ent.extent, ent.opba)
		}
		d.lbaMu.Unlock()

		d.prevCacheMu.Lock()
		found := d.prevCache.Remove(elem) != nil
		d.prevCacheMu.Unlock()

		if !found {
			d.log.Warn("unable to find cache in prev caches when removing")
		}

		name := wc.f.Name()

		wc.f.Close()

		defer os.Remove(name)

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

func NewExtent(sz int) BlockData {
	return BlockData{
		blocks: sz,
		data:   make([]byte, BlockSize*sz),
	}
}

func ExtentView(blk []byte) BlockData {
	cnt := len(blk) / BlockSize
	if cnt < 0 || len(blk)%BlockSize != 0 {
		panic("invalid block data size for extent")
	}

	return BlockData{
		blocks: cnt,
		data:   slices.Clone(blk),
	}
}

func ExtentOverlay(blk []byte) (BlockData, error) {
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

type LBA uint64

const perBlockHeader = 16

var emptyBlock = make([]byte, BlockSize)

func (d *Disk) checkPrevCaches(lba LBA, view []byte) (bool, error) {
	d.prevCacheMu.Lock()
	defer d.prevCacheMu.Unlock()

	for e := d.prevCache.Front(); e != nil; e = e.Next() {
		wc := e.Value

		if pba, ok := wc.m[lba]; ok {
			if pba == math.MaxUint32 {
				clear(view)
			} else {
				n, err := wc.f.ReadAt(view, int64(pba+perBlockHeader))
				if err != nil {
					d.log.Error("error reading data from write cache", "error", err, "pba", pba)
					return false, errors.Wrapf(err, "attempting to read from active (%d)", pba)
				}

				d.log.Trace("reading block from write cache", "block", lba, "pba", pba, "size", n)
			}
			return true, nil
		}
	}

	return false, nil
}

type Cover int

const (
	CoverCompletely Cover = iota
	CoverPartly
	CoverNone
)

func (e Extent) Range() (LBA, LBA) {
	return e.LBA, e.LBA + LBA(e.Blocks) - 1
}

func (e Extent) Cover(y Extent) Cover {
	es, ef := e.Range()
	ys, yf := y.Range()

	if es <= ys && ef >= ys {
		// es    ys     yf    ef
		if ef >= yf {
			// e is a superange of y
			return CoverCompletely
		}

		// es    ys     ef    yf
		// y begins but does not end within e
		return CoverPartly
	}

	if ys < es && yf <= es {
		// ys   es    ef   yf OR ys   es    yf   fs
		// e is located within y completely
		return CoverPartly
	}

	return CoverNone
}

func (d *Disk) computeOPBAs(rng Extent) []OPBA {
	d.lbaMu.Lock()
	defer d.lbaMu.Unlock()

	var ret []OPBA

loop:
	for i := d.ext2pba.LowerBound(rng); i.Valid(); i.Next() {
		switch i.Key().Cover(rng) {
		case CoverCompletely:
			ret = append(ret, i.Value())
			break loop
		case CoverNone:
			break loop
		case CoverPartly:
			ret = append(ret, i.Value())
		}
	}

	return ret
}

func (d *Disk) nextOPBA(rng Extent) (Extent, OPBA, bool) {
	d.lbaMu.Lock()
	defer d.lbaMu.Unlock()

	i := d.ext2pba.LowerBound(rng)
	if !i.Valid() {
		d.log.Trace("no lower bound for extent", "extent", rng)
		return Extent{}, OPBA{}, false
	}

	d.log.Trace("lower bound for extent",
		"extent", rng, "bound", i.Key(), "cover", i.Key().Cover(rng))
	switch i.Key().Cover(rng) {
	case CoverCompletely, CoverPartly:
		return i.Key(), i.Value(), true
	default:
		return Extent{}, OPBA{}, false
	}
}

func (d *Disk) fillFromWriteCache(ctx context.Context, rng Extent, data BlockData) ([]Extent, error) {

	var (
		inHole   bool
		nextHole Extent
		holes    []Extent
	)

	for i := 0; i < int(rng.Blocks); i++ {
		lba := rng.LBA + LBA(i)

		view := data.BlockView(i)

		if pba, ok := d.wcOffsets[lba]; ok {
			if inHole {
				holes = append(holes, nextHole)
				inHole = false
			}

			if pba == math.MaxUint32 {
				clear(view)
			} else {
				n, err := d.writeCache.ReadAt(view, int64(pba+perBlockHeader))
				if err != nil {
					d.log.Error("error reading data from write cache", "error", err, "pba", pba)
					return nil, errors.Wrapf(err, "attempting to read from active (%d)", pba)
				}

				d.log.Trace("reading block from write cache", "block", lba, "pba", pba, "size", n)
			}

			continue
		}

		found, err := d.checkPrevCaches(lba, view)
		if err != nil {
			return nil, err
		}

		if found {
			if inHole {
				holes = append(holes, nextHole)
				inHole = false
			}
			continue
		}

		if inHole {
			nextHole.Blocks++
		} else {
			inHole = true
			nextHole = Extent{LBA: lba, Blocks: 1}
		}
	}

	// Catch a hole that runs off the end.
	if inHole {
		holes = append(holes, nextHole)
	}

	return holes, nil
}

type readRequest struct {
	obpa    OPBA
	rng     Extent
	extents []Extent
}

func (d *Disk) ReadExtent(ctx context.Context, rng Extent) (BlockData, error) {
	start := time.Now()

	defer func() {
		blocksReadLatency.Observe(time.Since(start).Seconds())
	}()

	iops.Inc()

	data := NewExtent(int(rng.Blocks))

	d.log.Trace("attempting to fill request from write cache", "blocks", rng.Blocks)

	remaining, err := d.fillFromWriteCache(ctx, rng, data)
	if err != nil {
		return BlockData{}, err
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
		last  OPBA
	)

	for _, h := range remaining {
		orng, opba, ok := d.nextOPBA(h)
		if !ok {
			// nothing for range, and since the data is pre-zero'd, we
			// don't need to clear anything here.
		} else {
			if opba.Size == 0 {
				// it's empty! cool cool, we don't need to fill the hole
				continue
			}

			if windowRng, ok := orng.Constrain(rng); ok {
				d.log.Trace("calculate needs of subrange", "window", windowRng, "rng", rng)

				view := data.data[(windowRng.LBA-rng.LBA)*BlockSize:]

				var miss bool

				// Next, see if we can fill the extent from the read cache.
				// If any block misses, we bail because we're going to read the
				// whole range anyway now.
				for i := 0; i < int(orng.Blocks); i++ {
					lba := windowRng.LBA + LBA(i)

					err := d.readCache.ReadBlock(lba, view[:BlockSize])
					if err == ErrCacheMiss {
						d.log.Trace("miss read cache", "lba", lba)
						miss = true
						break
					}
				}

				if !miss {
					d.log.Trace("served read extent from read cache", "extent", orng)
					continue
				}
			}

			// Because the holes can be smaller than the read ranges,
			// 2 or more holes in sequence might be served by the same
			// object range.
			if last == opba {
				opbas[len(opbas)-1].extents = append(opbas[len(opbas)-1].extents, h)
			} else {
				r := &readRequest{
					obpa:    opba,
					rng:     orng,
					extents: []Extent{h},
				}

				opbas = append(opbas, r)
				last = opba
			}
		}
	}

	if d.log.IsTrace() {
		d.log.Trace("opbas needed", "total", len(opbas))

		for _, o := range opbas {
			d.log.Trace("opba needed",
				"segment", o.obpa.Segment, "offset", o.obpa.Offset, "size", o.obpa.Size)
		}
	}

	for _, o := range opbas {
		err := d.readOPBA(ctx, o.rng, o.obpa, o.extents, rng, data)
		if err != nil {
			return BlockData{}, err
		}
	}

	return data, nil
}

func ExtentFrom(a, b LBA) Extent {
	return Extent{LBA: a, Blocks: uint32(b - a)}
}

func (a Extent) Constrain(b Extent) (Extent, bool) {
	as, af := a.Range()
	bs, bf := b.Range()

	if as <= bs {
		// as af bs bf
		if af < bf {
			return Extent{}, false
		}

		// as bs bf af
		if af >= bf {
			return b, true
		}

		// as bs af bf
		return ExtentFrom(bs, af), true
	}

	return Extent{}, false
}

func (d *Disk) readOPBA(
	ctx context.Context,
	full Extent, addr OPBA,
	rngs []Extent,
	dataRange Extent, data BlockData,
) error {
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

	rawData := make([]byte, addr.Size)

	n, err := ci.ReadAt(rawData, int64(addr.Offset))
	if err != nil {
		return nil
	}

	if n != len(rawData) {
		return fmt.Errorf("short read detected")
	}

	switch rawData[0] {
	case 0:
		rawData = rawData[1:]
	case 1:
		sz := binary.BigEndian.Uint32(rawData[1:])

		uncomp := make([]byte, sz)

		n, err := lz4.UncompressBlock(rawData[5:], uncomp)
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

	// the bytes at the beginning of data are for LBA dataBegin.LBA.
	// the bytes at the beginning of rawData are for LBA full.LBA.
	// we want to compute the 2 byte ranges:
	//   1. the byte range for rng within data
	//   2. the byte range for rng within rawData
	// Then we copy the bytes from 2 to 1.
	for _, rng := range rngs {
		d.log.Trace("fill read range", "hole", rng, "obj", full, "data", dataRange)
		destOff := (rng.LBA - dataRange.LBA) * BlockSize
		dest := data.data[destOff : destOff+LBA(rng.Blocks*BlockSize)]

		srcOff := (rng.LBA - full.LBA) * BlockSize
		src := rawData[srcOff : srcOff+LBA(rng.Blocks*BlockSize)]

		copy(dest, src)
	}

	for i := 0; i < int(full.Blocks); i++ {
		lba := full.LBA + LBA(i)

		d.log.Trace("adding data to read cache", "lba", lba)
		err = d.readCache.WriteBlock(lba, rawData[:BlockSize])
		if err != nil {
			d.log.Error("error writing data to read cache", "error", err)
		}
		rawData = rawData[BlockSize:]
	}

	return nil
}

func (e Extent) String() string {
	return fmt.Sprintf("%d:%d", e.LBA, e.Blocks)
}

type PBA struct {
	Segment SegmentId
	Offset  uint32
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

func (d *Disk) blockIsEmpty(lba LBA) bool {
	// We skip any blocks that are unused currently since we'll
	// return them as zero when asked later.
	if pba, tracking := d.wcOffsets[lba]; tracking {
		// If we're tracking it and it has a valid offset in the write cache,
		// then it's not empty.
		if pba != math.MaxUint32 {
			return false
		}
	}

	i := d.ext2pba.LowerBound(Extent{LBA: lba, Blocks: 1})
	if i.Valid() {
		return i.Value().Size == 0
	}

	return true
}

func (d *Disk) ZeroBlocks(ctx context.Context, firstBlock LBA, numBlocks int64) error {
	iops.Inc()
	blocksWritten.Add(float64(numBlocks))

	if d.writeCache == nil {
		err := d.nextLog()
		if err != nil {
			return err
		}
	}

	dw := d.wcBufWrite
	defer d.wcBufWrite.Flush()

	for i := int64(0); i < numBlocks; i++ {
		lba := firstBlock + LBA(i)

		// We skip any blocks that are currently empty since we don't need
		// to re-zero them.
		if d.blockIsEmpty(lba) {
			continue
		}

		err := binary.Write(dw, binary.BigEndian, uint64(math.MaxUint64))
		if err != nil {
			return err
		}

		err = binary.Write(dw, binary.BigEndian, uint64(lba))
		if err != nil {
			return err
		}

		d.curOffset += perBlockHeader

		d.wcOffsets[lba] = math.MaxUint32

		// Zero them one at a time in the object so that we take into account
		// the above tracking logic to omit zeroing blocks that are already zero.
		err = d.curOC.ZeroBlocks(lba, 1)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Disk) WriteExtent(ctx context.Context, firstBlock LBA, data BlockData) error {
	start := time.Now()

	defer func() {
		blocksWriteLatency.Observe(time.Since(start).Seconds())
	}()

	iops.Inc()

	if d.writeCache == nil {
		err := d.nextLog()
		if err != nil {
			return err
		}
	}

	dw := d.wcBufWrite

	numBlocks := data.Blocks()

	blocksWritten.Add(float64(numBlocks))

	for i := 0; i < numBlocks; i++ {
		lba := firstBlock + LBA(i)

		view := data.BlockView(i)

		if emptyBytes(view) {
			// We skip any blocks that are unused currently since we'll
			// return them as zero when asked later.
			if d.blockIsEmpty(lba) {
				continue
			}

			err := binary.Write(dw, binary.BigEndian, uint64(math.MaxUint64))
			if err != nil {
				return err
			}

			err = binary.Write(dw, binary.BigEndian, uint64(lba))
			if err != nil {
				return err
			}

			d.curOffset += perBlockHeader

			d.wcOffsets[lba] = math.MaxUint32

			continue
		}

		crc := crc64.Update(crcLBA(0, lba), crcTable, view)

		err := binary.Write(dw, binary.BigEndian, crc)
		if err != nil {
			return err
		}

		err = binary.Write(dw, binary.BigEndian, uint64(lba))
		if err != nil {
			return err
		}

		n, err := dw.Write(view)
		if err != nil {
			return err
		}

		if n != BlockSize {
			return fmt.Errorf("invalid write size (%d != %d)", n, BlockSize)
		}

		d.log.Trace("wrote block to write cache", "block", lba, "pba", d.curOffset)
		d.wcOffsets[lba] = d.curOffset
		d.curOffset += uint32(perBlockHeader + BlockSize)
	}

	err := d.wcBufWrite.Flush()
	if err != nil {
		d.log.Error("error flushing write cache buffer", "error", err)
	}

	err = d.curOC.WriteExtent(firstBlock, data)
	if err != nil {
		d.log.Error("error write extents to object creator", "error", err)
		return err
	}

	if d.curOffset >= flushThreshHold {
		d.log.Debug("flushing object", "write-cache-size", d.curOffset)
		_, err = d.closeSegmentAsync(ctx)
		return err
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

func segmentFromName(name string) (SegmentId, bool) {
	_, intPart, ok := strings.Cut(name, ".")
	if !ok {
		return empty, false
	}

	data, err := ulid.Parse(intPart)
	if err != nil {
		return empty, false
	}

	return SegmentId(data), true
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

		d.ext2pba.Set(Extent{LBA: LBA(lba), Blocks: uint32(count)}, OPBA{
			Segment: seg,
			Offset:  dataBegin + uint32(blkOffset),
			Size:    uint32(blkSize),
		})
	}

	return nil
}

func (d *Disk) SyncWriteCache() error {
	iops.Inc()

	if d.writeCache != nil {
		return d.writeCache.Sync()
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
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return err
	}

	if d.writeCache != nil {
		d.writeCache.Close()
	}

	d.writeCache = f
	d.wcBufWrite = bufio.NewWriter(f)

	var hdr SegmentHeader

	err = binary.Read(f, binary.BigEndian, &hdr)
	if err != nil {
		return err
	}

	d.log.Debug("found orphan active log", "created-at", hdr.CreatedAt)

	offset := uint32(headerSize)

	view := make([]byte, BlockSize)

	var numBlocks, zeroBlocks int

	for {
		var lba, crc uint64

		err = binary.Read(f, binary.BigEndian, &crc)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		err = binary.Read(f, binary.BigEndian, &lba)
		if err != nil {
			return err
		}

		numBlocks++

		// This indicates that it's an empty block
		if crc == math.MaxUint64 {
			zeroBlocks++
			offset += perBlockHeader
			d.wcOffsets[LBA(lba)] = math.MaxUint32

			continue
		}

		n, err := io.ReadFull(f, view)
		if err != nil {
			return err
		}

		if n != BlockSize {
			return fmt.Errorf("block incorrect size in log.active (%d)", n)
		}

		if crc != crc64.Update(crcLBA(0, LBA(lba)), crcTable, view) {
			d.log.Warn("detected mis-match crc reloading log", "offset", offset)
			break
		}

		d.log.Trace("restoring mapping", "lba", lba, "offset", offset)

		d.wcOffsets[LBA(lba)] = offset

		offset += uint32(perBlockHeader + BlockSize)
	}

	d.log.Info("restored data from write cache", "blocks", numBlocks, "zero-blocks", zeroBlocks)

	if d.curOffset >= flushThreshHold {
		_, err = d.closeSegmentAsync(ctx)
		return err
	}

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

	return err
}

func (d *Disk) saveLBAMap(ctx context.Context) error {
	f, err := d.sa.WriteMetadata(ctx, d.volName, "head.map")
	if err != nil {
		return err
	}

	defer f.Close()

	return saveLBAMap(d.ext2pba, f)
}

func (d *Disk) loadLBAMap(ctx context.Context) (bool, error) {
	f, err := d.sa.ReadMetadata(ctx, d.volName, "head.map")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}

		return false, err
	}

	defer f.Close()

	d.log.Trace("reloading lba map from head.map")

	m, err := processLBAMap(f)
	if err != nil {
		return false, err
	}

	d.ext2pba = m

	return true, nil
}

func saveLBAMap(m *treemap.TreeMap[Extent, OPBA], f io.Writer) error {
	for it := m.Iterator(); it.Valid(); it.Next() {
		k := it.Key()

		err := binary.Write(f, binary.BigEndian, uint64(k.LBA))
		if err != nil {
			return err
		}

		err = binary.Write(f, binary.BigEndian, uint32(k.Blocks))
		if err != nil {
			return err
		}

		err = binary.Write(f, binary.BigEndian, it.Value())
		if err != nil {
			return err
		}
	}

	return nil
}

func processLBAMap(f io.Reader) (*treemap.TreeMap[Extent, OPBA], error) {
	m := treemap.NewWithKeyCompare[Extent, OPBA](CompareExtent)

	for {
		var (
			lba    uint64
			blocks uint32
			pba    OPBA
		)

		err := binary.Read(f, binary.BigEndian, &lba)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		err = binary.Read(f, binary.BigEndian, &blocks)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		err = binary.Read(f, binary.BigEndian, &pba)
		if err != nil {
			return nil, err
		}

		m.Set(Extent{LBA: LBA(lba), Blocks: blocks}, pba)
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

		opba, ok := d.ext2pba.Get(extent)
		//pba, ok := d.lba2obj.Get(LBA(lba))
		if ok && opba.Segment != seg {
			continue
		}

		view := view[:blkSize]

		_, err = f.ReadAt(view, int64(dataBegin+uint32(blkOffset)))
		if err != nil {
			return err
		}

		if view[0] == 1 {
			sz := binary.BigEndian.Uint32(view[1:])

			uncomp := make([]byte, sz)

			n, err := lz4.UncompressBlock(view[5:], uncomp)
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

		err = d.WriteExtent(ctx, LBA(lba), ExtentView(view))
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
