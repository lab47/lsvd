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

	"github.com/edsrzf/mmap-go"
	"github.com/hashicorp/go-hclog"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/igrmk/treemap/v2"
	"github.com/oklog/ulid/v2"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
)

type (
	Extent struct {
		blocks int
		data   []byte
	}

	SegmentId ulid.ULID
)

func (s SegmentId) String() string {
	return ulid.ULID(s).String()
}

const BlockSize = 4 * 1024

func (e Extent) Blocks() int {
	return e.blocks
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

const flushThreshHold = 5 * 1024 * 1024

type Disk struct {
	SeqGen func() ulid.ULID
	log    hclog.Logger
	path   string

	lastWC      *os.File
	lastOffsets map[LBA]uint32

	curSeq     ulid.ULID
	writeCache *os.File
	wcOffsets  map[LBA]uint32
	curOffset  uint32
	wcBufWrite *bufio.Writer

	lbaMu        sync.RWMutex
	lba2obj      *treemap.TreeMap[LBA, objPBA]
	readCache    *DiskCache
	openSegments *lru.Cache[SegmentId, ObjectReader]

	sa    SegmentAccess
	curOC *ObjectCreator
}

const diskCacheSize = 4096 // 16MB read cache

func NewDisk(ctx context.Context, log hclog.Logger, path string, options ...Option) (*Disk, error) {
	dc, err := NewDiskCache(filepath.Join(path, "readcache"), diskCacheSize)
	if err != nil {
		return nil, err
	}

	var o opts

	for _, opt := range options {
		opt(&o)
	}

	if o.sa == nil {
		o.sa = &LocalFileAccess{Dir: path}
	}

	d := &Disk{
		log:       log,
		path:      path,
		lba2obj:   treemap.New[LBA, objPBA](),
		readCache: dc,
		wcOffsets: make(map[LBA]uint32),
		sa:        o.sa,
		curOC:     &ObjectCreator{log: log},
	}

	openSegments, err := lru.NewWithEvict[SegmentId, ObjectReader](
		256, func(key SegmentId, value ObjectReader) {
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
		log.Info("reusing serialized LBA map", "blocks", d.lba2obj.Len())
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

func (d *Disk) CloseSegmentAsync(ctx context.Context) (chan struct{}, error) {
	d.lbaMu.Lock()
	defer d.lbaMu.Unlock()

	if d.writeCache == nil {
		return nil, nil
	}

	segId := SegmentId(d.curSeq)

	oc := d.curOC

	d.curOC = &ObjectCreator{log: d.log}

	d.lastWC = d.writeCache
	d.lastOffsets = d.wcOffsets

	d.curOffset = 0
	d.wcOffsets = make(map[LBA]uint32)
	d.writeCache = nil

	done := make(chan struct{})

	go func() {
		defer close(done)

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
		defer d.lbaMu.Unlock()

		for _, ent := range entries {
			d.lba2obj.Set(ent.lba, ent.pba)
		}

		d.lastWC.Close()

		defer os.Remove(d.lastWC.Name())

		d.lastWC = nil
		d.lastOffsets = nil

		d.log.Debug("finished background object flush")
	}()

	return done, nil
}

func (d *Disk) CloseSegment(ctx context.Context) error {
	ch, err := d.CloseSegmentAsync(ctx)
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

func NewExtent(sz int) Extent {
	return Extent{
		blocks: sz,
		data:   make([]byte, BlockSize*sz),
	}
}

func ExtentView(blk []byte) Extent {
	cnt := len(blk) / BlockSize
	if cnt < 0 || len(blk)%BlockSize != 0 {
		panic("invalid block data size for extent")
	}

	return Extent{
		blocks: cnt,
		data:   slices.Clone(blk),
	}
}

func ExtentOverlay(blk []byte) (Extent, error) {
	cnt := len(blk) / BlockSize
	if cnt < 0 || len(blk)%BlockSize != 0 {
		return Extent{}, fmt.Errorf("invalid extent length, not block sized")
	}

	return Extent{
		blocks: cnt,
		data:   blk,
	}, nil
}

func (e Extent) BlockView(cnt int) []byte {
	return e.data[BlockSize*cnt : (BlockSize*cnt)+BlockSize]
}

func (e Extent) SetBlock(blk int, data []byte) {
	if len(data) != BlockSize {
		panic("invalid data length, not block size")
	}

	copy(e.data[BlockSize*blk:], data)
}

type LBA uint64

const perBlockHeader = 16

var emptyBlock = make([]byte, BlockSize)

func (d *Disk) readBlock(ctx context.Context, lba LBA, view []byte) error {
	if pba, ok := d.wcOffsets[lba]; ok {
		if pba == math.MaxUint32 {
			clear(view)
		} else {
			n, err := d.writeCache.ReadAt(view, int64(pba+perBlockHeader))
			if err != nil {
				d.log.Error("error reading data from write cache", "error", err, "pba", pba)
				return errors.Wrapf(err, "attempting to read from active (%d)", pba)
			}

			d.log.Trace("reading block from write cache", "block", lba, "pba", pba, "size", n)
		}
		return nil
	}

	if d.lastWC != nil {
		if pba, ok := d.lastOffsets[lba]; ok {
			if pba == math.MaxUint32 {
				clear(view)
			} else {
				n, err := d.lastWC.ReadAt(view, int64(pba+perBlockHeader))
				if err != nil {
					d.log.Error("error reading data from write cache", "error", err, "pba", pba)
					return errors.Wrapf(err, "attempting to read from active (%d)", pba)
				}

				d.log.Trace("reading block from write cache", "block", lba, "pba", pba, "size", n)
			}
			return nil
		}
	}

	if err := d.readCache.ReadBlock(lba, view); err == nil {
		// got it!
		d.log.Trace("found block in read cache", "lba", lba)
		return nil
	}

	if pba, ok := d.lba2obj.Get(lba); ok {
		return d.readPBA(ctx, lba, pba, view)
	}

	clear(view)

	return nil
}

func (d *Disk) ReadExtent(ctx context.Context, firstBlock LBA, data Extent) error {
	d.lbaMu.RLock()
	defer d.lbaMu.RUnlock()

	numBlocks := data.Blocks()

	for i := 0; i < numBlocks; i++ {
		lba := firstBlock + LBA(i)

		view := data.BlockView(i)

		err := d.readBlock(ctx, lba, view)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Disk) readPBA(ctx context.Context, lba LBA, addr objPBA, data []byte) error {
	ci, ok := d.openSegments.Get(addr.Segment)
	if !ok {
		lf, err := d.sa.OpenSegment(ctx, addr.Segment)
		if err != nil {
			return err
		}

		ci = lf

		d.openSegments.Add(addr.Segment, ci)
	}

	switch addr.Flags {
	case 0:
		_, err := ci.ReadAt(data, int64(addr.Offset))
		if err != nil {
			return err
		}
	case 1:
		_, err := ci.ReadAtCompressed(data, int64(addr.Offset), int64(addr.Size))
		if err != nil {
			return err
		}
	case 2:
		clear(data[:BlockSize])
	default:
		return fmt.Errorf("unknown flag value: %x", addr.Flags)
	}

	err := d.readCache.WriteBlock(lba, data)
	if err != nil {
		d.log.Error("error populating read cache from object", "error", err, "lba", lba)
	}

	return nil
}

type PBA struct {
	Segment SegmentId
	Offset  uint32
}

func (d *Disk) cacheTranslate(addr LBA) (PBA, bool) {
	if offset, ok := d.wcOffsets[addr]; ok {
		return PBA{Offset: offset}, true
	}

	p, ok := d.lba2obj.Get(addr)
	return p.PBA, ok
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

	if addr, tracking := d.lba2obj.Get(lba); tracking {
		// If it's present in an object as empty already, then yeah, it's empty.
		if addr.Flags != 2 {
			return true
		}
	}

	return true
}

func (d *Disk) ZeroBlocks(ctx context.Context, firstBlock LBA, numBlocks int64) error {
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

func (d *Disk) WriteExtent(ctx context.Context, firstBlock LBA, data Extent) error {
	if d.writeCache == nil {
		err := d.nextLog()
		if err != nil {
			return err
		}
	}

	dw := d.wcBufWrite

	numBlocks := data.Blocks()
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
		_, err = d.CloseSegmentAsync(ctx)
		return err
	}

	return nil
}

func (d *Disk) rebuildFromObjects(ctx context.Context) error {
	entries, err := d.sa.ListSegments(ctx)
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

		flags, err := br.ReadByte()
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

		d.lba2obj.Set(LBA(lba), objPBA{
			PBA: PBA{
				Segment: seg,
				Offset:  dataBegin + uint32(blkOffset),
			},
			Flags: flags,
			Size:  uint32(blkSize),
		})
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
		_, err = d.CloseSegmentAsync(ctx)
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
	f, err := d.sa.WriteMetadata(ctx, "head.map")
	if err != nil {
		return err
	}

	defer f.Close()

	return saveLBAMap(d.lba2obj, f)
}

func (d *Disk) loadLBAMap(ctx context.Context) (bool, error) {
	f, err := d.sa.ReadMetadata(ctx, "head.map")
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

	d.lba2obj = m

	return true, nil
}

func saveLBAMap(m *treemap.TreeMap[LBA, objPBA], f io.Writer) error {
	for it := m.Iterator(); it.Valid(); it.Next() {
		err := binary.Write(f, binary.BigEndian, uint64(it.Key()))
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

func processLBAMap(f io.Reader) (*treemap.TreeMap[LBA, objPBA], error) {
	m := treemap.New[LBA, objPBA]()

	for {
		var (
			lba uint64
			pba objPBA
		)

		err := binary.Read(f, binary.BigEndian, &lba)
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

		m.Set(LBA(lba), pba)
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
	segments, err := d.sa.ListSegments(ctx)
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

	view := make([]byte, BlockSize)
	buf := make([]byte, BlockSize)

	for i := uint32(0); i < cnt; i++ {
		lba, err := binary.ReadUvarint(br)
		if err != nil {
			return err
		}

		flags, err := br.ReadByte()
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

		pba, ok := d.lba2obj.Get(LBA(lba))
		if ok && pba.Segment != seg {
			continue
		}

		if flags == 1 {
			_, err = f.ReadAt(buf[:blkSize], int64(uint64(dataBegin)+blkOffset))
			if err != nil {
				return err
			}

			sz, err := lz4.UncompressBlock(buf[:blkSize], view)
			if err != nil {
				return err
			}
			if sz != BlockSize {
				return fmt.Errorf("block uncompressed to wrong size (%d != %d)", sz, BlockSize)
			}
		} else {
			_, err = f.ReadAt(view[:blkSize], int64(blkOffset))
			if err != nil {
				return err
			}
		}

		err = d.WriteExtent(ctx, LBA(lba), ExtentView(view))
		if err != nil {
			return err
		}
	}

	return d.sa.RemoveSegment(ctx, seg)
}
