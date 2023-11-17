package lsvd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/edsrzf/mmap-go"
	"github.com/hashicorp/go-hclog"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/igrmk/treemap/v2"
	"github.com/mr-tron/base58"
	"lukechampine.com/blake3"
)

type (
	Extent struct {
		blocks int
		data   []byte
	}

	SegmentId [32]byte
)

const BlockSize = 4 * 1024

func (e Extent) Blocks() int {
	return e.blocks
}

type segmentInfo struct {
	f *os.File
	m mmap.MMap
}

type Disk struct {
	BlockSize int

	log     hclog.Logger
	path    string
	l1cache *lru.Cache[LBA, []byte]

	activeLog *os.File
	logCnt    uint64
	curOffset uint32
	logWriter io.Writer

	lba2disk     *treemap.TreeMap[LBA, PBA]
	activeTLB    map[LBA]uint32
	openSegments *lru.Cache[SegmentId, *segmentInfo]

	u64buf []byte

	parent SegmentId

	bh *blake3.Hasher
}

func NewDisk(log hclog.Logger, path string) (*Disk, error) {
	d := &Disk{
		BlockSize: 4 * 1024,
		log:       log,
		path:      path,
		u64buf:    make([]byte, 16),
		bh:        blake3.New(32, nil),
		lba2disk:  treemap.New[LBA, PBA](),
		activeTLB: make(map[LBA]uint32),
		parent:    empty,
	}

	l1cache, err := lru.New[LBA, []byte](4096)
	if err != nil {
		return nil, err
	}

	d.l1cache = l1cache

	openSegments, err := lru.NewWithEvict[SegmentId, *segmentInfo](
		256, func(key SegmentId, value *segmentInfo) {
			value.m.Unmap()
			value.f.Close()
		})
	if err != nil {
		return nil, err
	}

	d.openSegments = openSegments

	err = d.loadLBAMap()
	if err != nil {
		return nil, err
	}

	err = d.restoreActive()
	if err != nil {
		return nil, err
	}

	return d, nil
}

type logHeader struct {
	Parent    SegmentId
	CreatedAt uint64
}

var (
	headerSize = binary.Size(logHeader{})
	empty      SegmentId
)

func (d *Disk) nextLog(parent SegmentId) error {
	f, err := os.Create(filepath.Join(d.path, "log.active"))
	if err != nil {
		return err
	}

	d.bh.Reset()

	d.activeLog = f
	d.logWriter = io.MultiWriter(f, d.bh)

	ts := uint64(time.Now().Unix())

	err = binary.Write(d.logWriter, binary.BigEndian, logHeader{
		Parent:    parent,
		CreatedAt: ts,
	})
	if err != nil {
		return err
	}

	d.curOffset = uint32(headerSize)

	return nil
}

func (d *Disk) closeSegment() error {
	d.curOffset = 0

	d.activeLog.Close()

	sum := d.bh.Sum(nil)

	newPath := filepath.Join(d.path, "log."+base58.Encode(sum))

	err := os.Rename(d.activeLog.Name(), newPath)
	if err != nil {
		return err
	}

	err = os.WriteFile(filepath.Join(d.path, "head"), []byte(base58.Encode(sum)), 0755)
	if err != nil {
		return err
	}

	d.bh.Reset()

	d.activeLog = nil

	for lba, offset := range d.activeTLB {
		d.lba2disk.Set(lba, PBA{
			Segment: SegmentId(sum),
			Offset:  offset,
		})
	}

	clear(d.activeTLB)

	return err
}

func (d *Disk) NewExtent(sz int) Extent {
	return Extent{
		blocks: sz,
		data:   make([]byte, BlockSize*sz),
	}
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

type LBA uint64

const perBlockHeader = 16

func (d *Disk) ReadExtent(firstBlock LBA, data Extent) error {
	numBlocks := data.Blocks()

	for i := 0; i < numBlocks; i++ {
		lba := firstBlock + LBA(i)

		view := data.BlockView(i)

		if cacheData, ok := d.l1cache.Get(lba); ok {
			copy(view, cacheData)
		} else if pba, ok := d.activeTLB[lba]; ok {
			_, err := d.activeLog.ReadAt(view, int64(pba+perBlockHeader))
			if err != nil {
				return err
			}
		} else if pba, ok := d.lba2disk.Get(lba); ok {
			err := d.readPBA(lba, pba, view)
			if err != nil {
				return err
			}
		} else {
			clear(view)
		}
	}

	return nil
}

func (d *Disk) readPBA(lba LBA, addr PBA, data []byte) error {
	ci, ok := d.openSegments.Get(addr.Segment)
	if !ok {
		f, err := os.Open(filepath.Join(d.path,
			"log."+base58.Encode(addr.Segment[:])))
		if err != nil {
			return err
		}

		mm, err := mmap.Map(f, os.O_RDONLY, 0)
		if err != nil {
			return err
		}

		ci = &segmentInfo{
			f: f,
			m: mm,
		}

		d.openSegments.Add(addr.Segment, ci)
	}

	copy(data, ci.m[addr.Offset+perBlockHeader:])

	return nil
}

type PBA struct {
	Segment SegmentId
	Offset  uint32
}

func (d *Disk) cacheTranslate(addr LBA) (PBA, bool) {
	if offset, ok := d.activeTLB[addr]; ok {
		return PBA{Offset: offset}, true
	}

	p, ok := d.lba2disk.Get(addr)
	return p, ok
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

func (d *Disk) WriteExtent(firstBlock LBA, data Extent) error {

	if d.activeLog == nil {
		err := d.nextLog(d.parent)
		if err != nil {
			return err
		}

		d.logCnt = 0
	}

	d.logCnt++

	dw := d.activeLog // io.MultiWriter(d.activeLog, d.crc, d.bh)

	crc64.New(crc64.MakeTable(crc64.ECMA))

	numBlocks := data.Blocks()
	for i := 0; i < numBlocks; i++ {
		lba := firstBlock + LBA(i)

		view := data.BlockView(i)

		crc := crc64.Update(crcLBA(0, lba), crcTable, view)

		d.l1cache.Add(lba, slices.Clone(view))

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

		if n != d.BlockSize {
			return fmt.Errorf("invalid write size (%d != %d)", n, d.BlockSize)
		}

		d.activeTLB[lba] = d.curOffset
		d.curOffset += uint32(perBlockHeader + d.BlockSize)
	}

	return nil
}

func (d *Disk) rebuild() error {
	data, err := os.ReadFile(filepath.Join(d.path, "head"))
	if err != nil {
		return err
	}

	path := filepath.Join(d.path, "log."+string(data))

	for path != "" {
		d.log.Debug("rebuilding TLB", "path", path)
		hdr, err := d.rebuildTLB(path)
		if err != nil {
			return err
		}

		if hdr.Parent == empty {
			path = ""
		} else {
			path = filepath.Join(d.path, "log."+base58.Encode(hdr.Parent[:]))
		}
	}

	return nil
}

func segmentFromName(name string) (SegmentId, bool) {
	_, intPart, ok := strings.Cut(name, ".")
	if !ok {
		return empty, false
	}

	data, err := base58.Decode(intPart)
	if err != nil {
		return empty, false
	}

	if len(data) == 32 {
		return SegmentId(data), true
	}

	return empty, false
}

func (d *Disk) rebuildTLB(path string) (*logHeader, error) {
	seg, ok := segmentFromName(path)
	if !ok {
		return nil, fmt.Errorf("bad segment name")
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	var hdr logHeader

	err = binary.Read(f, binary.BigEndian, &hdr)
	if err != nil {
		return nil, err
	}

	cur := PBA{
		Segment: seg,
		Offset:  uint32(headerSize),
	}

	view := make([]byte, BlockSize)

	for {
		var crc, lba uint64

		err = binary.Read(f, binary.BigEndian, &crc)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		err = binary.Read(f, binary.BigEndian, &lba)
		if err != nil {
			return nil, err
		}

		_, err = io.ReadFull(f, view)
		if err != nil {
			return nil, err
		}

		if crc != crc64.Update(crcLBA(0, LBA(lba)), crcTable, view) {
			d.log.Warn("detected mis-match crc reloading log", "offset", cur.Offset)
			break
		}

		d.lba2disk.Set(LBA(lba), cur)

		cur.Offset += uint32(perBlockHeader + d.BlockSize)
	}

	return &hdr, nil
}

func (d *Disk) restoreActive() error {
	f, err := os.Open(filepath.Join(d.path, "log.active"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return err
	}

	defer f.Close()

	var hdr logHeader

	err = binary.Read(f, binary.BigEndian, &hdr)
	if err != nil {
		return err
	}

	d.log.Debug("found orphan active log", "created-at", hdr.CreatedAt)

	offset := uint32(headerSize)

	view := make([]byte, BlockSize)

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

		_, err = io.ReadFull(f, view)
		if err != nil {
			return err
		}

		if crc != crc64.Update(crcLBA(0, LBA(lba)), crcTable, view) {
			d.log.Warn("detected mis-match crc reloading log", "offset", offset)
			break
		}

		d.activeTLB[LBA(lba)] = offset

		offset += uint32(perBlockHeader + d.BlockSize)
	}

	return nil
}

func (d *Disk) Close() error {
	err := d.saveLBAMap()

	d.openSegments.Purge()

	return err
}

func (d *Disk) saveLBAMap() error {
	f, err := os.Create(filepath.Join(d.path, "head.map"))
	if err != nil {
		return err
	}

	defer f.Close()

	return saveLBAMap(d.lba2disk, f)
}

func (d *Disk) loadLBAMap() error {
	f, err := os.Open(filepath.Join(d.path, "head.map"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return err
	}

	defer f.Close()

	m, err := processLBAMap(f)
	if err != nil {
		return err
	}

	d.lba2disk = m

	return nil
}

func saveLBAMap(m *treemap.TreeMap[LBA, PBA], f io.Writer) error {
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

func processLBAMap(f io.Reader) (*treemap.TreeMap[LBA, PBA], error) {
	m := treemap.New[LBA, PBA]()

	for {
		var (
			lba uint64
			pba PBA
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
