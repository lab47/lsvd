package lsvd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
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
	Extent    []byte
	SegmentId [32]byte
)

const BlockSize = 4 * 1024

func (e Extent) Blocks() int {
	return len(e) / BlockSize
}

type chunkInfo struct {
	f *os.File
	m mmap.MMap
}

type Disk struct {
	BlockSize int

	log     hclog.Logger
	path    string
	l1cache *lru.Cache[LBA, Extent]

	activeLog *os.File
	logCnt    uint64
	crc       hash.Hash64
	curOffset uint32

	lba2disk   *treemap.TreeMap[LBA, PBA]
	activeTLB  map[LBA]uint32
	openChunks *lru.Cache[SegmentId, *chunkInfo]

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
		crc:       crc64.New(crc64.MakeTable(crc64.ECMA)),
		bh:        blake3.New(32, nil),
		lba2disk:  treemap.New[LBA, PBA](),
		activeTLB: make(map[LBA]uint32),
		parent:    empty,
	}

	l1cache, err := lru.New[LBA, Extent](4096)
	if err != nil {
		return nil, err
	}

	d.l1cache = l1cache

	openChunks, err := lru.NewWithEvict[SegmentId, *chunkInfo](
		256, func(key SegmentId, value *chunkInfo) {
			value.m.Unmap()
			value.f.Close()
		})
	if err != nil {
		return nil, err
	}

	d.openChunks = openChunks

	return d, nil
}

type logHeader struct {
	Count     uint64
	CRC       uint64
	Parent    SegmentId
	CreatedAt uint64
}

var (
	headerSize = 1024
	empty      SegmentId
)

func (d *Disk) nextLog(parent SegmentId) (*os.File, error) {
	f, err := os.Create(filepath.Join(d.path, "log.active"))
	if err != nil {
		return nil, err
	}

	d.crc.Reset()
	d.bh.Reset()

	ts := uint64(time.Now().Unix())

	// Calculate the initial CRC over a header with no count and
	// 0 as the crc
	err = binary.Write(d.crc, binary.BigEndian, logHeader{
		Parent:    parent,
		CreatedAt: ts,
	})
	if err != nil {
		return nil, err
	}

	// both will be patched later
	binary.Write(f, binary.BigEndian, uint64(0))
	binary.Write(f, binary.BigEndian, d.crc.Sum64())

	_, err = f.Write(parent[:])
	if err != nil {
		return nil, err
	}

	binary.Write(f, binary.BigEndian, uint64(time.Now().Unix()))

	_, err = f.Seek(int64(headerSize), io.SeekStart)
	if err != nil {
		return nil, err
	}

	d.curOffset = uint32(headerSize)

	return f, nil
}

func (d *Disk) flushLogHeader() error {
	if d.activeLog == nil {
		return nil
	}

	binary.BigEndian.PutUint64(d.u64buf, d.logCnt)
	binary.BigEndian.PutUint64(d.u64buf[8:], d.crc.Sum64())

	_, err := d.activeLog.WriteAt(d.u64buf, 0)
	if err != nil {
		return err
	}

	return nil
}

func (d *Disk) closeChunk() error {
	err := d.flushLogHeader()
	if err != nil {
		return err
	}

	d.curOffset = 0

	data := make([]byte, headerSize)

	n, err := d.activeLog.ReadAt(data, 0)
	if err != nil {
		return err
	}

	if n != headerSize {
		return fmt.Errorf("short read detected")
	}

	d.bh.Write(data)

	d.activeLog.Close()

	sum := d.bh.Sum(nil)

	newPath := filepath.Join(d.path, "log."+base58.Encode(sum))

	err = os.Rename(d.activeLog.Name(), newPath)
	if err != nil {
		return err
	}

	err = os.WriteFile(filepath.Join(d.path, "head"), []byte(base58.Encode(sum)), 0755)
	if err != nil {
		return err
	}

	d.crc.Reset()
	d.bh.Reset()

	d.activeLog = nil

	for lba, offset := range d.activeTLB {
		d.lba2disk.Set(lba, PBA{
			Chunk:  SegmentId(sum),
			Offset: offset,
		})
	}

	clear(d.activeTLB)

	return err
}

func (d *Disk) NewExtent(sz int) Extent {
	return make(Extent, d.BlockSize*sz)
}

type LBA int

func (d *Disk) ReadExtent(firstBlock LBA, data Extent) error {
	numBlocks := data.Blocks()

	for i := 0; i < numBlocks; i++ {
		lba := firstBlock + LBA(i)

		view := data[:BlockSize]

		if cacheData, ok := d.l1cache.Get(lba); ok {
			copy(view, cacheData)
		} else if pba, ok := d.activeTLB[lba]; ok {
			_, err := d.activeLog.ReadAt(view, int64(pba+8))
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

		data = data[BlockSize:]
	}

	return nil
}

func (d *Disk) readPBA(lba LBA, addr PBA, data Extent) error {
	ci, ok := d.openChunks.Get(addr.Chunk)
	if !ok {
		f, err := os.Open(filepath.Join(d.path,
			"log."+base58.Encode(addr.Chunk[:])))
		if err != nil {
			return err
		}

		mm, err := mmap.Map(f, os.O_RDONLY, 0)
		if err != nil {
			return err
		}

		ci = &chunkInfo{
			f: f,
			m: mm,
		}

		d.openChunks.Add(addr.Chunk, ci)
	}

	copy(data, ci.m[addr.Offset+8:])

	return nil
}

type PBA struct {
	Chunk  SegmentId
	Offset uint32
}

func (d *Disk) cacheTranslate(addr LBA) (PBA, bool) {
	if offset, ok := d.activeTLB[addr]; ok {
		return PBA{Offset: offset}, true
	}

	p, ok := d.lba2disk.Get(addr)
	return p, ok
}

func (d *Disk) WriteExtent(firstBlock LBA, data Extent) error {
	cacheCopy := slices.Clone(data)

	if d.activeLog == nil {
		l, err := d.nextLog(d.parent)
		if err != nil {
			return err
		}

		d.activeLog = l
		d.logCnt = 0
	}

	d.logCnt++

	dw := io.MultiWriter(d.activeLog, d.crc, d.bh)

	numBlocks := data.Blocks()
	for i := 0; i < numBlocks; i++ {
		lba := firstBlock + LBA(i)
		d.l1cache.Add(lba, cacheCopy[BlockSize*i:])

		err := binary.Write(dw, binary.BigEndian, uint64(lba))
		if err != nil {
			return err
		}

		n, err := dw.Write(data[:BlockSize])
		if err != nil {
			return err
		}

		data = data[BlockSize:]

		if n != d.BlockSize {
			return fmt.Errorf("invalid write size (%d != %d)", n, d.BlockSize)
		}

		d.activeTLB[lba] = d.curOffset
		d.curOffset += uint32(8 + d.BlockSize)
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

func chunkFromName(name string) (SegmentId, bool) {
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
	chunk, ok := chunkFromName(path)
	if !ok {
		return nil, fmt.Errorf("bad chunk name")
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	var hdr logHeader

	err = binary.Read(f, binary.BigEndian, &hdr)
	if err != nil {
		return nil, err
	}

	cur := PBA{
		Chunk:  chunk,
		Offset: 16,
	}

	for i := 0; i < int(hdr.Count); i++ {
		var lba uint64

		err = binary.Read(f, binary.BigEndian, &lba)
		if err != nil {
			return nil, err
		}

		d.lba2disk.Set(LBA(lba), cur)

		cur.Offset += uint32(8 + d.BlockSize)
	}

	return &hdr, nil
}

func (d *Disk) saveLBAMap() error {
	f, err := os.Create(filepath.Join(d.path, "head.map"))
	if err != nil {
		return err
	}

	defer f.Close()

	return saveLBAMap(d.lba2disk, f)
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

func processLBAMap(f io.Reader) (map[LBA]PBA, error) {
	m := make(map[LBA]PBA)

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

		m[LBA(lba)] = pba
	}

	return m, nil
}
