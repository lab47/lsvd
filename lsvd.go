package lsvd

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/hashicorp/go-hclog"
	"github.com/mr-tron/base58"
	"lukechampine.com/blake3"
)

type Disk struct {
	BlockSize int

	log         hclog.Logger
	path        string
	recentReads map[LBA]Block

	activeLog  *os.File
	nextLogIdx uint64
	logCnt     uint64
	crc        hash.Hash64
	curPBA     PBA

	cacheTLB  map[LBA]PBA
	activeTLB map[LBA]uint32

	u64buf []byte

	parent [32]byte

	bh *blake3.Hasher
}

type Block []byte

func NewDisk(log hclog.Logger, path string) (*Disk, error) {
	d := &Disk{
		BlockSize:   4 * 1024,
		log:         log,
		path:        path,
		recentReads: make(map[LBA]Block),
		nextLogIdx:  1,
		u64buf:      make([]byte, 16),
		crc:         crc64.New(crc64.MakeTable(crc64.ECMA)),
		bh:          blake3.New(32, nil),
		cacheTLB:    make(map[LBA]PBA),
		activeTLB:   make(map[LBA]uint32),
		parent:      empty,
	}

	return d, nil
}

type logHeader struct {
	Count  uint64
	CRC    uint64
	Parent [32]byte
}

var (
	headerSize = 1024
	empty      [32]byte
)

func (d *Disk) nextLog(parent [32]byte) (*os.File, error) {
	f, err := os.Create(filepath.Join(d.path, "log.active"))
	if err != nil {
		return nil, err
	}

	_, err = d.crc.Write(parent[:])
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

	pos, err := f.Seek(int64(headerSize), io.SeekStart)
	if err != nil {
		return nil, err
	}

	if pos != int64(headerSize) {
		panic("ack")
	}

	d.curPBA = PBA{Chunk: empty, Offset: uint32(headerSize)}

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

	d.curPBA = PBA{}

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
		d.cacheTLB[lba] = PBA{
			Chunk:  [32]byte(sum),
			Offset: offset,
		}
	}

	clear(d.activeTLB)

	return err
}

func (d *Disk) NewBlock() Block {
	return make(Block, d.BlockSize)
}

type LBA int

func (d *Disk) ReadBlock(block LBA, data Block) error {
	if cacheData, ok := d.recentReads[block]; ok {
		copy(data, cacheData)
		return nil
	}

	if pba, ok := d.activeTLB[block]; ok {
		_, err := d.activeLog.ReadAt(data, int64(pba+8))
		return err
	}

	if pba, ok := d.cacheTLB[block]; ok {
		return d.readPBA(block, pba, data)
	}

	clear(data)
	return nil
}

func (d *Disk) readPBA(lba LBA, addr PBA, data Block) error {
	f, err := os.Open(filepath.Join(d.path,
		"log."+base58.Encode(addr.Chunk[:])))
	if err != nil {
		return err
	}

	defer f.Close()

	_, err = f.ReadAt(data, int64(addr.Offset+8))
	if err != nil {
		return fmt.Errorf("attempting to read PBA at %d/%d: %w", addr.Chunk, addr.Offset, err)
	}

	cacheCopy := slices.Clone(data)
	d.recentReads[lba] = cacheCopy

	//clear(data)
	return nil
}

type PBA struct {
	Chunk  [32]byte
	Offset uint32
}

func (d *Disk) cacheTranslate(addr LBA) (PBA, bool) {
	if offset, ok := d.activeTLB[addr]; ok {
		return PBA{Offset: offset}, true
	}

	p, ok := d.cacheTLB[addr]
	return p, ok
}

func (d *Disk) WriteBlock(block LBA, data Block) error {
	cacheCopy := slices.Clone(data)
	d.recentReads[block] = cacheCopy

	if d.activeLog == nil {
		l, err := d.nextLog(d.parent)
		if err != nil {
			return err
		}

		d.activeLog = l
		d.logCnt = 0
	}

	d.logCnt++

	dw := io.MultiWriter(d.activeLog, d.crc)

	err := binary.Write(dw, binary.BigEndian, uint64(block))
	if err != nil {
		return err
	}

	n, err := dw.Write(data)
	if err != nil {
		return err
	}

	if n != d.BlockSize {
		return fmt.Errorf("invalid write size (%d != %d)", n, d.BlockSize)
	}

	d.activeTLB[block] = d.curPBA.Offset

	d.curPBA.Offset += uint32(8 + d.BlockSize)

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

	/*
		entries, err := os.ReadDir(d.path)
		if err != nil {
			return err
		}

		for _, ent := range entries {
			d.rebuildTLB(filepath.Join(d.path, ent.Name()))
		}
	*/

	return nil
}

func chunkFromName(name string) ([32]byte, bool) {
	_, intPart, ok := strings.Cut(name, ".")
	if !ok {
		return empty, false
	}

	data, err := base58.Decode(intPart)
	if err != nil {
		return empty, false
	}

	if len(data) == 32 {
		return [32]byte(data), true
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

		d.cacheTLB[LBA(lba)] = cur

		cur.Offset += uint32(8 + d.BlockSize)
	}

	return &hdr, nil
}
