package lsvd

import (
	"fmt"
	"slices"
	"sync"
)

type (
	BlockData struct {
		blocks int
		data   []byte
	}

	RangeData struct {
		BlockData
		Extent
	}
)

const (
	smallRangeBlocks = 20
	smallRange       = BlockSize * smallRangeBlocks
)

var smallDB = sync.Pool{
	New: func() any {
		return make([]byte, smallRange)
	},
}

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

func (r *RangeData) Discard() {
	if cap(r.data) >= smallRange {
		smallDB.Put(r.data[:cap(r.data)])
	}

	r.data = nil
}

func NewRangeData(ext Extent) RangeData {
	var data []byte

	if ext.Blocks <= smallRangeBlocks {
		data = smallDB.Get().([]byte)[:BlockSize*ext.Blocks]
	} else {
		data = make([]byte, BlockSize*ext.Blocks)
	}

	return RangeData{
		BlockData: BlockData{
			blocks: int(ext.Blocks),
			data:   data,
		},
		Extent: ext,
	}
}

func (r *RangeData) Reset(ext Extent) {
	needed := ext.Blocks * BlockSize

	if cap(r.data) < int(needed) {
		r.data = make([]byte, needed)
	} else {
		r.data = r.data[:needed]
	}

	clear(r.data)

	r.Extent = ext
}

func (r RangeData) SubRange(ext Extent) (RangeData, bool) {
	ext, ok := r.Clamp(ext)
	if !ok {
		return RangeData{}, false
	}

	byteOffset := (ext.LBA - r.LBA) * BlockSize
	byteEnd := byteOffset + LBA(ext.Blocks*BlockSize)

	q := RangeData{Extent: ext}
	q.blocks = int(ext.Blocks)
	q.data = r.data[byteOffset:byteEnd]

	return q, true
}
