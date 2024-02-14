package lsvd

import (
	"slices"
	"sync"
)

type (
	RawBlocks []byte

	blockData struct {
		data RawBlocks
	}

	RangeData struct {
		*blockData
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

func (e RawBlocks) Blocks() int {
	return len(e) / BlockSize
}

func (e RawBlocks) MapTo(lba LBA) RangeData {
	return RangeData{
		Extent: Extent{lba, uint32(e.Blocks())},
		blockData: &blockData{
			data: e,
		},
	}
}

func BlockDataView(blk []byte) RawBlocks {
	cnt := len(blk) / BlockSize
	if cnt < 0 || len(blk)%BlockSize != 0 {
		panic("invalid block data size for extent")
	}

	return RawBlocks(slices.Clone(blk))
}

func (e *RangeData) Blocks() int {
	return len(e.data) / BlockSize
}

func (e *RangeData) CopyTo(data []byte) error {
	copy(data, e.data)
	return nil
}

var emptyBlocks = [BlockSize * 10]byte{}

func (e *RangeData) BlockView(cnt int) []byte {
	if e.data == nil {
		e.allocate(e.Extent)
	}
	return e.data[BlockSize*cnt : (BlockSize*cnt)+BlockSize]
}

func (e *RangeData) SetBlock(blk int, data []byte) {
	if len(data) != BlockSize {
		panic("invalid data length, not block size")
	}

	copy(e.data[BlockSize*blk:], data)
}

func (r *blockData) Discard() {
	if cap(r.data) >= smallRange {
		smallDB.Put(r.data[:cap(r.data)])
	}

	r.data = nil
}

func (b *blockData) allocate(ext Extent) []byte {
	var data []byte

	if ext.Blocks <= smallRangeBlocks {
		data = smallDB.Get().([]byte)[:BlockSize*ext.Blocks]
		clear(data)
	} else {
		data = make([]byte, BlockSize*ext.Blocks)
	}

	b.data = data

	return data
}

func NewAllocatedRangeData(ext Extent) RangeData {
	var bd blockData
	bd.allocate(ext)

	return RangeData{
		blockData: &bd,
		Extent:    ext,
	}
}

func NewRangeData(ext Extent) RangeData {
	return RangeData{
		blockData: &blockData{
			data: nil,
		},
		Extent: ext,
	}
}

func MapRangeData(ext Extent, srcData []byte) RangeData {
	return RangeData{
		Extent: ext,
		blockData: &blockData{
			data: srcData,
		},
	}
}

func (r *RangeData) WriteData() []byte {
	if r.data == nil {
		return r.allocate(r.Extent)
	}

	return r.data
}

func (r *RangeData) ReadData() []byte {
	return r.data
}

func (r *RangeData) EmptyP() bool {
	if r.data == nil {
		return true
	}

	return emptyBytes(r.data)
}

func (r *RangeData) Reset(ext Extent) {
	if r.blockData == nil {
		r.blockData = &blockData{}
	}

	needed := ext.Blocks * BlockSize

	if cap(r.data) < int(needed) {
		r.data = make([]byte, needed)
	} else {
		r.data = r.data[:needed]
	}

	clear(r.data)

	r.Extent = ext
}

type RangeDataView struct {
	Extent
	r *RangeData

	start, end int
}

func (r RangeData) SubRange(ext Extent) (RangeDataView, bool) {
	ext, ok := r.Clamp(ext)
	if !ok {
		return RangeDataView{}, false
	}

	byteOffset := (ext.LBA - r.LBA) * BlockSize
	byteEnd := byteOffset + LBA(ext.Blocks*BlockSize)

	return RangeDataView{
		Extent: ext,
		r:      &r,
		start:  int(byteOffset),
		end:    int(byteEnd),
	}, true

	/*
		q := RangeData{Extent: ext}
		q.blocks = int(ext.Blocks)
		q.data = r.data[byteOffset:byteEnd]

		return q, true
	*/
}

func (r RangeData) View() RangeDataView {
	return RangeDataView{
		Extent: r.Extent,
		r:      &r,
		start:  0,
		end:    r.ByteSize(),
	}
}

func (r RangeDataView) SubRange(ext Extent) (RangeDataView, bool) {
	ext, ok := r.Clamp(ext)
	if !ok {
		return RangeDataView{}, false
	}

	byteOffset := r.start + int((ext.LBA-r.LBA)*BlockSize)
	byteEnd := byteOffset + int(ext.Blocks*BlockSize)

	return RangeDataView{
		Extent: ext,
		r:      r.r,
		start:  int(byteOffset),
		end:    int(byteEnd),
	}, true

	/*
		q := RangeData{Extent: ext}
		q.blocks = int(ext.Blocks)
		q.data = r.data[byteOffset:byteEnd]

		return q, true
	*/
}

func (v RangeDataView) WriteData() []byte {
	b := v.r.WriteData()

	return b[v.start:v.end]
}

func (v RangeDataView) ReadData() []byte {
	b := v.r.ReadData()

	return b[v.start:v.end]
}

func (v RangeDataView) ByteSize() int {
	return v.end - v.start
}

func (d RangeDataView) Copy(s RangeDataView) int {
	// if the source is empty, it's a noop.
	if s.r.data == nil {
		return d.end - d.start
	}

	return copy(d.WriteData(), s.ReadData())

	/*
		q := RangeData{Extent: ext}
		q.blocks = int(ext.Blocks)
		q.data = r.data[byteOffset:byteEnd]

		return q, true
	*/
}

func (r RangeData) Append(o RangeData) RangeData {
	if r.Extent.Blocks == 0 {
		return o
	}

	r.data = append(r.data, o.data...)
	r.Extent.Blocks += o.Extent.Blocks
	return r
}

func (d *RangeData) Copy(s RangeData) int {
	// if s is empty, skip the copy.
	if s.data == nil {
		return s.ByteSize()
	}

	db := d.WriteData()

	return copy(db, s.ReadData())
}
