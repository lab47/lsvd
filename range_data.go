package lsvd

import "sync"

type RangeData struct {
	BlockData
	Extent
}

const (
	smallRangeBlocks = 20
	smallRange       = BlockSize * smallRangeBlocks
)

var smallDB = sync.Pool{
	New: func() any {
		return make([]byte, smallRange)
	},
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
