package lsvd

import (
	"context"
)

const BufferSliceSize = 1024 * 1024

type Buffers struct {
	slice []byte

	next int
}

func NewBuffers() *Buffers {
	data := make([]byte, BufferSliceSize)
	return &Buffers{
		slice: data,
	}
}

type buffersKey struct{}

func B(ctx context.Context) *Buffers {
	val := ctx.Value(buffersKey{})
	if val == nil {
		return &Buffers{}
	}

	return val.(*Buffers)
}

func (b *Buffers) Inject(ctx context.Context) context.Context {
	return context.WithValue(ctx, buffersKey{}, b)
}

func (b *Buffers) Reset() {
	b.next = 0
}

func (b *Buffers) Marker() int {
	return b.next
}

func (b *Buffers) ResetTo(marker int) {
	b.next = marker
}

func (b *Buffers) alloc(sz int) []byte {
	if len(b.slice)-b.next < sz {
		if sz > BufferSliceSize {
			return make([]byte, sz)
		}

		dup := make([]byte, len(b.slice)+BufferSliceSize)
		copy(dup, b.slice)
		b.slice = dup
	}

	data := b.slice[b.next : b.next+sz]
	b.next += sz

	return data
}

func (b *Buffers) NewRangeData(rng Extent) RangeData {
	return RangeData{
		data:   b.alloc(rng.ByteSize()),
		Extent: rng,
	}
}
