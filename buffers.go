package lsvd

import (
	"context"
)

const BufferSliceSize = 1024 * 1024

type Buffers struct {
	slice []byte
	rest  []byte
}

func NewBuffers() *Buffers {
	return &Buffers{
		slice: make([]byte, BufferSliceSize),
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
	b.rest = b.slice
	clear(b.rest)
}

func (b *Buffers) alloc(sz int) []byte {
	if len(b.rest) < sz || sz > len(b.slice) {
		return make([]byte, sz)
	}

	data := b.rest[:sz]
	b.rest = b.rest[sz:]

	return data
}

func (b *Buffers) NewRangeData(rng Extent) RangeData {
	return RangeData{
		blockData: &blockData{
			data: b.alloc(rng.ByteSize()),
		},
		Extent: rng,
	}
}
