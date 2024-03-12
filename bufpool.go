package lsvd

import "sync"

type bufPool struct {
	small sync.Pool
}

const smallBuffer = 50 * BlockSize

func (p *bufPool) Get(sz int) []byte {
	if sz <= smallBuffer {
		var buf []byte
		v := p.small.Get()
		if v == nil {
			buf = make([]byte, smallBuffer)
		} else {
			buf = v.([]byte)
		}

		return buf[:sz]
	}

	return make([]byte, sz)
}

func (p *bufPool) Return(buf []byte) {
	buf = buf[:cap(buf)]

	if len(buf) == smallBuffer {
		p.small.Put(buf)
	}
}

var buffers bufPool
