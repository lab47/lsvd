package lsvd

import "bytes"

var emptyBlock = make([]byte, BlockSize)

func emptyBytes(b []byte) bool {
	for len(b) > BlockSize {
		if !bytes.Equal(b[:BlockSize], emptyBlock) {
			return false
		}

		b = b[BlockSize:]
	}

	return bytes.Equal(b, emptyBlock[:len(b)])
}
