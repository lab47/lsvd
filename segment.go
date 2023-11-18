package lsvd

import (
	"encoding/binary"
	"os"
)

func ReadSegmentHeader(path string) (*SegmentHeader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	var hdr SegmentHeader

	err = binary.Read(f, binary.BigEndian, &hdr)
	return &hdr, err
}
