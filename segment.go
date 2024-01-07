package lsvd

import (
	"encoding/binary"
	"os"
)

type Segment struct {
	Size uint64
	Used uint64

	TotalBytes uint64
	UsedBytes  uint64
}

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
