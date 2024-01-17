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

	deleted bool
	cleared []Extent
}

func (s *Segment) detectedCleared(ext Extent) (Extent, bool) {
	for _, x := range s.cleared {
		if ext.Cover(x) != CoverNone {
			return x, true
		}
	}

	return Extent{}, false
}

func (s *Segment) Density() float64 {
	if s.Size == 0 {
		return 0
	}

	return float64(s.Used) / float64(s.Size)
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
