package lsvd

import (
	"encoding/binary"
	"io"
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

type SegmentHeader struct {
	ExtentCount uint32
	DataOffset  uint32
}

func (s SegmentHeader) Write(w io.Writer) error {
	return binary.Write(w, binary.BigEndian, s)
}

func (s *SegmentHeader) Read(r io.Reader) error {
	err := binary.Read(r, binary.BigEndian, &s.ExtentCount)
	if err != nil {
		return err
	}

	err = binary.Read(r, binary.BigEndian, &s.DataOffset)
	if err != nil {
		return err
	}

	return nil
}

type ExtentHeader struct {
	Extent
	Flags   byte
	Size    uint64
	Offset  uint32
	RawSize uint32 // used when the extent is compressed
}

func (e *ExtentHeader) Read(r io.ByteReader) error {
	lba, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}

	e.LBA = LBA(lba)

	blocks, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}

	e.Blocks = uint32(blocks)

	e.Flags, err = r.ReadByte()
	if err != nil {
		return err
	}

	e.Size, err = binary.ReadUvarint(r)
	if err != nil {
		return err
	}

	off, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}

	e.Offset = uint32(off)

	rs, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}

	e.RawSize = uint32(rs)

	return nil
}

// PutUvarint encodes a uint64 into buf and returns the number of bytes written.
// If the buffer is too small, PutUvarint will panic.
func WriteUvarint(w io.ByteWriter, x uint64) (int, error) {
	i := 0
	var err error
	for x >= 0x80 {
		err = w.WriteByte(byte(x) | 0x80)
		if err != nil {
			return i, err
		}
		x >>= 7
		i++
	}
	err = w.WriteByte(byte(x))
	return i + 1, err
}

func (e *ExtentHeader) Write(w io.ByteWriter) error {
	_, err := WriteUvarint(w, uint64(e.LBA))
	if err != nil {
		return err
	}

	_, err = WriteUvarint(w, uint64(e.Blocks))
	if err != nil {
		return err
	}

	err = w.WriteByte(e.Flags)
	if err != nil {
		return err
	}

	_, err = WriteUvarint(w, uint64(e.Size))
	if err != nil {
		return err
	}

	_, err = WriteUvarint(w, uint64(e.Offset))
	if err != nil {
		return err
	}

	_, err = WriteUvarint(w, uint64(e.RawSize))
	if err != nil {
		return err
	}

	return nil
}
