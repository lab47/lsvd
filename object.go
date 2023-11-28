package lsvd

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/igrmk/treemap/v2"
	"github.com/pierrec/lz4/v4"
)

type ocBlock struct {
	lba          LBA
	flags        byte
	size, offset uint64
}

type ObjectCreator struct {
	cnt int

	offset uint64
	blocks []ocBlock

	header  bytes.Buffer
	body    bytes.Buffer
	compBuf bytes.Buffer
}

func (o *ObjectCreator) WriteExtent(firstBlock LBA, ext Extent) error {
	for i := 0; i < ext.Blocks(); i++ {
		lba := firstBlock + LBA(i)

		var flags byte

		o.compBuf.Reset()

		c := lz4.NewWriter(&o.compBuf)
		c.Apply(lz4.BlockSizeOption(lz4.Block64Kb))
		c.Write(ext.BlockView(i))
		err := c.Close()
		if err != nil {
			return err
		}

		body := ext.BlockView(i)

		if o.compBuf.Len() < BlockSize {
			body = o.compBuf.Bytes()
			flags = 1
		}

		_, err = o.body.Write(body)
		if err != nil {
			return err
		}

		o.cnt++

		o.blocks = append(o.blocks, ocBlock{
			lba:    lba,
			size:   uint64(len(body)),
			offset: o.offset,
			flags:  flags,
		})

		o.offset += uint64(len(body))
	}

	return nil
}

func (o *ObjectCreator) Reset() {
	o.blocks = nil
	o.cnt = 0
	o.offset = 0
	o.header.Reset()
	o.body.Reset()
}

func (o *ObjectCreator) Flush(path string, seg SegmentId, m *treemap.TreeMap[LBA, objPBA]) error {
	defer o.Reset()

	buf := make([]byte, 16)

	for _, blk := range o.blocks {
		lba := blk.lba

		sz := binary.PutUvarint(buf, uint64(lba))
		_, err := o.header.Write(buf[:sz])
		if err != nil {
			return err
		}

		err = o.header.WriteByte(blk.flags)
		if err != nil {
			return err
		}

		sz = binary.PutUvarint(buf, blk.size)
		_, err = o.header.Write(buf[:sz])
		if err != nil {
			return err
		}

		sz = binary.PutUvarint(buf, blk.offset)
		_, err = o.header.Write(buf[:sz])
		if err != nil {
			return err
		}
	}

	dataBegin := uint32(o.header.Len() + 8)

	for _, blk := range o.blocks {
		m.Set(blk.lba, objPBA{
			PBA: PBA{
				Segment: seg,
				Offset:  dataBegin + uint32(blk.offset),
			},
			Flags: blk.flags,
			Size:  uint32(blk.size),
		})

	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}

	defer f.Close()

	binary.BigEndian.PutUint32(buf, uint32(o.cnt))
	_, err = f.Write(buf[:4])
	if err != nil {
		return err
	}

	binary.BigEndian.PutUint32(buf, dataBegin)
	_, err = f.Write(buf[:4])
	if err != nil {
		return err
	}

	_, err = io.Copy(f, &o.header)
	if err != nil {
		return err
	}

	_, err = io.Copy(f, &o.body)
	if err != nil {
		return err
	}

	return nil
}
