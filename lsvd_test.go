package lsvd

import (
	"bytes"
	"encoding/binary"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
)

func isEmpty(d []byte) bool {
	for _, b := range d {
		if b != 0 {
			return false
		}
	}

	return true
}

var testData = make([]byte, 4*1024)

func init() {
	for i := 0; i < 10; i++ {
		testData[i] = 0x47
	}
}

func blockEqual(t *testing.T, a, b Block) {
	require.True(t, bytes.Equal(a, b), "blocks are not the same")
}

func TestLSVD(t *testing.T) {
	log := hclog.New(&hclog.LoggerOptions{
		Name:  "lsvdtest",
		Level: hclog.Trace,
	})
	t.Run("reads with no data return zeros", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		data := make([]byte, d.BlockSize)

		copy(data, testData)

		err = d.ReadBlock(0, data)
		r.NoError(err)

		r.True(isEmpty(data))
	})

	t.Run("writes are returned by next read", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		data := d.NewBlock()
		copy(data, testData)

		err = d.WriteBlock(0, data)
		r.NoError(err)

		d2 := d.NewBlock()

		err = d.ReadBlock(0, d2)
		r.NoError(err)

		r.Equal(d2, data)
	})

	t.Run("writes are written to a log file", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		data := d.NewBlock()
		copy(data, testData)

		err = d.WriteBlock(47, data)
		r.NoError(err)

		err = d.flushLogHeader()
		r.NoError(err)

		f, err := os.Open(filepath.Join(tmpdir, "log.active"))
		r.NoError(err)

		defer f.Close()

		var hdr logHeader

		r.NoError(binary.Read(f, binary.BigEndian, &hdr))

		r.Equal(uint64(1), hdr.Count)
		r.NotEqual(uint64(0), hdr.CRC)

		var lba uint64

		h := crc64.New(crc64.MakeTable(crc64.ECMA))

		h.Write(empty[:])

		f.Seek(int64(headerSize), io.SeekStart)

		r.NoError(binary.Read(io.TeeReader(f, h), binary.BigEndian, &lba))

		r.Equal(uint64(47), lba)

		blk := d.NewBlock()

		n, err := io.ReadFull(io.TeeReader(f, h), blk)
		r.NoError(err)

		r.Equal(d.BlockSize, n)

		r.Equal(data, blk)

		t.Logf("crc: %d", h.Sum64())

		r.Equal(hdr.CRC, h.Sum64())

		t.Run("and are tracked for reading back", func(t *testing.T) {
			r := require.New(t)
			pba, ok := d.cacheTranslate(47)
			r.True(ok)

			r.Equal(empty, pba.Chunk)
			r.Equal(uint32(headerSize), pba.Offset)
		})
	})

	t.Run("can access blocks from the log", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		data := d.NewBlock()
		copy(data, testData)

		err = d.WriteBlock(47, data)
		r.NoError(err)

		clear(d.recentReads)

		r.NotEmpty(d.activeTLB)

		d2 := d.NewBlock()

		err = d.ReadBlock(47, d2)
		r.NoError(err)

		r.Equal(d2, data)
	})

	t.Run("can access blocks from the log when the check isn't active", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		data := d.NewBlock()
		copy(data, testData)

		err = d.WriteBlock(47, data)
		r.NoError(err)

		clear(d.recentReads)

		r.NotEmpty(d.activeTLB)

		r.NoError(d.closeChunk())

		d2 := d.NewBlock()

		err = d.ReadBlock(47, d2)
		r.NoError(err)

		blockEqual(t, d2, data)

		r.NotEmpty(d.recentReads)
	})

	t.Run("rebuilds the LBA mappings", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		data := d.NewBlock()
		copy(data, testData)

		err = d.WriteBlock(47, data)
		r.NoError(err)

		clear(d.recentReads)
		clear(d.cacheTLB)

		r.NoError(d.closeChunk())

		r.Empty(d.activeTLB)

		r.NoError(d.rebuild())
		r.NotEmpty(d.cacheTLB)

		d2 := d.NewBlock()

		err = d.ReadBlock(47, d2)
		r.NoError(err)

		blockEqual(t, d2, data)

		r.NotEmpty(d.recentReads)
	})
}
