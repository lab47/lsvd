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
	"github.com/mr-tron/base58"
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

var (
	testData  = make([]byte, 4*1024)
	testData2 = make([]byte, 4*1024)
	testData3 = make([]byte, 4*1024)

	testExtent  Extent
	testExtent2 Extent
	testExtent3 Extent
)

func init() {
	for i := 0; i < 10; i++ {
		testData[i] = 0x47
	}

	for i := 0; i < 10; i++ {
		testData2[i] = 0x48
	}

	for i := 0; i < 10; i++ {
		testData3[i] = 0x49
	}

	testExtent = ExtentView(testData)
	testExtent2 = ExtentView(testData2)
	testExtent3 = ExtentView(testData3)
}

func blockEqual(t *testing.T, a, b []byte) {
	t.Helper()
	if !bytes.Equal(a, b) {
		t.Error("blocks are not the same")
	}
	//require.True(t, bytes.Equal(a, b), "blocks are not the same")
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

		data := d.NewExtent(1)

		err = d.ReadExtent(0, data)
		r.NoError(err)

		r.True(isEmpty(data.BlockView(0)))
	})

	t.Run("writes are returned by next read", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(0, testExtent)
		r.NoError(err)

		d2 := d.NewExtent(1)

		err = d.ReadExtent(0, d2)
		r.NoError(err)

		r.Equal(d2, testExtent)
	})

	t.Run("writes are written to a log file", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(47, testExtent)
		r.NoError(err)

		f, err := os.Open(filepath.Join(tmpdir, "log.active"))
		r.NoError(err)

		defer f.Close()

		var hdr logHeader

		r.NoError(binary.Read(f, binary.BigEndian, &hdr))

		var lba, crc uint64

		h := crc64.New(crc64.MakeTable(crc64.ECMA))

		binary.Write(h, binary.BigEndian, uint64(0))
		binary.Write(h, binary.BigEndian, uint64(0))
		h.Write(empty[:])
		binary.Write(h, binary.BigEndian, hdr.CreatedAt)

		f.Seek(int64(headerSize), io.SeekStart)

		r.NoError(binary.Read(io.TeeReader(f, h), binary.BigEndian, &crc))
		r.NoError(binary.Read(io.TeeReader(f, h), binary.BigEndian, &lba))

		r.Equal(uint64(47), lba)

		blk := d.NewExtent(1)

		n, err := io.ReadFull(io.TeeReader(f, h), blk.BlockView(0))
		r.NoError(err)

		r.Equal(d.BlockSize, n)

		r.Equal(testExtent, blk)

		t.Logf("crc: %d", h.Sum64())

		t.Run("and are tracked for reading back", func(t *testing.T) {
			r := require.New(t)
			pba, ok := d.cacheTranslate(47)
			r.True(ok)

			r.Equal(empty, pba.Segment)
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

		err = d.WriteExtent(47, testExtent)
		r.NoError(err)

		d.l1cache.Purge()

		r.NotEmpty(d.activeTLB)

		d2 := d.NewExtent(1)

		err = d.ReadExtent(47, d2)
		r.NoError(err)

		r.Equal(testExtent, d2)
	})

	t.Run("can access blocks from the log when the check isn't active", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(47, testExtent)
		r.NoError(err)

		d.l1cache.Purge()

		r.NotEmpty(d.activeTLB)

		r.NoError(d.closeSegment())

		d2 := d.NewExtent(1)

		err = d.ReadExtent(47, d2)
		r.NoError(err)

		blockEqual(t, d2.BlockView(0), testExtent.BlockView(0))
	})

	t.Run("rebuilds the LBA mappings", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(47, testExtent)
		r.NoError(err)

		d.l1cache.Purge()
		d.lba2disk.Clear()

		r.NoError(d.closeSegment())

		r.Empty(d.activeTLB)
		d.lba2disk.Clear()

		r.NoError(d.rebuild())
		r.NotZero(d.lba2disk.Len())

		pba, ok := d.lba2disk.Get(47)
		r.True(ok)

		r.Equal(uint32(headerSize), pba.Offset)

		d2 := d.NewExtent(1)

		err = d.ReadExtent(47, d2)
		r.NoError(err)

		blockEqual(t, d2.BlockView(0), testData)
	})

	t.Run("serializes the lba to pba mapping", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(47, testExtent)
		r.NoError(err)

		r.NoError(d.closeSegment())

		r.NoError(d.saveLBAMap())

		f, err := os.Open(filepath.Join(tmpdir, "head.map"))
		r.NoError(err)

		defer f.Close()

		m, err := processLBAMap(f)
		r.NoError(err)

		pba, ok := m.Get(47)
		r.True(ok)

		headName, err := os.ReadFile(filepath.Join(tmpdir, "head"))
		r.NoError(err)

		cdata, err := base58.Decode(string(headName))
		r.NoError(err)

		r.Equal(SegmentId(cdata), pba.Segment)
		r.Equal(uint32(headerSize), pba.Offset)
	})

	t.Run("reuses serialized lba to pba map on start", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(47, testExtent)
		r.NoError(err)

		r.NoError(d.closeSegment())

		r.NoError(d.Close())

		d2, err := NewDisk(log, tmpdir)
		r.NoError(err)

		r.NotZero(d2.lba2disk.Len())
	})

	t.Run("replays logs into l2p map if need be on load", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(47, testExtent)
		r.NoError(err)

		r.NoError(d.closeSegment())

		r.NoError(d.saveLBAMap())

		r.NoError(d.WriteExtent(48, testExtent2))

		disk2, err := NewDisk(log, tmpdir)
		r.NoError(err)

		r.NotEmpty(disk2.activeTLB)

		r.Equal(uint32(headerSize), disk2.activeTLB[48])
	})

	t.Run("with multiple blocks", func(t *testing.T) {
		t.Run("writes are returned by next read", func(t *testing.T) {
			r := require.New(t)

			tmpdir, err := os.MkdirTemp("", "lsvd")
			r.NoError(err)
			defer os.RemoveAll(tmpdir)

			d, err := NewDisk(log, tmpdir)
			r.NoError(err)

			data := d.NewExtent(2)
			copy(data.BlockView(0), testData)
			copy(data.BlockView(1), testData)

			err = d.WriteExtent(0, data)
			r.NoError(err)

			d2 := d.NewExtent(1)

			err = d.ReadExtent(1, d2)
			r.NoError(err)

			blockEqual(t, d2.BlockView(0), testData)

			d3 := d.NewExtent(1)

			d.l1cache.Purge()

			err = d.ReadExtent(1, d3)
			r.NoError(err)

			blockEqual(t, d3.BlockView(0), testData)
		})

		t.Run("reads can return multiple blocks", func(t *testing.T) {
			r := require.New(t)

			tmpdir, err := os.MkdirTemp("", "lsvd")
			r.NoError(err)
			defer os.RemoveAll(tmpdir)

			d, err := NewDisk(log, tmpdir)
			r.NoError(err)

			data := d.NewExtent(2)
			copy(data.BlockView(0), testData)
			copy(data.BlockView(1), testData)

			err = d.WriteExtent(0, data)
			r.NoError(err)

			d2 := d.NewExtent(2)

			err = d.ReadExtent(0, d2)
			r.NoError(err)

			blockEqual(t, d2.BlockView(0), testData)
			blockEqual(t, d2.BlockView(1), testData)

			d3 := d.NewExtent(1)

			d.l1cache.Purge()

			err = d.ReadExtent(0, d3)
			r.NoError(err)

			blockEqual(t, d2.BlockView(0), testData)
			blockEqual(t, d2.BlockView(1), testData)
		})

	})
}
