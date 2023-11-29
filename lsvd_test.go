package lsvd

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/oklog/ulid/v2"
	"github.com/pierrec/lz4/v4"
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

	testRand  = make([]byte, 4*1024)
	testRandX Extent

	testEmpty  = make([]byte, BlockSize)
	testEmptyX Extent
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

	io.ReadFull(rand.Reader, testRand)
	testRandX = ExtentView(testRand)

	testEmptyX = ExtentView(testEmpty)
}

func blockEqual(t *testing.T, a, b []byte) {
	t.Helper()
	if !bytes.Equal(a, b) {
		t.Error("blocks are not the same")
	}
	//require.True(t, bytes.Equal(a, b), "blocks are not the same")
}

func extentEqual(t *testing.T, a, b Extent) {
	t.Helper()

	require.Equal(t, a.Blocks(), b.Blocks())

	if !bytes.Equal(a.data, b.data) {
		t.Error("blocks are not the same")
	}
}

func TestLSVD(t *testing.T) {
	log := hclog.New(&hclog.LoggerOptions{
		Name:  "lsvdtest",
		Level: hclog.Trace,
	})

	testUlid := ulid.MustNew(ulid.Now(), rand.Reader)

	t.Run("reads with no data return zeros", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		data := NewExtent(1)

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

		d2 := NewExtent(1)

		err = d.ReadExtent(0, d2)
		r.NoError(err)

		extentEqual(t, d2, testExtent)
	})

	t.Run("stale reads aren't returned", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(0, testExtent)
		r.NoError(err)

		r.NoError(d.Close())

		d, err = NewDisk(log, tmpdir)
		r.NoError(err)

		d2 := NewExtent(1)

		err = d.ReadExtent(0, d2)
		r.NoError(err)

		extentEqual(t, d2, testExtent)

		err = d.WriteExtent(0, testExtent2)
		r.NoError(err)

		d3 := NewExtent(1)

		err = d.ReadExtent(0, d3)
		r.NoError(err)

		extentEqual(t, d3, testExtent2)
	})

	t.Run("writes are written to a log file", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		d.SeqGen = func() ulid.ULID {
			return testUlid
		}

		err = d.WriteExtent(47, testExtent)
		r.NoError(err)

		f, err := os.Open(filepath.Join(tmpdir, "writecache."+testUlid.String()))
		r.NoError(err)

		defer f.Close()

		var hdr SegmentHeader

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

		blk := NewExtent(1)

		n, err := io.ReadFull(io.TeeReader(f, h), blk.BlockView(0))
		r.NoError(err)

		r.Equal(BlockSize, n)

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

	t.Run("writes written out to an object", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		d.SeqGen = func() ulid.ULID {
			return testUlid
		}

		err = d.WriteExtent(47, testExtent)
		r.NoError(err)

		r.NoError(d.Close())

		f, err := os.Open(filepath.Join(tmpdir, "object."+testUlid.String()))
		r.NoError(err)

		defer f.Close()

		br := bufio.NewReader(f)

		var cnt uint32
		err = binary.Read(br, binary.BigEndian, &cnt)
		r.NoError(err)

		r.Equal(uint32(1), cnt)

		var hdrLen uint32
		err = binary.Read(br, binary.BigEndian, &hdrLen)
		r.NoError(err)

		r.Equal(uint32(4+8), hdrLen)

		lba, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(47), lba)

		flags, err := br.ReadByte()
		r.NoError(err)

		r.Equal(byte(1), flags)

		blkSize, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(0x28), blkSize)

		offset, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(0), offset)

		_, err = f.Seek(int64(uint64(hdrLen)+offset), io.SeekStart)
		r.NoError(err)

		view := make([]byte, BlockSize)

		buf := make([]byte, blkSize)

		_, err = io.ReadFull(f, buf)
		r.NoError(err)

		sz, err := lz4.UncompressBlock(buf, view)
		r.NoError(err)

		view = view[:sz]

		blockEqual(t, testData, view)
	})

	t.Run("objects that can't be compressed are flagged", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		d.SeqGen = func() ulid.ULID {
			return testUlid
		}

		err = d.WriteExtent(47, testRandX)
		r.NoError(err)

		r.NoError(d.Close())

		f, err := os.Open(filepath.Join(tmpdir, "object."+testUlid.String()))
		r.NoError(err)

		defer f.Close()

		br := bufio.NewReader(f)

		var cnt uint32
		err = binary.Read(br, binary.BigEndian, &cnt)
		r.NoError(err)

		r.Equal(uint32(1), cnt)

		var hdrLen uint32
		err = binary.Read(br, binary.BigEndian, &hdrLen)
		r.NoError(err)

		r.Equal(uint32(5+8), hdrLen)

		lba, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(47), lba)

		flags, err := br.ReadByte()
		r.NoError(err)

		r.Equal(byte(0), flags)

		blkSize, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(BlockSize), blkSize)

		offset, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(0), offset)

		_, err = f.Seek(int64(uint64(hdrLen)+offset), io.SeekStart)
		r.NoError(err)

		view := make([]byte, BlockSize)

		_, err = io.ReadFull(f, view)
		r.NoError(err)

		blockEqual(t, testRand, view)
	})

	t.Run("empty blocks are flagged specially", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		d.SeqGen = func() ulid.ULID {
			return testUlid
		}

		err = d.WriteExtent(47, testEmptyX)
		r.NoError(err)

		r.NoError(d.Close())

		f, err := os.Open(filepath.Join(tmpdir, "object."+testUlid.String()))
		r.NoError(err)

		defer f.Close()

		br := bufio.NewReader(f)

		var cnt uint32
		err = binary.Read(br, binary.BigEndian, &cnt)
		r.NoError(err)

		r.Equal(uint32(1), cnt)

		var hdrLen uint32
		err = binary.Read(br, binary.BigEndian, &hdrLen)
		r.NoError(err)

		r.Equal(uint32(4+8), hdrLen)

		lba, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(47), lba)

		flags, err := br.ReadByte()
		r.NoError(err)

		r.Equal(byte(2), flags)

		blkSize, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(0), blkSize)

		offset, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(0), offset)
	})

	t.Run("reads empty from a previous empty write", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		data := NewExtent(1)
		data.SetBlock(0, testRand)

		err = d.WriteExtent(0, testEmptyX)
		r.NoError(err)

		err = d.ReadExtent(0, data)
		r.NoError(err)

		r.True(isEmpty(data.BlockView(0)))

		data.SetBlock(0, testRand)

		err = d.ReadExtent(0, data)
		r.NoError(err)

		r.True(isEmpty(data.BlockView(0)))

		r.NoError(d.Close())

		d, err = NewDisk(log, tmpdir)
		r.NoError(err)

		data.SetBlock(0, testRand)

		err = d.ReadExtent(0, data)
		r.NoError(err)

		r.True(isEmpty(data.BlockView(0)))
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

		r.NotEmpty(d.wcOffsets)

		d2 := NewExtent(1)

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

		r.NotEmpty(d.wcOffsets)

		err = d.CloseSegment()
		r.NoError(err)

		d2 := NewExtent(1)

		err = d.ReadExtent(47, d2)
		r.NoError(err)

		blockEqual(t, d2.BlockView(0), testExtent.BlockView(0))
	})

	t.Run("segments contain the parent of their actual parent", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		d.SeqGen = func() ulid.ULID {
			return testUlid
		}

		err = d.WriteExtent(47, testExtent)
		r.NoError(err)

		err = d.CloseSegment()
		r.NoError(err)

		err = d.WriteExtent(48, testExtent2)
		r.NoError(err)
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

		d.lba2obj.Clear()

		err = d.CloseSegment()
		r.NoError(err)

		r.Empty(d.wcOffsets)
		d.lba2obj.Clear()

		r.NoError(d.rebuildFromObjects())
		r.NotZero(d.lba2obj.Len())

		_, ok := d.lba2obj.Get(47)
		r.True(ok)

		d2 := NewExtent(1)

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

		err = d.CloseSegment()
		r.NoError(err)

		r.NoError(d.saveLBAMap())

		f, err := os.Open(filepath.Join(tmpdir, "head.map"))
		r.NoError(err)

		defer f.Close()

		m, err := processLBAMap(f)
		r.NoError(err)

		_, ok := m.Get(47)
		r.True(ok)
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

		err = d.CloseSegment()
		r.NoError(err)

		r.NoError(d.Close())

		d2, err := NewDisk(log, tmpdir)
		r.NoError(err)

		r.NotZero(d2.lba2obj.Len())
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

		err = d.CloseSegment()
		r.NoError(err)

		r.NoError(d.saveLBAMap())

		r.NoError(d.WriteExtent(48, testExtent2))

		disk2, err := NewDisk(log, tmpdir)
		r.NoError(err)

		r.NotEmpty(disk2.wcOffsets)

		r.Equal(uint32(headerSize), disk2.wcOffsets[48])
	})

	t.Run("with multiple blocks", func(t *testing.T) {
		t.Run("writes are returned by next read", func(t *testing.T) {
			r := require.New(t)

			tmpdir, err := os.MkdirTemp("", "lsvd")
			r.NoError(err)
			defer os.RemoveAll(tmpdir)

			d, err := NewDisk(log, tmpdir)
			r.NoError(err)

			data := NewExtent(2)
			copy(data.BlockView(0), testData)
			copy(data.BlockView(1), testData)

			err = d.WriteExtent(0, data)
			r.NoError(err)

			d2 := NewExtent(1)

			err = d.ReadExtent(1, d2)
			r.NoError(err)

			blockEqual(t, d2.BlockView(0), testData)

			d3 := NewExtent(1)

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

			data := NewExtent(2)
			copy(data.BlockView(0), testData)
			copy(data.BlockView(1), testData)

			err = d.WriteExtent(0, data)
			r.NoError(err)

			d2 := NewExtent(2)

			err = d.ReadExtent(0, d2)
			r.NoError(err)

			blockEqual(t, d2.BlockView(0), testData)
			blockEqual(t, d2.BlockView(1), testData)

			d3 := NewExtent(1)

			err = d.ReadExtent(0, d3)
			r.NoError(err)

			blockEqual(t, d2.BlockView(0), testData)
			blockEqual(t, d2.BlockView(1), testData)
		})

	})

	t.Run("writes to the same block return the most recent", func(t *testing.T) {
		t.Run("in the same instance", func(t *testing.T) {
			r := require.New(t)

			tmpdir, err := os.MkdirTemp("", "lsvd")
			r.NoError(err)
			defer os.RemoveAll(tmpdir)

			d, err := NewDisk(log, tmpdir)
			r.NoError(err)

			err = d.WriteExtent(0, testExtent)
			r.NoError(err)

			err = d.WriteExtent(0, testExtent2)
			r.NoError(err)

			d2 := NewExtent(1)

			err = d.ReadExtent(0, d2)
			r.NoError(err)

			extentEqual(t, d2, testExtent2)
		})

		t.Run("in a different instance", func(t *testing.T) {
			r := require.New(t)

			tmpdir, err := os.MkdirTemp("", "lsvd")
			r.NoError(err)
			defer os.RemoveAll(tmpdir)

			d, err := NewDisk(log, tmpdir)
			r.NoError(err)

			err = d.WriteExtent(0, testExtent)
			r.NoError(err)

			err = d.WriteExtent(0, testExtent2)
			r.NoError(err)

			r.NoError(d.Close())

			d2, err := NewDisk(log, tmpdir)
			r.NoError(err)

			x2 := NewExtent(1)

			err = d2.ReadExtent(0, x2)
			r.NoError(err)

			extentEqual(t, x2, testExtent2)
		})

		t.Run("in a when recovering active", func(t *testing.T) {
			r := require.New(t)

			tmpdir, err := os.MkdirTemp("", "lsvd")
			r.NoError(err)
			defer os.RemoveAll(tmpdir)

			d, err := NewDisk(log, tmpdir)
			r.NoError(err)

			err = d.WriteExtent(0, testExtent)
			r.NoError(err)

			err = d.WriteExtent(0, testExtent2)
			r.NoError(err)

			d2, err := NewDisk(log, tmpdir)
			r.NoError(err)

			x2 := NewExtent(1)

			err = d2.ReadExtent(0, x2)
			r.NoError(err)

			extentEqual(t, x2, testExtent2)
		})

		t.Run("across segments", func(t *testing.T) {
			r := require.New(t)

			tmpdir, err := os.MkdirTemp("", "lsvd")
			r.NoError(err)
			defer os.RemoveAll(tmpdir)

			d, err := NewDisk(log, tmpdir)
			r.NoError(err)

			err = d.WriteExtent(0, testExtent)
			r.NoError(err)

			err = d.CloseSegment()
			r.NoError(err)

			err = d.WriteExtent(0, testExtent2)
			r.NoError(err)

			r.NoError(d.Close())

			d2, err := NewDisk(log, tmpdir)
			r.NoError(err)

			x2 := NewExtent(1)

			err = d2.ReadExtent(0, x2)
			r.NoError(err)

			extentEqual(t, x2, testExtent2)
		})

		t.Run("across segments without a lba map", func(t *testing.T) {
			r := require.New(t)

			tmpdir, err := os.MkdirTemp("", "lsvd")
			r.NoError(err)
			defer os.RemoveAll(tmpdir)

			d, err := NewDisk(log, tmpdir)
			r.NoError(err)

			err = d.WriteExtent(0, testExtent)
			r.NoError(err)

			err = d.CloseSegment()
			r.NoError(err)

			err = d.WriteExtent(0, testExtent2)
			r.NoError(err)

			r.NoError(d.Close())

			r.NoError(os.Remove(filepath.Join(tmpdir, "head.map")))

			d2, err := NewDisk(log, tmpdir)
			r.NoError(err)

			x2 := NewExtent(1)

			err = d2.ReadExtent(0, x2)
			r.NoError(err)

			extentEqual(t, x2, testExtent2)
		})

		t.Run("across and within segments without a lba map", func(t *testing.T) {
			r := require.New(t)

			tmpdir, err := os.MkdirTemp("", "lsvd")
			r.NoError(err)
			defer os.RemoveAll(tmpdir)

			d, err := NewDisk(log, tmpdir)
			r.NoError(err)

			err = d.WriteExtent(0, testExtent)
			r.NoError(err)

			err = d.CloseSegment()
			r.NoError(err)

			err = d.WriteExtent(0, testExtent2)
			r.NoError(err)

			err = d.WriteExtent(0, testExtent3)
			r.NoError(err)

			r.NoError(d.Close())

			r.NoError(os.Remove(filepath.Join(tmpdir, "head.map")))

			d2, err := NewDisk(log, tmpdir)
			r.NoError(err)

			x2 := NewExtent(1)

			err = d2.ReadExtent(0, x2)
			r.NoError(err)

			extentEqual(t, x2, testExtent3)
		})
	})

	t.Run("gc pulls still used blocks out of the oldest segment and moves them forward", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(log, tmpdir)
		r.NoError(err)

		origSeq := ulid.MustNew(ulid.Now(), monoRead)

		d.SeqGen = func() ulid.ULID {
			return origSeq
		}

		err = d.WriteExtent(0, testExtent)
		r.NoError(err)

		err = d.WriteExtent(1, testExtent2)
		r.NoError(err)

		d.SeqGen = nil

		err = d.CloseSegment()
		r.NoError(err)

		err = d.WriteExtent(0, testExtent3)
		r.NoError(err)

		err = d.CloseSegment()
		r.NoError(err)

		gcSeg, err := d.GCOnce()
		r.NoError(err)

		r.Equal(SegmentId(origSeq), gcSeg)

		_, err = os.Stat(filepath.Join(tmpdir, "object."+origSeq.String()))
		r.ErrorIs(err, os.ErrNotExist)

		d.Close()

		d2, err := NewDisk(log, tmpdir)
		r.NoError(err)

		x2 := NewExtent(1)

		err = d2.ReadExtent(0, x2)
		r.NoError(err)

		extentEqual(t, x2, testExtent3)

		err = d2.ReadExtent(1, x2)
		r.NoError(err)

		extentEqual(t, x2, testExtent2)
	})
}

func emptyBytesI(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}

	return true
}
func BenchmarkEmptyInline(b *testing.B) {
	for i := 0; i < b.N; i++ {
		emptyBytesI(emptyBlock)
	}
}

func emptyBytes2(b []byte) bool {
	y := byte(0)
	for _, x := range b {
		y |= x
	}

	return y == 0
}

func BenchmarkEmptyInline2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		emptyBytes2(emptyBlock)
	}
}

var local = make([]byte, BlockSize)

func BenchmarkEmptyEqual(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bytes.Equal(local, emptyBlock)
	}
}
