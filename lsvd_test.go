package lsvd

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/hashicorp/go-hclog"
	"github.com/lab47/lz4decode"
	"github.com/oklog/ulid/v2"
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

	testExtent  BlockData
	testExtent2 BlockData
	testExtent3 BlockData

	testRand  = make([]byte, 4*1024)
	testRandX BlockData

	testEmpty  = make([]byte, BlockSize)
	testEmptyX BlockData
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

	testExtent = BlockDataView(testData)
	testExtent2 = BlockDataView(testData2)
	testExtent3 = BlockDataView(testData3)

	io.ReadFull(rand.Reader, testRand)
	testRandX = BlockDataView(testRand)

	testEmptyX = BlockDataView(testEmpty)
}

func blockEqual(t *testing.T, a, b []byte) {
	t.Helper()
	if !bytes.Equal(a, b) {
		t.Error("blocks are not the same")
	}
	//require.True(t, bytes.Equal(a, b), "blocks are not the same")
}

func extentEqual(t *testing.T, actual BlockData, expected RangeData) {
	t.Helper()

	require.Equal(t, actual.Blocks(), expected.BlockData.Blocks())

	if !bytes.Equal(actual.data, expected.data) {
		t.Error("blocks are not the same")
	}
}

func TestLSVD(t *testing.T) {
	log := hclog.New(&hclog.LoggerOptions{
		Name:  "lsvdtest",
		Level: hclog.Trace,
	})

	testUlid := ulid.MustNew(ulid.Now(), rand.Reader)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("reads with no data return zeros", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		data, err := d.ReadExtent(ctx, Extent{LBA: 1, Blocks: 1})
		r.NoError(err)

		r.True(isEmpty(data.data))
	})

	t.Run("writes are returned by next read", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(ctx, testRandX.MapTo(0))
		r.NoError(err)

		d2, err := d.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
		r.NoError(err)

		extentEqual(t, testRandX, d2)
	})

	t.Run("can read from across writes from the write cache", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		data := NewRangeData(Extent{0, 10})
		_, err = io.ReadFull(rand.Reader, data.data)
		r.NoError(err)

		err = d.WriteExtent(ctx, data)
		r.NoError(err)

		err = d.WriteExtent(ctx, testRandX.MapTo(1))
		r.NoError(err)

		d2, err := d.ReadExtent(ctx, Extent{LBA: 0, Blocks: 4})
		r.NoError(err)

		n := d2.data
		blockEqual(t, data.data[:BlockSize], n[:BlockSize])
		n = n[BlockSize:]
		blockEqual(t, testRandX.data[:BlockSize], n[:BlockSize])
		n = n[BlockSize:]
		blockEqual(t, data.data[BlockSize*2:BlockSize*4], n)
	})

	t.Run("can read the middle of an write cache range", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		data := NewRangeData(Extent{1, 19})
		_, err = io.ReadFull(rand.Reader, data.data)
		r.NoError(err)

		err = d.WriteExtent(ctx, data)
		r.NoError(err)

		d2, err := d.ReadExtent(ctx, Extent{LBA: 4, Blocks: 8})
		r.NoError(err)

		blockEqual(t, data.data[BlockSize*3:BlockSize*11], d2.data)
	})

	t.Run("can read from objects", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(ctx, testRandX.MapTo(0))
		r.NoError(err)

		r.NoError(d.Close(ctx))

		t.Log("reopening disk")
		d, err = NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		d2, err := d.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
		r.NoError(err)

		extentEqual(t, testRandX, d2)

		t.Run("and from the read cache", func(t *testing.T) {
			r := require.New(t)

			d2, err := d.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
			r.NoError(err)

			extentEqual(t, testRandX, d2)
		})
	})

	t.Run("can read from partial objects", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		big := make([]byte, 4*4*1024)

		_, err = io.ReadFull(rand.Reader, big)
		r.NoError(err)

		err = d.WriteExtent(ctx, BlockDataView(big).MapTo(0))
		r.NoError(err)

		r.NoError(d.Close(ctx))

		d, err = NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		d2, err := d.ReadExtent(ctx, Extent{LBA: 1, Blocks: 1})
		r.NoError(err)

		blockEqual(t, d2.data, big[BlockSize:BlockSize+BlockSize])

		t.Run("and from the read cache", func(t *testing.T) {
			r := require.New(t)

			d2, err := d.ReadExtent(ctx, Extent{LBA: 1, Blocks: 1})
			r.NoError(err)

			blockEqual(t, d2.data, big[BlockSize:BlockSize+BlockSize])
		})
	})

	t.Run("writes to clear blocks don't corrupt the cache", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		r.NoError(d.WriteExtent(ctx, testEmptyX.MapTo(47)))

		err = d.WriteExtent(ctx, testRandX.MapTo(0))
		r.NoError(err)

		d2, err := d.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
		r.NoError(err)

		extentEqual(t, testRandX, d2)
	})

	t.Run("stale reads aren't returned", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent.MapTo(0))
		r.NoError(err)

		r.NoError(d.Close(ctx))

		d, err = NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		d2, err := d.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
		r.NoError(err)

		extentEqual(t, testExtent, d2)

		err = d.WriteExtent(ctx, testExtent2.MapTo(0))
		r.NoError(err)

		d3, err := d.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
		r.NoError(err)

		extentEqual(t, testExtent2, d3)
	})

	/*
		t.Run("writes are written to a log file", func(t *testing.T) {
			r := require.New(t)

			tmpdir, err := os.MkdirTemp("", "lsvd")
			r.NoError(err)
			defer os.RemoveAll(tmpdir)

			d, err := NewDisk(ctx, log, tmpdir)
			r.NoError(err)

			d.SeqGen = func() ulid.ULID {
				return testUlid
			}

			err = d.WriteExtent(ctx, 47, testExtent)
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
		})
	*/

	t.Run("writes written out to an object", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir, WithSeqGen(func() ulid.ULID {
			return testUlid
		}))
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent.MapTo(47))
		r.NoError(err)

		r.NoError(d.Close(ctx))

		f, err := os.Open(filepath.Join(tmpdir, "objects", "object."+testUlid.String()))
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

		r.Equal(uint32(4+9), hdrLen)

		lba, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(47), lba)

		blocks, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(1), blocks)

		flags, err := br.ReadByte()
		r.NoError(err)

		r.Equal(byte(0), flags)

		blkSize, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(0x2d), blkSize)

		offset, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(0), offset)

		_, err = f.Seek(int64(uint64(hdrLen)+offset), io.SeekStart)
		r.NoError(err)

		view := make([]byte, BlockSize)

		buf := make([]byte, blkSize)

		_, err = io.ReadFull(f, buf)
		r.NoError(err)

		r.Equal(byte(1), buf[0])

		uncompSize := binary.BigEndian.Uint32(buf[1:])
		r.Equal(uint32(BlockSize), uncompSize)

		buf = buf[5:]

		sz, err := lz4decode.UncompressBlock(buf, view, nil)
		r.NoError(err)

		view = view[:sz]

		blockEqual(t, testData, view)

		g, err := os.Open(filepath.Join(tmpdir, "volumes", "default", "objects"))
		r.NoError(err)

		defer g.Close()

		gbr := bufio.NewReader(g)

		var iseg SegmentId

		_, err = gbr.Read(iseg[:])
		r.NoError(err)

		r.Equal(ulid.ULID(iseg), testUlid)
	})

	t.Run("objects that can't be compressed are flagged", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir, WithSeqGen(func() ulid.ULID {
			return testUlid
		}))
		r.NoError(err)

		err = d.WriteExtent(ctx, testRandX.MapTo(47))
		r.NoError(err)

		r.NoError(d.Close(ctx))

		f, err := os.Open(filepath.Join(tmpdir, "objects", "object."+testUlid.String()))
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

		r.Equal(uint32(5+9), hdrLen)

		lba, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(47), lba)

		bloccks, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(1), bloccks)

		flags, err := br.ReadByte()
		r.NoError(err)

		r.Equal(byte(0), flags)

		blkSize, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(BlockSize+1), blkSize)

		offset, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(0), offset)

		_, err = f.Seek(int64(uint64(hdrLen)+offset), io.SeekStart)
		r.NoError(err)

		view := make([]byte, blkSize)

		_, err = io.ReadFull(f, view)
		r.NoError(err)

		r.Equal(byte(0), view[0])

		blockEqual(t, testRand, view[1:])

		d2, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		x1, err := d2.ReadExtent(ctx, Extent{LBA: 47, Blocks: 1})
		r.NoError(err)

		extentEqual(t, testRandX, x1)
	})

	t.Run("empty blocks are flagged specially", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir, WithSeqGen(func() ulid.ULID {
			return testUlid
		}))
		r.NoError(err)

		err = d.WriteExtent(ctx, testEmptyX.MapTo(47))
		r.NoError(err)

		r.NoError(d.Close(ctx))

		f, err := os.Open(filepath.Join(tmpdir, "objects", "object."+testUlid.String()))
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

		r.Equal(uint32(4+9), hdrLen)

		lba, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(47), lba)

		blocks, err := binary.ReadUvarint(br)
		r.NoError(err)

		r.Equal(uint64(1), blocks)

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

		d, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(ctx, testEmptyX.MapTo(0))
		r.NoError(err)

		data, err := d.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
		r.NoError(err)

		r.True(isEmpty(data.BlockView(0)))

		data, err = d.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
		r.NoError(err)

		r.True(isEmpty(data.BlockView(0)))

		r.NoError(d.Close(ctx))

		d, err = NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		data.SetBlock(0, testRand)

		data, err = d.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
		r.NoError(err)

		r.True(isEmpty(data.BlockView(0)))
	})

	t.Run("can access blocks from the log", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent.MapTo(47))
		r.NoError(err)

		d2, err := d.ReadExtent(ctx, Extent{LBA: 47, Blocks: 1})
		r.NoError(err)

		extentEqual(t, testExtent, d2)
	})

	t.Run("can access blocks from the log when the check isn't active", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent.MapTo(47))
		r.NoError(err)

		err = d.CloseSegment(ctx)
		r.NoError(err)

		d2, err := d.ReadExtent(ctx, Extent{LBA: 47, Blocks: 1})
		r.NoError(err)

		blockEqual(t, d2.BlockView(0), testExtent.BlockView(0))
	})

	t.Run("segments contain the parent of their actual parent", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir, WithSeqGen(func() ulid.ULID {
			return testUlid
		}))
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent.MapTo(47))
		r.NoError(err)

		err = d.CloseSegment(ctx)
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent2.MapTo(48))
		r.NoError(err)
	})

	t.Run("rebuilds the LBA mappings", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent.MapTo(47))
		r.NoError(err)

		d.lba2pba.m.Clear()

		err = d.CloseSegment(ctx)
		r.NoError(err)

		d.lba2pba.m.Clear()

		r.NoError(d.rebuildFromObjects(ctx))
		r.NotZero(d.lba2pba.Len())

		_, ok := d.lba2pba.m.Get(47)
		r.True(ok)

		d2, err := d.ReadExtent(ctx, Extent{LBA: 47, Blocks: 1})
		r.NoError(err)

		blockEqual(t, d2.BlockView(0), testData)
	})

	t.Run("serializes the lba to pba mapping", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent.MapTo(47))
		r.NoError(err)

		err = d.CloseSegment(ctx)
		r.NoError(err)

		r.NoError(d.saveLBAMap(ctx))

		f, err := os.Open(filepath.Join(tmpdir, "head.map"))
		r.NoError(err)

		defer f.Close()

		m, err := processLBAMap(log, f)
		r.NoError(err)

		_, ok := m.m.Get(47)
		r.True(ok)
	})

	t.Run("reuses serialized lba to pba map on start", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent.MapTo(47))
		r.NoError(err)

		r.NoError(d.Close(ctx))

		d2, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		r.NotZero(d2.lba2pba.Len())
	})

	t.Run("replays logs into l2p map if need be on load", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent.MapTo(47))
		r.NoError(err)

		err = d.CloseSegment(ctx)
		r.NoError(err)

		r.NoError(d.saveLBAMap(ctx))

		r.NoError(d.WriteExtent(ctx, testExtent2.MapTo(48)))

		t.Log("reloading disk hot")

		d.extentCache.Close()

		disk2, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		d2, err := disk2.ReadExtent(ctx, Extent{48, 1})
		r.NoError(err)

		blockEqual(t, testExtent2.data, d2.data)

		//r.NotEmpty(disk2.wcOffsets)

		//r.Equal(uint32(headerSize), disk2.wcOffsets[48])
	})

	t.Run("with multiple blocks", func(t *testing.T) {
		t.Run("writes are returned by next read", func(t *testing.T) {
			r := require.New(t)

			tmpdir, err := os.MkdirTemp("", "lsvd")
			r.NoError(err)
			defer os.RemoveAll(tmpdir)

			d, err := NewDisk(ctx, log, tmpdir)
			r.NoError(err)

			data := NewRangeData(Extent{0, 2})
			copy(data.BlockView(0), testData)
			copy(data.BlockView(1), testData)

			err = d.WriteExtent(ctx, data)
			r.NoError(err)

			d2, err := d.ReadExtent(ctx, Extent{LBA: 1, Blocks: 1})
			r.NoError(err)

			blockEqual(t, d2.BlockView(0), testData)

			d3, err := d.ReadExtent(ctx, Extent{LBA: 1, Blocks: 1})
			r.NoError(err)

			blockEqual(t, d3.BlockView(0), testData)
		})

		t.Run("reads can return multiple blocks", func(t *testing.T) {
			r := require.New(t)

			tmpdir, err := os.MkdirTemp("", "lsvd")
			r.NoError(err)
			defer os.RemoveAll(tmpdir)

			d, err := NewDisk(ctx, log, tmpdir)
			r.NoError(err)

			data := NewRangeData(Extent{0, 2})
			copy(data.BlockView(0), testData)
			copy(data.BlockView(1), testData)

			err = d.WriteExtent(ctx, data)
			r.NoError(err)

			d2, err := d.ReadExtent(ctx, Extent{LBA: 0, Blocks: 2})
			r.NoError(err)

			blockEqual(t, d2.BlockView(0), testData)
			blockEqual(t, d2.BlockView(1), testData)

			d3, err := d.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
			r.NoError(err)

			blockEqual(t, d3.BlockView(0), testData)
			blockEqual(t, d2.BlockView(1), testData)
		})

	})

	t.Run("writes to the same block return the most recent", func(t *testing.T) {
		t.Run("in the same instance", func(t *testing.T) {
			r := require.New(t)

			tmpdir, err := os.MkdirTemp("", "lsvd")
			r.NoError(err)
			defer os.RemoveAll(tmpdir)

			d, err := NewDisk(ctx, log, tmpdir)
			r.NoError(err)

			err = d.WriteExtent(ctx, testExtent.MapTo(0))
			r.NoError(err)

			err = d.WriteExtent(ctx, testExtent2.MapTo(0))
			r.NoError(err)

			d2, err := d.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
			r.NoError(err)

			extentEqual(t, testExtent2, d2)
		})

		t.Run("in a different instance", func(t *testing.T) {
			r := require.New(t)

			tmpdir, err := os.MkdirTemp("", "lsvd")
			r.NoError(err)
			defer os.RemoveAll(tmpdir)

			d, err := NewDisk(ctx, log, tmpdir)
			r.NoError(err)

			err = d.WriteExtent(ctx, testExtent.MapTo(0))
			r.NoError(err)

			err = d.WriteExtent(ctx, testExtent2.MapTo(0))
			r.NoError(err)

			r.NoError(d.Close(ctx))

			ents, err := os.ReadDir(tmpdir)
			r.NoError(err)
			spew.Dump(ents)

			t.Log("reopening disk")

			d2, err := NewDisk(ctx, log, tmpdir)
			r.NoError(err)

			x2, err := d2.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
			r.NoError(err)

			extentEqual(t, testExtent2, x2)
		})

		t.Run("in a when recovering active", func(t *testing.T) {
			r := require.New(t)

			tmpdir, err := os.MkdirTemp("", "lsvd")
			r.NoError(err)
			defer os.RemoveAll(tmpdir)

			d, err := NewDisk(ctx, log, tmpdir)
			r.NoError(err)

			err = d.WriteExtent(ctx, testExtent.MapTo(0))
			r.NoError(err)

			err = d.WriteExtent(ctx, testExtent2.MapTo(0))
			r.NoError(err)

			d.extentCache.Close()

			d2, err := NewDisk(ctx, log, tmpdir)
			r.NoError(err)

			x2, err := d2.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
			r.NoError(err)

			extentEqual(t, testExtent2, x2)
		})

		t.Run("across segments", func(t *testing.T) {
			r := require.New(t)

			tmpdir, err := os.MkdirTemp("", "lsvd")
			r.NoError(err)
			defer os.RemoveAll(tmpdir)

			d, err := NewDisk(ctx, log, tmpdir)
			r.NoError(err)

			err = d.WriteExtent(ctx, testExtent.MapTo(0))
			r.NoError(err)

			err = d.CloseSegment(ctx)
			r.NoError(err)

			err = d.WriteExtent(ctx, testExtent2.MapTo(0))
			r.NoError(err)

			r.NoError(d.Close(ctx))

			d2, err := NewDisk(ctx, log, tmpdir)
			r.NoError(err)

			x2, err := d2.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
			r.NoError(err)

			extentEqual(t, testExtent2, x2)
		})

		t.Run("across segments without a lba map", func(t *testing.T) {
			r := require.New(t)

			tmpdir, err := os.MkdirTemp("", "lsvd")
			r.NoError(err)
			defer os.RemoveAll(tmpdir)

			d, err := NewDisk(ctx, log, tmpdir)
			r.NoError(err)

			err = d.WriteExtent(ctx, testExtent.MapTo(0))
			r.NoError(err)

			err = d.CloseSegment(ctx)
			r.NoError(err)

			err = d.WriteExtent(ctx, testExtent2.MapTo(0))
			r.NoError(err)

			r.NoError(d.Close(ctx))

			r.NoError(os.Remove(filepath.Join(tmpdir, "head.map")))

			d2, err := NewDisk(ctx, log, tmpdir)
			r.NoError(err)

			x2, err := d2.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
			r.NoError(err)

			extentEqual(t, testExtent2, x2)
		})

		t.Run("across and within segments without a lba map", func(t *testing.T) {
			r := require.New(t)

			tmpdir, err := os.MkdirTemp("", "lsvd")
			r.NoError(err)
			defer os.RemoveAll(tmpdir)

			d, err := NewDisk(ctx, log, tmpdir)
			r.NoError(err)

			err = d.WriteExtent(ctx, testExtent.MapTo(0))
			r.NoError(err)

			err = d.CloseSegment(ctx)
			r.NoError(err)

			err = d.WriteExtent(ctx, testExtent2.MapTo(0))
			r.NoError(err)

			err = d.WriteExtent(ctx, testExtent3.MapTo(0))
			r.NoError(err)

			r.NoError(d.Close(ctx))

			r.NoError(os.Remove(filepath.Join(tmpdir, "head.map")))

			d2, err := NewDisk(ctx, log, tmpdir)
			r.NoError(err)

			x2, err := d2.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
			r.NoError(err)

			extentEqual(t, testExtent3, x2)
		})
	})

	t.Run("gc pulls still used blocks out of the oldest segment and moves them forward", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		origSeq := ulid.MustNew(ulid.Now(), monoRead)

		d, err := NewDisk(ctx, log, tmpdir, WithSeqGen(func() ulid.ULID {
			return origSeq
		}))
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent.MapTo(0))
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent2.MapTo(1))
		r.NoError(err)

		d.SeqGen = nil

		err = d.CloseSegment(ctx)
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent3.MapTo(0))
		r.NoError(err)

		err = d.CloseSegment(ctx)
		r.NoError(err)

		gcSeg, err := d.GCOnce(ctx)
		r.NoError(err)

		r.Equal(SegmentId(origSeq), gcSeg)

		_, err = os.Stat(filepath.Join(tmpdir, "objects", "object."+origSeq.String()))
		r.ErrorIs(err, os.ErrNotExist)

		d.Close(ctx)

		t.Log("reloading disk")

		d2, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		x2, err := d2.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
		r.NoError(err)

		extentEqual(t, testExtent3, x2)

		x2, err = d2.ReadExtent(ctx, Extent{LBA: 1, Blocks: 1})
		r.NoError(err)

		extentEqual(t, testExtent2, x2)
	})

	t.Run("zero blocks works like an empty write", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		err = d.WriteExtent(ctx, testRandX.MapTo(0))
		r.NoError(err)

		err = d.ZeroBlocks(ctx, Extent{0, 1})
		r.NoError(err)

		d2, err := d.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
		r.NoError(err)

		extentEqual(t, testEmptyX, d2)
	})

	t.Run("can use the write cache while currently uploading", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		var sa slowLocal

		sa.Dir = tmpdir
		sa.wait = make(chan struct{})

		d, err := NewDisk(ctx, log, tmpdir, WithSegmentAccess(&sa))
		r.NoError(err)

		err = d.WriteExtent(ctx, testRandX.MapTo(0))
		r.NoError(err)

		_, err = d.closeSegmentAsync(ctx)
		r.NoError(err)

		time.Sleep(100 * time.Millisecond)

		r.True(sa.waiting)

		d2, err := d.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
		r.NoError(err)

		extentEqual(t, testRandX, d2)
	})

	t.Run("reads partly from both write caches and an object", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		var sa slowLocal

		sa.Dir = tmpdir

		d, err := NewDisk(ctx, log, tmpdir, WithSegmentAccess(&sa))
		r.NoError(err)

		data := NewRangeData(Extent{0, 10})
		_, err = io.ReadFull(rand.Reader, data.data)
		r.NoError(err)

		err = d.WriteExtent(ctx, data)
		r.NoError(err)

		err = d.CloseSegment(ctx)
		r.NoError(err)

		err = d.WriteExtent(ctx, testRandX.MapTo(0))
		r.NoError(err)

		sa.wait = make(chan struct{})

		_, err = d.closeSegmentAsync(ctx)
		r.NoError(err)

		time.Sleep(100 * time.Millisecond)

		r.True(sa.waiting)

		err = d.WriteExtent(ctx, testExtent.MapTo(1))
		r.NoError(err)

		t.Log("performing read")
		d2, err := d.ReadExtent(ctx, Extent{LBA: 0, Blocks: 4})
		r.NoError(err)

		blockEqual(t, testRandX.data, d2.data[:BlockSize])
		blockEqual(t, testExtent.data, d2.data[BlockSize:BlockSize*2])
		blockEqual(t,
			data.data[BlockSize*2:BlockSize*4],
			d2.data[BlockSize*2:BlockSize*4],
		)
	})
}

type slowLocal struct {
	LocalFileAccess
	waiting bool
	wait    chan struct{}
}

func (s *slowLocal) WriteSegment(ctx context.Context, seg SegmentId) (io.WriteCloser, error) {
	s.waiting = true

	if s.wait != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.wait:
			// ok
		}
	}

	return s.LocalFileAccess.WriteSegment(ctx, seg)
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
