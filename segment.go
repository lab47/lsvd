package lsvd

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/klauspost/compress/zstd"
	"github.com/lab47/lz4decode"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
)

type SegmentCreator struct {
	log hclog.Logger

	builder SegmentBuilder

	volName string

	buf []byte

	logF *os.File
	logW *bufio.Writer

	em *ExtentMap
}

type SegmentBuilder struct {
	cnt int

	emptyBlocks   int
	totalBlocks   int
	inputBytes    int64
	storageBytes  int64
	storageRatio  float64
	compRateHisto [10]int

	buf    []byte
	header bytes.Buffer
	body   bytes.Buffer

	offset  uint64
	extents []ExtentHeader

	comp    lz4.Compressor
	useZstd bool
}

var histogramBands = []float64{1, 2, 3, 5, 10, 20, 50, 100, 200, 1000}

func NewSegmentCreator(log hclog.Logger, vol, path string) (*SegmentCreator, error) {
	oc := &SegmentCreator{
		log:     log,
		volName: vol,
		em:      NewExtentMap(),
	}

	err := oc.OpenWrite(path)
	if err != nil {
		return nil, err
	}

	return oc, nil
}

func (o *SegmentCreator) UseZstd() {
	o.builder.useZstd = true
}

func (o *SegmentBuilder) addToHistogram(val float64) {
	for i, v := range histogramBands {
		if v >= val {
			o.compRateHisto[i]++
			return
		}
	}
}

func (o *SegmentCreator) Sync() error {
	if o.logW != nil {
		o.logW.Flush()
	}

	if o.logF != nil {
		return o.logF.Sync()
	}

	return nil
}

func (o *SegmentCreator) OpenWrite(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	err = o.readLog(f)
	if err != nil {
		return errors.Wrapf(err, "error reading segment log")
	}

	o.logF = f
	o.logW = bufio.NewWriter(f)

	return nil
}

func (o *SegmentCreator) TotalBlocks() int {
	return o.builder.totalBlocks
}

func (o *SegmentCreator) ZeroBlocks(rng Extent) error {
	// The empty size will signal that it's empty blocks.
	_, err := o.em.Update(o.log, ExtentLocation{
		ExtentHeader: ExtentHeader{
			Extent: rng,
		},
	})
	if err != nil {
		return err
	}

	return o.builder.ZeroBlocks(rng)
}

func (o *SegmentBuilder) ZeroBlocks(rng Extent) error {
	o.cnt++

	o.extents = append(o.extents, ExtentHeader{
		Extent: rng,
		Flags:  Empty,
	})

	return nil
}

func (o *SegmentBuilder) ShouldFlush(sizeThreshold int) bool {
	return o.BodySize() >= sizeThreshold
}

func (o *SegmentBuilder) BodySize() int {
	return o.body.Len()
}

func (o *SegmentCreator) ShouldFlush(sizeThreshold int) bool {
	return o.BodySize() >= sizeThreshold
}

func (o *SegmentCreator) BodySize() int {
	return o.builder.body.Len()
}

func (o *SegmentCreator) Entries() int {
	return o.builder.cnt
}

func (o *SegmentCreator) EmptyBlocks() int {
	return o.builder.emptyBlocks
}

func (o *SegmentCreator) InputBytes() int64 {
	return o.builder.inputBytes
}

func (o *SegmentCreator) StorageBytes() int64 {
	return o.builder.storageBytes
}

func (o *SegmentCreator) CompressionRate() float64 {
	return float64(o.builder.inputBytes) / float64(o.builder.storageBytes)
}

func (o *SegmentCreator) StorageRatio() float64 {
	return float64(o.builder.storageBytes) / float64(o.builder.inputBytes)
}

func (o *SegmentCreator) AvgStorageRatio() float64 {
	return o.builder.storageRatio / float64(o.builder.cnt)
}

func (o *SegmentCreator) CompressionRateHistogram() []int {
	return o.builder.compRateHisto[:]
}

// writeLog writes the header and the data to the log so that we can
// recover the write with readLog if need be.
func (o *SegmentCreator) writeLog(
	eh ExtentHeader,
	data []byte,
) error {
	dw := o.logW

	if eh.Flags == Compressed && eh.RawSize == 0 {
		panic("bad header at writeLog")
	}

	err := eh.Write(dw)
	if err != nil {
		return err
	}

	if n, err := dw.Write(data); err != nil || n != len(data) {
		if err != nil {
			return err
		}

		return fmt.Errorf("short write to log: %d != %d", n, len(data))
	}

	return dw.Flush()
}

// readLog is used to restore the state of the SegmentCreator from the
// log written to data.
func (o *SegmentCreator) readLog(f *os.File) error {
	o.log.Trace("rebuilding memory from log", "path", f.Name())

	br := bufio.NewReader(f)

	for {
		var eh ExtentHeader

		if err := eh.Read(br); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			o.log.Error("observed error reading extent header", "error", err)
			return err
		}

		o.log.Trace("read extent header", "extent", eh.Extent, "flags", eh.Flags, "raw-size", eh.RawSize)

		o.builder.totalBlocks += int(eh.Blocks)

		o.builder.cnt++

		if eh.Size > 0 {
			n, err := io.CopyN(&o.builder.body, br, int64(eh.Size))
			if err != nil {
				return errors.Wrapf(err, "error copying body, expecting %d, got %d", eh.Size, n)
			}
			if n != int64(eh.Size) {
				return fmt.Errorf("short copy: %d != %d", n, eh.Size)
			}

			if eh.RawSize > 0 {
				o.builder.storageRatio += float64(eh.Size) / float64(eh.RawSize)
			} else {
				o.builder.storageRatio += 1
			}

			// Update offset to match where it is in body
			eh.Offset = uint32(o.builder.offset)
		}

		o.builder.extents = append(o.builder.extents, eh)

		_, err := o.em.Update(o.log, ExtentLocation{
			ExtentHeader: eh,
		})
		if err != nil {
			return err
		}

		o.builder.offset += eh.Size
	}

	return nil
}

// FillExtent attempts to fill as much of +data+ as possible, returning
// a list of Extents that was unable to fill. That later list is then
// feed to the system that reads data from segments.
func (o *SegmentCreator) FillExtent(data RangeDataView) ([]Extent, error) {
	rng := data.Extent

	ranges, err := o.em.Resolve(o.log, rng)
	if err != nil {
		return nil, err
	}

	body := o.builder.body.Bytes()

	var ret []Extent

	for _, srcRng := range ranges {
		subDest, ok := data.SubRange(srcRng.Live)
		if !ok {
			o.log.Error("error calculating subrange")
			return nil, fmt.Errorf("error calculating subrange")
		}

		o.log.Trace("calculating relevant ranges",
			"data", rng,
			"src", srcRng.Live,
			"dest", subDest.Extent,
			"offset", srcRng.Offset,
		)

		ret = append(ret, subDest.Extent)

		// It's a empty range
		if srcRng.Size == 0 {
			continue
		}

		srcBytes := body[srcRng.Offset:]

		o.log.Trace("reading partial from write cache", "rng", srcRng.Live, "dest", subDest.Extent, "flags", srcRng.Flags)

		var srcData []byte

		switch srcRng.Flags {
		case Uncompressed:
			srcData = srcBytes[:srcRng.Size]
		case Compressed:
			origSize := srcRng.RawSize // binary.BigEndian.Uint32(srcBytes)
			if origSize == 0 {
				panic("missing rawsize")
			}

			srcBytes = srcBytes[:srcRng.Size]

			o.log.Trace("compressed range", "offset", srcRng.Offset)

			if len(o.buf) < int(origSize) {
				o.buf = make([]byte, origSize)
			}

			o.log.Trace("original size of compressed extent", "len", origSize, "comp size", srcRng.Size)

			n, err := lz4decode.UncompressBlock(srcBytes, o.buf, nil)
			if err != nil {
				return nil, fmt.Errorf("error uncompressing (src=%d, dest=%d): %w", len(srcBytes), len(o.buf), err)
			}

			if n > int(origSize) {
				o.log.Warn("unusual long write detected", "expected", origSize, "actual", n, "buf-len", len(o.buf))
			} else if n < int(origSize) {
				return nil, fmt.Errorf("didn't fill destination (%d != %d)", n, origSize)
			}

			srcData = o.buf[:origSize]
		case ZstdCompressed:
			origSize := srcRng.RawSize // binary.BigEndian.Uint32(srcBytes)
			if origSize == 0 {
				panic("missing rawsize")
			}

			srcBytes = srcBytes[:srcRng.Size]

			o.log.Trace("compressed range", "offset", srcRng.Offset)

			if len(o.buf) < int(origSize) {
				o.buf = make([]byte, origSize)
			}

			o.log.Trace("original size of compressed extent", "len", origSize, "comp size", srcRng.Size)

			d, err := zstd.NewReader(nil)
			if err != nil {
				return nil, err
			}

			res, err := d.DecodeAll(srcBytes, o.buf[:0])
			if err != nil {
				return nil, errors.Wrapf(err, "error uncompressing data (rawsize: %d, compdata: %d)", len(srcBytes), origSize)
			}

			n := len(res)

			if n != int(origSize) {
				return nil, fmt.Errorf("didn't fill destination (%d != %d)", n, origSize)
			}

			srcData = o.buf[:origSize]
		case Empty:
			// handled above, shouldn't be here.
			return nil, fmt.Errorf("invalid flag 2, should have size == 0, did not")
		default:
			return nil, fmt.Errorf("invalid flag %d", srcRng.Flags)
		}

		src := MapRangeData(srcRng.Extent, srcData)

		subSrc, ok := src.SubRange(subDest.Extent)
		if !ok {
			o.log.Error("error calculating src subrange")
			return nil, fmt.Errorf("error calculating src subrange")
		}

		o.log.Trace("mapping src range", "rng", subSrc.Extent)

		n := subDest.Copy(subSrc)

		o.log.Trace("copied range", "bytes", n, "blocks", n/BlockSize)
	}

	return ret, nil
}

func (o *SegmentCreator) WriteExtent(ext RangeData) error {
	data, eh, err := o.builder.WriteExtent(o.log, ext)
	if err != nil {
		return err
	}

	if o.em == nil {
		o.em = NewExtentMap()
	}

	_, err = o.em.Update(o.log, ExtentLocation{
		ExtentHeader: eh,
	})

	if err != nil {
		return err
	}

	return o.writeLog(
		eh,
		data,
	)
}

type SegmentStats struct {
	Blocks     uint64
	TotalBytes uint64
}

func (o *SegmentCreator) Flush(ctx context.Context,
	sa SegmentAccess, seg SegmentId,
) ([]ExtentLocation, *SegmentStats, error) {
	locs, stats, err := o.builder.Flush(ctx, o.log, sa, seg, o.volName)
	if err != nil {
		return locs, stats, err
	}

	if o.logF != nil {
		o.logF.Close()

		o.log.Trace("removing log file", "name", o.logF.Name())
		err = os.Remove(o.logF.Name())
		if err != nil {
			o.log.Error("error removing log file", "error", err)
		}
	}

	return locs, stats, nil
}

func (o *SegmentBuilder) WriteExtent(log hclog.Logger, ext RangeData) ([]byte, ExtentHeader, error) {
	extBytes := ext.ByteSize()
	if o.buf == nil {
		o.buf = make([]byte, extBytes*2)
	}

	o.totalBlocks += int(ext.Extent.Blocks)
	o.inputBytes += int64(len(ext.data))

	var data []byte

	eh := ExtentHeader{
		Extent: ext.Extent,
	}

	o.cnt++

	if ext.EmptyP() {
		eh.Flags = Empty
		o.emptyBlocks += int(ext.Extent.Blocks)
	} else {
		bound := lz4.CompressBlockBound(extBytes)

		if len(o.buf) < bound {
			o.buf = make([]byte, bound)
		}
		var (
			compressedSize int
			err            error
			flag           byte
		)

		if o.useZstd {
			e, err := zstd.NewWriter(nil)
			if err != nil {
				return nil, eh, err
			}
			res := e.EncodeAll(ext.ReadData(), o.buf[:0])

			compressedSize = len(res)
			flag = ZstdCompressed
		} else {
			compressedSize, err = o.comp.CompressBlock(ext.ReadData(), o.buf)
			if err != nil {
				return nil, eh, err
			}
			flag = Compressed
		}

		if compressedSize > 0 && compressedSize < extBytes {
			eh.Flags = flag
			eh.RawSize = uint32(extBytes)
			eh.Size = uint64(compressedSize)

			data = o.buf[:compressedSize]

			log.Trace("writing compressed range",
				"offset", o.offset,
				"size", eh.Size,
				"input-size", eh.RawSize,
			)

			o.addToHistogram(float64(len(ext.data)) / float64(len(data)))
		} else {
			eh.Flags = Uncompressed
			eh.Size = uint64(extBytes)

			data = ext.ReadData()

			log.Trace("writing uncompressed range",
				"offset", o.offset,
				"size", eh.Size,
				"raw-size", eh.RawSize,
			)
			o.addToHistogram(1)
		}

		o.storageBytes += int64(len(data))
		o.storageRatio += (float64(len(data)) / float64(len(ext.data)))

		n, err := o.body.Write(data)
		if err != nil {
			return nil, eh, err
		}

		eh.Offset = uint32(o.offset)

		o.offset += uint64(n)
	}

	o.extents = append(o.extents, eh)

	return data, eh, nil
}

func (o *SegmentBuilder) Flush(ctx context.Context, log hclog.Logger,
	sa SegmentAccess, seg SegmentId, volName string,
) ([]ExtentLocation, *SegmentStats, error) {
	start := time.Now()
	defer func() {
		segmentTime.Observe(time.Since(start).Seconds())
	}()

	stats := &SegmentStats{}

	for _, blk := range o.extents {
		stats.Blocks += uint64(blk.Blocks)
		err := blk.Write(&o.header)
		if err != nil {
			return nil, nil, err
		}
	}

	dataBegin := uint32(o.header.Len() + 8)

	log.Debug("segment constructed",
		"header-size", o.header.Len(),
		"body-size", o.offset,
		"blocks", len(o.extents),
	)

	writtenBytes.Add(float64(o.inputBytes))
	segmentsBytes.Add(float64(o.storageBytes))

	entries := make([]ExtentLocation, len(o.extents))

	for i, eh := range o.extents {
		if eh.Flags == Compressed && eh.RawSize == 0 {
			panic("bad header")
		}

		eh.Offset += dataBegin
		entries[i] = ExtentLocation{
			ExtentHeader: eh,
			Segment:      seg,
		}
	}

	f, err := sa.WriteSegment(ctx, seg)
	if err != nil {
		return nil, nil, err
	}

	defer f.Close()

	err = SegmentHeader{
		ExtentCount: uint32(o.cnt),
		DataOffset:  dataBegin,
	}.Write(f)
	if err != nil {
		return nil, nil, err
	}

	_, err = io.Copy(f, bytes.NewReader(o.header.Bytes()))
	if err != nil {
		return nil, nil, err
	}

	_, err = io.Copy(f, bytes.NewReader(o.body.Bytes()))
	if err != nil {
		return nil, nil, err
	}

	f.Close()

	err = sa.AppendToSegments(ctx, volName, seg)
	if err != nil {
		return nil, nil, err
	}

	return entries, stats, nil
}
