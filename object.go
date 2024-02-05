package lsvd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/lab47/lz4decode"
	"github.com/oklog/ulid/v2"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
)

type ocBlock struct {
	rng          Extent
	flags        byte
	size, offset uint64
}

type ObjectCreator struct {
	log hclog.Logger
	cnt int

	totalBlocks  int
	storageRatio float64

	volName string

	offset  uint64
	extents []ExtentHeader

	buf    []byte
	header bytes.Buffer
	body   bytes.Buffer

	logF *os.File
	logW *bufio.Writer

	em *ExtentMap

	comp lz4.Compressor
}

func NewObjectCreator(log hclog.Logger, vol, path string) (*ObjectCreator, error) {
	oc := &ObjectCreator{
		log:     log,
		volName: vol,
		em:      NewExtentMap(log),
	}

	err := oc.OpenWrite(path)
	if err != nil {
		return nil, err
	}

	return oc, nil
}

func (o *ObjectCreator) Sync() error {
	if o.logW != nil {
		o.logW.Flush()
	}

	if o.logF != nil {
		return o.logF.Sync()
	}

	return nil
}

func (o *ObjectCreator) OpenWrite(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	err = o.readLog(f)
	if err != nil {
		return errors.Wrapf(err, "error reading object log")
	}

	o.logF = f
	o.logW = bufio.NewWriter(f)

	return nil
}

func emptyBytes(b []byte) bool {
	for len(b) > BlockSize {
		if !bytes.Equal(b[:BlockSize], emptyBlock) {
			return false
		}

		b = b[BlockSize:]
	}

	return bytes.Equal(b, emptyBlock[:len(b)])
}

func (o *ObjectCreator) TotalBlocks() int {
	return o.totalBlocks
}

func (o *ObjectCreator) ZeroBlocks(rng Extent) error {
	// The empty size will signal that it's empty blocks.
	_, err := o.em.Update(rng, OPBA{})
	if err != nil {
		return err
	}
	o.cnt++

	o.extents = append(o.extents, ExtentHeader{
		Extent: rng,
		Flags:  2,
	})

	return nil
}

func (o *ObjectCreator) BodySize() int {
	return o.body.Len()
}

func (o *ObjectCreator) Entries() int {
	return o.cnt
}

func (o *ObjectCreator) AvgStorageRatio() float64 {
	return o.storageRatio / float64(o.cnt)
}

func (o *ObjectCreator) writeLog(
	ext Extent,
	eh ExtentHeader,
	flags byte,
	origSize int,
	data []byte,
) error {
	dw := o.logW

	if eh.Flags == 1 && eh.RawSize == 0 {
		panic("bad header at writeLog")
	}

	err := eh.Write(dw)
	if err != nil {
		return err
	}

	/*
		if err := binary.Write(dw, binary.BigEndian, ext); err != nil {
			return err
		}

		if err := dw.WriteByte(flags); err != nil {
			return err
		}

		if err := binary.Write(dw, binary.BigEndian, uint32(origSize)); err != nil {
			return err
		}

		if err := binary.Write(dw, binary.BigEndian, uint32(len(data))); err != nil {
			return err
		}

	*/
	if n, err := dw.Write(data); err != nil || n != len(data) {
		if err != nil {
			return err
		}

		return fmt.Errorf("short write to log: %d != %d", n, len(data))
	}

	return dw.Flush()
}

func (o *ObjectCreator) readLog(f *os.File) error {
	o.log.Trace("rebuilding memory from log", "path", f.Name())

	br := bufio.NewReader(f)

	for {
		var (
			//ext      Extent
			//flags    byte
			//origSize uint32
			//dataLen  uint32
			//err      error
			eh ExtentHeader
		)

		if err := eh.Read(br); err != nil {
			if errors.Is(err, io.EOF) {
				o.log.Error("observed error reading extent header", "error", err)
				break
			}

			o.log.Error("observed error reading extent header 2", "error", err)
			return err
		}

		o.log.Trace("read extent header", "extent", eh.Extent, "flags", eh.Flags, "raw-size", eh.RawSize)

		o.totalBlocks += int(eh.Blocks)

		if eh.Flags == 2 {
			o.cnt++

			o.extents = append(o.extents, ExtentHeader{
				Extent: eh.Extent,
				Flags:  2,
			})

			// The empty size will signal that it's empty blocks.
			_, err := o.em.Update(eh.Extent, OPBA{Flag: 2, Header: eh})
			if err != nil {
				return errors.Wrapf(err, "error updating extent-map")
			}

			continue
		}

		var headerSz int

		/*
			if eh.Flags == 1 {
				binary.Write(&o.body, binary.BigEndian, uint32(eh.RawSize))

				headerSz = 4
			} else {
				headerSz = 0
			}
		*/

		n, err := io.CopyN(&o.body, br, int64(eh.Size))
		if err != nil {
			return errors.Wrapf(err, "error copying body, expecting %d, got %d", eh.Size, n)
		}
		if n != int64(eh.Size) {
			return fmt.Errorf("short copy: %d != %d", n, eh.Size)
		}

		o.storageRatio += float64(eh.Size) / float64(eh.RawSize)

		o.cnt++

		o.extents = append(o.extents, ExtentHeader{
			Extent: eh.Extent,
			Size:   uint64(headerSz + int(eh.Size)),
			Offset: uint32(o.offset),
		})

		_, err = o.em.Update(eh.Extent, OPBA{
			Offset: uint32(o.offset),
			Size:   uint32(eh.Size),
			Flag:   eh.Flags,
			Header: eh,
		})
		if err != nil {
			return err
		}

		o.offset += uint64(headerSz + int(eh.Size))
	}

	return nil
}

func (o *ObjectCreator) FillExtent(data RangeData) ([]Extent, error) {
	rng := data.Extent

	ranges, err := o.em.Resolve(rng)
	if err != nil {
		return nil, err
	}

	body := o.body.Bytes()

	var ret []Extent

	for _, srcRng := range ranges {
		subDest, ok := data.SubRange(srcRng.Range)
		if !ok {
			o.log.Error("error calculating subrange")
			return nil, fmt.Errorf("error calculating subrange")
		}

		o.log.Trace("calculating relevant ranges",
			"data", rng,
			"src", srcRng.Range,
			"dest", subDest.Extent,
		)

		ret = append(ret, subDest.Extent)

		// It's a empty range
		if srcRng.Size == 0 {
			continue
		}

		srcBytes := body[srcRng.Offset:]

		o.log.Trace("reading partial from write cache", "rng", srcRng.Range, "dest", subDest.Extent)

		var srcData []byte

		if srcRng.Flag == 0 {
			srcData = srcBytes[:srcRng.Size]
		} else if srcRng.Flag == 1 {
			origSize := srcRng.Header.RawSize // binary.BigEndian.Uint32(srcBytes)
			if origSize == 0 {
				panic("missing rawsize")
			}

			srcBytes = srcBytes[:srcRng.Size]

			o.log.Trace("compressed range", "offset", srcRng.Offset)

			if len(o.buf) < int(origSize) {
				o.buf = make([]byte, origSize)
			}

			o.log.Trace("original size of compressed extent", "len", origSize)

			n, err := lz4decode.UncompressBlock(srcBytes, o.buf, nil)
			if err != nil {
				return nil, fmt.Errorf("error uncompressing (src=%d, dest=%d): %w", len(srcBytes), len(o.buf), err)
			}

			if n != int(origSize) {
				return nil, fmt.Errorf("didn't fill destination (%d != %d)", n, origSize)
			}

			srcData = o.buf[:origSize]
		}

		src := RangeData{
			Extent: srcRng.Full,
			BlockData: BlockData{
				blocks: int(srcRng.Size) / BlockSize,
				data:   srcData,
			},
		}

		subSrc, ok := src.SubRange(subDest.Extent)
		if !ok {
			o.log.Error("error calculating src subrange")
			return nil, fmt.Errorf("error calculating src subrange")
		}

		o.log.Trace("mapping src range", "rng", subSrc.Extent)

		n := copy(subDest.data, subSrc.data)

		o.log.Trace("copied range", "bytes", n, "blocks", n/BlockSize)
	}

	return ret, nil
}

func (o *ObjectCreator) WriteExtent(ext RangeData) error {
	if o.buf == nil {
		o.buf = make([]byte, len(ext.data)*2)
	}

	if o.em == nil {
		o.em = NewExtentMap(o.log)
	}

	o.totalBlocks += int(ext.Extent.Blocks)

	rng := ext.Extent

	var data []byte

	eh := ExtentHeader{
		Extent: ext.Extent,
	}

	if emptyBytes(ext.data) {
		o.cnt++

		eh.Flags = 2

		// The empty size will signal that it's empty blocks.
		_, err := o.em.Update(rng, OPBA{
			Flag:   eh.Flags,
			Header: eh,
		})
		if err != nil {
			return err
		}
	} else {
		bound := lz4.CompressBlockBound(len(ext.data))

		if len(o.buf) < bound {
			o.buf = make([]byte, bound)
		}

		sz, err := o.comp.CompressBlock(ext.data, o.buf)
		if err != nil {
			return err
		}

		var headerSz int

		if sz > 0 && sz < len(ext.data) {
			eh.Flags = 1
			eh.RawSize = uint32(len(ext.data))

			// binary.Write(&o.body, binary.BigEndian, uint32(len(ext.data)))

			data = o.buf[:sz]
			o.body.Write(o.buf[:sz])

			headerSz = 0
		} else {
			o.body.Write(ext.data)

			data = ext.data
			sz = len(ext.data)

			headerSz = 0
		}

		o.storageRatio += (float64(sz) / float64(len(ext.data)))

		o.cnt++

		eh.Size = uint64(headerSz + sz)
		eh.Offset = uint32(o.offset)

		o.log.Trace("writing compressed range",
			"offset", o.offset,
			"size", sz,
			"raw-size", eh.RawSize,
		)

		_, err = o.em.Update(rng, OPBA{
			Offset: uint32(o.offset),
			Size:   uint32(sz),
			Flag:   eh.Flags,
			Header: eh,
		})
		if err != nil {
			return err
		}

		o.offset += uint64(headerSz + sz)
	}

	o.extents = append(o.extents, eh)

	return o.writeLog(
		ext.Extent,
		eh,
		eh.Flags,
		len(ext.data),
		data,
	)
}

func (o *ObjectCreator) Reset() {
	o.extents = nil
	o.cnt = 0
	o.offset = 0
	o.totalBlocks = 0
	o.storageRatio = 0
	o.header.Reset()
	o.body.Reset()
}

type objectEntry struct {
	extent Extent
	opba   OPBA
}

type SegmentStats struct {
	Blocks     uint64
	TotalBytes uint64
}

func (o *ObjectCreator) Flush(ctx context.Context,
	sa SegmentAccess, seg SegmentId,
) ([]objectEntry, *SegmentStats, error) {
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

	o.log.Debug("object constructed",
		"header-size", o.header.Len(),
		"body-size", o.offset,
		"blocks", len(o.extents),
	)

	segmentsBytes.Add(float64(o.header.Len() + int(o.offset)))

	entries := make([]objectEntry, len(o.extents))

	for i, eh := range o.extents {
		if eh.Flags == 1 && eh.RawSize == 0 {
			panic("bad header")
		}
		entries[i] = objectEntry{
			extent: eh.Extent,
			opba: OPBA{
				Segment: seg,
				Offset:  dataBegin + uint32(eh.Offset),
				Size:    uint32(eh.Size),
				Flag:    eh.Flags,
				Header:  eh,
			},
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

	err = sa.AppendToObjects(ctx, o.volName, seg)
	if err != nil {
		return nil, nil, err
	}

	if o.logF != nil {
		o.logF.Close()

		o.log.Trace("removing log file", "name", o.logF.Name())
		err = os.Remove(o.logF.Name())
		if err != nil {
			o.log.Error("error removing log file", "error", err)
		}
	}

	return entries, stats, nil
}

type ObjectReader interface {
	io.ReaderAt
	io.Closer

	ReadAtCompressed(b []byte, off, compSize int64) (int, error)
}

type LocalFile struct {
	f *os.File
}

func (l *LocalFile) ReadAt(b []byte, off int64) (int, error) {
	return l.f.ReadAt(b, off)
}

func (l *LocalFile) ReadAtCompressed(dest []byte, off, compSize int64) (int, error) {
	buf := make([]byte, compSize)

	_, err := l.f.ReadAt(buf, off)
	if err != nil {
		return 0, err
	}

	sz, err := lz4decode.UncompressBlock(buf, dest, nil)
	if err != nil {
		return 0, err
	}

	if sz != BlockSize {
		return 0, fmt.Errorf("compressed block uncompressed wrong size (%d != %d)", sz, BlockSize)
	}

	return len(dest), nil
}

func OpenLocalFile(path string) (*LocalFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &LocalFile{f: f}, nil
}

func (l *LocalFile) Close() error {
	return l.f.Close()
}

type LocalFileAccess struct {
	Dir string
}

func (l *LocalFileAccess) OpenSegment(ctx context.Context, seg SegmentId) (ObjectReader, error) {
	return OpenLocalFile(
		filepath.Join(l.Dir, "objects", "object."+ulid.ULID(seg).String()))
}

func ReadSegments(f io.Reader) ([]SegmentId, error) {
	var out []SegmentId

	br := bufio.NewReader(f)

	for {
		var seg SegmentId

		_, err := br.Read(seg[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		out = append(out, seg)
	}

	return out, nil
}

func (l *LocalFileAccess) ListSegments(ctx context.Context, vol string) ([]SegmentId, error) {
	f, err := os.Open(filepath.Join(l.Dir, "volumes", vol, "objects"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}

		return nil, err
	}

	defer f.Close()

	return ReadSegments(f)
}

func (l *LocalFileAccess) WriteMetadata(ctx context.Context, vol, name string) (io.WriteCloser, error) {
	f, err := os.Create(filepath.Join(l.Dir, "volumes", vol, name))
	return f, err
}

func (l *LocalFileAccess) ReadMetadata(ctx context.Context, vol, name string) (io.ReadCloser, error) {
	f, err := os.Open(filepath.Join(l.Dir, "volumes", vol, name))
	return f, err
}

func (l *LocalFileAccess) RemoveSegment(ctx context.Context, seg SegmentId) error {
	return os.Remove(
		filepath.Join(l.Dir, "objects", "object."+ulid.ULID(seg).String()))
}

func (l *LocalFileAccess) WriteSegment(ctx context.Context, seg SegmentId) (io.WriteCloser, error) {
	path := filepath.Join(l.Dir, "objects", "object."+ulid.ULID(seg).String())
	return os.Create(path)
}

func (l *LocalFileAccess) AppendToObjects(ctx context.Context, vol string, seg SegmentId) error {
	segments, err := l.ListSegments(ctx, vol)
	if err != nil {
		return err
	}

	path := filepath.Join(l.Dir, "volumes", vol, "objects")

	segments = append(segments, seg)

	f, err := os.Create(path)
	if err != nil {
		return err
	}

	defer f.Close()

	bw := bufio.NewWriter(f)

	defer bw.Flush()

	for _, seg := range segments {
		bw.Write(seg[:])
	}

	return nil
}

func (l *LocalFileAccess) RemoveSegmentFromVolume(ctx context.Context, vol string, seg SegmentId) error {
	var buf bytes.Buffer

	objectsPath := filepath.Join(l.Dir, "volumes", vol, "objects")
	f, err := os.OpenFile(objectsPath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}

	defer f.Close()

	var cur SegmentId

	for {
		_, err = f.Read(cur[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return err
		}

		if seg != cur {
			buf.Write(cur[:])
		}
	}

	f.Close()

	f, err = os.Create(objectsPath)
	if err != nil {
		return err
	}

	io.Copy(f, &buf)

	return nil
}

type VolumeInfo struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func (l *LocalFileAccess) InitContainer(ctx context.Context) error {
	for _, p := range []string{"objects", "volumes"} {
		path := filepath.Join(l.Dir, p)
		fi, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			err = os.Mkdir(path, 0755)
			if err != nil {
				return err
			}
		} else if !fi.Mode().IsDir() {
			return fmt.Errorf("sub path not a directory: %s", path)
		}
	}

	return nil
}

func (l *LocalFileAccess) InitVolume(ctx context.Context, vol *VolumeInfo) error {
	if vol.Name == "" {
		return fmt.Errorf("volume name must not be empty")
	}

	path := filepath.Join(l.Dir, "volumes", vol.Name)

	_, err := os.Stat(path)
	if err == nil {
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	err = os.MkdirAll(path, 0755)
	if err != nil {
		return err
	}

	f, err := os.Create(filepath.Join(path, "info.json"))
	if err != nil {
		return err
	}

	defer f.Close()

	return json.NewEncoder(f).Encode(&vol)
}

func (l *LocalFileAccess) ListVolumes(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(l.Dir, "volumes"))
	if err != nil {
		return nil, err
	}

	var volumes []string

	for _, ent := range entries {
		volumes = append(volumes, ent.Name())
	}

	return volumes, nil
}

func (l *LocalFileAccess) GetVolumeInfo(ctx context.Context, vol string) (*VolumeInfo, error) {
	f, err := os.Open(filepath.Join("volumes", vol, "info.json"))
	if err != nil {
		return nil, err
	}

	defer f.Close()

	var vi VolumeInfo
	err = json.NewDecoder(f).Decode(&vi)
	if err != nil {
		return nil, err
	}

	return &vi, nil
}

type SegmentAccess interface {
	InitContainer(ctx context.Context) error
	InitVolume(ctx context.Context, vol *VolumeInfo) error
	ListVolumes(ctx context.Context) ([]string, error)
	GetVolumeInfo(ctx context.Context, vol string) (*VolumeInfo, error)

	ListSegments(ctx context.Context, vol string) ([]SegmentId, error)
	OpenSegment(ctx context.Context, seg SegmentId) (ObjectReader, error)
	WriteSegment(ctx context.Context, seg SegmentId) (io.WriteCloser, error)

	RemoveSegment(ctx context.Context, seg SegmentId) error
	RemoveSegmentFromVolume(ctx context.Context, vol string, seg SegmentId) error
	WriteMetadata(ctx context.Context, vol, name string) (io.WriteCloser, error)
	ReadMetadata(ctx context.Context, vol, name string) (io.ReadCloser, error)

	AppendToObjects(ctx context.Context, volume string, seg SegmentId) error
}

var _ SegmentAccess = (*LocalFileAccess)(nil)

type ReaderAtAsReader struct {
	f   io.ReaderAt
	off int64
}

func (r *ReaderAtAsReader) Read(b []byte) (int, error) {
	n, err := r.f.ReadAt(b, r.off)
	r.off += int64(n)
	return n, err
}

func ToReader(ra io.ReaderAt) io.Reader {
	return &ReaderAtAsReader{
		f: ra,
	}
}
