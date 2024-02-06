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

func (o *ObjectCreator) TotalBlocks() int {
	return o.totalBlocks
}

func (o *ObjectCreator) ZeroBlocks(rng Extent) error {
	// The empty size will signal that it's empty blocks.
	_, err := o.em.Update(ExtentLocation{
		ExtentHeader: ExtentHeader{
			Extent: rng,
		},
	})
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
	eh ExtentHeader,
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
		var eh ExtentHeader

		if err := eh.Read(br); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			o.log.Error("observed error reading extent header", "error", err)
			return err
		}

		o.log.Trace("read extent header", "extent", eh.Extent, "flags", eh.Flags, "raw-size", eh.RawSize)

		o.totalBlocks += int(eh.Blocks)

		o.cnt++

		if eh.Size > 0 {
			n, err := io.CopyN(&o.body, br, int64(eh.Size))
			if err != nil {
				return errors.Wrapf(err, "error copying body, expecting %d, got %d", eh.Size, n)
			}
			if n != int64(eh.Size) {
				return fmt.Errorf("short copy: %d != %d", n, eh.Size)
			}

			if eh.RawSize > 0 {
				o.storageRatio += float64(eh.Size) / float64(eh.RawSize)
			} else {
				o.storageRatio += 1
			}

			// Update offset to match where it is in body
			eh.Offset = uint32(o.offset)
		}

		o.extents = append(o.extents, eh)

		_, err := o.em.Update(ExtentLocation{
			ExtentHeader: eh,
		})
		if err != nil {
			return err
		}

		o.offset += eh.Size
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
		subDest, ok := data.SubRange(srcRng.Partial)
		if !ok {
			o.log.Error("error calculating subrange")
			return nil, fmt.Errorf("error calculating subrange")
		}

		o.log.Trace("calculating relevant ranges",
			"data", rng,
			"src", srcRng.Partial,
			"dest", subDest.Extent,
			"offset", srcRng.Offset,
		)

		ret = append(ret, subDest.Extent)

		// It's a empty range
		if srcRng.Size == 0 {
			continue
		}

		srcBytes := body[srcRng.Offset:]

		o.log.Trace("reading partial from write cache", "rng", srcRng.Partial, "dest", subDest.Extent, "flags", srcRng.Flags)

		var srcData []byte

		switch srcRng.Flags {
		case 0:
			srcData = srcBytes[:srcRng.Size]
		case 1:
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

			if n != int(origSize) {
				return nil, fmt.Errorf("didn't fill destination (%d != %d)", n, origSize)
			}

			srcData = o.buf[:origSize]
		case 2:
			// handled above, shouldn't be here.
			return nil, fmt.Errorf("invalid flag 2, should have size == 0, did not")
		default:
			return nil, fmt.Errorf("invalid flag %d", srcRng.Flags)
		}

		src := RangeData{
			Extent: srcRng.Extent,
			BlockData: BlockData{
				blocks: len(srcData) / BlockSize,
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

	var data []byte

	eh := ExtentHeader{
		Extent: ext.Extent,
	}

	o.cnt++

	if emptyBytes(ext.data) {
		eh.Flags = 2
	} else {
		bound := lz4.CompressBlockBound(len(ext.data))

		if len(o.buf) < bound {
			o.buf = make([]byte, bound)
		}

		compressedSize, err := o.comp.CompressBlock(ext.data, o.buf)
		if err != nil {
			return err
		}

		if compressedSize > 0 && compressedSize < len(ext.data) {
			eh.Flags = 1
			eh.RawSize = uint32(len(ext.data))
			eh.Size = uint64(compressedSize)

			data = o.buf[:compressedSize]

			o.log.Trace("writing compressed range",
				"offset", o.offset,
				"size", eh.Size,
			)
		} else {
			eh.Flags = 0
			eh.Size = uint64(len(ext.data))

			data = ext.data

			o.log.Trace("writing uncompressed range",
				"offset", o.offset,
				"size", eh.Size,
				"raw-size", eh.RawSize,
			)
		}

		n, err := o.body.Write(data)
		if err != nil {
			return err
		}

		o.storageRatio += (float64(n) / float64(len(ext.data)))

		eh.Offset = uint32(o.offset)
		_, err = o.em.Update(ExtentLocation{
			ExtentHeader: eh,
		})
		if err != nil {
			return err
		}

		o.offset += uint64(n)
	}

	o.extents = append(o.extents, eh)

	_, err := o.em.Update(ExtentLocation{
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

func (o *ObjectCreator) Flush(ctx context.Context,
	sa SegmentAccess, seg SegmentId,
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

	o.log.Debug("object constructed",
		"header-size", o.header.Len(),
		"body-size", o.offset,
		"blocks", len(o.extents),
	)

	segmentsBytes.Add(float64(o.header.Len() + int(o.offset)))

	entries := make([]ExtentLocation, len(o.extents))

	for i, eh := range o.extents {
		if eh.Flags == 1 && eh.RawSize == 0 {
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
}

type LocalFile struct {
	f *os.File
}

func (l *LocalFile) ReadAt(b []byte, off int64) (int, error) {
	return l.f.ReadAt(b, off)
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
