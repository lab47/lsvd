package lsvd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/oklog/ulid/v2"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
)

type ocBlock struct {
	lba          LBA
	count        int
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
	extents []ocBlock

	buf    []byte
	header bytes.Buffer
	body   bytes.Buffer

	logF *os.File
	logW *bufio.Writer

	em *ExtentMap
}

func NewObjectCreator(log hclog.Logger, vol string) *ObjectCreator {
	return &ObjectCreator{
		log:     log,
		volName: vol,
		em:      NewExtentMap(log),
	}
}

func (o *ObjectCreator) OpenWrite(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
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

func (o *ObjectCreator) ZeroBlocks(firstBlock LBA, numBlocks int64) error {
	rng := Extent{LBA: firstBlock, Blocks: uint32(numBlocks)}

	// The empty size will signal that it's empty blocks.
	err := o.em.Update(rng, OPBA{})
	if err != nil {
		return err
	}
	o.cnt++

	o.extents = append(o.extents, ocBlock{
		lba:   firstBlock,
		count: int(numBlocks),
		flags: 2,
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

type serializedExtent struct {
	ext   Extent
	empty bool

	compressed bool
	origSize   int
	data       []byte

	ratio float64
}

func (o *ObjectCreator) PrepareExtent(firstBlock LBA, ext BlockData) (*serializedExtent, error) {
	if o.buf == nil {
		o.buf = make([]byte, len(ext.data)*2)
	}

	o.totalBlocks += ext.Blocks()

	rng := Extent{LBA: firstBlock, Blocks: uint32(ext.Blocks())}
	if emptyBytes(ext.data) {
		return &serializedExtent{
			ext:   rng,
			empty: true,
		}, nil
	}

	bound := lz4.CompressBlockBound(len(ext.data))

	if len(o.buf) < bound {
		o.buf = make([]byte, bound)
	}

	sz, err := lz4.CompressBlock(ext.data, o.buf, nil)
	if err != nil {
		return nil, err
	}

	if sz > 0 && sz < len(ext.data) {
		return &serializedExtent{
			ext:        rng,
			compressed: true,
			origSize:   len(ext.data),
			data:       o.buf[:sz],
			ratio:      (float64(sz) / float64(len(ext.data))),
		}, nil
	} else {
		return &serializedExtent{
			ext:      rng,
			origSize: len(ext.data),
			data:     ext.data,
			ratio:    1,
		}, nil
	}
}

func (o *ObjectCreator) writeLog(
	ext Extent,
	flags byte,
	origSize int,
	data []byte,
) error {
	dw := o.logW
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
			ext      Extent
			flags    byte
			origSize uint32
			dataLen  uint32
			err      error
		)

		if err := binary.Read(br, binary.BigEndian, &ext); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return err
		}

		if flags, err = br.ReadByte(); err != nil {
			return err
		}

		if err := binary.Read(br, binary.BigEndian, &origSize); err != nil {
			return err
		}

		if err := binary.Read(br, binary.BigEndian, &dataLen); err != nil {
			return err
		}

		o.totalBlocks += int(ext.Blocks)

		if flags == 2 {
			o.cnt++

			o.extents = append(o.extents, ocBlock{
				lba:   ext.LBA,
				count: int(ext.Blocks),
				flags: 2,
			})

			// The empty size will signal that it's empty blocks.
			err := o.em.Update(ext, OPBA{})
			if err != nil {
				return err
			}

			continue
		}

		var headerSz int

		if flags == 1 {
			o.body.WriteByte(1)
			binary.Write(&o.body, binary.BigEndian, uint32(origSize))

			headerSz = 1 + 4
		} else {
			o.body.WriteByte(0)

			headerSz = 1
		}

		n, err := io.CopyN(&o.body, br, int64(dataLen))
		if err != nil {
			return err
		}
		if n != int64(dataLen) {
			return fmt.Errorf("short copy: %d != %d", n, dataLen)
		}

		o.storageRatio += float64(dataLen) / float64(origSize)

		o.cnt++

		o.extents = append(o.extents, ocBlock{
			lba:    ext.LBA,
			count:  int(ext.Blocks),
			size:   uint64(headerSz + int(dataLen)),
			offset: o.offset,
		})

		err = o.em.Update(ext, OPBA{
			Offset: uint32(o.offset),
			Size:   uint32(dataLen),
		})
		if err != nil {
			return err
		}

		o.offset += uint64(headerSz + int(dataLen))
	}

	return nil
}

func (o *ObjectCreator) WritePrepared(se *serializedExtent) error {
	o.totalBlocks += int(se.ext.Blocks)

	if se.empty {
		o.cnt++

		o.extents = append(o.extents, ocBlock{
			lba:   se.ext.LBA,
			count: int(se.ext.Blocks),
			flags: 2,
		})

		return nil
	}

	var headerSz int

	if se.compressed {
		o.body.WriteByte(1)
		binary.Write(&o.body, binary.BigEndian, uint32(se.origSize))
		o.body.Write(se.data)

		headerSz = 1 + 4
	} else {
		o.body.WriteByte(0)
		o.body.Write(se.data)

		headerSz = 1
	}

	o.storageRatio += se.ratio

	o.cnt++

	o.extents = append(o.extents, ocBlock{
		lba:    se.ext.LBA,
		count:  int(se.ext.Blocks),
		size:   uint64(headerSz + len(se.data)),
		offset: o.offset,
	})

	o.offset += uint64(headerSz + len(se.data))

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

		ret = append(ret, subDest.Extent)

		// It's a empty range
		if srcRng.Size == 0 {
			continue
		}

		srcBytes := body[srcRng.Offset:] //  srcRng.Offset+srcRng.Size+5]

		var srcData []byte

		if srcBytes[0] == 0 {
			srcData = srcBytes[1 : 1+srcRng.Size]
		} else {
			origSize := binary.BigEndian.Uint32(srcBytes[1:])
			srcBytes = srcBytes[5 : 5+srcRng.Size]

			o.log.Trace("compressed range", "offset", srcRng.Offset, "sum", rangeSum(srcBytes))

			if len(o.buf) < int(origSize) {
				o.buf = make([]byte, origSize)
			}

			o.log.Trace("original size of compressed extent", "len", origSize)

			n, err := lz4.UncompressBlock(srcBytes, o.buf)
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

		subSrc, ok := src.SubRange(srcRng.Range)
		if !ok {
			o.log.Error("error calculating src subrange")
			return nil, fmt.Errorf("error calculating src subrange")
		}

		copy(subDest.data, subSrc.data)
	}

	return ret, nil
}

func (o *ObjectCreator) WriteExtent(firstBlock LBA, ext BlockData) error {
	if o.buf == nil {
		o.buf = make([]byte, len(ext.data)*2)
	}

	if o.em == nil {
		o.em = NewExtentMap(o.log)
	}

	o.totalBlocks += ext.Blocks()

	rng := Extent{firstBlock, uint32(ext.Blocks())}

	var (
		flags byte
		data  []byte
	)

	if emptyBytes(ext.data) {
		flags = 2
		o.cnt++

		o.extents = append(o.extents, ocBlock{
			lba:   firstBlock,
			count: ext.Blocks(),
			flags: 2,
		})

		// The empty size will signal that it's empty blocks.
		err := o.em.Update(rng, OPBA{})
		if err != nil {
			return err
		}
	} else {
		bound := lz4.CompressBlockBound(len(ext.data))

		if len(o.buf) < bound {
			o.buf = make([]byte, bound)
		}

		sz, err := lz4.CompressBlock(ext.data, o.buf, nil)
		if err != nil {
			return err
		}

		var headerSz int

		if sz > 0 && sz < len(ext.data) {
			flags = 1
			o.body.WriteByte(1)
			binary.Write(&o.body, binary.BigEndian, uint32(len(ext.data)))

			data = o.buf[:sz]
			o.body.Write(o.buf[:sz])

			headerSz = 1 + 4
		} else {
			o.body.WriteByte(0)
			o.body.Write(ext.data)

			data = ext.data
			sz = len(ext.data)

			headerSz = 1
		}

		o.storageRatio += (float64(sz) / float64(len(ext.data)))

		o.cnt++

		o.extents = append(o.extents, ocBlock{
			lba:    firstBlock,
			count:  ext.Blocks(),
			size:   uint64(headerSz + sz),
			offset: o.offset,
		})

		o.log.Trace("writing compressed range",
			"offset", o.offset, "sum", rangeSum(data),
			"size", sz)

		err = o.em.Update(rng, OPBA{
			Offset: uint32(o.offset),
			Size:   uint32(sz),
		})
		if err != nil {
			return err
		}

		o.offset += uint64(headerSz + sz)
	}

	return o.writeLog(
		Extent{firstBlock, uint32(ext.Blocks())},
		flags,
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
	lba LBA
	pba objPBA

	extent Extent
	opba   OPBA
}

func (o *ObjectCreator) Flush(ctx context.Context,
	sa SegmentAccess, seg SegmentId,
) ([]objectEntry, error) {
	start := time.Now()
	defer func() {
		segmentTime.Observe(time.Since(start).Seconds())
	}()

	buf := make([]byte, 16)

	for _, blk := range o.extents {
		lba := blk.lba

		sz := binary.PutUvarint(buf, uint64(lba))
		_, err := o.header.Write(buf[:sz])
		if err != nil {
			return nil, err
		}

		sz = binary.PutUvarint(buf, uint64(blk.count))
		_, err = o.header.Write(buf[:sz])
		if err != nil {
			return nil, err
		}

		err = o.header.WriteByte(blk.flags)
		if err != nil {
			return nil, err
		}

		sz = binary.PutUvarint(buf, blk.size)
		_, err = o.header.Write(buf[:sz])
		if err != nil {
			return nil, err
		}

		sz = binary.PutUvarint(buf, blk.offset)
		_, err = o.header.Write(buf[:sz])
		if err != nil {
			return nil, err
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

	for i, blk := range o.extents {
		entries[i] = objectEntry{
			lba: blk.lba,
			pba: objPBA{
				PBA: PBA{
					Segment: seg,
					Offset:  dataBegin + uint32(blk.offset),
				},
				Flags: blk.flags,
				Size:  uint32(blk.size),
			},
			extent: Extent{LBA: blk.lba, Blocks: uint32(blk.count)},
			opba: OPBA{
				Segment: seg,
				Offset:  dataBegin + uint32(blk.offset),
				Size:    uint32(blk.size),
			},
		}
	}

	f, err := sa.WriteSegment(ctx, seg)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	binary.BigEndian.PutUint32(buf, uint32(o.cnt))
	_, err = f.Write(buf[:4])
	if err != nil {
		return nil, err
	}

	binary.BigEndian.PutUint32(buf, dataBegin)
	_, err = f.Write(buf[:4])
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(f, bytes.NewReader(o.header.Bytes()))
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(f, bytes.NewReader(o.body.Bytes()))
	if err != nil {
		return nil, err
	}

	f.Close()

	err = sa.AppendToObjects(ctx, o.volName, seg)
	if err != nil {
		return nil, err
	}

	if o.logF != nil {
		o.logF.Close()

		o.log.Trace("removing log file", "name", o.logF.Name())
		err = os.Remove(o.logF.Name())
		if err != nil {
			o.log.Error("error removing log file", "error", err)
		}
	}

	return entries, nil
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

	sz, err := lz4.UncompressBlock(buf, dest)
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
