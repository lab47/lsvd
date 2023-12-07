package lsvd

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hashicorp/go-hclog"
	"github.com/oklog/ulid/v2"
	"github.com/pierrec/lz4/v4"
)

type S3Access struct {
	sc       *s3.Client
	uploader *manager.Uploader
	bucket   string
}

func NewS3Access(log hclog.Logger, host, bucket string, cfg aws.Config) (*S3Access, error) {
	sc := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = &host
	})

	up := manager.NewUploader(sc)
	return &S3Access{
		sc:       sc,
		bucket:   bucket,
		uploader: up,
	}, nil
}

type S3ObjectReader struct {
	ctx context.Context
	sc  *s3.Client
	buk string
	key string
	seg SegmentId
}

func (s *S3ObjectReader) Close() error {
	return nil
}

func (s *S3ObjectReader) ReadAt(dest []byte, off int64) (int, error) {
	rng := fmt.Sprintf("bytes=%d-%d", off, int(off)+len(dest)-1)

	r, err := s.sc.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: &s.buk,
		Key:    &s.key,
		Range:  &rng,
	})
	if err != nil {
		return 0, err
	}

	defer r.Body.Close()

	n, err := io.ReadFull(r.Body, dest)
	if err != nil {
		if n > 0 {
			return n, nil
		}
	}

	return n, err
}

func (s *S3ObjectReader) ReadAtCompressed(dest []byte, off, compSize int64) (int, error) {
	buf := make([]byte, compSize)

	_, err := s.ReadAt(buf, off)
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

func (s *S3Access) OpenSegment(ctx context.Context, seg SegmentId) (ObjectReader, error) {
	key := "object." + ulid.ULID(seg).String()

	// Validate the object exists.
	_, err := s.sc.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}

	return &S3ObjectReader{
		sc:  s.sc,
		ctx: ctx,
		seg: seg,
		buk: s.bucket,
		key: key,
	}, nil
}

func (s *S3Access) ListSegments(ctx context.Context) ([]SegmentId, error) {
	var (
		token    *string
		segments []SegmentId

		per = int32(100)
	)

	for {
		out, err := s.sc.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &s.bucket,
			ContinuationToken: token,
			MaxKeys:           &per,
		})
		if err != nil {
			return nil, err
		}

		token = out.NextContinuationToken

		for _, c := range out.Contents {
			if seg, ok := segmentFromName(*c.Key); ok {
				segments = append(segments, seg)
			}
		}

		if token == nil {
			break
		}
	}

	return segments, nil
}

type mdWriter struct {
	ctx    context.Context
	sc     *manager.Uploader
	bucket string
	key    string

	buf bytes.Buffer
}

func (m *mdWriter) Write(b []byte) (int, error) {
	return m.buf.Write(b)
}

func (m *mdWriter) Close() error {
	_, err := m.sc.Upload(m.ctx, &s3.PutObjectInput{
		Bucket: &m.bucket,
		Key:    &m.key,
		Body:   &m.buf,
	})

	return err
}

type bgWriter struct {
	io.Writer

	bw  *bufio.Writer
	w   *io.PipeWriter
	ctx context.Context
	err error

	sc     *manager.Uploader
	bucket string
	key    string
}

func (b *bgWriter) Close() error {
	b.bw.Flush()
	b.w.Close()

	<-b.ctx.Done()

	return b.err
}

func (s *S3Access) WriteSegment(ctx context.Context, seg SegmentId) (io.WriteCloser, error) {
	r, w := io.Pipe()

	bw := bufio.NewWriter(w)

	ctx, cancel := context.WithCancel(context.Background())

	bg := &bgWriter{
		Writer: bw,
		bw:     bw,
		w:      w,
		ctx:    ctx,
	}

	key := "object." + ulid.ULID(seg).String()

	go func() {
		defer cancel()
		_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: &s.bucket,
			Key:    &key,
			Body:   r,
		})
		bg.err = err
	}()

	return bg, nil
}

func (s *S3Access) WriteMetadata(ctx context.Context, name string) (io.WriteCloser, error) {
	var mw mdWriter
	mw.ctx = ctx
	mw.sc = s.uploader
	mw.bucket = s.bucket
	mw.key = name

	return &mw, nil
}

func (s *S3Access) ReadMetadata(ctx context.Context, name string) (io.ReadCloser, error) {
	out, err := s.sc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &name,
	})

	if err != nil {
		return nil, err
	}

	return out.Body, nil
}

func (s *S3Access) RemoveSegment(ctx context.Context, seg SegmentId) error {
	key := "object." + ulid.ULID(seg).String()

	_, err := s.sc.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})

	return err
}

var _ SegmentAccess = (*S3Access)(nil)
