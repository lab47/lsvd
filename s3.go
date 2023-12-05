package lsvd

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/davecgh/go-spew/spew"
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
	sc  *s3.Client
	buk string
	key string
	seg SegmentId
}

func (s *S3ObjectReader) Close() error {
	return nil
}

func (s *S3ObjectReader) ReadAt(dest []byte, off int64) (int, error) {
	rng := fmt.Sprintf("bytes=%d-%d", off, int(off)+len(dest))

	spew.Dump(s.buk, s.key, rng)

	r, err := s.sc.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &s.buk,
		Key:    &s.key,
		Range:  &rng,
	})
	if err != nil {
		return 0, err
	}

	defer r.Body.Close()

	n, err := r.Body.Read(dest)
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

func (s *S3Access) OpenSegment(seg SegmentId) (ObjectReader, error) {
	return &S3ObjectReader{
		sc:  s.sc,
		seg: seg,
		buk: s.bucket,
		key: "object." + ulid.ULID(seg).String(),
	}, nil
}

func (s *S3Access) ListSegments() ([]SegmentId, error) {
	var (
		token    *string
		segments []SegmentId

		per = int32(100)
	)

	for {
		out, err := s.sc.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
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
	sc     *manager.Uploader
	bucket string
	key    string

	buf bytes.Buffer
}

func (m *mdWriter) Write(b []byte) (int, error) {
	return m.buf.Write(b)
}

func (m *mdWriter) Close() error {
	_, err := m.sc.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: &m.bucket,
		Key:    &m.key,
		Body:   &m.buf,
	})

	return err
}

func (s *S3Access) WriteMetadata(name string) (io.WriteCloser, error) {
	var mw mdWriter
	mw.sc = s.uploader
	mw.bucket = s.bucket
	mw.key = name

	return &mw, nil
}

func (s *S3Access) ReadMetadata(name string) (io.ReadCloser, error) {
	out, err := s.sc.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &name,
	})

	if err != nil {
		return nil, err
	}

	return out.Body, nil
}

func (s *S3Access) RemoveSegment(seg SegmentId) error {
	key := "object." + ulid.ULID(seg).String()

	_, err := s.sc.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})

	return err
}
