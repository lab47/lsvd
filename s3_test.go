package lsvd

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hashicorp/go-hclog"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

func TestS3(t *testing.T) {
	host := os.Getenv("S3_URL")
	if host == "" {
		t.Skip("no s3 url provided to test with")
	}

	access := "admin"
	secret := "password"

	log := hclog.New(&hclog.LoggerOptions{
		Name:  "s3access",
		Level: hclog.Trace,
	})

	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx, func(lo *config.LoadOptions) error {
		lo.Region = "us-east-1"
		lo.Credentials = credentials.NewStaticCredentialsProvider(access, secret, "")
		return nil
	})

	require.NoError(t, err)

	sc := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = &host
	})

	bucketName := "lsvdtest"

	sc.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: &bucketName,
	})
	//require.NoError(t, err)

	t.Run("can read a segment", func(t *testing.T) {
		r := require.New(t)

		seg, err := ulid.New(ulid.Now(), monoRead)
		r.NoError(err)

		objName := "object." + ulid.ULID(seg).String()

		_, err = sc.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &bucketName,
			Key:    &objName,
			Body:   strings.NewReader("this is a segment"),
		})
		r.NoError(err)

		defer sc.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &bucketName,
			Key:    &objName,
		})

		s, err := NewS3Access(log, host, bucketName, cfg)
		r.NoError(err)

		or, err := s.OpenSegment(SegmentId(seg))
		r.NoError(err)

		buf := make([]byte, 1024)

		n, err := or.ReadAt(buf, 0)
		r.NoError(err)

		r.Equal("this is a segment", string(buf[:n]))
	})

	t.Run("lists objects", func(t *testing.T) {
		r := require.New(t)

		var expected []SegmentId

		for i := 0; i < 3; i++ {
			seg, err := ulid.New(ulid.Now(), monoRead)
			r.NoError(err)

			expected = append(expected, SegmentId(seg))

			objName := "object." + ulid.ULID(seg).String()

			_, err = sc.PutObject(ctx, &s3.PutObjectInput{
				Bucket: &bucketName,
				Key:    &objName,
				Body:   strings.NewReader("this is a segment"),
			})
			r.NoError(err)

			defer sc.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: &bucketName,
				Key:    &objName,
			})
		}

		s, err := NewS3Access(log, host, bucketName, cfg)
		r.NoError(err)

		segs, err := s.ListSegments()
		r.NoError(err)

		r.Equal(expected, segs)
	})

	t.Run("accesses metadata", func(t *testing.T) {
		r := require.New(t)

		s, err := NewS3Access(log, host, bucketName, cfg)
		r.NoError(err)

		w, err := s.WriteMetadata("head")
		r.NoError(err)

		_, err = fmt.Fprintln(w, "this is metadata")
		r.NoError(err)
		r.NoError(w.Close())

		mr, err := s.ReadMetadata("head")
		r.NoError(err)

		data, err := io.ReadAll(mr)
		r.NoError(err)

		r.Equal("this is metadata\n", string(data))
	})
}
