package lsvd

import (
	"context"
	"fmt"
	"io"
	"os"

	lru "github.com/hashicorp/golang-lru/v2"
)

type rangeCacheKey struct {
	Seg   SegmentId
	Chunk int64
}

type RangeCache struct {
	path  string
	f     *os.File
	chunk int64
	max   int64
	fetch func(ctx context.Context, seg SegmentId, data []byte, off int64) error

	lru *lru.Cache[rangeCacheKey, int64]

	chunkBuf []byte
}

type RangeCacheOptions struct {
	Path      string
	ChunkSize int64
	MaxSize   int64
	Fetch     func(ctx context.Context, seg SegmentId, data []byte, off int64) error
}

func NewRangeCache(opts RangeCacheOptions) (*RangeCache, error) {
	f, err := os.Create(opts.Path)
	if err != nil {
		return nil, err
	}

	maxChunks := opts.MaxSize / opts.ChunkSize

	if maxChunks == 0 {
		return nil, fmt.Errorf("max size too small")
	}

	l, err := lru.New[rangeCacheKey, int64](int(maxChunks))
	if err != nil {
		return nil, err
	}

	rc := &RangeCache{
		path:  opts.Path,
		f:     f,
		chunk: opts.ChunkSize,
		max:   maxChunks,
		fetch: opts.Fetch,

		lru:      l,
		chunkBuf: make([]byte, opts.ChunkSize),
	}

	return rc, nil
}

func (r *RangeCache) Close() error {
	return r.f.Close()
}

func (r *RangeCache) ReadAt(ctx context.Context, seg SegmentId, buf []byte, off int64) (int, error) {
	firstChunk := off / r.chunk
	lastChunk := (off + int64(len(buf)) - 1) / r.chunk

	tot := len(buf)

	innerOff := off % r.chunk

	chunkData := r.chunkBuf

	for chunk := firstChunk; chunk <= lastChunk; chunk++ {
		ok, err := r.readChunk(seg, chunk, chunkData)
		if err != nil {
			return 0, err
		}

		if !ok {
			extentCacheMiss.Inc()

			err := r.fetch(ctx, seg, chunkData, chunk*r.chunk)
			if err != nil {
				return 0, err
			}

			err = r.saveChunk(seg, chunk, chunkData)
			if err != nil {
				return 0, err
			}
		} else {
			extentCacheHits.Inc()
		}

		copied := copy(buf, chunkData[innerOff:])

		if copied < len(buf) {
			buf = buf[copied:]
		}

		// Reset back because we want to read from the front of all future chunks
		innerOff = 0
	}

	return tot, nil
}

func (r *RangeCache) readChunk(seg SegmentId, chunk int64, data []byte) (bool, error) {
	off, ok := r.lru.Get(rangeCacheKey{seg, chunk})
	if !ok {
		return false, nil
	}

	n, err := r.f.ReadAt(data, off)
	if err != nil {
		return false, err
	}

	if n != len(data) {
		return false, io.ErrShortWrite
	}

	return true, nil
}

func (r *RangeCache) saveChunk(seg SegmentId, chunk int64, data []byte) error {
	if r.lru.Len() < int(r.max) {
		off, err := r.f.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		n, err := r.f.Write(data)
		if err != nil {
			return err
		}

		if n != len(data) {
			return io.ErrShortWrite
		}

		r.lru.Add(rangeCacheKey{seg, chunk}, off)
		return nil
	}

	_, off, ok := r.lru.RemoveOldest()
	if !ok {
		return fmt.Errorf("misused lru is empty")
	}

	n, err := r.f.WriteAt(data, off)
	if err != nil {
		return err
	}

	if n != len(data) {
		return io.ErrShortWrite
	}

	r.lru.Add(rangeCacheKey{seg, chunk}, off)

	return nil
}
