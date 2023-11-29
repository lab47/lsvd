package lsvd

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMMapCache(t *testing.T) {
	t.Run("saves a block to the cache", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "cache")
		r.NoError(err)

		defer os.RemoveAll(tmpdir)

		c, err := NewDiskCache(filepath.Join(tmpdir, "cache"), 50)
		r.NoError(err)

		defer c.Close()

		block := make([]byte, BlockSize)
		block[70] = 47

		err = c.WriteBlock(LBA(47), block)
		r.NoError(err)

		b2 := make([]byte, BlockSize)

		err = c.ReadBlock(LBA(47), b2)
		r.NoError(err)

		r.Equal(block, b2)
	})

	t.Run("can handle multiple blocks", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "cache")
		r.NoError(err)

		defer os.RemoveAll(tmpdir)

		c, err := NewDiskCache(filepath.Join(tmpdir, "cache"), 50)
		r.NoError(err)

		defer c.Close()

		block := make([]byte, BlockSize)
		block[70] = 47

		err = c.WriteBlock(LBA(47), block)
		r.NoError(err)

		err = c.WriteBlock(LBA(8), block)
		r.NoError(err)

		err = c.WriteBlock(LBA(8098), block)
		r.NoError(err)

		b2 := make([]byte, BlockSize)

		err = c.ReadBlock(LBA(47), b2)
		r.NoError(err)

		r.Equal(block, b2)

		clear(b2)

		err = c.ReadBlock(LBA(8), b2)
		r.NoError(err)

		r.Equal(block, b2)

		clear(b2)

		err = c.ReadBlock(LBA(8098), b2)
		r.NoError(err)

		r.Equal(block, b2)

		clear(b2)
	})

	t.Run("evicts old blocks when needed", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "cache")
		r.NoError(err)

		defer os.RemoveAll(tmpdir)

		c, err := NewDiskCache(filepath.Join(tmpdir, "cache"), 2)
		r.NoError(err)

		defer c.Close()

		block := make([]byte, BlockSize)
		block[70] = 47

		err = c.WriteBlock(LBA(47), block)
		r.NoError(err)

		err = c.WriteBlock(LBA(8), block)
		r.NoError(err)

		err = c.WriteBlock(LBA(8098), block)
		r.NoError(err)

		b2 := make([]byte, BlockSize)

		err = c.ReadBlock(LBA(47), b2)
		r.Error(err)

		err = c.ReadBlock(LBA(8), b2)
		r.NoError(err)

		r.True(bytes.Equal(block, b2))

		clear(b2)

		err = c.ReadBlock(LBA(8098), b2)
		r.NoError(err)

		r.Equal(block, b2)

		clear(b2)
	})

	t.Run("evicts blocks that haven't been read", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "cache")
		r.NoError(err)

		defer os.RemoveAll(tmpdir)

		c, err := NewDiskCache(filepath.Join(tmpdir, "cache"), 2)
		r.NoError(err)

		defer c.Close()

		block := make([]byte, BlockSize)
		block[70] = 47

		err = c.WriteBlock(LBA(47), block)
		r.NoError(err)

		err = c.WriteBlock(LBA(8), block)
		r.NoError(err)

		b2 := make([]byte, BlockSize)

		err = c.ReadBlock(LBA(47), b2)
		r.NoError(err)

		clear(b2)

		err = c.WriteBlock(LBA(8098), block)
		r.NoError(err)

		err = c.ReadBlock(LBA(47), b2)
		r.NoError(err)

		r.Equal(block, b2)

		err = c.ReadBlock(LBA(8), b2)
		r.Error(err)

		clear(b2)

		err = c.ReadBlock(LBA(8098), b2)
		r.NoError(err)

		r.Equal(block, b2)

		clear(b2)
	})
}
