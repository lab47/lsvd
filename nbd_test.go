package lsvd

import (
	"context"
	"crypto/rand"
	"io"
	"os"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestNBD(t *testing.T) {
	log := hclog.New(&hclog.LoggerOptions{
		Name:  "lsvdtest",
		Level: hclog.Trace,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var testRand RawBlocks = make([]byte, 4*1024)

	_, err := io.ReadFull(rand.Reader, testRand)
	if err != nil {
		panic(err)
	}

	t.Run("returns data from the cache", func(t *testing.T) {
		r := require.New(t)

		dir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(dir)

		d, err := NewDisk(ctx, log, dir)
		r.NoError(err)

		err = d.WriteExtent(ctx, testRandX.MapTo(0))
		r.NoError(err)

		r.NoError(d.Close(ctx))

		d, err = NewDisk(ctx, log, dir)
		r.NoError(err)

		b := NBDWrapper(ctx, log, d)

		fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
		r.NoError(err)

		rp := os.NewFile(uintptr(fds[0]), "sp1")
		wp := os.NewFile(uintptr(fds[1]), "sp2")

		defer rp.Close()
		defer wp.Close()

		buf := make([]byte, BlockSize)
		ok, err := b.ReadIntoConn(buf, 0, wp)
		r.NoError(err)

		wp.Close()

		r.True(ok)

		data, err := io.ReadAll(rp)
		r.NoError(err)

		blockEqual(t, testRandX, data)
	})
}
