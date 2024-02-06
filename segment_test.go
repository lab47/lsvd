package lsvd

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
)

func TestSegmentCreator(t *testing.T) {
	log := hclog.New(&hclog.LoggerOptions{
		Name:  "octest",
		Level: hclog.Trace,
	})

	t.Run("logs writes to disk", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "oc")
		r.NoError(err)

		defer os.RemoveAll(tmpdir)

		f, err := os.Create(filepath.Join(tmpdir, "log"))
		r.NoError(err)

		defer f.Close()

		oc := &SegmentCreator{
			log: log,

			logF: f,
			logW: bufio.NewWriter(f),
		}

		data := NewBlockData(5)

		for i := range data.data {
			data.data[i] = byte(i)
		}

		err = oc.WriteExtent(data.MapTo(47))
		r.NoError(err)

		_, err = f.Seek(0, io.SeekStart)
		r.NoError(err)

		oc2 := &SegmentCreator{
			log: log,
			em:  NewExtentMap(log),
		}

		err = oc2.readLog(f)
		r.NoError(err)

		r.Equal(oc.body.Bytes(), oc2.body.Bytes())
	})

	t.Run("can serve reads from the write cache", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "oc")
		r.NoError(err)

		defer os.RemoveAll(tmpdir)

		f, err := os.Create(filepath.Join(tmpdir, "log"))
		r.NoError(err)

		defer f.Close()

		oc := &SegmentCreator{
			log: log,

			logF: f,
			logW: bufio.NewWriter(f),
		}

		data := NewBlockData(5)

		for i := range data.data {
			data.data[i] = byte(i)
		}

		err = oc.WriteExtent(data.MapTo(47))
		r.NoError(err)

		readRequest := NewRangeData(Extent{48, 1})

		ret, err := oc.FillExtent(readRequest)
		r.NoError(err)

		r.Equal(data.data[BlockSize:BlockSize*2], readRequest.data)

		r.Len(ret, 1)

		r.Equal(Extent{48, 1}, ret[0])
	})

	t.Run("serves read requests according to write order", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "oc")
		r.NoError(err)

		defer os.RemoveAll(tmpdir)

		f, err := os.Create(filepath.Join(tmpdir, "log"))
		r.NoError(err)

		defer f.Close()

		oc := &SegmentCreator{
			log: log,

			logF: f,
			logW: bufio.NewWriter(f),
		}

		data := NewBlockData(5)

		for i := range data.data {
			data.data[i] = byte(i)
		}

		err = oc.WriteExtent(data.MapTo(47))
		r.NoError(err)

		d2 := NewBlockData(1)
		for i := range d2.data {
			d2.data[i] = byte(i + 1)
		}

		err = oc.WriteExtent(d2.MapTo(48))
		r.NoError(err)

		req := NewRangeData(Extent{48, 2})

		ret, err := oc.FillExtent(req)
		r.NoError(err)

		r.Equal(d2.data[:BlockSize], req.data[:BlockSize])
		r.Equal(data.data[BlockSize*3:BlockSize*4], req.data[BlockSize:])

		r.Len(ret, 2)

		r.Equal(Extent{48, 1}, ret[0])
		r.Equal(Extent{49, 1}, ret[1])
	})
}
