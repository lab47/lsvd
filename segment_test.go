package lsvd

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/lab47/lsvd/logger"
	"github.com/stretchr/testify/require"
)

func TestSegmentCreator(t *testing.T) {
	log := logger.New(logger.Trace)

	t.Run("logs writes to disk", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "oc")
		r.NoError(err)

		defer os.RemoveAll(tmpdir)

		path := filepath.Join(tmpdir, "log")

		oc, err := NewSegmentCreator(log, "", path)
		r.NoError(err)

		data := NewRangeData(Extent{47, 5})

		for i := range data.data {
			data.data[i] = byte(i)
		}

		err = oc.WriteExtent(data)
		r.NoError(err)

		f := oc.builder.logF

		_, err = f.Seek(0, io.SeekStart)
		r.NoError(err)

		oc2 := &SegmentCreator{
			log: log,
			em:  NewExtentMap(),
		}

		oc2.builder.em = oc2.em

		err = oc2.builder.readLog(f, log)
		r.NoError(err)

		r.Equal(oc.builder.body.Bytes(), oc2.builder.body.Bytes())
	})

	t.Run("can serve reads from the write cache", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "oc")
		r.NoError(err)

		defer os.RemoveAll(tmpdir)

		path := filepath.Join(tmpdir, "log")

		oc, err := NewSegmentCreator(log, "", path)
		r.NoError(err)

		data := NewRangeData(Extent{47, 5})

		d := data.WriteData()
		for i := range d {
			d[i] = byte(i)
		}

		err = oc.WriteExtent(data)
		r.NoError(err)

		readRequest := NewRangeData(Extent{48, 1})

		ret, err := oc.FillExtent(readRequest.View())
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

		path := filepath.Join(tmpdir, "log")

		oc, err := NewSegmentCreator(log, "", path)
		r.NoError(err)

		data := NewRangeData(Extent{47, 5})

		d := data.WriteData()
		for i := range d {
			d[i] = byte(i)
		}

		err = oc.WriteExtent(data)
		r.NoError(err)

		d2 := NewRangeData(Extent{48, 1})

		d = d2.WriteData()
		for i := range d {
			d[i] = byte(i + 1)
		}

		err = oc.WriteExtent(d2)
		r.NoError(err)

		req := NewRangeData(Extent{48, 2})

		ret, err := oc.FillExtent(req.View())
		r.NoError(err)

		r.Equal(d2.data[:BlockSize], req.data[:BlockSize])
		r.Equal(data.data[BlockSize*3:BlockSize*4], req.data[BlockSize:])

		r.Len(ret, 2)

		r.Equal(Extent{48, 1}, ret[0])
		r.Equal(Extent{49, 1}, ret[1])
	})
}
