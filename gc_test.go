package lsvd

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lab47/lsvd/logger"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

func TestGC(t *testing.T) {
	gctx := context.Background()
	ctx := NewContext(gctx)

	log := logger.Test()

	pat := func(id, count int) RawBlocks {
		b := make(RawBlocks, BlockSize*count)
		for i := range b {
			b[i] = byte(id)
		}

		return b
	}

	t.Run("works serially", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		origSeq := ulid.MustNew(ulid.Now(), ulid.DefaultEntropy())

		d, err := NewDisk(ctx, log, tmpdir, WithSeqGen(func() ulid.ULID {
			return origSeq
		}))
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent.MapTo(0))
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent2.MapTo(1))
		r.NoError(err)

		d.SeqGen = nil

		err = d.CloseSegment(ctx)
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent3.MapTo(0))
		r.NoError(err)

		err = d.CloseSegment(ctx)
		r.NoError(err)

		gcSeg, err := d.GCOnce(ctx)
		r.NoError(err)

		r.Equal(SegmentId(origSeq), gcSeg)

		d.Close(ctx)

		// We delete entries AFTER we write the segment that contains the remaints
		_, err = os.Stat(filepath.Join(tmpdir, "segments", "segment."+origSeq.String()))
		r.ErrorIs(err, os.ErrNotExist)

		t.Log("reloading disk")

		d2, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		x2, err := d2.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
		r.NoError(err)

		extentEqual(t, testExtent3, x2)

		x2, err = d2.ReadExtent(ctx, Extent{LBA: 1, Blocks: 1})
		r.NoError(err)

		extentEqual(t, testExtent2, x2)
	})

	t.Run("works concurrently", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		origSeq := ulid.MustNew(ulid.Now(), ulid.DefaultEntropy())

		d, err := NewDisk(ctx, log, tmpdir, WithSeqGen(func() ulid.ULID {
			return origSeq
		}))
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent.MapTo(0))
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent2.MapTo(1))
		r.NoError(err)

		d.SeqGen = nil

		err = d.CloseSegment(ctx)
		r.NoError(err)

		err = d.WriteExtent(ctx, testExtent3.MapTo(0))
		r.NoError(err)

		err = d.CloseSegment(ctx)
		r.NoError(err)

		t.Log("starting gc in bg")
		gcSeg, more, err := d.GCInBackground(ctx, 1.0)
		r.NoError(err)

		r.True(more)

		r.Equal(SegmentId(origSeq), gcSeg)

		time.Sleep(200 * time.Millisecond)

		t.Log("closing disk")
		d.Close(ctx)

		// We delete entries AFTER we write the segment that contains the remaints
		_, err = os.Stat(filepath.Join(tmpdir, "segments", "segment."+origSeq.String()))
		r.ErrorIs(err, os.ErrNotExist, "%s not removed", origSeq)

		t.Log("reloading disk")

		d2, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		x2, err := d2.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
		r.NoError(err)

		extentEqual(t, testExtent3, x2)

		x2, err = d2.ReadExtent(ctx, Extent{LBA: 1, Blocks: 1})
		r.NoError(err)

		extentEqual(t, testExtent2, x2)
	})

	t.Run("copies only the live range", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		origSeq := ulid.MustNew(ulid.Now(), ulid.DefaultEntropy())

		d, err := NewDisk(ctx, log, tmpdir, WithSeqGen(func() ulid.ULID {
			return origSeq
		}))
		r.NoError(err)

		e1 := make(RawBlocks, BlockSize*4)
		for i := range e1 {
			e1[i] = 1
		}

		err = d.WriteExtent(ctx, e1.MapTo(0))
		r.NoError(err)

		d.SeqGen = nil

		err = d.CloseSegment(ctx)
		r.NoError(err)

		e2 := make(RawBlocks, BlockSize*4)
		for i := range e2 {
			e2[i] = 2
		}

		err = d.WriteExtent(ctx, e2.MapTo(1))
		r.NoError(err)

		pes, err := d.resolveSegmentAccess(Extent{LBA: 0, Blocks: 1})
		r.NoError(err)

		r.Len(pes, 1)

		pe := pes[0]

		// This shows that currently, there are 3 "garbage" blocks.
		r.Equal(pe.Extent, Extent{LBA: 0, Blocks: 4})
		r.Equal(pe.Live, Extent{LBA: 0, Blocks: 4})

		err = d.CloseSegment(ctx)
		r.NoError(err)

		gcSeg, err := d.GCOnce(ctx)
		r.NoError(err)

		r.Equal(SegmentId(origSeq), gcSeg)

		d.Close(ctx)

		// We delete entries AFTER we write the segment that contains the remaints
		_, err = os.Stat(filepath.Join(tmpdir, "segments", "segment."+origSeq.String()))
		r.ErrorIs(err, os.ErrNotExist)

		t.Log("reloading disk")

		d2, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		pes, err = d2.resolveSegmentAccess(Extent{LBA: 0, Blocks: 1})
		r.NoError(err)

		r.Len(pes, 1)

		pe = pes[0]

		// This shows that currently, the garbage blocks are gone and we only have
		// the one still referenced.
		r.Equal(pe.Extent, Extent{LBA: 0, Blocks: 1})
		r.Equal(pe.Live, Extent{LBA: 0, Blocks: 1})

		x2, err := d2.ReadExtent(ctx, Extent{LBA: 0, Blocks: 1})
		r.NoError(err)

		extentEqual(t, e1[:BlockSize], x2)

		x2, err = d2.ReadExtent(ctx, Extent{LBA: 1, Blocks: 1})
		r.NoError(err)

		extentEqual(t, e2[:BlockSize], x2)
	})

	t.Run("does not add another segment when background is checkpointed", func(t *testing.T) {
		r := require.New(t)

		tmpdir, err := os.MkdirTemp("", "lsvd")
		r.NoError(err)
		defer os.RemoveAll(tmpdir)

		d, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		e1 := pat(1, 4)

		err = d.WriteExtent(ctx, e1.MapTo(0))
		r.NoError(err)

		d.SeqGen = nil

		err = d.CloseSegment(ctx)
		r.NoError(err)

		e2 := pat(2, 4)

		err = d.WriteExtent(ctx, e2.MapTo(1))
		r.NoError(err)

		err = d.CloseSegment(ctx)
		r.NoError(err)

		e3 := pat(3, 4)

		err = d.WriteExtent(ctx, e3.MapTo(2))
		r.NoError(err)

		err = d.CloseSegment(ctx)
		r.NoError(err)

		// should have 3 segments, let's be sure

		pes, err := d.resolveSegmentAccess(Extent{LBA: 0, Blocks: 3})
		r.NoError(err)

		r.Len(pes, 3)

		r.Equal(Extent{LBA: 0, Blocks: 4}, pes[0].Extent) // e1
		r.Equal(Extent{LBA: 1, Blocks: 4}, pes[1].Extent) // e2
		r.Equal(Extent{LBA: 2, Blocks: 4}, pes[2].Extent) // e3

		t.Log("gc start")

		_, _, err = d.GCInBackground(ctx, 1.0)
		r.NoError(err)

		time.Sleep(100 * time.Millisecond)

		t.Log("checkpointing")

		err = d.CheckpointGC(ctx)
		r.NoError(err)

		time.Sleep(100 * time.Millisecond)

		t.Log("closing disk")

		t.Log(d.lba2pba.RenderExpanded())
		d.Close(ctx)

		t.Log("reloading disk")

		d2, err := NewDisk(ctx, log, tmpdir)
		r.NoError(err)

		pes, err = d2.resolveSegmentAccess(Extent{LBA: 0, Blocks: 3})
		r.NoError(err)

		r.Len(pes, 3)

		r.Equal(Extent{LBA: 0, Blocks: 1}, pes[0].Extent)
		r.Equal(Extent{LBA: 1, Blocks: 4}, pes[1].Extent)
		r.Equal(Extent{LBA: 2, Blocks: 4}, pes[2].Extent)
	})
}
