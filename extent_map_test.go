package lsvd

import (
	"crypto/rand"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/lab47/mode"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

func TestExtentMap(t *testing.T) {
	t.Logf("build mode: %s", mode.Mode())

	log := hclog.New(&hclog.LoggerOptions{
		Name:  "extentmap",
		Level: hclog.Trace,
	})

	s1 := SegmentId(ulid.MustNew(ulid.Now(), rand.Reader))

	t.Run("disjoint updates prefix", func(t *testing.T) {
		r := require.New(t)

		d := NewExtentMap(log)

		x := Extent{47, 10}
		_, err := d.Update(x, OPBA{
			Segment: s1,
			Offset:  47,
		})
		r.NoError(err)

		y := Extent{0, 8}
		_, err = d.Update(y, OPBA{
			Segment: s1,
			Offset:  0,
		})
		r.NoError(err)

		r1, ok := d.m.Get(0)
		r.True(ok)

		r.Equal(y, r1.Range)

		r2, ok := d.m.Get(47)
		r.True(ok)

		r.Equal(x, r2.Range)
	})

	t.Run("disjoint updates suffix", func(t *testing.T) {
		r := require.New(t)

		d := NewExtentMap(log)

		y := Extent{0, 8}
		_, err := d.Update(y, OPBA{
			Segment: s1,
			Offset:  0,
		})
		r.NoError(err)

		x := Extent{47, 10}
		_, err = d.Update(x, OPBA{
			Segment: s1,
			Offset:  47,
		})
		r.NoError(err)

		r1, ok := d.m.Get(0)
		r.True(ok)

		r.Equal(y, r1.Range)

		r2, ok := d.m.Get(47)
		r.True(ok)

		r.Equal(x, r2.Range)
	})

	t.Run("splits the ranges on update", func(t *testing.T) {
		r := require.New(t)

		d := NewExtentMap(log)

		x := Extent{LBA: 0, Blocks: 10}

		_, err := d.Update(x, OPBA{
			Segment: s1,
			Offset:  1,
		})
		r.NoError(err)

		y := Extent{1, 1}
		_, err = d.Update(y, OPBA{
			Segment: s1,
			Offset:  2,
		})
		r.NoError(err)

		r.Equal(3, d.m.Len())

		r1, ok := d.m.Get(0)
		r.True(ok)

		r.Equal(Extent{0, 1}, r1.Range)
		r.Equal(uint32(1), r1.Offset)

		r2, ok := d.m.Get(1)
		r.True(ok)

		r.Equal(Extent{1, 1}, r2.Range)
		r.Equal(uint32(2), r2.Offset)

		r3, ok := d.m.Get(2)
		r.True(ok)

		r.Equal(Extent{2, 8}, r3.Range)
		r.Equal(uint32(1), r3.Offset)
	})

	t.Run("wipes out a smaller range", func(t *testing.T) {
		r := require.New(t)

		m := NewExtentMap(log)

		_, err := m.Update(Extent{2, 1}, OPBA{
			Offset: 1,
		})
		r.NoError(err)

		_, err = m.Update(Extent{0, 10}, OPBA{
			Offset: 2,
		})
		r.NoError(err)

		r.Equal(1, m.m.Len())

		_, ok := m.m.Get(2)
		r.False(ok)

		r1, ok := m.m.Get(0)
		r.True(ok)

		r.Equal(Extent{0, 10}, r1.Range)
	})

	t.Run("adjusts an earlier overlapping range", func(t *testing.T) {
		r := require.New(t)

		m := NewExtentMap(log)

		_, err := m.Update(Extent{0, 5}, OPBA{
			Offset: 1,
		})
		r.NoError(err)

		_, err = m.Update(Extent{3, 10}, OPBA{
			Offset: 2,
		})
		r.NoError(err)

		r.Equal(2, m.m.Len())

		r1, ok := m.m.Get(0)
		r.True(ok)

		r.Equal(Extent{0, 3}, r1.Range)

		r2, ok := m.m.Get(3)
		r.True(ok)

		r.Equal(Extent{3, 10}, r2.Range)
	})

	t.Run("adjusts a later overlapping range", func(t *testing.T) {
		r := require.New(t)

		m := NewExtentMap(log)

		_, err := m.Update(Extent{3, 10}, OPBA{
			Offset: 1,
		})
		r.NoError(err)

		_, err = m.Update(Extent{0, 5}, OPBA{
			Offset: 2,
		})
		r.NoError(err)

		r.Equal(2, m.m.Len())

		r1, ok := m.m.Get(0)
		r.True(ok)

		r.Equal(Extent{0, 5}, r1.Range)

		r2, ok := m.m.Get(5)
		r.True(ok)

		r.Equal(Extent{5, 8}, r2.Range)
	})

	t.Run("adjusts a later boundary range", func(t *testing.T) {
		r := require.New(t)

		m := NewExtentMap(log)

		_, err := m.Update(Extent{3, 2}, OPBA{
			Offset: 1,
		})
		r.NoError(err)

		_, err = m.Update(Extent{0, 5}, OPBA{
			Offset: 2,
		})
		r.NoError(err)

		r.Equal(1, m.m.Len())

		r1, ok := m.m.Get(0)
		r.True(ok)

		r.Equal(Extent{0, 5}, r1.Range)
	})

	t.Run("removes a range that starts at the same place and is smaller", func(t *testing.T) {
		r := require.New(t)

		m := NewExtentMap(log)

		_, err := m.Update(Extent{1, 1}, OPBA{
			Offset: 1,
		})
		r.NoError(err)

		_, err = m.Update(Extent{1, 5}, OPBA{
			Offset: 2,
		})
		r.NoError(err)

		t.Log(m.Render())

		r.Equal(1, m.m.Len())

		r1, ok := m.m.Get(1)
		r.True(ok)

		r.Equal(Extent{1, 5}, r1.Range)
	})

	t.Run("doesn't removes non overlapping range", func(t *testing.T) {
		r := require.New(t)

		m := NewExtentMap(log)

		_, err := m.Update(Extent{0, 1}, OPBA{
			Offset: 1,
		})
		r.NoError(err)

		_, err = m.Update(Extent{1, 1}, OPBA{
			Offset: 2,
		})
		r.NoError(err)

		t.Log(m.Render())

		r.Equal(2, m.m.Len())

		_, err = m.Update(Extent{1, 1}, OPBA{
			Offset: 2,
		})
		r.NoError(err)

		r.Equal(2, m.m.Len())

		r1, ok := m.m.Get(0)
		r.True(ok)

		r.Equal(Extent{0, 1}, r1.Range)
	})

	t.Run("removes multiple ranges", func(t *testing.T) {
		r := require.New(t)

		m := NewExtentMap(log)

		_, err := m.Update(Extent{1, 1}, OPBA{
			Offset: 1,
		})
		r.NoError(err)

		_, err = m.Update(Extent{2, 1}, OPBA{
			Offset: 2,
		})
		r.NoError(err)

		_, err = m.Update(Extent{0, 5}, OPBA{
			Offset: 2,
		})
		r.NoError(err)

		r.Equal(1, m.m.Len())

		r1, ok := m.m.Get(0)
		r.True(ok)

		r.Equal(Extent{0, 5}, r1.Range)
	})

	t.Run("adjusts multiple ranges", func(t *testing.T) {
		r := require.New(t)

		m := NewExtentMap(log)

		_, err := m.Update(Extent{8, 1}, OPBA{
			Offset: 1,
		})
		r.NoError(err)

		_, err = m.Update(Extent{11, 1}, OPBA{
			Offset: 2,
		})
		r.NoError(err)

		_, err = m.Update(Extent{12, 10}, OPBA{
			Offset: 3,
		})
		r.NoError(err)

		_, err = m.Update(Extent{10, 5}, OPBA{
			Offset: 4,
		})
		r.NoError(err)

		r.Equal(3, m.m.Len())

		r1, ok := m.m.Get(8)
		r.True(ok)

		r.Equal(Extent{8, 1}, r1.Range)

		r2, ok := m.m.Get(10)
		r.True(ok)

		r.Equal(Extent{10, 5}, r2.Range)

		r3, ok := m.m.Get(15)
		r.True(ok)

		t.Log(m.Render())

		r.Equal(Extent{15, 7}, r3.Range)
		r.Equal(Extent{12, 10}.Last(), Extent{15, 7}.Last())
	})

	t.Run("report all pbas for a range", func(t *testing.T) {
		r := require.New(t)

		m := NewExtentMap(log)

		_, err := m.Update(Extent{0, 5}, OPBA{
			Offset: 1,
		})
		r.NoError(err)

		_, err = m.Update(Extent{5, 5}, OPBA{
			Offset: 2,
		})
		r.NoError(err)

		_, err = m.Update(Extent{10, 5}, OPBA{
			Offset: 3,
		})
		r.NoError(err)

		_, err = m.Update(Extent{15, 5}, OPBA{
			Offset: 4,
		})
		r.NoError(err)

		_, err = m.Update(Extent{100, 5}, OPBA{
			Offset: 4,
		})
		r.NoError(err)

		r.Equal(5, m.m.Len())

		pbas, err := m.Resolve(Extent{7, 20})
		r.NoError(err)

		r.Len(pbas, 3)

		r.Equal(uint32(2), pbas[0].Offset)
		r.Equal(uint32(3), pbas[1].Offset)
		r.Equal(uint32(4), pbas[2].Offset)
	})

	t.Run("resolves a range that matches the lba", func(t *testing.T) {
		r := require.New(t)

		m := NewExtentMap(log)

		_, err := m.Update(Extent{0, 5}, OPBA{
			Offset: 1,
		})
		r.NoError(err)

		r.Equal(1, m.m.Len())

		t.Log(m.Render())

		pbas, err := m.Resolve(Extent{0, 5})
		r.NoError(err)

		r.Len(pbas, 1)

		r.Equal(uint32(1), pbas[0].Offset)
	})

	t.Run("resolves a range that starts before the lba", func(t *testing.T) {
		r := require.New(t)

		m := NewExtentMap(log)

		_, err := m.Update(Extent{1, 1}, OPBA{
			Offset: 1,
		})
		r.NoError(err)

		r.Equal(1, m.m.Len())

		t.Log(m.Render())

		pbas, err := m.Resolve(Extent{0, 5})
		r.NoError(err)

		r.Len(pbas, 1)

		r.Equal(uint32(1), pbas[0].Offset)
	})

	t.Run("tc", func(t *testing.T) {
		r := require.New(t)

		m := NewExtentMap(log)

		inject := []Extent{
			{5799956, 5},
			{LBA: 5799968, Blocks: 32},
			{LBA: 5799936, Blocks: 1},
		}

		for i, e := range inject {
			_, err := m.Update(e, OPBA{
				Offset: uint32(i),
			})
			r.NoError(err)
		}

		r.Equal(3, m.m.Len())

		t.Log(m.Render())

		_, err := m.Update(Extent{5799956, 13}, OPBA{
			Offset: 2,
		})
		r.NoError(err)

		r.Equal(3, m.m.Len())

		_, ok := m.m.Get(5799968)
		r.False(ok)

		r1, ok := m.m.Get(5799969)
		r.True(ok)

		r.Equal(Extent{5799969, 31}, r1.Range)

		r2, ok := m.m.Get(5799956)
		r.True(ok)

		r.Equal(Extent{5799956, 13}, r2.Range)
	})
}
