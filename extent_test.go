package lsvd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtent(t *testing.T) {
	e := func(lba LBA, blocks uint32) Extent {
		return Extent{lba, blocks}
	}

	t.Run("covers", func(t *testing.T) {
		r := require.New(t)

		r.Equal(CoverExact, e(1, 1).Cover(e(1, 1)))

		for _, x := range []Extent{e(0, 1), e(1, 2), e(9, 1)} {
			r.Equal(CoverSuperRange, e(0, 10).Cover(x))
		}

		for _, x := range []Extent{e(9, 2), e(15, 20), e(0, 100)} {
			r.Equal(CoverPartly, e(10, 10).Cover(x))
		}

		for _, x := range []Extent{e(0, 10), e(20, 1)} {
			r.Equal(CoverNone, e(10, 10).Cover(x), "%s covers but shouldn't", x)
		}
	})

	t.Run("clamp", func(t *testing.T) {
		r := require.New(t)

		chk := func(res, lhs, rhs Extent) {
			act, ok := lhs.Clamp(rhs)
			r.True(ok, "unable to clamp")
			r.Equal(res, act)
		}

		chk(e(2, 4), e(1, 10), e(2, 4))

		chk(e(28, 5), e(1, 32), e(28, 32))

		chk(e(121667583, 1), e(121667583, 2), e(121667583, 1))

	})
}
