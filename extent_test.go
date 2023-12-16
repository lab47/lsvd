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

		for _, x := range []Extent{e(0, 1), e(1, 2), e(9, 1)} {
			r.Equal(CoverCompletely, e(0, 10).Cover(x))
		}

		for _, x := range []Extent{e(9, 2), e(15, 20), e(0, 100)} {
			r.Equal(CoverPartly, e(10, 10).Cover(x))
		}

		for _, x := range []Extent{e(0, 10), e(20, 1)} {
			r.Equal(CoverNone, e(10, 10).Cover(x), "%s covers but shouldn't", x)
		}
	})
}
