package lsvd

import (
	"fmt"
	"sort"
)

type Extent struct {
	LBA    LBA
	Blocks uint32
}

func (e Extent) String() string {
	return fmt.Sprintf("%d:%d", e.LBA, e.Blocks)
}

func (e Extent) Contains(lba LBA) bool {
	return lba >= e.LBA && lba < (e.LBA+LBA(e.Blocks))
}

func (e Extent) Last() LBA {
	return (e.LBA + LBA(e.Blocks) - 1)
}

func (e Extent) Range() (LBA, LBA) {
	return e.LBA, e.LBA + LBA(e.Blocks) - 1
}

func (e Extent) Cover(y Extent) Cover {
	es, ef := e.Range()
	ys, yf := y.Range()

	if ef < ys || yf < es {
		return CoverNone
	}

	if es == ys && ef == yf {
		return CoverExact
	}

	// es    ys     yf    ef
	if es <= ys && ef >= yf {
		// e is a superange of y
		return CoverSuperRange
	}

	return CoverPartly
}

func (e Extent) Clamp(y Extent) (Extent, bool) {
	es, ef := e.Range()
	ys, yf := y.Range()

	if ef < ys || yf < es {
		return Extent{}, false
	}

	if es == ys && ef == yf {
		return y, true
	}

	var start, end LBA

	if ys <= es {
		start = es
	} else {
		start = ys
	}

	if yf >= ef {
		end = ef
	} else {
		end = yf
	}

	if start > end {
		return Extent{}, false
	}

	return ExtentFrom(start, end)
}

func (e Extent) Sub(o Extent) ([]Extent, bool) {
	es, ef := e.Range()
	os, of := o.Range()

	if ef < os || es > of {
		return nil, false
	}

	if es == os && ef == of {
		return nil, true
	}

	if es >= os {
		suffix, ok := ExtentFrom(of+1, ef)
		if !ok {
			return nil, false
		}

		return []Extent{suffix}, true
	}

	// o falls within e but not at the beginning

	prefix, ok := ExtentFrom(es, os-1)
	if !ok {
		return nil, false
	}

	if of >= ef {
		return []Extent{prefix}, true
	}

	suffix, ok := ExtentFrom(of+1, ef)
	if !ok {
		return nil, false
	}

	return []Extent{prefix, suffix}, true
}

func (e Extent) SubSpecific(o Extent) (Extent, Extent, bool) {
	es, ef := e.Range()
	os, of := o.Range()

	if ef < os || es > of {
		return Extent{}, Extent{}, false
	}

	if es == os && ef == of {
		return Extent{}, Extent{}, true
	}

	if es >= os {
		suffix, ok := ExtentFrom(of+1, ef)
		if !ok {
			return Extent{}, Extent{}, false
		}

		return Extent{}, suffix, true
	}

	// o falls within e but not at the beginning

	prefix, ok := ExtentFrom(es, os-1)
	if !ok {
		return Extent{}, Extent{}, false
	}

	if of >= ef {
		return prefix, Extent{}, true
	}

	suffix, ok := ExtentFrom(of+1, ef)
	if !ok {
		return Extent{}, Extent{}, false
	}

	return prefix, suffix, true
}

func (e Extent) Valid() bool {
	return e.Blocks > 0
}

func (e Extent) SubMany(subs []Extent) ([]Extent, bool) {
	sort.Slice(subs, func(i, j int) bool {
		a := subs[i]
		b := subs[j]

		if a.LBA < b.LBA {
			return true
		}

		if a.LBA == b.LBA {
			return a.Blocks < b.Blocks
		}

		return false
	})

	var holes []Extent

	considering := e

	for _, s := range subs {
		prefix, suffix, ok := considering.SubSpecific(s)
		if !ok {
			return nil, false
		}

		if prefix.Valid() {
			holes = append(holes, prefix)
		}

		if suffix.Valid() {
			considering = suffix
		} else {
			break
		}
	}

	return holes, true
}

type Mask struct {
	remaining []Extent
}

func (e Extent) StartMask() *Mask {
	return &Mask{remaining: []Extent{e}}
}

func (m *Mask) Cover(x Extent) error {
	return nil
}

func (h *Mask) Holes() []Extent {
	return nil
}
