package lsvd

import "fmt"

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
