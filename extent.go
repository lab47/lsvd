package lsvd

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
