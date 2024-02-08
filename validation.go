package lsvd

import (
	"context"
	"strings"
)

type extentValidator struct {
	sums    map[Extent]string
	resi    map[Extent][]*PartialExtent
	entries []ExtentLocation
}

func (e *extentValidator) populate(d *Disk, oc *SegmentCreator, entries []ExtentLocation) {
	e.sums = map[Extent]string{}
	e.resi = map[Extent][]*PartialExtent{}
	e.entries = entries

	var data RangeData

	for _, ent := range entries {
		data.Reset(ent.Extent)

		_, err := oc.FillExtent(data)
		if err != nil {
			d.log.Error("error reading extent for validation", "error", err)
		}
		sum := rangeSum(data.data)
		e.sums[ent.Extent] = sum

		ranges, err := d.lba2pba.Resolve(ent.Extent)
		if err != nil {
			d.log.Error("error performing resolution for block read check")
		} else {
			e.resi[ent.Extent] = ranges
		}
	}
}

func (e *extentValidator) validate(ctx context.Context, d *Disk) {
	entries := e.entries

	d.log.Info("performing extent validation")
	passed := 0
	for _, ent := range entries {
		data, err := d.ReadExtent(ctx, ent.Extent)
		if err != nil {
			d.log.Error("error reading extent for validation", "error", err)
		}
		sum := rangeSum(data.data)

		if sum != e.sums[ent.Extent] {
			d.log.Error("block read validation failed", "extent", ent.Extent,
				"sum", sum, "expected", e.sums[ent.Extent])
			ranges, err := d.lba2pba.Resolve(ent.Extent)
			if err != nil {
				d.log.Error("unable to resolve for check", "error", err)
			} else {
				var before []string
				for _, r := range e.resi[ent.Extent] {
					before = append(before, r.String())
				}

				var after []string
				for _, r := range ranges {
					after = append(after, r.String())
				}

				d.log.Error("block read validation ranges",
					"before", strings.Join(before, " "),
					"after", strings.Join(after, " "))
			}
		} else {
			passed++
		}
	}

	d.log.Warn("finished block read validation", "passed", passed)
}
