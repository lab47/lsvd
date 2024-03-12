package lsvd

import (
	"context"
	"strings"

	"github.com/lab47/lsvd/logger"
)

type extentValidator struct {
	sums    map[Extent]string
	resi    map[Extent][]PartialExtent
	entries []ExtentLocation
}

func (e *extentValidator) populate(log logger.Logger, d *Disk, oc *SegmentCreator, entries []ExtentLocation) {
	e.sums = map[Extent]string{}
	e.resi = map[Extent][]PartialExtent{}
	e.entries = entries

	for _, ent := range entries {
		data := NewRangeData(ent.Extent)

		_, err := oc.FillExtent(data.View())
		if err != nil {
			d.log.Error("error reading extent for validation", "error", err, "extent", ent.Extent, "offset", ent.Offset, "size", ent.Size)
		}

		sum := "0"
		if !data.EmptyP() {
			sum = rangeSum(data.ReadData())
		}

		e.sums[ent.Extent] = sum
		d.log.Trace("sum of extent", "extent", ent.Extent, "sum", sum)

		ranges, err := d.lba2pba.Resolve(log, ent.Extent, nil)
		if err != nil {
			d.log.Error("error performing resolution for block read check")
		} else {
			e.resi[ent.Extent] = ranges
		}

		data.Discard()
	}
}

func (e *extentValidator) validate(ctx context.Context, log logger.Logger, d *Disk) {
	entries := e.entries

	d.log.Info("performing extent validation")
	passed := 0
	for _, ent := range entries {
		data, err := d.ReadExtent(ctx, ent.Extent)
		if err != nil {
			d.log.Error("error reading extent for validation", "error", err)
		}

		sum := "0"
		if !data.EmptyP() {
			sum = rangeSum(data.ReadData())
		}

		if sum != e.sums[ent.Extent] {
			d.log.Error("block read validation failed", "extent", ent.Extent,
				"sum", sum, "expected", e.sums[ent.Extent])
			ranges, err := d.lba2pba.Resolve(log, ent.Extent, nil)
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
