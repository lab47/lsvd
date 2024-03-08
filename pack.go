package lsvd

import "context"

type Packer struct {
	d *Disk
	m *ExtentMap
}

func (p *Packer) iterateExtents(ctx context.Context) error {
	var live RangeData

	sb := &SegmentBuilder{
		useZstd: true,
	}

	d := p.d
	for i := p.m.Iterator(); i.Valid(); i.Next() {
		d.log.Debug("packing extent", "extent", i.Value().Live)
		data, err := d.ReadExtent(ctx, i.Value().Live)
		if err != nil {
			return err
		}

		if live.Blocks == 0 {
			live = data
			continue
		}

		// combine conjoined extents
		if live.Last()+1 == i.Key() {
			live = live.Append(data)

			if live.Blocks >= 100 {
				d.log.Debug("writing packed extent (big)", "extent", live.Extent)
				_, _, err := sb.WriteExtent(d.log, live.View())
				if err != nil {
					return err
				}
				live = RangeData{}
			}
		} else {
			d.log.Debug("writing packed extent (disjoint)", "extent", live.Extent)
			_, _, err := sb.WriteExtent(d.log, live.View())
			if err != nil {
				return err
			}

			live = data
		}

		if sb.ShouldFlush(FlushThreshHold) {
			err = p.flushSegment(ctx, sb)
			if err != nil {
				return err
			}

			sb = &SegmentBuilder{
				useZstd: true,
			}
		}
	}

	if live.Blocks > 0 {
		d.log.Debug("writing packed extent (final)", "extent", live.Extent)
		_, _, err := sb.WriteExtent(d.log, live.View())
		if err != nil {
			return err
		}
	}

	return p.flushSegment(ctx, sb)
}

func (p *Packer) flushSegment(ctx context.Context, sb *SegmentBuilder) error {
	d := p.d

	newSeg, err := d.nextSeq()
	if err != nil {
		return err
	}

	sid := SegmentId(newSeg)

	d.log.Debug("creating packed segment", "id", sid)

	locs, stats, err := sb.Flush(ctx, d.log, d.sa, sid, d.volName)
	if err != nil {
		return err
	}

	d.s.Create(sid, stats)

	err = p.m.UpdateBatch(d.log, locs, sid, d.s)
	if err != nil {
		return err
	}

	return nil
}

func (p *Packer) Pack(ctx context.Context) error {
	err := p.iterateExtents(ctx)
	if err != nil {
		return err
	}

	return p.removeOldSegments(ctx)
}

func (p *Packer) removeOldSegments(ctx context.Context) error {
	segments, err := p.d.s.AllDeadSegments()
	if err != nil {
		return err
	}

	for _, seg := range segments {
		p.d.log.Debug("removing dead segment", "id", seg)
		err := p.d.removeSegmentIfPossible(ctx, seg)
		if err != nil {
			return err
		}

		p.d.s.SetDeleted(seg)
	}

	p.d.log.Debug("removed dead segments", "count", len(segments))

	return nil
}

func (d *Disk) Pack(ctx context.Context) error {
	err := d.CloseSegment(ctx)
	if err != nil {
		return err
	}

	packer := &Packer{d: d, m: d.lba2pba}
	return packer.Pack(ctx)
}
