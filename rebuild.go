package lsvd

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"

	"github.com/lab47/lsvd/logger"
	"github.com/pkg/errors"
)

func (d *Disk) rebuildFromSegments(ctx context.Context) error {
	for idx, ld := range d.readDisks {
		// We don't populate from... ourselves.
		if ld == d {
			continue
		}

		ld.lba2pba.Populate(d.log, d.lba2pba, uint16(idx))
	}

	entries, err := d.sa.ListSegments(ctx, d.volName)
	if err != nil {
		return err
	}

	for _, ent := range entries {
		err := d.rebuildFromSegment(ctx, ent)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Disk) rebuildFromSegment(ctx context.Context, seg SegmentId) error {
	d.log.Info("rebuilding mappings from segment", "id", seg)

	f, err := d.sa.OpenSegment(ctx, seg)
	if err != nil {
		return err
	}

	defer f.Close()

	br := bufio.NewReader(ToReader(f))

	var hdr SegmentHeader

	err = hdr.Read(br)
	if err != nil {
		return err
	}

	d.log.Debug("extent header info", "count", hdr.ExtentCount, "data-begin", hdr.DataOffset)

	stats := &SegmentStats{}

	d.s.Create(seg, stats)

	for i := uint32(0); i < hdr.ExtentCount; i++ {
		var eh ExtentHeader

		_, err := eh.Read(br)
		if err != nil {
			return err
		}

		stats.Blocks += uint64(eh.Blocks)

		eh.Offset += hdr.DataOffset

		affected, err := d.lba2pba.Update(d.log, ExtentLocation{
			ExtentHeader: eh,
			Segment:      seg,
		})
		if err != nil {
			return err
		}

		d.s.UpdateUsage(d.log, seg, affected)
	}

	// Now reset the stats for our seg to the correct ones.
	d.s.Create(seg, stats)

	return nil
}

func (d *Disk) restoreWriteCache(ctx context.Context) error {
	entries, err := filepath.Glob(filepath.Join(d.path, "writecache.*"))
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		return nil
	}

	d.log.Info("restoring write cache", "entries", entries)

	for _, ent := range entries {
		err := d.restoreWriteCacheFile(ctx, ent)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Disk) restoreWriteCacheFile(ctx context.Context, path string) error {
	oc, err := NewSegmentCreator(d.log, d.volName, path)
	if err != nil {
		return err
	}

	d.curSeq, err = d.nextSeq()
	if err != nil {
		return err
	}

	d.curOC = oc

	return nil
}

func (d *Disk) saveLBAMap(ctx context.Context) error {
	f, err := os.Create(filepath.Join(d.path, "head.map"))
	if err != nil {
		return err
	}

	defer f.Close()

	return saveLBAMap(d.lba2pba, f)
}

func (d *Disk) loadLBAMap(ctx context.Context) (bool, error) {
	f, err := os.Open(filepath.Join(d.path, "head.map"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}

		return false, err
	}

	defer f.Close()

	d.log.Debug("reloading lba map from head.map")

	m, err := processLBAMap(d.log, f)
	if err != nil {
		return false, err
	}

	for i := m.m.Iterator(); i.Valid(); i.Next() {
		ro := i.Value()

		d.s.CreateOrUpdate(ro.Segment, ro.Size, uint64(ro.Live.Blocks))
	}

	d.lba2pba = m

	return true, nil
}

func saveLBAMap(m *ExtentMap, f io.Writer) error {
	for it := m.m.Iterator(); it.Valid(); it.Next() {
		cur := it.Value()

		err := binary.Write(f, binary.BigEndian, cur)
		if err != nil {
			return err
		}
	}

	return nil
}

func processLBAMap(log logger.Logger, f io.Reader) (*ExtentMap, error) {
	m := NewExtentMap()

	for {
		var (
			pba PartialExtent
		)

		err := binary.Read(f, binary.BigEndian, &pba)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		// log.Trace("read from lba map", "extent", pba.Live, "flag", pba.Flags)

		m.m.Set(pba.Live.LBA, pba)
	}

	return m, nil
}
