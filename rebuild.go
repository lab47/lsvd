package lsvd

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/go-hclog"
	"github.com/pkg/errors"
)

func (d *Disk) rebuildFromSegments(ctx context.Context) error {
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

	segTrack := &Segment{}

	d.segments[seg] = segTrack

	for i := uint32(0); i < hdr.ExtentCount; i++ {
		var eh ExtentHeader

		err := eh.Read(br)
		if err != nil {
			return err
		}

		segTrack.Size += uint64(eh.Blocks)
		segTrack.Used += uint64(eh.Blocks)

		segTrack.TotalBytes += eh.Size
		segTrack.UsedBytes += eh.Size

		eh.Offset += hdr.DataOffset

		affected, err := d.lba2pba.Update(ExtentLocation{
			ExtentHeader: eh,
			Segment:      seg,
		})
		if err != nil {
			return err
		}

		d.updateUsage(seg, affected)
	}

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

	d.log.Info("restoring write cache")

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

	d.log.Trace("reloading lba map from head.map")

	m, err := processLBAMap(d.log, f)
	if err != nil {
		return false, err
	}

	for i := m.m.Iterator(); i.Valid(); i.Next() {
		ro := i.Value()

		seg := d.segments[ro.Segment]
		if seg == nil {
			seg = &Segment{}
			d.segments[ro.Segment] = seg
		}

		seg.UsedBytes += uint64(ro.Size)
		seg.Used += uint64(ro.Partial.Blocks)
	}

	d.lba2pba = m

	return true, nil
}

func saveLBAMap(m *ExtentMap, f io.Writer) error {
	for it := m.m.Iterator(); it.Valid(); it.Next() {
		cur := it.Value()

		hclog.L().Error("write to lba map", "extent", cur.Partial, "flag", cur.Flags)

		err := binary.Write(f, binary.BigEndian, cur)
		if err != nil {
			return err
		}
	}

	return nil
}

func processLBAMap(log hclog.Logger, f io.Reader) (*ExtentMap, error) {
	m := NewExtentMap(log)

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

		log.Trace("read from lba map", "extent", pba.Partial, "flag", pba.Flags)

		m.m.Set(pba.Partial.LBA, &pba)
	}

	return m, nil
}
