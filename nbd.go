package lsvd

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/lab47/lsvd/logger"
	"github.com/lab47/lsvd/pkg/nbd"
	"github.com/lab47/mode"
	"golang.org/x/sys/unix"
)

type nbdWrapper struct {
	log logger.Logger
	ctx context.Context
	d   *Disk

	mu sync.Mutex

	gcRunning      bool
	lastCheckpoint time.Time

	buf *Buffers

	pendingWrite     Extent
	pendingWriteData bytes.Buffer

	pendingTrim Extent
}

type NBDBackendOpen struct {
	Ctx  context.Context
	Log  logger.Logger
	Disk *Disk
}

func (n *NBDBackendOpen) Open() nbd.Backend {
	return NBDWrapper(n.Ctx, n.Log, n.Disk)
}

func (n *NBDBackendOpen) Close(b nbd.Backend) {}

var _ nbd.Backend = &nbdWrapper{}

func NBDWrapper(ctx context.Context, log logger.Logger, d *Disk) *nbdWrapper {
	w := &nbdWrapper{
		log: log,
		ctx: ctx,
		d:   d,
		buf: NewBuffers(),
	}

	w.ctx = w.buf.Inject(w.ctx)

	d.SetAfterNS(w.AfterNS)

	return w
}

func logBlocks(log logger.Logger, msg string, idx LBA, data []byte) {
	for len(data) > 0 {
		log.Debug(msg, "block", idx, "sum", blkSum(data))
		data = data[BlockSize:]
		idx++
	}
}

func (n *nbdWrapper) Idle() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if time.Since(n.lastCheckpoint) > 1*time.Minute {
		n.lastCheckpoint = time.Now()

		ctx := context.Background()

		err := n.d.CheckpointGC(ctx)
		if err != nil {
			n.log.Error("error checkpointing gc", "error", err)
		}
	}
}

func (n *nbdWrapper) ReadAt(b []byte, off int64) (int, error) {
	blk := LBA(off / BlockSize)
	blocks := uint32(len(b) / BlockSize)

	ext := Extent{LBA: blk, Blocks: blocks}

	n.log.Debug("nbd read-at",
		"size", len(b), "offset", off,
		"extent", ext,
	)

	defer n.buf.Reset()

	err := n.flushPendingWrite()
	if err != nil {
		return 0, err
	}

	data := MapRangeData(ext, b)

	cps, err := n.d.ReadExtentInto(n.ctx, data)
	if err != nil {
		n.log.Error("nbd read-at error", "error", err, "block", blk)
		return 0, err
	}

	if cps.fd != nil {
		err = FillFromeCache(b, []CachePosition{cps})
		if err != nil {
			return 0, err
		}
	}

	return len(b), nil
}

func (n *nbdWrapper) ReadIntoConn(b []byte, off int64, output *os.File) (bool, error) {
	defer n.buf.Reset()

	blk := LBA(off / BlockSize)
	blocks := uint32(len(b) / BlockSize)

	ext := Extent{LBA: blk, Blocks: blocks}

	var isDebug = n.log.Is(logger.Debug)

	if isDebug {
		n.log.Debug("nbd read-at",
			"size", len(b), "offset", off,
			"extent", ext,
		)
	}

	err := n.flushPendingWrite()
	if err != nil {
		return false, err
	}

	data := MapRangeData(ext, b)

	cps, err := n.d.ReadExtentInto(n.ctx, data)
	if err != nil {
		n.log.Error("nbd read-at error", "error", err, "block", blk)
		return false, err
	}

	wfd := output.Fd()

	var written int

	left := len(b)

	if cps.fd == nil {
		writeResponses.Inc()

		off := 0
		for left > 0 {
			written, err = unix.Write(int(wfd), b[off:])
			if err != nil {
				n.log.Error("error sending data via write(2)", "error", err)
				return true, nil
			}

			left -= written
			off += written

			if isDebug {
				n.log.LogAttrs(n.ctx, logger.Debug,
					"wrote data back data to nbd directly",
					slog.Int64("request", cps.size),
					slog.Int64("written", int64(written)),
				)
			}
		}
		return true, nil
	}

	rfd := cps.fd.Fd()
	sendfileResponses.Inc()

	off = cps.off

	for left > 0 {
		written, err = unix.Sendfile(int(wfd), int(rfd), &off, left)
		if err != nil {
			return true, nil
		}

		if isDebug {
			n.log.Debug("sendfile complete", "request", cps.size, "written", written, "left", left)
		}
		left -= written
		off += int64(written)
	}

	return true, nil
}

func (n *nbdWrapper) flushPendingWrite() error {
	if n.pendingTrim.Blocks > 0 {
		err := n.d.ZeroBlocks(n.ctx, n.pendingTrim)
		n.pendingTrim = Extent{}
		if err != nil {
			return err
		}
	}

	if n.pendingWriteData.Len() == 0 {
		return nil
	}

	defer n.pendingWriteData.Reset()

	pending := n.pendingWrite
	n.pendingWrite = Extent{}

	return n.d.WriteExtent(n.ctx, MapRangeData(pending, n.pendingWriteData.Bytes()))
}

func (n *nbdWrapper) queuePendingWrite(ext Extent, data []byte) bool {
	if n.pendingTrim.Blocks > 0 {
		return false
	}

	if n.pendingWriteData.Len() == 0 {
		n.pendingWrite = ext
		n.pendingWriteData.Write(data)
		return true
	}

	if n.pendingWrite.Last()+1 != ext.LBA {
		return false
	}

	if n.pendingWrite.Blocks+ext.Blocks > 20 {
		return false
	}

	n.pendingWrite.Blocks += ext.Blocks
	n.pendingWriteData.Write(data)

	return true
}

func (n *nbdWrapper) queueTrim(ext Extent) bool {
	if n.pendingWrite.Blocks > 0 {
		return false
	}

	if n.pendingTrim.Blocks == 0 {
		n.pendingTrim = ext
		return true
	}

	if n.pendingTrim.Last()+1 == ext.LBA {
		n.pendingTrim.Blocks += ext.Blocks
		return true
	}

	return false
}

func (n *nbdWrapper) WriteAt(b []byte, off int64) (int, error) {
	n.log.Debug("nbd write-at", "size", len(b), "offset", off)

	defer n.buf.Reset()

	blk := LBA(off / BlockSize)

	ext := Extent{
		LBA:    blk,
		Blocks: uint32(len(b) / BlockSize),
	}

	if mode.Debug() {
		logBlocks(n.log, "write block sums", blk, b)
	}

	if n.queuePendingWrite(ext, b) {
		return len(b), nil
	}

	err := n.flushPendingWrite()
	if err != nil {
		return 0, err
	}

	if !n.queuePendingWrite(ext, b) {
		err := n.d.WriteExtent(n.ctx, MapRangeData(ext, b))
		if err != nil {
			n.log.Error("nbd write-at error", "error", err, "block", blk)
			return 0, err
		}
	}

	return len(b), nil
}

func (n *nbdWrapper) BeginGC() {
	ctx := context.Background()

	n.mu.Lock()
	defer n.mu.Unlock()

	n.beginGC(ctx)
}

func (n *nbdWrapper) beginGC(ctx context.Context) {
	if n.gcRunning {
		n.log.Debug("currently mid-GC, not starting a new one")
		return
	}

	seg, running, err := n.d.GCInBackground(ctx, 0.30)
	if err != nil {
		n.log.Error("error starting GC", "error", err)
		return
	}

	if !running {
		return
	}

	n.log.Info("starting GC", "segment", seg)

	n.lastCheckpoint = time.Now()
	n.gcRunning = true
}

func (n *nbdWrapper) AfterNS(_ SegmentId) {
	n.BeginGC()
}

func (n *nbdWrapper) ZeroAt(off, size int64) error {
	blk := LBA(off / BlockSize)

	defer n.buf.Reset()

	numBlocks := uint32(size / BlockSize)

	n.log.LogAttrs(n.ctx, logger.Debug,
		"nbd zero-at",
		slog.Int64("size", size),
		slog.Int64("offset", off),
		slog.Int64("lba", int64(blk)),
		slog.Int64("blocks", int64(numBlocks)),
	)

	ext := Extent{LBA: blk, Blocks: numBlocks}

	if n.queueTrim(ext) {
		return nil
	}

	err := n.flushPendingWrite()
	if err != nil {
		return err
	}

	if !n.queueTrim(ext) {
		err := n.d.ZeroBlocks(n.ctx, Extent{blk, numBlocks})
		if err != nil {
			n.log.Error("nbd write-at error", "error", err, "block", blk)
			return err
		}
	}

	return nil
}

func (n *nbdWrapper) Trim(off, size int64) error {
	return n.ZeroAt(off, size)
}

func RoundToBlockSize(sz int64) int64 {
	diff := sz % BlockSize
	if diff == 0 {
		return sz
	}

	return sz - diff
}

var maxSize = RoundToBlockSize(1024 * 1024 * 1024 * 100) // 100GB

func (n *nbdWrapper) Size() (int64, error) {
	sz := n.d.Size()
	if sz == 0 {
		n.log.Warn("no size configured on disk, reporting default 100GB")
		// Use our default size
		return maxSize, nil
	}

	sz = RoundToBlockSize(sz)

	n.log.Info("reporting size to nbd", "size", sz)
	return sz, nil
}

func (n *nbdWrapper) Sync() error {
	defer n.buf.Reset()

	n.log.Debug("nbd sync")

	err := n.flushPendingWrite()
	if err != nil {
		return err
	}

	return n.d.SyncWriteCache()
}
