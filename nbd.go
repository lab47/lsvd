package lsvd

import (
	"context"
	"sync"
	"syscall"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/hashicorp/go-hclog"
	"github.com/lab47/lsvd/pkg/nbd"
	"github.com/lab47/mode"
	"golang.org/x/sys/unix"
)

type nbdWrapper struct {
	log hclog.Logger
	ctx context.Context
	d   *Disk

	mu sync.Mutex
	ci *CopyIterator

	buf *Buffers
}

type NBDBackendOpen struct {
	Ctx  context.Context
	Log  hclog.Logger
	Disk *Disk
}

func (n *NBDBackendOpen) Open() nbd.Backend {
	return NBDWrapper(n.Ctx, n.Log, n.Disk)
}

func (n *NBDBackendOpen) Close(b nbd.Backend) {}

var _ nbd.Backend = &nbdWrapper{}

func NBDWrapper(ctx context.Context, log hclog.Logger, d *Disk) nbd.Backend {
	log = log.Named("nbd")
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

func logBlocks(log hclog.Logger, msg string, idx LBA, data []byte) {
	for len(data) > 0 {
		log.Trace(msg, "block", idx, "sum", blkSum(data))
		data = data[BlockSize:]
		idx++
	}
}

func (n *nbdWrapper) Idle() {
	return
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.ci != nil {
		defer n.buf.Reset()

		n.log.Trace("processing GC copy iterator")
		ctx := context.Background()
		done, err := n.ci.Process(ctx, 100*time.Millisecond)
		if err != nil {
			n.log.Error("error processing GC copy", "error", err)
		}

		if done {
			n.ci.Close()
			n.ci = nil
			n.log.Info("finished GC copy process")
		}
	}
}

func (n *nbdWrapper) ReadAt(b []byte, off int64) (int, error) {
	blk := LBA(off / BlockSize)
	blocks := uint32(len(b) / BlockSize)

	ext := Extent{LBA: blk, Blocks: blocks}

	n.log.Trace("nbd read-at",
		"size", len(b), "offset", off,
		"extent", ext,
	)

	defer n.buf.Reset()

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

func (n *nbdWrapper) ReadIntoConn(b []byte, off int64, output syscall.Conn) (bool, error) {
	blk := LBA(off / BlockSize)
	blocks := uint32(len(b) / BlockSize)

	ext := Extent{LBA: blk, Blocks: blocks}

	n.log.Trace("nbd read-at",
		"size", len(b), "offset", off,
		"extent", ext,
	)

	defer n.buf.Reset()

	data := MapRangeData(ext, b)

	cps, err := n.d.ReadExtentInto(n.ctx, data)
	if err != nil {
		n.log.Error("nbd read-at error", "error", err, "block", blk)
		return false, err
	}

	if cps.fd == nil {
		return false, nil
	}

	sc, err := output.SyscallConn()
	if err != nil {
		return false, err
	}

	sc2, err := cps.fd.SyscallConn()
	if err != nil {
		return false, err
	}

	var written int
	sc2.Read(func(rfd uintptr) (done bool) {
		sc.Write(func(wfd uintptr) (done bool) {
			off := cps.off
			written, err = unix.Sendfile(int(wfd), int(rfd), &off, int(cps.size))
			return true
		})
		return true
	})

	spew.Dump(written)

	return true, nil
}

func (n *nbdWrapper) WriteAt(b []byte, off int64) (int, error) {
	n.log.Trace("nbd write-at", "size", len(b), "offset", off)

	defer n.buf.Reset()

	blk := LBA(off / BlockSize)

	ext := Extent{
		LBA:    blk,
		Blocks: uint32(len(b) / BlockSize),
	}

	if mode.Debug() {
		logBlocks(n.log, "write block sums", blk, b)
	}

	err := n.d.WriteExtent(n.ctx, MapRangeData(ext, b))
	if err != nil {
		n.log.Error("nbd write-at error", "error", err, "block", blk)
		return 0, err
	}

	return len(b), nil
}

func (n *nbdWrapper) AfterNS(_ SegmentId) {
	ctx := context.Background()

	defer n.buf.Reset()

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.ci != nil {
		n.log.Debug("currently mid-GC, not starting a new one")
		return
	}

	seg, ci, err := n.d.StartGC(ctx, 0.30)
	if err != nil {
		n.log.Error("error starting GC", "error", err)
		return
	}

	if ci == nil {
		n.log.Debug("no segments for GC detected")
		return
	}

	n.log.Info("starting GC", "segment", seg)

	n.ci = ci
}

func (n *nbdWrapper) ZeroAt(off, size int64) error {
	blk := LBA(off / BlockSize)

	defer n.buf.Reset()

	numBlocks := uint32(size / BlockSize)

	n.log.Trace("nbd zero-at",
		"size", size, "offset", off,
		"extent", Extent{blk, uint32(numBlocks)},
	)

	err := n.d.ZeroBlocks(n.ctx, Extent{blk, numBlocks})
	if err != nil {
		n.log.Error("nbd write-at error", "error", err, "block", blk)
		return err
	}

	return nil
}

func (n *nbdWrapper) Trim(off, size int64) error {
	blk := LBA(off / BlockSize)

	defer n.buf.Reset()

	numBlocks := uint32(size / BlockSize)

	n.log.Trace("nbd trim",
		"size", size, "offset", off,
		"extent", Extent{blk, uint32(numBlocks)},
	)

	err := n.d.ZeroBlocks(n.ctx, Extent{blk, numBlocks})
	if err != nil {
		n.log.Error("nbd trim error", "error", err, "block", blk)
		return err
	}

	return nil
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

	n.log.Trace("nbd sync")
	return n.d.SyncWriteCache()
}
