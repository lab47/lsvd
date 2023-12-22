package lsvd

import (
	"context"
	"crypto/sha256"

	"github.com/hashicorp/go-hclog"
	"github.com/lab47/lsvd/pkg/nbd"
	"github.com/mr-tron/base58"
)

type nbdWrapper struct {
	log hclog.Logger
	ctx context.Context
	d   *Disk
}

var _ nbd.Backend = &nbdWrapper{}

func NBDWrapper(ctx context.Context, log hclog.Logger, d *Disk) nbd.Backend {
	log = log.Named("nbd")
	return &nbdWrapper{log, ctx, d}
}

func blkSum(b []byte) string {
	b = b[:BlockSize]

	return rangeSum(b[:BlockSize])
}

func rangeSum(b []byte) string {
	empty := true

	for _, x := range b {
		if x != 0 {
			empty = false
			break
		}
	}

	if empty {
		return "0"
	}

	x := sha256.Sum256(b)
	return base58.Encode(x[:])
}

func logBlocks(log hclog.Logger, msg string, idx LBA, data []byte) {
	for len(data) > 0 {
		log.Trace(msg, "block", idx, "sum", blkSum(data))
		data = data[BlockSize:]
		idx++
	}
}

func (n *nbdWrapper) ReadAt(b []byte, off int64) (int, error) {
	blk := LBA(off / BlockSize)
	blocks := uint32(len(b) / BlockSize)

	n.log.Trace("nbd read-at",
		"size", len(b), "offset", off,
		"extent", Extent{blk, blocks},
	)

	data, err := n.d.ReadExtent(n.ctx, Extent{LBA: blk, Blocks: blocks})
	if err != nil {
		n.log.Error("nbd read-at error", "error", err, "block", blk)
		return 0, err
	}

	logBlocks(n.log, "read block sums", blk, b)

	err = data.CopyTo(b)
	if err != nil {
		n.log.Error("nbd read-at error", "error", err, "block", blk)
		return 0, err
	}

	return len(b), nil
}

func (n *nbdWrapper) WriteAt(b []byte, off int64) (int, error) {
	n.log.Trace("nbd write-at", "size", len(b), "offset", off)

	ext, err := ExtentOverlay(b)
	if err != nil {
		return 0, err
	}

	blk := LBA(off / BlockSize)

	logBlocks(n.log, "write block sums", blk, b)

	err = n.d.WriteExtent(n.ctx, ext.MapTo(blk))
	if err != nil {
		n.log.Error("nbd write-at error", "error", err, "block", blk)
		return 0, err
	}

	return len(b), nil
}

func (n *nbdWrapper) ZeroAt(off, size int64) error {
	blk := LBA(off / BlockSize)

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
	n.log.Trace("nbd sync")
	return n.d.SyncWriteCache()
}
