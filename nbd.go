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
	return &nbdWrapper{log, ctx, d}
}

func blkSum(b []byte) string {
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

func (n *nbdWrapper) ReadAt(b []byte, off int64) (int, error) {
	ext, err := ExtentOverlay(b)
	if err != nil {
		return 0, err
	}

	blk := LBA(off / BlockSize)

	err = n.d.ReadExtent(n.ctx, blk, ext)
	if err != nil {
		n.log.Error("nbd read-at error", "error", err, "block", blk)
		return 0, err
	}

	n.log.Trace("nbd read-at", "size", len(b), "offset", off)

	return len(b), nil
}

func (n *nbdWrapper) WriteAt(b []byte, off int64) (int, error) {
	n.log.Trace("nbd write-at", "size", len(b), "offset", off)

	ext, err := ExtentOverlay(b)
	if err != nil {
		return 0, err
	}

	blk := LBA(off / BlockSize)

	err = n.d.WriteExtent(n.ctx, blk, ext)
	if err != nil {
		n.log.Error("nbd write-at error", "error", err, "block", blk)
		return 0, err
	}

	return len(b), nil
}

func (n *nbdWrapper) ZeroAt(off, size int64) error {
	n.log.Trace("nbd zero-at", "size", size, "offset", off)

	blk := LBA(off / BlockSize)

	numBlocks := size / BlockSize

	err := n.d.ZeroBlocks(n.ctx, blk, numBlocks)
	if err != nil {
		n.log.Error("nbd write-at error", "error", err, "block", blk)
		return err
	}

	return nil
}

func (n *nbdWrapper) Trim(off, size int64) error {
	n.log.Trace("nbd trim", "size", size, "offset", off)

	blk := LBA(off / BlockSize)

	numBlocks := size / BlockSize

	err := n.d.ZeroBlocks(n.ctx, blk, numBlocks)
	if err != nil {
		n.log.Error("nbd trim error", "error", err, "block", blk)
		return err
	}

	return nil
}

const maxSize = 1024 * 1024 * 1024 * 100 // 100GB

func (n *nbdWrapper) Size() (int64, error) {
	return maxSize, nil
}

func (n *nbdWrapper) Sync() error {
	n.log.Trace("nbd sync")
	return nil
}
