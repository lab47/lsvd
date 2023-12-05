package lsvd

import (
	"crypto/sha256"

	"github.com/hashicorp/go-hclog"
	"github.com/mr-tron/base58"
	"github.com/pojntfx/go-nbd/pkg/backend"
)

type nbdWrapper struct {
	log hclog.Logger
	d   *Disk
}

var _ backend.Backend = &nbdWrapper{}

func NBDWrapper(log hclog.Logger, d *Disk) backend.Backend {
	return &nbdWrapper{log, d}
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

	err = n.d.ReadExtent(blk, ext)
	if err != nil {
		n.log.Error("nbd read-at error", "error", err, "block", blk)
		return 0, err
	}

	n.log.Trace("nbd read-at", "size", len(b), "offset", off, "id", blkSum(b))

	return len(b), nil
}

func (n *nbdWrapper) WriteAt(b []byte, off int64) (int, error) {
	n.log.Trace("nbd write-at", "size", len(b), "offset", off, "id", blkSum(b))

	ext, err := ExtentOverlay(b)
	if err != nil {
		return 0, err
	}

	blk := LBA(off / BlockSize)

	err = n.d.WriteExtent(blk, ext)
	if err != nil {
		n.log.Error("nbd write-at error", "error", err, "block", blk)
		return 0, err
	}

	return len(b), nil
}

const maxSize = 1024 * 1024 * 1024 * 100 // 100GB

func (n *nbdWrapper) Size() (int64, error) {
	return maxSize, nil
}

func (n *nbdWrapper) Sync() error {
	return nil
}
