package nbd

import "io"

type Backend interface {
	io.ReaderAt
	io.WriterAt

	ZeroAt(off, sz int64) error
	Trim(off, sz int64) error

	Size() (int64, error)
	Sync() error
}
