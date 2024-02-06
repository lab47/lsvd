package lsvd

import "github.com/oklog/ulid/v2"

type SegmentId ulid.ULID

func (s SegmentId) String() string {
	return ulid.ULID(s).String()
}

const SegmentIdSize = 16
