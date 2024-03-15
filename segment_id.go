package lsvd

import "github.com/oklog/ulid/v2"

type SegmentId ulid.ULID

func (s SegmentId) String() string {
	return ulid.ULID(s).String()
}

func (s SegmentId) Valid() bool {
	return s != SegmentId{}
}

const SegmentIdSize = 16
