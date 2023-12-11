package lsvd

type opts struct {
	sa      SegmentAccess
	volName string
}

type Option func(o *opts)

func WithSegmentAccess(sa SegmentAccess) Option {
	return func(o *opts) {
		o.sa = sa
	}
}

func WithVolumeName(name string) Option {
	return func(o *opts) {
		o.volName = name
	}
}
