package lsvd

type opts struct {
	sa SegmentAccess
}

type Option func(o *opts)

func WithSegmentAccess(sa SegmentAccess) Option {
	return func(o *opts) {
		o.sa = sa
	}
}
