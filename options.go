package lsvd

type opts struct {
	sa         SegmentAccess
	volName    string
	autoCreate bool
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

func AutoCreate(ok bool) Option {
	return func(o *opts) {
		o.autoCreate = ok
	}
}
