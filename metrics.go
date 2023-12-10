package lsvd

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	blocksWritten = promauto.NewCounter(prometheus.CounterOpts{
		Name: "lsvd_blocks_written",
		Help: "The total number of blocks written",
	})

	blocksRead = promauto.NewCounter(prometheus.CounterOpts{
		Name: "lsvd_blocks_read",
		Help: "The total number of blocks read",
	})

	blocksReadLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "lsvd_blocks_read_time",
		Help:    "The total number of blocks read",
		Buckets: prometheus.DefBuckets,
	})

	blocksWriteLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "lsvd_blocks_write_time",
		Help:    "The total number of blocks read",
		Buckets: prometheus.DefBuckets,
	})

	iops = promauto.NewCounter(prometheus.CounterOpts{
		Name: "lsvd_iops",
		Help: "The total number of iops",
	})

	segmentsWritten = promauto.NewCounter(prometheus.CounterOpts{
		Name: "lsvd_segments_written",
		Help: "The total number of segments written",
	})

	segmentsBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "lsvd_segments_bytes_written",
		Help: "The total number of segments bytes written",
	})

	segmentTime = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "lsvd_segments_upload_time",
		Help:    "The total number of segments bytes written",
		Buckets: prometheus.DefBuckets,
	})
)
