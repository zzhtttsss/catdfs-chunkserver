package internal

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	csChunkNumberMonitor = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "chunkserver_chunk_number",
		Help: "the number of chunks in chunkserver",
	})
)
