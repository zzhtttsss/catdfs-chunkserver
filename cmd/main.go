package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"tinydfs-base/common"
	"tinydfs-base/config"
	"tinydfs-chunkserver/internal"
)

const (
	MetricsServerPort = "9101"
)

func init() {
	config.InitConfig()
	internal.CreateGlobalChunkServerHandler()
	internal.DNInfo = internal.RegisterDataNode()
	go internal.ConsumeSendingTasks()
	go internal.MonitorChunks()
}

func main() {
	go internal.GlobalChunkServerHandler.Server()
	http.Handle("/metrics", promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{
			EnableOpenMetrics: true,
		},
	))
	err := http.ListenAndServe(common.AddressDelimiter+MetricsServerPort, nil)
	if err != nil {
		internal.Logger.Errorf("Http server error, Error detail %s", err)
	}
	select {}
}
