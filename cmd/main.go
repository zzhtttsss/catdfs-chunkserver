package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
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
		log.Println("Http server error, port 9101", err)
	}
	select {}
}
