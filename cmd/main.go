package main

import (
	"tinydfs-chunkserver/internal/service"
)

func init() {
	service.CreateChunkServerHandler()
}

func main() {
	go service.GlobalChunkServerHandler.GlobalDataNode.Heartbeat()
	select {}
}
