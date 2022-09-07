package main

import (
	"tinydfs-base/config"
	"tinydfs-chunkserver/internal"
)

func init() {
	config.InitConfig()
	internal.DNInfo = internal.RegisterDataNode()
}

func main() {
	internal.GlobalChunkServerHandler.Server()
	select {}
}
