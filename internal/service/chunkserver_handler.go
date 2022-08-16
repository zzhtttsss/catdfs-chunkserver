package service

import (
	"tinydfs-base/config"
	"tinydfs-base/protocol/pb"
	"tinydfs-chunkserver/internal/model"
)

var GlobalChunkServerHandler *ChunkServerHandler

type ChunkServerHandler struct {
	GlobalDataNode *model.DataNode
	pb.UnimplementedRegisterServiceServer
}

//CreateChunkServerHandler 创建ChunkServerHandler
func CreateChunkServerHandler() {
	config.InitConfig()
	GlobalChunkServerHandler = &ChunkServerHandler{
		GlobalDataNode: model.CreateDataNode(),
	}
}
