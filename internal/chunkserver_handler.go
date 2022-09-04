package internal

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"net"
	"os"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
)

var GlobalChunkServerHandler = &ChunkServerHandler{}

type ChunkServerHandler struct {
	pb.UnimplementedRegisterServiceServer
	pb.UnimplementedPipLineServiceServer
}

////CreateChunkServerHandler 创建ChunkServerHandler
//func CreateChunkServerHandler() {
//	GlobalChunkServerHandler = &ChunkServerHandler{}
//}

// TransferFile 由Chunkserver调用该方法，维持心跳
func (handler *ChunkServerHandler) TransferFile(stream pb.PipLineService_TransferFileServer) error {
	p, _ := peer.FromContext(stream.Context())
	address := p.Addr.String()
	logrus.Infof("start to receive snd send chunk from: %s", address)
	return DoTransferFile(stream)
}

func (handler *ChunkServerHandler) Server() {
	go Heartbeat()
	listener, err := net.Listen(common.TCP, common.AddressDelimiter+viper.GetString(common.ChunkPort))
	if err != nil {
		logrus.Errorf("Fail to server, error code: %v, error detail: %s,", common.ChunkServerRPCServerFailed, err.Error())
		os.Exit(1)
	}
	server := grpc.NewServer()
	pb.RegisterRegisterServiceServer(server, handler)
	pb.RegisterPipLineServiceServer(server, handler)
	logrus.Infof("Chunkserver is running, listen on %s%s", common.LocalIP, viper.GetString(common.ChunkPort))
	server.Serve(listener)
}
