package internal

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
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

// TransferChunk Called by client or chunkserver.
// Transfer a chunk of the file to a chunkserver using stream and let that chunkserver transfer
// this chunk to another chunkserver if needed.
func (handler *ChunkServerHandler) TransferChunk(stream pb.PipLineService_TransferChunkServer) error {
	p, _ := peer.FromContext(stream.Context())
	address := p.Addr.String()
	logrus.Infof("start to receive snd send chunk from: %s", address)
	err := DoTransferFile(stream)
	if err != nil {
		logrus.Errorf("Fail to check path and filename for add operation, error code: %v, error detail: %s,", common.MasterCheckArgs4AddFailed, err.Error())
		details, _ := status.New(codes.Unavailable, err.Error()).WithDetails(&pb.RPCError{
			Code: common.ChunkServerTransferChunkFailed,
			Msg:  err.Error(),
		})
		return details.Err()
	}
	return nil
}

func (handler *ChunkServerHandler) Server() {
	go Heartbeat()
	listener, err := net.Listen(common.TCP, common.AddressDelimiter+viper.GetString(common.ChunkPort))
	if err != nil {
		logrus.Errorf("Fail to server, error code: %v, error detail: %s,", common.ChunkServerRPCServerFailed, err.Error())
		os.Exit(1)
	}
	server := grpc.NewServer()
	pb.RegisterPipLineServiceServer(server, handler)
	logrus.Infof("Chunkserver is running, listen on %s%s", common.LocalIP, viper.GetString(common.ChunkPort))
	server.Serve(listener)
}
