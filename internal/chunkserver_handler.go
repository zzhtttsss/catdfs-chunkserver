package internal

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"os"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
)

var GlobalChunkServerHandler *ChunkServerHandler

// ChunkServerHandler represent a chunkserver node to handle all incoming requests.
type ChunkServerHandler struct {
	EtcdClient *clientv3.Client
	pb.UnimplementedRegisterServiceServer
	pb.UnimplementedPipLineServiceServer
	pb.UnimplementedSetupStreamServer
}

func CreateGlobalChunkServerHandler() {
	var err error
	GlobalChunkServerHandler = &ChunkServerHandler{}
	GlobalChunkServerHandler.EtcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{viper.GetString(common.EtcdEndPoint)},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logrus.Panicf("Fail to get etcd client, error detail : %s", err.Error())
	}
}

// TransferChunk Called by client or chunkserver.
// Transfer a chunk of the file to a chunkserver using stream and let that
// chunkserver transfer this chunk to another chunkserver if needed.
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

func (handler *ChunkServerHandler) SetupStream2DataNode(args *pb.SetupStream2DataNodeArgs, stream pb.SetupStream_SetupStream2DataNodeServer) error {
	logrus.Infof("Get request for set up stream with data node, DataNodeId: %s, ChunkId: %s", args.DataNodeId, args.ChunkId)
	err := DoSendStream2Client(args, stream)
	if err != nil {
		logrus.Errorf("Fail to send stream to client for get operation, error code: %v, error detail: %s,", common.MasterCheckArgs4AddFailed, err.Error())
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
	log.Println("Listen addr ", listener.Addr().String())
	if err != nil {
		logrus.Errorf("Fail to server, error code: %v, error detail: %s,", common.ChunkServerRPCServerFailed, err.Error())
		os.Exit(1)
	}
	server := grpc.NewServer()
	pb.RegisterPipLineServiceServer(server, handler)
	pb.RegisterSetupStreamServer(server, handler)
	logrus.Infof("Chunkserver is running, listen on %s%s", common.LocalIP, viper.GetString(common.ChunkPort))
	server.Serve(listener)
}
