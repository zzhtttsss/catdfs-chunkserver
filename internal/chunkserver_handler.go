package internal

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"net"
	"os"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/config"
	"tinydfs-base/protocol/pb"
	"tinydfs-base/util"
)

var GlobalChunkServerHandler *ChunkServerHandler
var Logger *logrus.Logger

// ChunkServerHandler represent a chunkserver node to handle all incoming requests.
type ChunkServerHandler struct {
	EtcdClient *clientv3.Client
	pb.UnimplementedRegisterServiceServer
	pb.UnimplementedPipLineServiceServer
	pb.UnimplementedSetupStreamServer
}

func CreateGlobalChunkServerHandler() {
	var err error
	Logger = config.InitLogger(Logger, true)
	Logger.SetLevel(logrus.Level(viper.GetInt(common.ChunkLogLevel)))
	GlobalChunkServerHandler = &ChunkServerHandler{}
	GlobalChunkServerHandler.EtcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{viper.GetString(common.EtcdEndPoint)},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		Logger.Panicf("Fail to get etcd client, error detail : %s", err.Error())
	}
}

// TransferChunk Called by client or chunkserver.
// Transfer a chunk of the file to a chunkserver using stream and let that
// chunkserver transfer this chunk to another chunkserver if needed.
func (handler *ChunkServerHandler) TransferChunk(stream pb.PipLineService_TransferChunkServer) error {
	p, _ := peer.FromContext(stream.Context())
	address := p.Addr.String()
	Logger.Infof("Start to receive snd send chunk from: %s", address)
	err := DoTransferFile(stream)
	if err != nil {
		Logger.Errorf("Fail to receive snd send chunk from: %s", address)
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.ChunkServerTransferChunkFailed,
			Msg:  err.Error(),
		})
		return details.Err()
	}
	return nil
}

func (handler *ChunkServerHandler) SetupStream2DataNode(args *pb.SetupStream2DataNodeArgs, stream pb.SetupStream_SetupStream2DataNodeServer) error {
	Logger.Infof("Get request for setting up stream with data node, DataNodeId: %s, ChunkId: %s", args.DataNodeId, args.ChunkId)
	err := DoSendStream2Client(args, stream)
	if err != nil {
		Logger.Errorf("Fail to send stream to client for get operation, error code: %v, error detail: %s,", common.MasterCheckArgs4AddFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.ChunkServerTransferChunkFailed,
			Msg:  err.Error(),
		})
		return details.Err()
	}
	return nil
}

func (handler *ChunkServerHandler) Server() {
	go Heartbeat()
	ip, _ := util.GetLocalIP()
	Logger.Infof("local ip is: %s", ip)
	listener, err := net.Listen(common.TCP, util.CombineString(common.AddressDelimiter, viper.GetString(common.ChunkPort)))
	Logger.Infof("Listen address: %s", listener.Addr().String())
	if err != nil {
		Logger.Errorf("Fail to server, error code: %v, error detail: %s,", common.ChunkServerRPCServerFailed, err.Error())
		os.Exit(1)
	}
	server := grpc.NewServer()
	pb.RegisterPipLineServiceServer(server, handler)
	pb.RegisterSetupStreamServer(server, handler)
	Logger.Infof("Chunkserver is running, listen on %s%s", common.LocalIP, viper.GetString(common.ChunkPort))
	server.Serve(listener)
}
