package internal

import (
	"context"
	"errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"io"
	"os"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
)

const (
	addressKey = "address"
)

// RegisterDataNode 向NameNode注册DataNode，取得ID
func RegisterDataNode() *DataNodeInfo {
	addr := viper.GetString(common.MasterAddr) + viper.GetString(common.MasterPort)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	c := pb.NewRegisterServiceClient(conn)
	ctx := context.Background()
	res, err := c.Register(ctx, &pb.DNRegisterArgs{})
	if err != nil {
		logrus.Panicf("Fail to register, error code: %v, error detail: %s,", common.ChunkServerRegisterFailed, err.Error())
		//Todo 根据错误类型进行重试（当前master的register不会报错，所以err直接panic并重启即可）
	}
	logrus.Infof("Register Success,get ID: %s", res.Id)

	return &DataNodeInfo{
		Id:   res.Id,
		Conn: conn,
	}
}

func Heartbeat() {
	reconnectCount := 0
	for {
		c := pb.NewHeartbeatServiceClient(DNInfo.Conn)
		_, err := c.Heartbeat(context.Background(), &pb.HeartbeatArgs{Id: DNInfo.Id})

		if err != nil {
			logrus.Errorf("Fail to heartbeat, error code: %v, error detail: %s,", common.ChunkServerHeartbeatFailed, err.Error())
			rpcError, _ := status.FromError(err)

			if (rpcError.Details()[0]).(pb.RPCError).Code == common.MasterHeartbeatFailed {
				logrus.Errorf("[Id=%s] Heartbeat failed. Get ready to reconnect[Times=%d].\n", DNInfo.Id, reconnectCount+1)
				reconnect()
				reconnectCount++

				if reconnectCount == viper.GetInt(common.ChunkHeartbeatReconnectCount) {
					logrus.Fatalf("[Id=%s] Reconnect failed. Offline.\n", DNInfo.Id)
					break
				}
				continue
			}
		}
		time.Sleep(time.Duration(viper.GetInt(common.ChunkHeartbeatSendTime)) * time.Second)
	}
}

//重连：NameNode挂了，重连并重新注册；DataNode或NameNode网络波动，不需要重新注册，重连并继续发送心跳即可
func reconnect() {
	addr := viper.GetString(common.MasterAddr) + viper.GetString(common.MasterPort)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	_ = DNInfo.Conn.Close()
	DNInfo.Conn = conn
}

func DoTransferFile(stream pb.PipLineService_TransferFileServer) error {
	var (
		pieceOfChunk *pb.PieceOfChunk
		pieceChan    = make(chan *pb.PieceOfChunk)
		nextAddress  string
		err          error
		nextStream   pb.PipLineService_TransferFileClient
	)

	md, _ := metadata.FromIncomingContext(stream.Context())
	chunkId := md.Get(chunkIdString)[0]
	addresses := md.Get(addressKey)
	if len(addresses) != 0 {
		nextAddress = addresses[0]
		addresses = addresses[1:]
		conn, _ := grpc.Dial(nextAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		c := pb.NewPipLineServiceClient(conn)
		ctx := context.Background()
		nextStream, err = c.TransferFile(ctx)
	}
	go storeFile(pieceChan, chunkId)
	for {
		pieceOfChunk, err = stream.Recv()
		pieceChan <- pieceOfChunk
		if nextAddress != "" {
			err := nextStream.Send(pieceOfChunk)
			if err != nil {
				return err
			}
		}
		if err == io.EOF {
			close(pieceChan)
			_, err = nextStream.CloseAndRecv()
			if err != nil {
				err = errors.New("failed to send status code")
				return err
			}
			err = stream.SendAndClose(&pb.TransferFileReply{})
			if err != nil {
				err = errors.New("failed to send status code")
				return err
			}
			return nil
		}

	}
}

func storeFile(pieceChan chan *pb.PieceOfChunk, chunkId string) {
	chunkFile, err := os.Create(viper.GetString(common.ChunkStoragePath) + chunkId)
	if err != nil {
		logrus.Errorf("fail to open a chunk file, error detail: %s", err.Error())
	}
	defer func() {
		chunkFile.Close()
	}()
	for piece := range pieceChan {
		if _, err := chunkFile.Write(piece.Piece); err != nil {
			logrus.Errorf("fail to write a piece to chunk file, error detail: %s", err.Error())
		}
	}
}