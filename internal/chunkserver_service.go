package internal

import (
	"context"
	"fmt"
	"github.com/avast/retry-go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
)

const (
	heartbeatRetryTime = 5
)

// RegisterDataNode 向NameNode注册DataNode，取得ID
func RegisterDataNode() *DataNodeInfo {
	conn, _ := getMasterConn()
	c := pb.NewRegisterServiceClient(conn)
	ctx := context.Background()
	res, err := c.Register(ctx, &pb.DNRegisterArgs{})
	if err != nil {
		logrus.Panicf("Fail to register, error code: %v, error detail: %s,", common.ChunkServerRegisterFailed, err.Error())
		// Todo 根据错误类型进行重试（当前master的register不会报错，所以err直接panic并重启即可）
		// Todo 错误可能是因为master的leader正好挂了，所以可以重新获取leader地址来重试
	}
	logrus.Infof("Register Success,get ID: %s", res.Id)

	return &DataNodeInfo{
		Id:   res.Id,
		Conn: conn,
	}
}

func getMasterConn() (*grpc.ClientConn, error) {
	ctx := context.Background()
	kv := clientv3.NewKV(GlobalChunkServerHandler.EtcdClient)
	getResp, err := kv.Get(ctx, common.LeaderAddressKey)
	if err != nil {
		logrus.Errorf("Fail to get kv when init, error detail: %s", err.Error())
		return nil, err
	}
	addr := string(getResp.Kvs[0].Value)
	addr = strings.Split(addr, common.AddressDelimiter)[0] + viper.GetString(common.MasterPort)
	logrus.Infof("leader master address is: %s", addr)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logrus.Errorf("Fail to get connection to leader , error detail: %s", err.Error())
		return nil, err
	}
	return conn, nil
}

func Heartbeat() {
	for {
		err := retry.Do(func() error {
			c := pb.NewHeartbeatServiceClient(DNInfo.Conn)
			_, err := c.Heartbeat(context.Background(), &pb.HeartbeatArgs{Id: DNInfo.Id})
			if err != nil {
				conn, _ := getMasterConn()
				_ = DNInfo.Conn.Close()
				DNInfo.Conn = conn
				return err
			}
			return nil
		}, retry.Attempts(heartbeatRetryTime), retry.Delay(time.Second*5))
		if err != nil {
			logrus.Fatalf("[Id=%s] Reconnect failed. Offline.\n", DNInfo.Id)
		}
		time.Sleep(time.Second * 5)
	}

}

func DoTransferFile(stream pb.PipLineService_TransferChunkServer) error {
	var (
		pieceOfChunk *pb.PieceOfChunk
		nextStream   pb.PipLineService_TransferChunkClient
		wg           sync.WaitGroup
		err          error
	)

	// Get chunkId and slice including all chunkserver address that need to store this chunk
	md, _ := metadata.FromIncomingContext(stream.Context())
	chunkId := md.Get(common.ChunkIdString)[0]
	addresses := md.Get(common.AddressString)

	AddChunk(chunkId)
	if len(addresses) != 0 {
		nextStream, err = getNextStream(chunkId, addresses)
		if err != nil {
			logrus.Errorf("fail to get next stream, error detail: %s", err.Error())
			return err
		}
	}
	// Every piece of chunk will be added into pieceChan so that storeChunk function can get all pieces sequentially
	pieceChan := make(chan *pb.PieceOfChunk)
	errChan := make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()
		storeChunk(pieceChan, errChan, chunkId)
	}()
	// Receive pieces of chunk until there are no more pieces
	for {
		pieceOfChunk, err = stream.Recv()
		if err == io.EOF {
			close(pieceChan)
			if nextStream != nil {
				_, err = nextStream.CloseAndRecv()
				if err != nil {
					logrus.Errorf("fail to close send stream, error detail: %s", err.Error())
					return err
				}
			}
			err = stream.SendAndClose(&pb.TransferChunkReply{})
			if err != nil {
				logrus.Errorf("fail to close receive stream, error detail: %s", err.Error())
				return err
			}
			// Main thread will wait until goroutine success to store the block.
			wg.Wait()
			if len(errChan) != 0 {
				err = <-errChan
				return err
			}
			logrus.Info("OHHHHHHHHHHHH!")
			return nil
		}
		pieceChan <- pieceOfChunk
		if nextStream != nil {
			err := nextStream.Send(pieceOfChunk)
			if err != nil {
				logrus.Errorf("fail to send a piece to next chunkserver, error detail: %s", err.Error())
				return err
			}
		}
	}
}

// getNextStream Build stream to transfer this chunk to next chunkserver.
func getNextStream(chunkId string, addresses []string) (pb.PipLineService_TransferChunkClient, error) {
	nextAddress := addresses[0]
	addresses = addresses[1:]
	conn, _ := grpc.Dial(nextAddress+common.AddressDelimiter+viper.GetString(common.ChunkPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	c := pb.NewPipLineServiceClient(conn)
	newCtx := context.Background()
	for _, address := range addresses {
		newCtx = metadata.AppendToOutgoingContext(newCtx, common.AddressString, address)
	}
	newCtx = metadata.AppendToOutgoingContext(newCtx, common.ChunkIdString, chunkId)
	return c.TransferChunk(newCtx)
}

// StoreChunk store a chunk as a file named its id in this chunkserver.
// For I/O operation is very slow, this function will be run in a goroutine to not block the main thread transferring
// the chunk to another chunkserver.
func storeChunk(pieceChan chan *pb.PieceOfChunk, errChan chan error, chunkId string) {
	chunkFile, err := os.Create(viper.GetString(common.ChunkStoragePath) + chunkId)
	if err != nil {
		logrus.Errorf("fail to open a chunk file, error detail: %s", err.Error())
		errChan <- err
	}
	defer func() {
		close(errChan)
		chunkFile.Close()
	}()
	// Goroutine will be blocked until main thread receive pieces of chunk and put them into pieceChan
	for piece := range pieceChan {
		if _, err := chunkFile.Write(piece.Piece); err != nil {
			logrus.Errorf("fail to write a piece to chunk file, chunkId = %s, error detail: %s", chunkId, err.Error())
			errChan <- err
			break
		}
	}
}

// DoSendStream2Client call rpc to send data to client
func DoSendStream2Client(args *pb.SetupStream2DataNodeArgs, stream pb.SetupStream_SetupStream2DataNodeServer) error {
	//TODO 检查资源完整性
	//wait to return until sendChunk is finished or err occurs
	err := sendChunk(stream, args.ChunkId)
	if err != nil {
		return err
	}
	return nil
}

func sendChunk(stream pb.SetupStream_SetupStream2DataNodeServer, chunkId string) error {
	file, err := os.Open(fmt.Sprintf("./chunks/%s", chunkId))
	defer file.Close()
	if err != nil {
		return err
	}
	for i := 0; i < common.ChunkMBNum; i++ {
		buffer := make([]byte, common.MB)
		n, _ := file.Read(buffer)
		// sending done
		if n == 0 {
			return nil
		}
		logrus.Printf("Reading chunkMB index %d, reading bytes num %d", i, n)
		err = stream.Send(&pb.Piece{
			Piece: buffer[:n],
		})
		if err != nil {
			log.Println("stream.Send error ", err)
			return err
		}
	}
	return nil
}
