package internal

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"io"
	"log"
	"os"
	"sync"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
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
	log.Println("Get setup stream request from client.Ready to send ", args.ChunkId)
	//chunkIndex, _ := strconv.ParseInt(strings.Split(args.ChunkId, common.ChunkIdDelimiter)[1], 10, 32)
	//wait to return until sendChunk is finished or err occurs
	err := sendChunk(stream, args.ChunkId)
	if err != nil {
		return err
	}
	return nil
}

// setupStream2Client builds up stream with client to transfer this chunk's data to client
func setupStream2Client(clientAddr string, chunkIndex int32) (pb.PipLineService_TransferChunkClient, error) {
	log.Printf("set up stream 2 client %s", clientAddr)
	conn, _ := grpc.Dial(clientAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	c := pb.NewPipLineServiceClient(conn)
	newCtx := context.Background()
	newCtx = metadata.AppendToOutgoingContext(newCtx, common.ChunkIndexString, string(chunkIndex))
	return c.TransferChunk(newCtx)
}

func sendChunk(stream pb.SetupStream_SetupStream2DataNodeServer, chunkId string) error {
	file, err := os.Open(fmt.Sprintf("./chunks/%s", chunkId))
	defer file.Close()
	if err != nil {
		return err
	}
	info, _ := file.Stat()
	log.Println("file size ", info.Size())
	log.Println("sending chunk ", chunkId)
	for i := 0; i < common.ChunkMBNum; i++ {
		buffer := make([]byte, common.MB)
		n, _ := file.Read(buffer)
		// sending done
		if n == 0 {
			return nil
		}
		log.Printf("Reading chunkMB index %d, reading bytes num %d", i, n)
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
