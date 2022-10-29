package internal

import (
	"context"
	"fmt"
	"github.com/avast/retry-go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
	"tinydfs-base/util"
)

const (
	heartbeatRetryTime = 5
	maxGoroutineNum    = 5
)

// RegisterDataNode registers in the Master and get the datanode id
func RegisterDataNode() *DataNodeInfo {
	conn, _ := getMasterConn()
	c := pb.NewRegisterServiceClient(conn)
	ctx := context.Background()
	res, err := c.Register(ctx, &pb.DNRegisterArgs{
		ChunkIds: getLocalChunksId(),
	})
	if err != nil {
		logrus.Panicf("Fail to register, error code: %v, error detail: %s,", common.ChunkServerRegisterFailed, err.Error())
		// Todo 根据错误类型进行重试（当前master的register不会报错，所以err直接panic并重启即可）
		// Todo 错误可能是因为master的leader正好挂了，所以可以重新获取leader地址来重试
	}
	logrus.Infof("Register Success,get ID: %s", res.Id)
	var ioLoad atomic.Int64
	ioLoad.Store(0)
	return &DataNodeInfo{
		Id:               res.Id,
		Conn:             conn,
		ioLoad:           ioLoad,
		pendingChunkChan: make(chan *PendingChunks),
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
			failChunkInfos, successChunkInfos := HandleSendResult()
			c := pb.NewHeartbeatServiceClient(DNInfo.Conn)
			heartbeatArgs := &pb.HeartbeatArgs{
				Id:                DNInfo.Id,
				ChunkId:           GetAllChunkIds(),
				IOLoad:            DNInfo.GetIOLoad(),
				SuccessChunkInfos: successChunkInfos,
				FailChunkInfos:    failChunkInfos,
			}
			heartbeatReply, err := c.Heartbeat(context.Background(), heartbeatArgs)
			if err != nil {
				conn, _ := getMasterConn()
				_ = DNInfo.Conn.Close()
				DNInfo.Conn = conn
				return err
			}
			if len(heartbeatReply.ChunkInfos) != 0 {
				// 存储一个chunk要去哪些DataNode
				infosMap := make(map[PendingChunk][]string)
				// 存储一个DataNode对应的Address地址
				addsMap := make(map[string]string)
				for i, info := range heartbeatReply.ChunkInfos {
					pc := PendingChunk{
						chunkId:  info.ChunkId,
						sendType: int(info.SendType),
					}
					dataNodeIds, ok := infosMap[pc]
					addsMap[info.DataNodeId] = heartbeatReply.DataNodeAddress[i]
					if ok {
						dataNodeIds = append(dataNodeIds, info.DataNodeId)
					} else {
						infosMap[pc] = []string{info.DataNodeId}
					}
				}
				DNInfo.Add2chan(&PendingChunks{
					infos: infosMap,
					adds:  addsMap,
				})
			}
			return nil
		}, retry.Attempts(heartbeatRetryTime), retry.Delay(time.Second*5))
		if err != nil {
			logrus.Fatalf("[Id=%s] Reconnect failed. Offline.\n", DNInfo.Id)
		}
		time.Sleep(time.Second * time.Duration(viper.GetInt(common.ChunkHeartbeatTime)))
	}

}

func DoTransferFile(stream pb.PipLineService_TransferChunkServer) error {
	var (
		pieceOfChunk   *pb.PieceOfChunk
		nextStream     pb.PipLineService_TransferChunkClient
		wg             sync.WaitGroup
		err            error
		isStoreSuccess bool
	)
	failAdds := make([]string, 0)
	// Get chunkId and slice including all chunkserver address that need to store this chunk
	md, _ := metadata.FromIncomingContext(stream.Context())
	chunkId := md.Get(common.ChunkIdString)[0]
	addresses := md.Get(common.AddressString)

	AddPendingChunk(chunkId)
	defer func() {
		currentReply := &pb.TransferChunkReply{
			ChunkId:  chunkId,
			FailAdds: failAdds,
		}
		err = stream.SendAndClose(currentReply)
		// If current chunkserver can not send the result to previous chunkserver, we can not get how many
		// chunkserver have failed, so we treat this situation as a failure.
		if err != nil {
			logrus.Errorf("Fail to close receive stream, error detail: %s", err.Error())
			isStoreSuccess = false
		}
		FinishChunk(chunkId, isStoreSuccess)
	}()
	if len(addresses) != 0 {
		// Todo: try each address until success
		nextStream, err = getNextStream(chunkId, addresses)
		if err != nil {
			logrus.Errorf("Fail to get next stream, error detail: %s", err.Error())
			// It doesn't matter if we can't get the next stream, just handle current chunkserver as the last one.
			nextStream = nil
		}
	}
	// Every piece of chunk will be added into pieceChan so that storeChunk function can get all pieces sequentially.
	pieceChan := make(chan *pb.PieceOfChunk)
	errChan := make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()
		storeChunk(pieceChan, errChan, chunkId)
	}()
	DNInfo.IncIOLoad()
	defer DNInfo.DecIOLoad()
	// Receive pieces of chunk until there are no more pieces.
	for {
		pieceOfChunk, err = stream.Recv()
		if err == io.EOF {
			close(pieceChan)
			if nextStream != nil {
				previousReply, err := nextStream.CloseAndRecv()
				if err != nil {
					logrus.Errorf("Fail to close send stream, error detail: %s", err.Error())
					failAdds = append(failAdds, addresses[1:]...)
				} else {
					failAdds = append(failAdds, previousReply.FailAdds...)
				}
			}
			// Main thread will wait until goroutine success to store the block.
			wg.Wait()
			if len(errChan) != 0 {
				err = <-errChan
				logrus.Errorf("Fail to store a chunk, error detail: %s", err.Error())
				isStoreSuccess = false
				failAdds = append(failAdds, addresses[0])
				return err
			}
			isStoreSuccess = true
			logrus.Infof("Success to store a chunk, id: %s", chunkId)
			return nil
		} else if err != nil {
			logrus.Errorf("Fail to receive a piece from previous chunkserver, error detail: %s", err.Error())
			close(pieceChan)
			isStoreSuccess = false
			return err
		}
		pieceChan <- pieceOfChunk
		if nextStream != nil {
			err := nextStream.Send(pieceOfChunk)
			if err != nil {
				logrus.Errorf("Fail to send a piece to next chunkserver, error detail: %s", err.Error())
				failAdds = append(failAdds, addresses[1:]...)
				nextStream = nil
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
	defer close(errChan)
	chunkFile, err := os.Create(viper.GetString(common.ChunkStoragePath) + chunkId)
	if err != nil {
		logrus.Errorf("fail to open a chunk file, error detail: %s", err.Error())
		errChan <- err
	}
	defer chunkFile.Close()
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
	DNInfo.IncIOLoad()
	defer DNInfo.DecIOLoad()
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

// getLocalChunksId walk through the chunk directory and get all chunks names
func getLocalChunksId() []string {
	var chunksId []string
	filepath.Walk(viper.GetString(common.ChunkStoragePath), func(path string, info fs.FileInfo, err error) error {
		if info != nil && !info.IsDir() {
			chunksId = append(chunksId, info.Name())
		}
		return nil
	})
	csChunkNumberMonitor.Set(float64(len(chunksId)))
	return chunksId
}

func ConsumePendingChunks() {
	for chunks := range DNInfo.pendingChunkChan {
		var wg sync.WaitGroup
		infoChan := make(chan *ChunkSendInfo)
		resultChan := make(chan *util.ChunkSendResult)
		goroutineNum := maxGoroutineNum
		if len(chunks.infos) < maxGoroutineNum {
			goroutineNum = len(chunks.infos)
		}
		for i := 0; i < goroutineNum; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				go consumeSingleChunk(infoChan, resultChan)
			}()
		}
		for pc, dnId := range chunks.infos {
			chunkId := pc.chunkId
			dataNodeIds := dnId
			adds := make([]string, len(dataNodeIds))
			for i := 0; i < len(dataNodeIds); i++ {
				adds = append(adds, chunks.adds[dataNodeIds[i]])
			}
			stream, err := getNextStream(chunkId, adds)
			if err != nil {
				// Todo
				logrus.Errorf("fail to get next stream, error detail: %s", err.Error())
			}
			infoChan <- &ChunkSendInfo{
				stream:      stream,
				chunkId:     chunkId,
				dataNodeIds: dataNodeIds,
				adds:        adds,
				sendType:    pc.sendType,
			}
		}
		wg.Wait()
		close(resultChan)
		var newFailSendResult = make(map[string][]string)
		var newSuccessSendResult = make(map[string][]string)
		for result := range resultChan {
			if result.SendType == common.Copy {
				newFailSendResult[result.ChunkId] = result.FailDataNodes
				newSuccessSendResult[result.ChunkId] = result.SuccessDataNodes
			} else if result.SendType == common.Move {
				newFailSendResult[result.ChunkId] = []string{}
				newSuccessSendResult[result.ChunkId] = result.SuccessDataNodes
				go removeChunkById(result.ChunkId)
			}
		}
		Merge2SendResult(newFailSendResult, newSuccessSendResult)
	}
}

type ChunkSendInfo struct {
	stream      pb.PipLineService_TransferChunkClient
	chunkId     string
	dataNodeIds []string
	adds        []string
	sendType    int
}

func consumeSingleChunk(infoChan chan *ChunkSendInfo, resultChan chan *util.ChunkSendResult) {
	for info := range infoChan {
		DNInfo.IncIOLoad()

		defaultSingleResult := &util.ChunkSendResult{
			ChunkId:          info.chunkId,
			FailDataNodes:    info.dataNodeIds,
			SuccessDataNodes: info.dataNodeIds[0:0],
			SendType:         info.sendType,
		}
		file, err := os.Open(fmt.Sprintf("./chunks/%s", info.chunkId))
		if err != nil {
			//TODO 出现错误没有处理
			resultChan <- defaultSingleResult
			return
		}
		for i := 0; i < common.ChunkMBNum; i++ {
			buffer := make([]byte, common.MB)
			n, _ := file.Read(buffer)
			// sending done
			if n == 0 {
				transferChunkReply, err := info.stream.CloseAndRecv()
				if err != nil {
					resultChan <- defaultSingleResult
					return
				}
				resultChan <- util.ConvReply2SingleResult(transferChunkReply, info.dataNodeIds, info.adds, info.sendType)
				return
			}
			logrus.Printf("Reading chunkMB index %d, reading bytes num %d", i, n)
			err = info.stream.Send(&pb.PieceOfChunk{
				Piece: buffer[:n],
			})
			if err != nil {
				log.Println("stream.Send error ", err)
				resultChan <- defaultSingleResult
				return
			}
		}
		transferChunkReply, err := info.stream.CloseAndRecv()
		if err != nil {
			resultChan <- defaultSingleResult
			return
		}
		resultChan <- util.ConvReply2SingleResult(transferChunkReply, info.dataNodeIds, info.adds, info.sendType)
		DNInfo.DecIOLoad()
		file.Close()
	}
}

func removeChunkById(chunkId string) {
	_ = os.Remove(fmt.Sprintf("./chunks/%s", chunkId))
}
