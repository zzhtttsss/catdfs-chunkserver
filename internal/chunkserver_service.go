package internal

import (
	"context"
	"fmt"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
	"tinydfs-base/util"
)

const (
	heartbeatRetryTime   = 5
	maxGoroutineNum      = 5
	inCompleteFileSuffix = "_incomplete"
	checkSumFileSuffix   = ".crc"
)

// RegisterDataNode register this chunkserver to the master.
func RegisterDataNode() *DataNodeInfo {
	conn, _ := getMasterConn()
	c := pb.NewRegisterServiceClient(conn)
	ctx := context.Background()
	localChunksId := getLocalChunksId()
	diskStatus := GetDiskStatus(viper.GetString(common.ChunkStoragePath))
	res, err := c.Register(ctx, &pb.DNRegisterArgs{
		ChunkIds:     localChunksId,
		FullCapacity: diskStatus.All,
		UsedCapacity: diskStatus.Used,
	})
	if err != nil {
		Logger.Panicf("Fail to register, error code: %v, error detail: %s,", common.ChunkServerRegisterFailed, err.Error())
		// Todo 根据错误类型进行重试（当前master的register不会报错，所以err直接panic并重启即可）
		// Todo 错误可能是因为master的leader正好挂了，所以可以重新获取leader地址来重试
	}
	Logger.Infof("Register Success, get ID: %s, get pandingCount: %d", res.Id, res.PendingCount)
	var ioLoad atomic.Int64
	ioLoad.Store(0)
	return &DataNodeInfo{
		Id:             res.Id,
		Conn:           conn,
		ioLoad:         ioLoad,
		taskChan:       make(chan *SendingTask),
		futureChunkNum: int(res.PendingCount),
		IsReady:        res.PendingCount == uint32(len(localChunksId)),
	}
}

// getMasterConn get the RPC connection with the leader of the master cluster.
func getMasterConn() (*grpc.ClientConn, error) {
	ctx := context.Background()
	kv := clientv3.NewKV(GlobalChunkServerHandler.EtcdClient)
	getResp, err := kv.Get(ctx, common.LeaderAddressKey)
	if err != nil {
		Logger.Errorf("Fail to get kv when init, error detail: %s", err.Error())
		return nil, err
	}
	addr := string(getResp.Kvs[0].Value)
	addr = strings.Split(addr, common.AddressDelimiter)[0] + viper.GetString(common.MasterPort)
	Logger.Debugf("Leader master address is: %s", addr)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		Logger.Errorf("Fail to get connection to leader , error detail: %s", err.Error())
		return nil, err
	}
	return conn, nil
}

// Heartbeat run in a goroutine. It keeps looping to do heartbeat with master
// every several seconds. It sends the result of transferring Chunk and receive
// the information which Chunk need to be transferred next and put the information
// into the taskChan.
func Heartbeat() {
	for {
		errCount := 0
		failChunkInfos, successChunkInfos := HandleSendResult()
		c := pb.NewHeartbeatServiceClient(DNInfo.Conn)
		chunkIds := GetAllChunkIds()
		isReadyThreshold := int(float64(DNInfo.futureChunkNum) * viper.GetFloat64(common.ChunkReadyThreshold))
		diskStatus := GetDiskStatus(viper.GetString(common.ChunkStoragePath))
		heartbeatArgs := &pb.HeartbeatArgs{
			Id:                DNInfo.Id,
			ChunkId:           chunkIds,
			IOLoad:            DNInfo.GetIOLoad(),
			FullCapacity:      diskStatus.All,
			UsedCapacity:      diskStatus.Used,
			SuccessChunkInfos: successChunkInfos,
			FailChunkInfos:    failChunkInfos,
			IsReady:           len(chunkIds) >= isReadyThreshold,
		}
		if heartbeatArgs.IsReady {
			DNInfo.futureChunkNum = 0
		}
		Logger.Debugf("Send heartbeat, isReady: %v", heartbeatArgs.IsReady)
		heartbeatReply, err := c.Heartbeat(context.Background(), heartbeatArgs)
		if err != nil {
			Logger.Warnf("Heartbeat send failed, have tried to reconnect %v time.", errCount)
			conn, _ := getMasterConn()
			_ = DNInfo.Conn.Close()
			DNInfo.Conn = conn
			errCount++
		}
		errCount = 0
		if heartbeatReply.ChunkInfos != nil {
			Logger.Debugf("Some chunks need to be proceed.")
			// Store the destination of pending chunk
			infosMap := make(map[PendingChunk][]string)
			// Store the address of datanode
			addsMap := make(map[string]string)
			removedChunks := make([]string, 0)
			updateMapLock.Lock()
			for i, info := range heartbeatReply.ChunkInfos {
				Logger.Debugf("ChunkId %s with SendType %v", info.ChunkId, info.SendType)
				pc := PendingChunk{
					chunkId:  info.ChunkId,
					sendType: int(info.SendType),
				}
				if info.SendType == common.DeleteSendType {
					if dataNodeIds, ok := successSendResult[pc]; ok {
						dataNodeIds = append(dataNodeIds, dataNodeIds...)
					} else {
						successSendResult[pc] = dataNodeIds
					}
					removedChunks = append(removedChunks, info.ChunkId)
					continue
				}
				dataNodeIds, ok := infosMap[pc]
				addsMap[info.DataNodeId] = heartbeatReply.DataNodeAddress[i]
				if ok {
					dataNodeIds = append(dataNodeIds, info.DataNodeId)
				} else {
					infosMap[pc] = []string{info.DataNodeId}
				}
			}
			updateMapLock.Unlock()
			DNInfo.Add2chan(&SendingTask{
				Infos: infosMap,
				Adds:  addsMap,
			})
			BatchRemoveChunkById(removedChunks)
		}
		if errCount >= heartbeatRetryTime {
			Logger.Fatalf("[Id=%s] disconnected", DNInfo.Id)
			break
		}
		time.Sleep(time.Second * time.Duration(viper.GetInt(common.ChunkHeartbeatSendTime)))
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
	// Get chunkId and slice including all chunkserver address that need to store this chunk.
	md, _ := metadata.FromIncomingContext(stream.Context())
	chunkId := md.Get(common.ChunkIdString)[0]
	addresses := md.Get(common.AddressString)
	chunkSize, _ := strconv.Atoi(md.Get(common.ChunkSizeString)[0])
	checkSums := md.Get(common.CheckSumString)
	checkSumString := strings.Join(checkSums, "")
	AddPendingChunk(chunkId)
	defer func() {
		Logger.Debugf("Chunk: %s, failAdds: %v", chunkId, failAdds)
		currentReply := &pb.TransferChunkReply{
			ChunkId:  chunkId,
			FailAdds: failAdds,
		}
		err = stream.SendAndClose(currentReply)
		// If current chunkserver can not send the result to previous chunkserver, we can not get
		// how many chunkserver have failed, so we treat this situation as a failure.
		if err != nil {
			Logger.Errorf("Fail to close receive stream, error detail: %s", err.Error())
			isStoreSuccess = false
		}
		if isStoreSuccess {
			FinishChunk(chunkId)
		}
	}()
	if len(addresses) > 1 {
		// Todo: try each address until success
		nextStream, err = getNextStream(chunkId, addresses[1:], chunkSize)
		if err != nil {
			Logger.Errorf("Fail to get next stream, error detail: %s", err.Error())
			// It doesn't matter if we can't get the next stream, just handle current chunkserver as the last one.
			failAdds = append(failAdds, addresses[1:]...)
			nextStream = nil
		}
	}
	// Every piece of chunk will be added into pieceChan so that storeChunk function
	// can get all pieces sequentially. If storeChunk occurs error and return, there
	// will be no consumer to consume pieceChan, so we need make pieceChan has cache.
	pieceChan := make(chan *pb.PieceOfChunk, common.ChunkMBNum)
	errChan := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		storeChunk(pieceChan, errChan, chunkId, chunkSize, checkSumString)
	}()
	DNInfo.IncIOLoad()
	defer DNInfo.DecIOLoad()
	// Receive pieces of chunk until there are no more pieces.
	pieceIndex := 0
	for {
		pieceOfChunk, err = stream.Recv()
		if err == io.EOF {
			close(pieceChan)
			if nextStream != nil {
				previousReply, err := nextStream.CloseAndRecv()
				if err != nil {
					Logger.Errorf("Fail to close send stream, error detail: %s", err.Error())
					failAdds = append(failAdds, addresses[1:]...)
				} else {
					failAdds = append(failAdds, previousReply.FailAdds...)
				}
			}
			// Main thread will wait until goroutine success to store the block.
			wg.Wait()
			if len(errChan) != 0 {
				err = <-errChan
				Logger.Errorf("Fail to store a chunk, error detail: %s", err.Error())
				isStoreSuccess = false
				failAdds = append(failAdds, addresses[0])
				return err
			}
			isStoreSuccess = true
			Logger.Infof("Success to store a chunk, id: %s", chunkId)
			return nil
		} else if err != nil {
			Logger.Errorf("Fail to receive a piece from previous chunkserver or client, error detail: %s", err.Error())
			close(pieceChan)
			isStoreSuccess = false
			return err
		} else if util.CRC32String(pieceOfChunk.Piece) != checkSums[pieceIndex] {
			err = fmt.Errorf("checksum is invalid")
			Logger.Errorf("Fail to receive a piece from previous chunkserver or client, error detail: %s", err.Error())
			close(pieceChan)
			isStoreSuccess = false
			return err
		}
		pieceChan <- pieceOfChunk
		if nextStream != nil {
			err := nextStream.Send(pieceOfChunk)
			if err != nil {
				Logger.Errorf("Fail to send a piece to next chunkserver, error detail: %s", err.Error())
				failAdds = append(failAdds, addresses[1:]...)
				nextStream = nil
			}
		}
	}
}

// getNextStream builds stream to transfer this chunk to next chunkserver in the
// pipeline.
func getNextStream(chunkId string, addresses []string, chunkSize int) (pb.PipLineService_TransferChunkClient, error) {
	nextAddress := addresses[0]
	Logger.Infof("Get stream, chunk id: %s, next address: %s", chunkId, nextAddress)
	conn, _ := grpc.Dial(nextAddress+common.AddressDelimiter+viper.GetString(common.ChunkPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	c := pb.NewPipLineServiceClient(conn)
	newCtx := context.Background()
	for _, address := range addresses {
		newCtx = metadata.AppendToOutgoingContext(newCtx, common.AddressString, address)
	}
	newCtx = metadata.AppendToOutgoingContext(newCtx, common.ChunkIdString, chunkId)
	newCtx = metadata.AppendToOutgoingContext(newCtx, common.ChunkSizeString, strconv.Itoa(chunkSize))
	return c.TransferChunk(newCtx)
}

// StoreChunk stores a chunk as a file named its id in this chunkserver. For I/O
// operation is very slow, this function will be run in a goroutine to not block
// the main thread transferring the chunk to another chunkserver.
func storeChunk(pieceChan chan *pb.PieceOfChunk, errChan chan error, chunkId string, chunkSize int, checkSumString string) {
	var err error
	defer func() {
		if err != nil {
			Logger.Errorf("Fail to store a chunk, chunkId = %s, error detail: %s", chunkId, err.Error())
			errChan <- err
		}
		close(errChan)
	}()
	chunkFile, err := os.OpenFile(viper.GetString(common.ChunkStoragePath)+chunkId+inCompleteFileSuffix,
		os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	defer chunkFile.Close()
	err = chunkFile.Truncate(int64(chunkSize))
	if err != nil {
		return
	}
	chunkData, err := unix.Mmap(int(chunkFile.Fd()), 0, chunkSize, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return
	}
	// Goroutine will be blocked until main thread receive pieces of chunk and put them into pieceChan.
	index := 0
	for piece := range pieceChan {
		for _, b := range piece.Piece {
			chunkData[index] = b
			index++
		}
	}
	err = unix.Munmap(chunkData)
	checkSumFile, err := os.OpenFile(viper.GetString(common.ChecksumStoragePath)+chunkId+checkSumFileSuffix+
		inCompleteFileSuffix, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	defer checkSumFile.Close()
	_, err = checkSumFile.WriteString(checkSumString)
	if err != nil {
		return
	}
}

// DoSendStream2Client calls rpc to send data to client.
func DoSendStream2Client(args *pb.SetupStream2DataNodeArgs, stream pb.SetupStream_SetupStream2DataNodeServer) error {
	//TODO 检查资源完整性
	//wait to return until sendChunk is finished or err occurs
	err := sendChunk(stream, args.ChunkId)
	if err != nil {
		return err
	}
	return nil
}

// sendChunk establishes a pipeline and sends a Chunk to next chunkserver in the
// pipeline. It is used to implement Chunk transmission between chunkservers.
func sendChunk(stream pb.SetupStream_SetupStream2DataNodeServer, chunkId string) error {
	DNInfo.IncIOLoad()
	defer DNInfo.DecIOLoad()
	file, err := os.Open(fmt.Sprintf("%s%s", viper.GetString(common.ChunkStoragePath), chunkId))
	defer file.Close()
	if err != nil {
		return err
	}
	for i := 0; i < common.ChunkMBNum; i++ {
		buffer := make([]byte, common.MB)
		n, _ := file.Read(buffer)
		if n == 0 {
			return nil
		}
		err = stream.Send(&pb.Piece{
			Piece: buffer[:n],
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// getLocalChunksId walks through the chunk directory and get all chunks names.
func getLocalChunksId() []string {
	updateChunksLock.Lock()
	defer updateChunksLock.Unlock()
	var chunksId []string
	filepath.Walk(viper.GetString(common.ChunkStoragePath), func(path string, info fs.FileInfo, err error) error {
		if info != nil && !info.IsDir() && !strings.HasSuffix(info.Name(), inCompleteFileSuffix) {
			infos := strings.Split(info.Name(), common.ChunkIdDelimiter)
			index, _ := strconv.Atoi(infos[1])
			chunksMap[info.Name()] = &Chunk{
				Id:         info.Name(),
				FileId:     infos[0],
				Index:      index,
				IsComplete: true,
				AddTime:    info.ModTime(),
			}
			chunksId = append(chunksId, info.Name())
		}
		return nil
	})
	csChunkNumberMonitor.Set(float64(len(chunksId)))
	return chunksId
}

// ConsumeSendingTasks runs in a goroutine. It gets and consumes SendingTask from
// taskChan. In particular, it will send all Chunk to the target chunkservers in
// the SendingTask and merge the result of SendingTask into existing results.
func ConsumeSendingTasks() {
	for chunks := range DNInfo.taskChan {
		var wg sync.WaitGroup
		infoChan := make(chan *ChunkSendInfo)
		resultChan := make(chan *util.ChunkSendResult, len(chunks.Infos))
		goroutineNum := maxGoroutineNum
		if len(chunks.Infos) < maxGoroutineNum {
			goroutineNum = len(chunks.Infos)
		}
		for i := 0; i < goroutineNum; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				consumeSingleChunk(infoChan, resultChan)
			}()
		}
		for pc, dnId := range chunks.Infos {
			chunkId := pc.chunkId
			dataNodeIds := dnId
			adds := make([]string, 0, len(dataNodeIds))
			for i := 0; i < len(dataNodeIds); i++ {
				adds = append(adds, chunks.Adds[dataNodeIds[i]])
			}
			stat, err := os.Stat(viper.GetString(common.ChunkStoragePath) + chunkId)
			// Let consumeSingleChunk handle this error.
			if err != nil {
				Logger.Errorf("Chunk not exist, error detail: %s", err.Error())
			}
			stream, err := getNextStream(chunkId, adds, int(stat.Size()))
			if err != nil {
				// Todo
				stream = nil
				Logger.Errorf("Fail to get next stream, error detail: %s", err.Error())
			}
			infoChan <- &ChunkSendInfo{
				stream:      stream,
				ChunkId:     chunkId,
				DataNodeIds: dataNodeIds,
				Adds:        adds,
				SendType:    pc.sendType,
			}
		}
		wg.Wait()
		close(resultChan)
		var (
			newFailSendResult    = make(map[PendingChunk][]string)
			newSuccessSendResult = make(map[PendingChunk][]string)
			removedChunkIds      = make([]string, 0)
		)
		for result := range resultChan {
			newSuccessSendResult[PendingChunk{
				chunkId:  result.ChunkId,
				sendType: result.SendType,
			}] = result.SuccessDataNodes
			if result.SendType == common.CopySendType {
				newFailSendResult[PendingChunk{
					chunkId:  result.ChunkId,
					sendType: result.SendType,
				}] = result.FailDataNodes
			} else if result.SendType == common.MoveSendType {
				Logger.Debugf("Delete Chunk: %s", result.ChunkId)
				removedChunkIds = append(removedChunkIds, result.ChunkId)
			}
		}
		Merge2SendResult(newFailSendResult, newSuccessSendResult)
		BatchRemoveChunkById(removedChunkIds)
	}
}

type ChunkSendInfo struct {
	stream      pb.PipLineService_TransferChunkClient
	ChunkId     string   `json:"chunk_id"`
	DataNodeIds []string `json:"data_node_ids"`
	Adds        []string `json:"adds"`
	SendType    int      `json:"send_type"`
}

// consumeSingleChunk establishes a pipeline, sends a Chunk to all target chunkserver
// through the pipeline and return the result by the resultChan.
func consumeSingleChunk(infoChan chan *ChunkSendInfo, resultChan chan *util.ChunkSendResult) {
	for info := range infoChan {
		DNInfo.IncIOLoad()
		defaultSingleResult := &util.ChunkSendResult{
			ChunkId:          info.ChunkId,
			FailDataNodes:    info.DataNodeIds,
			SuccessDataNodes: info.DataNodeIds[0:0],
			SendType:         info.SendType,
		}
		if info.stream == nil {
			resultChan <- defaultSingleResult
			DNInfo.DecIOLoad()
			continue
		}

		file, err := os.OpenFile(viper.GetString(common.ChunkStoragePath)+info.ChunkId, os.O_RDWR, 0644)
		if err != nil {
			//TODO 出现错误没有处理
			resultChan <- defaultSingleResult
			DNInfo.DecIOLoad()
			continue
		}
		fInfo, _ := file.Stat()
		pieceNum := int(fInfo.Size() / common.MB)
		buffer, _ := unix.Mmap(int(file.Fd()), 0, int(fInfo.Size()), unix.PROT_WRITE, unix.MAP_SHARED)

		for i := 0; i < pieceNum; i++ {
			if i == pieceNum-1 {
				err = info.stream.Send(&pb.PieceOfChunk{
					Piece: buffer[i*common.MB : fInfo.Size()],
				})
			} else {
				err = info.stream.Send(&pb.PieceOfChunk{
					Piece: buffer[i*common.MB : (i+1)*common.MB],
				})
			}

			if err != nil {
				resultChan <- defaultSingleResult
				DNInfo.DecIOLoad()
				file.Close()
				break
			}
			// sending done
			if i == pieceNum-1 {
				transferChunkReply, err := info.stream.CloseAndRecv()
				if err != nil {
					DNInfo.DecIOLoad()
					resultChan <- defaultSingleResult
					file.Close()
					break
				}
				resultChan <- util.ConvReply2SingleResult(transferChunkReply, info.DataNodeIds, info.Adds, info.SendType)
				DNInfo.DecIOLoad()
				file.Close()
			}
		}
		_ = unix.Munmap(buffer)
	}
}
