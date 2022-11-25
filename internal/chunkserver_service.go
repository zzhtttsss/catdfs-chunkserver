package internal

import (
	"context"
	"errors"
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
	incompleteFileSuffix = "_incomplete"
	checkSumFileSuffix   = ".crc"
	checksumDelimiter    = " "
)

// RegisterDataNode register this chunkserver to the master. Chunkserver will
// report its disk status and chunk information to master. Master will return
// the chunkserver's ID and the number of chunks that will be sent to this
// chunkserver.
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
		Id:                 res.Id,
		Conn:               conn,
		ioLoad:             ioLoad,
		BatchChunkTaskChan: make(chan *BatchChunkTask),
		futureChunkNum:     int(res.PendingCount),
		IsReady:            res.PendingCount == uint32(len(localChunksId)),
	}
}

// getMasterConn gets the RPC connection with the leader of the master cluster.
func getMasterConn() (*grpc.ClientConn, error) {
	ctx := context.Background()
	kv := clientv3.NewKV(GlobalChunkServerHandler.EtcdClient)
	getResp, err := kv.Get(ctx, common.LeaderAddressKey)
	if err != nil {
		Logger.Errorf("Fail to get kv when init, error detail: %s", err.Error())
		return nil, err
	}
	addr := string(getResp.Kvs[0].Value)
	addr = util.CombineString(strings.Split(addr, common.AddressDelimiter)[0], viper.GetString(common.MasterPort))
	Logger.Debugf("Leader master address is: %s", addr)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		Logger.Errorf("Fail to get connection to leader , error detail: %s", err.Error())
		return nil, err
	}
	return conn, nil
}

// Heartbeat run in a goroutine. It keeps looping to do heartbeat with master
// every several seconds. It sends the result of all Chunk tasks that have
// finished but not been sent to master and receive a batch of new Chunk tasks
// and put them into the BatchChunkTaskChan.
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
			InvalidChunks:     HandleInvalidChunks(),
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
		handleHeartbeatReply(heartbeatReply)
		if errCount >= heartbeatRetryTime {
			Logger.Fatalf("[Id=%s] disconnected", DNInfo.Id)
			break
		}
		errCount = 0
		time.Sleep(time.Second * time.Duration(viper.GetInt(common.ChunkHeartbeatSendTime)))
	}
}

// handleHeartbeatReply handles the reply of heartbeat. It will convert all new
// Chunk tasks to BatchChunkTask and put it into the BatchChunkTaskChan.
func handleHeartbeatReply(reply *pb.HeartbeatReply) {
	if reply.ChunkInfos != nil {
		Logger.Debugf("Some chunks need to be proceed.")
		var (
			// Store the destination of pending chunk
			infosMap = make(map[PendingChunk][]string)
			// Store the address of datanode
			addsMap       = make(map[string]string)
			removedChunks = make([]string, 0)
		)

		updateMapLock.Lock()
		for i, info := range reply.ChunkInfos {
			Logger.Debugf("ChunkId %s with SendType %v", info.ChunkId, info.SendType)
			pc := PendingChunk{
				chunkId:  info.ChunkId,
				sendType: int(info.SendType),
			}
			// Immediately execute remove task.
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
			addsMap[info.DataNodeId] = reply.DataNodeAddress[i]
			if ok {
				dataNodeIds = append(dataNodeIds, info.DataNodeId)
			} else {
				infosMap[pc] = []string{info.DataNodeId}
			}
		}
		updateMapLock.Unlock()
		DNInfo.Add2chan(&BatchChunkTask{
			Infos: infosMap,
			Adds:  addsMap,
		})
		// Delete a batch of chunks at a time to avoid frequent locking
		BatchRemoveChunkById(removedChunks)
	}
}

// ChunkTransferInfo include all information of a chunk that needs to be transferred.
type ChunkTransferInfo struct {
	chunkId    string
	addresses  []string
	chunkSize  int
	checkSums  []string
	failAdds   []string
	pieceChan  chan *pb.PieceOfChunk
	errChan    chan error
	stream     pb.PipLineService_TransferChunkServer
	nextStream pb.PipLineService_TransferChunkClient
}

// DoTransferChunk receive the chunk from a grpc stream and send it to the next
// chunkserver(if needed).It will:
// 1. get a ChunkTransferInfo from the stream.
// 2. add the incomplete chunk to the chunkMap.
// 3. get next stream if needed.
// 4. start a IO goroutine to store the chunk to disk.
// 5. receive the chunk from the stream and send it to the IO goroutine and the
//    next stream(if needed).
// 6. handle transfer result and reply to last chunkserver or client.
func DoTransferChunk(stream pb.PipLineService_TransferChunkServer) error {
	var (
		wg             sync.WaitGroup
		isStoreSuccess = true
	)
	info, err := getChunkTransferInfo(stream)
	if err != nil {
		return err
	}

	AddIncompleteChunk(info.chunkId)

	defer func() {
		handleTransferResult(info, isStoreSuccess)
	}()
	if len(info.addresses) > 1 {
		setNextStream(info)
	}
	// Every piece of chunk will be added into pieceChan so that storeChunk function
	// can get all pieces sequentially. If storeChunk occurs error and return, there
	// will be no consumer to consume pieceChan, so we need make pieceChan has cache.
	info.pieceChan = make(chan *pb.PieceOfChunk, common.ChunkMBNum)
	info.errChan = make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		storeChunk(info)
	}()
	DNInfo.IncIOLoad()
	defer DNInfo.DecIOLoad()

	// Receive pieces of chunk until there are no more pieces.
	err = consumePiece(info)
	if err != nil {
		_ = finishConsume(info, &wg, false)
		isStoreSuccess = false
		return err
	}
	err = finishConsume(info, &wg, true)
	if err != nil {
		isStoreSuccess = false
		return err
	}
	return nil
}

// getChunkTransferInfo get a ChunkTransferInfo from the stream.
func getChunkTransferInfo(stream pb.PipLineService_TransferChunkServer) (*ChunkTransferInfo, error) {
	failAdds := make([]string, 0)
	// Get chunkId and slice including all chunkserver address that need to store this chunk.
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return nil, errors.New("fail to get metadata from context")
	}
	chunkId := md.Get(common.ChunkIdString)[0]
	addresses := md.Get(common.AddressString)
	chunkSize, _ := strconv.Atoi(md.Get(common.ChunkSizeString)[0])
	checkSums := md.Get(common.CheckSumString)

	return &ChunkTransferInfo{
		chunkId:    chunkId,
		addresses:  addresses,
		chunkSize:  chunkSize,
		checkSums:  checkSums,
		failAdds:   failAdds,
		stream:     stream,
		nextStream: nil,
	}, nil
}

// handleTransferResult converts ChunkTransferInfo to pb.TransferChunkReply and
// send it to the last chunkserver or client.
func handleTransferResult(info *ChunkTransferInfo, isStoreSuccess bool) {
	Logger.Debugf("Chunk: %s, failAdds: %v", info.chunkId, info.failAdds)
	currentReply := &pb.TransferChunkReply{
		ChunkId:  info.chunkId,
		FailAdds: info.failAdds,
	}
	err := info.stream.SendAndClose(currentReply)
	// If current chunkserver can not send the result to previous chunkserver, we can
	// not get how many chunkserver have failed, so we treat this situation as a failure.
	if err != nil {
		Logger.Errorf("Fail to close receive stream, error detail: %s", err.Error())
		isStoreSuccess = false
	}
	if isStoreSuccess {
		FinishChunk(info.chunkId)
	}
}

// consumePiece receives pieces of chunk from stream and send them to IO goroutine
// and next stream(if needed).
func consumePiece(info *ChunkTransferInfo) error {
	pieceIndex := 0
	for {
		pieceOfChunk, err := info.stream.Recv()
		if err == io.EOF && pieceIndex == len(info.checkSums) {
			return nil
		} else if err == io.EOF {
			err = errors.New("stream is closed by sender before transferring is complete")
			Logger.Errorf("Fail to receive a piece from previous chunkserver or client, error detail: %s", err.Error())
			return err
		} else if err != nil {
			Logger.Errorf("Fail to receive a piece from previous chunkserver or client, error detail: %s", err.Error())
			return err
		} else if util.CRC32String(pieceOfChunk.Piece) != info.checkSums[pieceIndex] {
			err = fmt.Errorf("checksum is invalid")
			Logger.Errorf("Fail to receive a piece from previous chunkserver or client, error detail: %s", err.Error())
			return err
		}
		info.pieceChan <- pieceOfChunk
		if info.nextStream != nil {
			err := info.nextStream.Send(pieceOfChunk)
			if err != nil {
				Logger.Errorf("Fail to send a piece to next chunkserver, error detail: %s", err.Error())
				info.failAdds = append(info.failAdds, info.addresses[1:]...)
				info.nextStream = nil
			}
		}
		pieceIndex++
	}
}

// finishConsume closes pieceChan，waits for storeChunk function to finish and
// handles error.
func finishConsume(info *ChunkTransferInfo, wg *sync.WaitGroup, isSuccess bool) error {
	close(info.pieceChan)
	if info.nextStream != nil {
		previousReply, err := info.nextStream.CloseAndRecv()
		if !isSuccess {
			info.failAdds = append(info.failAdds, info.addresses...)
			return err
		}
		if err != nil {
			Logger.Errorf("Fail to close send stream, error detail: %s", err.Error())
			info.failAdds = append(info.failAdds, info.addresses[1:]...)
		} else {
			info.failAdds = append(info.failAdds, previousReply.FailAdds...)
		}
	}
	// Main thread will wait until goroutine success to store the block.
	wg.Wait()
	if len(info.errChan) != 0 {
		err := <-info.errChan
		Logger.Errorf("Fail to store a chunk, error detail: %s", err.Error())
		info.failAdds = append(info.failAdds, info.addresses[0])
		return err
	}
	Logger.Infof("Success to store a chunk, id: %s", info.chunkId)
	return nil
}

// setNextStream uses a ChunkTransferInfo to build a stream.
func setNextStream(info *ChunkTransferInfo) {
	var err error
	info.nextStream, err = getNextStream(info.chunkId, info.addresses[1:], info.chunkSize, info.checkSums)
	if err != nil {
		Logger.Errorf("Fail to get next stream, error detail: %s", err.Error())
		// It doesn't matter if we can't get the next stream, just handle current
		// chunkserver as the last one.
		info.failAdds = append(info.failAdds, info.addresses[1:]...)
		info.nextStream = nil
	}
}

// getNextStream builds stream to transfer this chunk to next chunkserver in the
// pipeline.
func getNextStream(chunkId string, addresses []string, chunkSize int, checkSums []string) (pb.PipLineService_TransferChunkClient, error) {
	nextAddress := addresses[0]
	Logger.Infof("Get next stream, chunk id: %s, next address: %s", chunkId, nextAddress)
	conn, _ := grpc.Dial(util.CombineString(nextAddress, common.AddressDelimiter, viper.GetString(common.ChunkPort)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	c := pb.NewPipLineServiceClient(conn)
	newCtx := context.Background()
	for _, address := range addresses {
		newCtx = metadata.AppendToOutgoingContext(newCtx, common.AddressString, address)
	}
	newCtx = metadata.AppendToOutgoingContext(newCtx, common.ChunkIdString, chunkId)
	newCtx = metadata.AppendToOutgoingContext(newCtx, common.ChunkSizeString, strconv.Itoa(chunkSize))
	for _, checkSum := range checkSums {
		newCtx = metadata.AppendToOutgoingContext(newCtx, common.CheckSumString, checkSum)
	}
	return c.TransferChunk(newCtx)
}

// storeChunk stores a chunk as a file named its id in this chunkserver. For I/O
// operation is very slow, this function will be run in a goroutine to not block
// the main thread transferring the chunk to another chunkserver.
func storeChunk(info *ChunkTransferInfo) {
	var err error
	checkSumString := strings.Join(info.checkSums, checksumDelimiter)
	defer func() {
		if err != nil {
			Logger.Errorf("Fail to store a chunk, chunkId = %s, error detail: %s", info.chunkId, err.Error())
			info.errChan <- err
		}
		close(info.errChan)
	}()
	chunkFile, err := os.OpenFile(util.CombineString(viper.GetString(common.ChunkStoragePath), info.chunkId, incompleteFileSuffix),
		os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	defer chunkFile.Close()
	err = chunkFile.Truncate(int64(info.chunkSize))
	if err != nil {
		return
	}
	chunkData, err := unix.Mmap(int(chunkFile.Fd()), 0, info.chunkSize, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return
	}
	// Goroutine will be blocked until main thread receive pieces of chunk and put them into pieceChan.
	index := 0
	for piece := range info.pieceChan {
		for _, b := range piece.Piece {
			chunkData[index] = b
			index++
		}
	}
	err = unix.Munmap(chunkData)
	// Store checksum of the chunk to disk.
	checkSumFile, err := os.OpenFile(util.CombineString(viper.GetString(common.ChecksumStoragePath), info.chunkId,
		checkSumFileSuffix, incompleteFileSuffix), os.O_RDWR|os.O_CREATE, 0644)
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
	// TODO 检查资源完整性
	// Wait to return until sendChunk2Client is finished or err occurs
	err := sendChunk2Client(stream, args.ChunkId)
	if err != nil {
		return err
	}
	return nil
}

// sendChunk2Client establishes a pipeline and sends a Chunk to next chunkserver in the
// pipeline. It is used to implement Chunk transmission between chunkservers.
func sendChunk2Client(stream pb.SetupStream_SetupStream2DataNodeServer, chunkId string) error {
	DNInfo.IncIOLoad()
	defer DNInfo.DecIOLoad()
	checkSums, err := getChecksumFromFile(chunkId)
	Logger.Warnf("get checkSums: %v", checkSums)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(util.CombineString(viper.GetString(common.ChunkStoragePath), chunkId), os.O_RDWR, 0644)
	defer file.Close()
	if err != nil {
		return err
	}
	fInfo, err := file.Stat()
	if err != nil {
		return err
	}
	pieceNum := int(fInfo.Size() / common.MB)
	if fInfo.Size()%common.MB != 0 {
		pieceNum++
	}
	buffer, err := unix.Mmap(int(file.Fd()), 0, int(fInfo.Size()), unix.PROT_WRITE, unix.MAP_SHARED)
	defer unix.Munmap(buffer)
	if err != nil {
		return err
	}
	for i := 0; i < pieceNum-1; i++ {
		if util.CRC32String(buffer[i*common.MB:(i+1)*common.MB]) != checkSums[i] {
			Logger.Warnf("Checksum of chunk %s is not correct, piece index: %d, cuurent checksum is: %v, want: %v",
				chunkId, i, util.CRC32String(buffer[i*common.MB:(i+1)*common.MB]), checkSums[i])
			MarkInvalidChunk(chunkId)
			return errors.New("checksum is invalid")
		}
		err = stream.Send(&pb.Piece{
			Piece: buffer[i*common.MB : (i+1)*common.MB],
		})
		if err != nil {
			return err
		}
	}
	if util.CRC32String(buffer[(pieceNum-1)*common.MB:]) != checkSums[pieceNum-1] {
		Logger.Warnf("Checksum of chunk %s is not correct, piece index: %d, cuurent checksum is: %v, want: %v",
			chunkId, pieceNum-1, util.CRC32String(buffer[(pieceNum-1)*common.MB:]), checkSums[pieceNum-1])
		MarkInvalidChunk(chunkId)
		return errors.New("checksum is invalid")
	}
	err = stream.Send(&pb.Piece{
		Piece: buffer[(pieceNum-1)*common.MB:],
	})
	if err != nil {
		return err
	}
	return nil
}

// getChecksumFromFile reads checksum of a chunk from disk.
func getChecksumFromFile(chunkId string) ([]string, error) {
	checksums := make([]string, 0, common.ChunkMBNum)
	file, err := os.Open(util.CombineString(viper.GetString(common.ChecksumStoragePath), chunkId, checkSumFileSuffix))
	if err != nil {
		return nil, err
	}
	buffer := make([]byte, common.KB)
	n, err := file.Read(buffer)
	if err != nil {
		return nil, err
	}
	checksums = strings.Split(string(buffer[:n]), checksumDelimiter)
	return checksums, nil
}

// getLocalChunksId walks through the chunk directory and get all chunks names.
func getLocalChunksId() []string {
	updateChunksLock.Lock()
	defer updateChunksLock.Unlock()
	var chunksId []string
	filepath.Walk(viper.GetString(common.ChunkStoragePath), func(path string, info fs.FileInfo, err error) error {
		if info != nil && !info.IsDir() && !strings.HasSuffix(info.Name(), incompleteFileSuffix) {
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

type BatchChunkTaskInfo struct {
	task          *BatchChunkTask
	wg            *sync.WaitGroup
	chunkTaskChan chan *ChunkTask
	resultChan    chan *util.ChunkTaskResult
}

// ConsumeBatchChunkTasks runs in a goroutine. It gets and consumes BatchChunkTask
// from BatchChunkTaskChan. In particular, it will handle all chunk tasks in incoming
// BatchChunkTask and merge the result of BatchChunkTask into existing results.
func ConsumeBatchChunkTasks() {
	for batchChunkTask := range DNInfo.BatchChunkTaskChan {
		info := startConsumeBatchTask(batchChunkTask)
		sendTasks2Consumer(info)
		info.wg.Wait()
		close(info.resultChan)
		newSuccessSendResult, newFailSendResult, removedChunkIds := handleConsumeResult(info.resultChan)
		Merge2SendResult(newFailSendResult, newSuccessSendResult)
		BatchRemoveChunkById(removedChunkIds)
	}
}

// startConsumeBatchTask converts a BatchChunkTaskInfo to a BatchChunkTaskInfo
// and starts several goroutines to consume the BatchChunkTaskInfo.
func startConsumeBatchTask(batchChunkTask *BatchChunkTask) *BatchChunkTaskInfo {
	var wg sync.WaitGroup
	info := &BatchChunkTaskInfo{
		task:          batchChunkTask,
		wg:            &wg,
		chunkTaskChan: make(chan *ChunkTask),
		resultChan:    make(chan *util.ChunkTaskResult, len(batchChunkTask.Infos)),
	}
	taskNum := len(info.task.Infos)
	goroutineNum := maxGoroutineNum
	if taskNum < maxGoroutineNum {
		goroutineNum = taskNum
	}
	for i := 0; i < goroutineNum; i++ {
		info.wg.Add(1)
		go func() {
			defer info.wg.Done()
			consumeSingleTask(info.chunkTaskChan, info.resultChan)
		}()
	}
	return info
}

// sendTasks2Consumer sends all ChunkTask of a BatchChunkTaskInfo to consumer
// through a channel.
func sendTasks2Consumer(info *BatchChunkTaskInfo) {
	for pc, dnIds := range info.task.Infos {
		chunkId := pc.chunkId
		dataNodeIds := dnIds
		adds := make([]string, 0, len(dataNodeIds))
		for i := 0; i < len(dataNodeIds); i++ {
			adds = append(adds, info.task.Adds[dataNodeIds[i]])
		}
		chunkTask, _ := getSingleChunkTask(chunkId, dataNodeIds, adds, pc.sendType)
		info.chunkTaskChan <- chunkTask
	}
}

// getSingleChunkTask gets a ChunkTask from a BatchChunkTaskInfo.
func getSingleChunkTask(chunkId string, dataNodeIds []string, adds []string, sendType int) (*ChunkTask, error) {
	var stream pb.PipLineService_TransferChunkClient
	stat, err := os.Stat(util.CombineString(viper.GetString(common.ChunkStoragePath), chunkId))
	// Let consumeSingleTask handle this error.
	if err != nil {
		Logger.Errorf("Chunk not exist, error detail: %s", err.Error())
		return nil, err
	}
	checkSums, err := getChecksumFromFile(chunkId)
	if err != nil {
		Logger.Errorf("Fail to get checksum of the chunk, error detail: %s", err.Error())
		return nil, err
	}
	stream, err = getNextStream(chunkId, adds, int(stat.Size()), checkSums)
	if err != nil {
		// Todo
		Logger.Errorf("Fail to get next stream, error detail: %s", err.Error())
		return nil, err
	}
	return &ChunkTask{
		stream:      stream,
		ChunkId:     chunkId,
		DataNodeIds: dataNodeIds,
		Adds:        adds,
		SendType:    sendType,
	}, nil
}

// handleConsumeResult converts the result of BatchChunkTask into a map of chunkId and
// a map of dataNodeId. It also returns the chunkIds that should be removed.
func handleConsumeResult(resultChan chan *util.ChunkTaskResult) (map[PendingChunk][]string, map[PendingChunk][]string,
	[]string) {
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
	return newSuccessSendResult, newFailSendResult, removedChunkIds
}

// ChunkTask is the task that will be sent to the target chunkservers.
type ChunkTask struct {
	stream      pb.PipLineService_TransferChunkClient
	ChunkId     string   `json:"chunk_id"`
	DataNodeIds []string `json:"data_node_ids"`
	Adds        []string `json:"adds"`
	SendType    int      `json:"send_type"`
}

// consumeSingleTask continuously gets ChunkTask from chunkTaskChan and consume it.
func consumeSingleTask(taskChan chan *ChunkTask, resultChan chan *util.ChunkTaskResult) {
	for task := range taskChan {
		currentTaskResult, _ := doConsumeSingleTask(task)
		resultChan <- currentTaskResult
	}
}

// doConsumeSingleTask consumes a ChunkTask and return the result.
func doConsumeSingleTask(task *ChunkTask) (*util.ChunkTaskResult, error) {
	currentTaskResult := &util.ChunkTaskResult{
		ChunkId:          task.ChunkId,
		FailDataNodes:    task.DataNodeIds,
		SuccessDataNodes: task.DataNodeIds[0:0],
		SendType:         task.SendType,
	}
	DNInfo.IncIOLoad()
	defer DNInfo.DecIOLoad()
	if task.stream == nil {
		return currentTaskResult, errors.New("stream is nil")
	}
	err := sendChunk2Cs(task.ChunkId, task.stream)
	if err != nil {
		Logger.Errorf("Fail to send chunk, error detail: %s", err.Error())
		_, _ = task.stream.CloseAndRecv()
		return currentTaskResult, err
	}
	reply, err := task.stream.CloseAndRecv()
	if err != nil {
		Logger.Errorf("Fail to close stream, error detail: %s", err.Error())
		return currentTaskResult, err
	}
	currentTaskResult = util.ConvReply2SingleResult(reply, task.DataNodeIds, task.Adds, task.SendType)
	return currentTaskResult, nil
}

// sendChunk2Cs sends a chunk to all target chunkservers through a pipeline.
func sendChunk2Cs(chunkId string, stream pb.PipLineService_TransferChunkClient) error {
	var err error
	checkSums, err := getChecksumFromFile(chunkId)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(util.CombineString(viper.GetString(common.ChunkStoragePath), chunkId), os.O_RDWR, 0644)
	defer file.Close()
	if err != nil {
		return err
	}
	fInfo, err := file.Stat()
	if err != nil {
		return err
	}
	pieceNum := int(fInfo.Size() / common.MB)
	if fInfo.Size()%common.MB != 0 {
		pieceNum++
	}
	buffer, err := unix.Mmap(int(file.Fd()), 0, int(fInfo.Size()), unix.PROT_WRITE, unix.MAP_SHARED)
	defer unix.Munmap(buffer)
	if err != nil {
		return err
	}
	for i := 0; i < pieceNum-1; i++ {
		if util.CRC32String(buffer[i*common.MB:(i+1)*common.MB]) != checkSums[i] {
			Logger.Warnf("Checksum of chunk %s is not correct, piece index: %d, cuurent checksum is: %v, want: %v",
				chunkId, pieceNum-1, util.CRC32String(buffer[(pieceNum-1)*common.MB:]), checkSums[pieceNum-1])
			MarkInvalidChunk(chunkId)
			return errors.New("checksum is invalid")
		}
		err = stream.Send(&pb.PieceOfChunk{
			Piece: buffer[i*common.MB : (i+1)*common.MB],
		})
		if err != nil {
			return err
		}
	}
	if util.CRC32String(buffer[(pieceNum-1)*common.MB:]) != checkSums[pieceNum-1] {
		Logger.Warnf("Checksum of chunk %s is not correct, piece index: %d, cuurent checksum is: %v, want: %v",
			chunkId, pieceNum-1, util.CRC32String(buffer[(pieceNum-1)*common.MB:]), checkSums[pieceNum-1])
		MarkInvalidChunk(chunkId)
		return errors.New("checksum is invalid")
	}
	err = stream.Send(&pb.PieceOfChunk{
		Piece: buffer[(pieceNum-1)*common.MB:],
	})
	if err != nil {
		return err
	}
	return nil
}
