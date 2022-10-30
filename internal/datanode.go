package internal

import (
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"sync"
	"tinydfs-base/protocol/pb"
)

var DNInfo *DataNodeInfo

type DataNodeInfo struct {
	Id string
	// Conn is RPC connection with the leader of the master cluster.
	Conn *grpc.ClientConn
	// ioLoad represent IO load of a chunkserver.
	ioLoad atomic.Int64
	// taskChan is used to cache all incoming SendingTask.
	taskChan chan *SendingTask
	//pendingChunkNum means the number of pending chunks in expansion operation
	pendingChunkNum int
	// IsReady represents this datanode is ready to serve with master.
	IsReady bool
}

var (
	// failSendResult contains all fail results of SendingTask that have been completed
	// but not notified to the master.
	// key: Chunk' id; value: slice of DataNode' id
	failSendResult = make(map[string][]string)
	// successSendResult contains all success results of SendingTask that have been
	// completed but not notified to the master.
	// key: Chunk' id; value: slice of DataNode' id
	successSendResult = make(map[string][]string)
	updateMapLock     = &sync.RWMutex{}
)

type PendingChunk struct {
	chunkId  string
	sendType int
}

// SendingTask represent all Chunk sending jobs given by the master in one heartbeat.
type SendingTask struct {
	// Infos key: Chunk' id; value: slice of DataNode' id
	Infos map[PendingChunk][]string `json:"infos"`
	// Adds key: DataNode' id; value: DataNode' address
	Adds map[string]string `json:"adds"`
}

func (d *DataNodeInfo) IncIOLoad() {
	d.ioLoad.Inc()
}

func (d *DataNodeInfo) DecIOLoad() {
	d.ioLoad.Dec()
}

func (d *DataNodeInfo) GetIOLoad() int64 {
	return d.ioLoad.Load()
}

func (d *DataNodeInfo) Add2chan(chunks *SendingTask) {
	d.taskChan <- chunks
}

func Merge2SendResult(newFailSendResult map[string][]string, newSuccessSendResult map[string][]string) {
	updateMapLock.Lock()
	defer updateMapLock.Unlock()
	for chunkId, dnIds := range newFailSendResult {
		if dataNodeIds, ok := failSendResult[chunkId]; ok {
			dataNodeIds = append(dataNodeIds, dnIds...)
		} else {
			failSendResult[chunkId] = dnIds
		}
	}

	for chunkId, dnIds := range newSuccessSendResult {
		if dataNodeIds, ok := successSendResult[chunkId]; ok {
			dataNodeIds = append(dataNodeIds, dnIds...)
		} else {
			successSendResult[chunkId] = dnIds
		}
	}
}

// HandleSendResult converts all results in failSendResult and successSendResult
// to protocol type and clear failSendResult and successSendResult.
func HandleSendResult() ([]*pb.ChunkInfo, []*pb.ChunkInfo) {
	updateMapLock.Lock()
	defer updateMapLock.Unlock()
	failChunkInfos := make([]*pb.ChunkInfo, 0)
	successChunkInfos := make([]*pb.ChunkInfo, 0)
	for chunkId, dataNodeIds := range failSendResult {
		for _, id := range dataNodeIds {
			failChunkInfos = append(failChunkInfos, &pb.ChunkInfo{
				ChunkId:    chunkId,
				DataNodeId: id,
			})
		}
	}
	for chunkId, dataNodeIds := range successSendResult {
		for _, id := range dataNodeIds {
			successChunkInfos = append(successChunkInfos, &pb.ChunkInfo{
				ChunkId:    chunkId,
				DataNodeId: id,
			})
		}
	}
	failSendResult = make(map[string][]string)
	successSendResult = make(map[string][]string)
	return failChunkInfos, successChunkInfos
}
