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
	//futureChunkNum means the number of pending chunks in expansion operation
	futureChunkNum int
	// IsReady represents this datanode is ready to serve with master.
	IsReady bool
}

var (
	// failSendResult contains all fail results of SendingTask that have been completed
	// but not notified to the master.
	// key: Chunk' id and sendType; value: slice of DataNode' id
	failSendResult = make(map[PendingChunk][]string)
	// successSendResult contains all success results of SendingTask that have been
	// completed but not notified to the master.
	// key: Chunk' id and sendType; value: slice of DataNode' id
	successSendResult = make(map[PendingChunk][]string)
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

func Merge2SendResult(newFailSendResult map[PendingChunk][]string, newSuccessSendResult map[PendingChunk][]string) {
	updateMapLock.Lock()
	defer updateMapLock.Unlock()
	for pc, dnIds := range newFailSendResult {
		if dataNodeIds, ok := failSendResult[pc]; ok {
			dataNodeIds = append(dataNodeIds, dnIds...)
		} else {
			failSendResult[pc] = dnIds
		}
	}

	for pc, dnIds := range newSuccessSendResult {
		if dataNodeIds, ok := successSendResult[pc]; ok {
			dataNodeIds = append(dataNodeIds, dnIds...)
		} else {
			successSendResult[pc] = dnIds
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
	for pc, dataNodeIds := range failSendResult {
		if dataNodeIds == nil || len(dataNodeIds) == 0 {
			successChunkInfos = append(successChunkInfos, &pb.ChunkInfo{
				ChunkId:    pc.chunkId,
				DataNodeId: "",
				SendType:   int32(pc.sendType),
			})
		} else {
			for _, id := range dataNodeIds {
				failChunkInfos = append(failChunkInfos, &pb.ChunkInfo{
					ChunkId:    pc.chunkId,
					DataNodeId: id,
					SendType:   int32(pc.sendType),
				})
			}
		}

	}
	for pc, dataNodeIds := range successSendResult {
		if dataNodeIds == nil || len(dataNodeIds) == 0 {
			successChunkInfos = append(successChunkInfos, &pb.ChunkInfo{
				ChunkId:    pc.chunkId,
				DataNodeId: "",
				SendType:   int32(pc.sendType),
			})
		} else {
			for _, id := range dataNodeIds {
				successChunkInfos = append(successChunkInfos, &pb.ChunkInfo{
					ChunkId:    pc.chunkId,
					DataNodeId: id,
					SendType:   int32(pc.sendType),
				})
			}
		}

	}
	failSendResult = make(map[PendingChunk][]string)
	successSendResult = make(map[PendingChunk][]string)
	return failChunkInfos, successChunkInfos
}
