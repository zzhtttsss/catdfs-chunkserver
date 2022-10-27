package internal

import (
	"encoding/json"
	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"sync"
	"tinydfs-base/protocol/pb"
)

var DNInfo *DataNodeInfo

type DataNodeInfo struct {
	Id string
	// 与NN的rpc连接
	Conn *grpc.ClientConn

	ioLoad atomic.Int64

	pendingChunkChan chan *PendingChunks
}

var failSendResult = make(map[string][]string)

var successSendResult = make(map[string][]string)

var updateMapLock = &sync.RWMutex{}

type PendingChunks struct {
	Infos map[string][]string `json:"infos"`
	Adds  map[string]string   `json:"adds"`
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

func (d *DataNodeInfo) Add2chan(chunks *PendingChunks) {
	d.pendingChunkChan <- chunks
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
	bytes, err := json.Marshal(failSendResult)
	if err != nil {
		logrus.Errorf("Fail to marshal failSendResult, error detail: %s", err.Error())
	}
	logrus.Infof("failSendResult detail: %s", string(bytes))
	bytes, err = json.Marshal(successSendResult)
	if err != nil {
		logrus.Errorf("Fail to marshal successSendResult, error detail: %s", err.Error())
	}
	logrus.Infof("successSendResult detail: %s", string(bytes))
	failSendResult = make(map[string][]string)
	successSendResult = make(map[string][]string)
	return failChunkInfos, successChunkInfos
}
