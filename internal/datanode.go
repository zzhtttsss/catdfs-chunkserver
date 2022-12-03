package internal

import (
	"github.com/spf13/viper"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"math"
	"os"
	"path/filepath"
	"sync"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
)

var DNInfo *DataNodeInfo

type DataNodeInfo struct {
	Id string
	// Conn is RPC connection with the leader of the master cluster.
	Conn *grpc.ClientConn
	// ioLoad represent IO load of a chunkserver.
	ioLoad atomic.Int64
	// BatchChunkTaskChan is used to cache all incoming BatchChunkTask.
	BatchChunkTaskChan chan *BatchChunkTask
	//futureChunkNum means the number of pending chunks in expansion operation
	futureChunkNum int
	// IsReady represents this datanode is ready to serve with master.
	IsReady bool
}

var (
	// failSendResult contains all fail results of BatchChunkTask that have been completed
	// but not notified to the master.
	// key: Chunk' id and sendType; value: slice of DataNode' id
	failSendResult = make(map[PendingChunk][]string)
	// successSendResult contains all success results of BatchChunkTask that have been
	// completed but not notified to the master.
	// key: Chunk' id and sendType; value: slice of DataNode' id
	successSendResult = make(map[PendingChunk][]string)
	updateMapLock     = &sync.RWMutex{}
)

type PendingChunk struct {
	chunkId  string
	sendType int
}

// BatchChunkTask represent all Chunk sending tasks given by the master in one heartbeat.
type BatchChunkTask struct {
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

func (d *DataNodeInfo) Add2chan(chunks *BatchChunkTask) {
	d.BatchChunkTaskChan <- chunks
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

type DiskStatus struct {
	All   int64
	Used  int64
	Free  int64
	Usage int
}

// GetDiskStatus gets disk status of path/disk
func GetDiskStatus(path string) DiskStatus {
	var disk DiskStatus
	//fs := syscall.Statfs_t{}
	//err := syscall.Statfs(path, &fs)
	//if err != nil {
	//	return disk
	//}
	//disk.All = int64(fs.Blocks * uint64(fs.Bsize))
	//disk.Free = int64(fs.Bfree * uint64(fs.Bsize))
	//disk.Used = disk.All - disk.Free
	//disk.Usage = int(math.Ceil(float64(disk.Used) / float64(disk.All) * 100))
	disk.Used = getDirSize(path)
	disk.All = int64(viper.GetInt(common.ChunkCapacity))
	disk.Usage = int(math.Ceil(float64(disk.Used) / float64(disk.All) * 100))
	return disk
}

// getDirSize get the size of a directory.
func getDirSize(path string) int64 {
	var size int64
	_ = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size
}
