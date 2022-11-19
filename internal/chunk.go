package internal

import (
	"github.com/spf13/viper"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"tinydfs-base/common"
)

var (
	// Store all Chunk, using id as the key
	chunksMap        = make(map[string]*Chunk)
	updateChunksLock = &sync.RWMutex{}
)

type Chunk struct {
	// Id is FileId + "_" + Index
	Id     string
	FileId string
	Index  int
	// IsComplete represents if the Chunk has been stored completely.
	IsComplete bool
	// AddTime is the time this Chunk add to chunksMap.
	AddTime time.Time
}

func AddPendingChunk(chunkId string) {
	chunkInfo := strings.Split(chunkId, common.ChunkIdDelimiter)
	index, _ := strconv.Atoi(chunkInfo[1])
	updateChunksLock.Lock()
	chunksMap[chunkId] = &Chunk{
		Id:         chunkId,
		FileId:     chunkInfo[0],
		Index:      index,
		IsComplete: false,
		AddTime:    time.Now(),
	}
	updateChunksLock.Unlock()
}

func FinishChunk(chunkId string) {
	_ = os.Rename(viper.GetString(common.ChunkStoragePath)+chunkId+inCompleteFileSuffix,
		viper.GetString(common.ChunkStoragePath)+chunkId)
	_ = os.Rename(viper.GetString(common.ChecksumStoragePath)+chunkId+checkSumFileSuffix+inCompleteFileSuffix,
		viper.GetString(common.ChecksumStoragePath)+chunkId+checkSumFileSuffix)
	updateChunksLock.Lock()
	defer updateChunksLock.Unlock()
	chunksMap[chunkId].IsComplete = true
}

func GetChunk(id string) *Chunk {
	updateChunksLock.RLock()
	defer func() {
		updateChunksLock.RUnlock()
	}()
	return chunksMap[id]
}

func GetAllChunkIds() []string {
	updateChunksLock.RLock()
	defer updateChunksLock.RUnlock()
	ids := make([]string, 0, len(chunksMap))
	for id, chunk := range chunksMap {
		if chunk.IsComplete {
			ids = append(ids, id)
		}
	}
	return ids
}

// MonitorChunks runs in a goroutine. It keeps looping to clear all incomplete
// and timed out Chunk.
func MonitorChunks() {
	for {
		updateChunksLock.Lock()
		for id, chunk := range chunksMap {
			if !chunk.IsComplete && int(time.Now().Sub(chunk.AddTime).Seconds()) > viper.GetInt(common.ChunkDeadTime) {
				err := os.Remove(common.ChunkStoragePath + chunk.Id + inCompleteFileSuffix)
				err = os.Remove(common.ChunkStoragePath + chunk.Id + checkSumFileSuffix + inCompleteFileSuffix)
				if err != nil {
					continue
				}
				delete(chunksMap, id)
			}
		}
		updateChunksLock.Unlock()
		time.Sleep(time.Duration(viper.GetInt(common.ChunkCheckTime)) * time.Second)
	}
}

func BatchRemoveChunkById(chunkIds []string) {
	updateChunksLock.Lock()
	defer updateChunksLock.Unlock()
	for _, chunkId := range chunkIds {
		if node, ok := chunksMap[chunkId]; ok {
			node.IsComplete = false
		}
	}
}
