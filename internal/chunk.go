package internal

import (
	"github.com/spf13/viper"
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
	Id         string
	FileId     string
	Index      int
	IsComplete bool
	AddTime    time.Time
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

func FinishChunk(chunkId string, isSuccess bool) {
	updateChunksLock.Lock()
	defer updateChunksLock.Unlock()
	chunksMap[chunkId].IsComplete = isSuccess
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
	defer func() {
		updateChunksLock.RUnlock()
	}()

	ids := make([]string, 0, len(chunksMap))
	for id := range chunksMap {
		ids = append(ids, id)
	}
	return ids
}

func MonitorChunks() {
	for {
		updateChunksLock.Lock()
		for id, chunk := range chunksMap {
			if !chunk.IsComplete && int(time.Now().Sub(chunk.AddTime).Seconds()) > viper.GetInt(common.ChunkDeadTime) {
				delete(chunksMap, id)
			}
		}
		updateChunksLock.Unlock()
		time.Sleep(time.Duration(viper.GetInt(common.ChunkCheckTime)) * time.Second)
	}
}
