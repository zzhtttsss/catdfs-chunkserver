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
	updateChunksLock.RLock()
	defer updateChunksLock.RUnlock()
	chunksMap[chunkId].IsComplete = isSuccess
}

func GetChunk(id string) *Chunk {
	updateChunksLock.RLock()
	defer func() {
		updateChunksLock.RUnlock()
	}()
	return chunksMap[id]
}

func MonitorChunks() {
	for {

		time.Sleep(time.Duration(viper.GetInt(common.ChunkCheckTime)) * time.Second)
	}
}
