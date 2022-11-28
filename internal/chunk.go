package internal

import (
	set "github.com/deckarep/golang-set"
	"github.com/spf13/viper"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/util"
)

var (
	// Store all Chunk, using id as the key
	chunksMap        = make(map[string]*Chunk)
	invalidChunkSet  = set.NewSet()
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

func AddIncompleteChunk(chunkId string) {
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
	_ = makeFileComplete(util.CombineString(viper.GetString(common.ChunkStoragePath), chunkId))
	_ = makeFileComplete(util.CombineString(viper.GetString(common.ChecksumStoragePath), chunkId, checkSumFileSuffix))
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
		for id, chunk := range chunksMap {
			if !chunk.IsComplete && int(time.Now().Sub(chunk.AddTime).Seconds()) > viper.GetInt(common.ChunkDeadTime) {
				EraseChunk(id)
			}
		}
		time.Sleep(time.Duration(viper.GetInt(common.ChunkCheckTime)) * time.Second)
	}
}

func BatchRemoveChunkById(chunkIds []string) {
	updateChunksLock.Lock()
	defer updateChunksLock.Unlock()
	for _, chunkId := range chunkIds {
		if node, ok := chunksMap[chunkId]; ok {
			node.IsComplete = false
			_ = makeFileIncomplete(util.CombineString(viper.GetString(common.ChunkStoragePath), chunkId))
			_ = makeFileIncomplete(util.CombineString(viper.GetString(common.ChunkStoragePath), chunkId, checkSumFileSuffix))
		}
	}
}

func makeFileComplete(filename string) error {
	return os.Rename(util.CombineString(filename, incompleteFileSuffix), filename)
}

func makeFileIncomplete(filename string) error {
	return os.Rename(filename, util.CombineString(filename, incompleteFileSuffix))
}

// EraseChunk totally removes a Chunk from the disk.
func EraseChunk(chunkId string) {
	updateChunksLock.Lock()
	defer updateChunksLock.Unlock()
	delete(chunksMap, chunkId)
	_ = os.Remove(util.CombineString(viper.GetString(common.ChunkStoragePath), chunkId))
	_ = os.Remove(util.CombineString(viper.GetString(common.ChecksumStoragePath), chunkId, checkSumFileSuffix))
}

// MarkInvalidChunk marks a Chunk as invalid.
func MarkInvalidChunk(chunkId string) {
	EraseChunk(chunkId)
	invalidChunkSet.Add(chunkId)
}

// HandleInvalidChunks gets all invalid chunks which have been erased.
func HandleInvalidChunks() []string {
	num := invalidChunkSet.Cardinality()
	chunkIds := make([]string, num)
	for i := 0; i < num; i++ {
		chunkIds[i] = invalidChunkSet.Pop().(string)
	}
	return chunkIds
}
