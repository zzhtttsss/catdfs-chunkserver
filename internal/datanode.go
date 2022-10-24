package internal

import (
	"go.uber.org/atomic"
	"google.golang.org/grpc"
)

var DNInfo *DataNodeInfo

type DataNodeInfo struct {
	Id string
	// 与NN的rpc连接
	Conn *grpc.ClientConn

	IOLoad atomic.Int64
}

func IncIOLoad() {
	DNInfo.IOLoad.Inc()
}

func DecIOLoad() {
	DNInfo.IOLoad.Dec()
}

func GetIOLoad() int64 {
	return DNInfo.IOLoad.Load()
}
