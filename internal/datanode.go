package internal

import (
	"google.golang.org/grpc"
)

var DNInfo *DataNodeInfo

type DataNodeInfo struct {
	Id string
	// 与NN的rpc连接
	Conn *grpc.ClientConn
}
