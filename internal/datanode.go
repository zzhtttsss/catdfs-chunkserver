package internal

import (
	"google.golang.org/grpc"
)

var DNInfo *DataNodeInfo

type DataNodeInfo struct {
	Id   string           // DataNode标识符
	Conn *grpc.ClientConn // 与NN的rpc连接
}
