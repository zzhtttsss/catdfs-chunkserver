package model

import (
	"context"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"net"
	"sync"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
)

type DataNode struct {
	Id   string           // DataNode标识符
	Conn *grpc.ClientConn // 与NN的rpc连接
	Addr string           // 与NN连接后，分配的地址
	// 文件复制需要
	IsMain  bool                 // 是否是主DN
	TCPAddr string               // 作为主DN，监听的TCP地址。当IsMain为真，该地址有效。计算方式为Addr+1024
	TCPCon  map[string]*net.Conn // 存储非主DN与主DN的连接
	mu      *sync.Mutex
}

func CreateDataNode() *DataNode {
	conn, id, addr := RegisterDataNode()
	return &DataNode{
		Id:     id,
		Conn:   conn,
		Addr:   addr,
		TCPCon: map[string]*net.Conn{},
		mu:     &sync.Mutex{},
	}
}

// RegisterDataNode 向NameNode注册DataNode，取得ID
func RegisterDataNode() (*grpc.ClientConn, string, string) {
	addr := viper.GetString(common.MasterAddr) + viper.GetString(common.MasterPort)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	c := pb.NewRegisterServiceClient(conn)
	ctx := context.Background()
	res, err := c.Register(ctx, &pb.DNRegisterArgs{})
	if err != nil {
		logrus.Panicf("Fail to register, error code: %v, error detail: %s,", common.ChunkServerRegisterFailed, err.Error())
		//Todo 根据错误类型进行重试（当前master的register不会报错，所以err直接panic并重启即可）
	}
	logrus.Infof("Register Success,get ID: %s", res.Id)
	return conn, res.Id, res.Addr
}

func (dn *DataNode) Heartbeat() {
	reconnectCount := 0
	for {
		c := pb.NewHeartbeatServiceClient(dn.Conn)
		_, err := c.Heartbeat(context.Background(), &pb.HeartbeatArgs{Id: dn.Id})

		if err != nil {
			logrus.Errorf("Fail to heartbeat, error code: %v, error detail: %s,", common.ChunkServerHeartbeatFailed, err.Error())
			rpcError, _ := status.FromError(err)

			if (rpcError.Details()[0]).(pb.RPCError).Code == common.MasterHeartbeatFailed {
				logrus.Errorf("[Id=%s] Heartbeat failed. Get ready to reconnect[Times=%d].\n", dn.Id, reconnectCount+1)
				dn.reconnect()
				reconnectCount++

				if reconnectCount == viper.GetInt(common.ChunkHeartbeatReconnectCount) {
					logrus.Fatalf("[Id=%s] Reconnect failed. Offline.\n", dn.Id)
					break
				}
				continue
			}
		}
		time.Sleep(time.Duration(viper.GetInt(common.ChunkHeartbeatSendTime)) * time.Second)
	}
}

//重连：NameNode挂了，重连并重新注册；DataNode或NameNode网络波动，不需要重新注册，重连并继续发送心跳即可
func (dn *DataNode) reconnect() {
	addr := viper.GetString(common.MasterAddr) + viper.GetString(common.MasterPort)
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	_ = dn.Conn.Close()
	dn.Conn = conn
}
