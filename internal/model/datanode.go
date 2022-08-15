package model

import (
	"context"
	"github.com/spf13/viper"
	"log"
	"net"
	"sync"
	"tinydfs-chunkserver/config"
	"tinydfs-chunkserver/internal/service"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type DataNode struct {
	Id   uint32           // DataNode标识符
	Conn *grpc.ClientConn // 与NN的rpc连接
	Addr string           // 与NN连接后，分配的地址
	// 文件复制需要
	IsMain  bool                 // 是否是主DN
	TCPAddr string               // 作为主DN，监听的TCP地址。当IsMain为真，该地址有效。计算方式为Addr+1024
	TCPCon  map[string]*net.Conn // 存储非主DN与主DN的连接
	mu      *sync.Mutex
}

func MakeDataNode() *DataNode {
	conn, id, addr := DNRegister()
	return &DataNode{
		Id:     id,
		Conn:   conn,
		Addr:   addr,
		TCPCon: map[string]*net.Conn{},
		mu:     &sync.Mutex{},
	}
}

//向NameNode注册DataNode，取得ID
func DNRegister() (*grpc.ClientConn, uint32, string) {
	config.Config()
	addr := viper.GetString("master.addr") + viper.GetString("master.port")
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	c := service.NewRegisterServiceClient(conn)
	ctx := context.Background()
	res, err := c.Register(ctx, &service.DNRegisterArgs{})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Register Success,get ID: ", res.Id)
	return conn, res.Id, res.Addr
}

//重连：NameNode挂了，重连并重新注册；DataNode或NameNode网络波动，不需要重新注册，重连并继续发送心跳即可
func (dn *DataNode) reconnect() {
	addr := viper.GetString("master.addr") + viper.GetString("master.port")
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	dn.Conn.Close()
	dn.Conn = conn
}
