package tcp

import (
	"code/regis/base"
	"code/regis/conf"
	"code/regis/database"
	"code/regis/ds"
	"code/regis/file"
	log "code/regis/lib"
	"code/regis/lib/utils"
	"code/regis/redis"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"golang.org/x/sync/semaphore"
)

var (
	ctx    = context.Background()
	Server *RegisServer
	Client *RegisClient
)

type RoleType int

func (r RoleType) String() string {
	switch r {
	case RoleMaster:
		return "master"
	case RoleSlave:
		return "slave"
	}
	return "unknown"
}

const (
	RoleMaster RoleType = iota
	RoleSlave
)

type replica struct {
	Role   RoleType // RoleMaster -> master, RoleSlave -> slave
	Replid string
	Slave  *ds.Dict     // 用于存储slave的client, ip:port -> *RegisClient
	Master *RegisClient // 用于存储master的client

	// 当前节点作为master时，往缓冲区存进去的字节数
	// 如果不是主从结构，写命令来时不会增加
	// 如果是主从结构，且自己是master，将用户命令写入缓冲区并进行计数
	//	如果一个slave来要求全量同步，发出 fullsync replid masterReplOffset，并发出rdb文件
	//	表示："我发给你的rdb文件同步进度是 masterReplOffset"
	//  如果自己是slave，在改变 slaveReplOffset 的时候，也要同时改变 masterReplOffset
	//  因为谁也不知道会不会在后面，有新的slave认领该服务器为master
	// TODO 如果是部分同步呢
	MasterReplOffset int64

	// 当前节点作为slave时，与master的同步offset
	// 收到 fullsync ID offset 之后，会收到一个rdb文件，将rdb load到db中之后
	// 令 slaveReplOffset = offset
	// 表示 "我收到的rdb文件的同步进度是 slaveReplOffset"
	SlaveReplOffset int64

	ReplPingSlavePeriod int // 给slave发心跳包的周期

	Ringbuffer io.ReadWriteSeeker
}

type RegisServer struct {
	replica
	lock      sync.Mutex          // 对list操作的锁
	semp      *semaphore.Weighted // 用于控制最大客户端连接数量
	list      *ds.LinkedList      // 存储已连接的connection
	closeChan chan int64          // 用于监听conn的断连，close时就将client从list中删除se可连接的信号量+1

	// DB 是服务端的主数据库
	DB base.DB

	workChan chan *Command // 用于给主协程输送命令的

	// 保存所有频道的订阅关系 channel -> list(base.Connection)
	pubsubDict    *ds.Dict
	pubsubPattern *ds.LinkedList

	address    string
	maxClients int64
}

func (s *RegisServer) GetAddr() string {
	return s.address
}

func (s *RegisServer) FlushDB() {
	s.DB = database.NewMultiDB()
}

func (s *RegisServer) LoadRDB(fn string) {
	s.FlushDB()
	query := file.LoadRDB(fn)
	for i := range query {
		Client.Send(redis.CmdReply(query[i]...))
	}
}

func (s *RegisServer) GetWorkChan() <-chan *Command {
	return s.workChan
}
func (s *RegisServer) SetWorkChan(ch chan *Command) {
	s.workChan = ch
}

func (s *RegisServer) GetPubSub() *ds.Dict {
	return s.pubsubDict
}
func (s *RegisServer) GetPPubSub() *ds.LinkedList {
	return s.pubsubPattern
}

func (s *RegisServer) GetInfo() string {
	serverInfo := `# RegisServer
role:%v
connected_slaves:%v
master_repl_offset:%v
slave_repl_offset:%v
run_id:%v
tcp_port:%v
`
	port := strings.Split(s.address, ":")[1]
	return fmt.Sprintf(serverInfo, s.Replid, s.Slave.Len(), s.MasterReplOffset, s.SlaveReplOffset, s.Replid, port)
}

func InitServer(prop *conf.RegisConf) *RegisServer {
	server := &RegisServer{}
	server.Replid = utils.GetRandomHexChars(40)
	server.address = fmt.Sprintf("%s:%d", prop.Bind, prop.Port)
	server.maxClients = prop.MaxClients

	server.list = &ds.LinkedList{}
	server.DB = database.NewMultiDB()

	server.semp = semaphore.NewWeighted(prop.MaxClients)
	server.workChan = make(chan *Command)
	server.closeChan = make(chan int64)

	server.pubsubDict = ds.NewDict(128)
	server.pubsubPattern = ds.NewLinkedList()

	server.Slave = ds.NewDict(8)

	server.ReplPingSlavePeriod = 10

	//server.ringbuffer = io.ReadWriteSeeker()

	go server.closeClient()
	return server
}

func (s *RegisServer) closeClient() {
	for {
		id := <-s.closeChan
		log.Debug("server close Conn %v", id)
		c := s.list.RemoveFirst(func(conn interface{}) bool {
			//sid := utils.GetConnFd(Conn.(*RegisConn).Conn)
			sid := conn.(*RegisConn).ID
			return sid == id
		})
		_ = c.(*RegisConn).Conn.Close()
		s.semp.Release(1)
	}
}

func (s *RegisServer) addClient(ctx context.Context, conn net.Conn) {
	//log.Debug("wait semp now is %v", s.list.Len())
	err := s.semp.Acquire(ctx, 1)
	if err != nil {
		log.Error("semp acquire fail %v", err)
	}
	//log.Debug("get semp now is %v", s.list.Len())
	c := initConnection(conn, s)
	s.lock.Lock()
	defer s.lock.Unlock()
	s.list.PushTail(c)
	//log.Debug("get client now is %v", s.list.Len())
}

func ListenAndServer(server *RegisServer) error {
	address := fmt.Sprintf("%s:%d", conf.Conf.Bind, conf.Conf.Port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	Client = MustNewClient(Server.GetAddr(), Server)
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		log.Info("accept ok %v", conn.RemoteAddr())
		server.addClient(ctx, conn)
	}
}
