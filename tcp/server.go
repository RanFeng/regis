package tcp

import (
	"code/regis/base"
	"code/regis/conf"
	"code/regis/database"
	"code/regis/ds"
	log "code/regis/lib"
	"code/regis/lib/utils"
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
	role   RoleType // RoleMaster -> master, RoleSlave -> slave
	replid string
	slave  *ds.Dict     // 用于存储slave的client, ip:port -> *RegisClient
	master *RegisClient // 用于存储master的client

	masterReplOffset int64 // 当前节点作为master时，发出的offset
	slaveReplOffset  int64 // 当前节点作为slave时，与master的同步offset

	replPingSlavePeriod int // 给slave发心跳包的周期

	ringbuffer io.ReadWriteSeeker
}

type RegisServer struct {
	replica
	lock      sync.Mutex          // 对list操作的锁
	semp      *semaphore.Weighted // 用于控制最大客户端连接数量
	list      *ds.LinkedList      // 存储已连接的connection
	closeChan chan int64          // 用于监听conn的断连，close时就将client从list中删除se可连接的信号量+1

	// db 是服务端的主数据库
	db base.DB

	workChan chan *base.Command // 用于给主协程输送命令的

	// 保存所有频道的订阅关系 channel -> list(base.Conn)
	pubsubDict    *ds.Dict
	pubsubPattern *ds.LinkedList

	address    string
	maxClients int64
}

func (s *RegisServer) GetReplid() string {
	return s.replid
}

func (s *RegisServer) SetReplid(id string) {
	s.replid = id
}

func (s *RegisServer) GetAddr() string {
	return s.address
}
func (s *RegisServer) GetDB() base.DB {
	return s.db
}
func (s *RegisServer) FlushDB() {
	s.SetDB(database.NewMultiDB())
}
func (s *RegisServer) SetDB(db base.DB) {
	s.db = db
}
func (s *RegisServer) GetWorkChan() <-chan *base.Command {
	return s.workChan
}
func (s *RegisServer) SetWorkChan(ch chan *base.Command) {
	s.workChan = ch
}

func (s *RegisServer) GetPubSub() *ds.Dict {
	return s.pubsubDict
}
func (s *RegisServer) GetPPubSub() *ds.LinkedList {
	return s.pubsubPattern
}
func (s *RegisServer) GetRole() RoleType {
	return s.role
}

func (s *RegisServer) GetInfo() string {
	serverInfo := `# RegisServer
role:%v
connected_slaves:%v
redis_version:6.2.5
redis_mode:standalone
run_id:%v
tcp_port:%v
`
	port := strings.Split(s.address, ":")[1]
	return fmt.Sprintf(serverInfo, s.role, s.slave.Len(), s.replid, port)
}

func InitServer(prop *conf.RegisConf) *RegisServer {
	server := &RegisServer{}
	server.replid = utils.GetRandomHexChars(40)
	server.address = fmt.Sprintf("%s:%d", prop.Bind, prop.Port)
	server.maxClients = prop.MaxClients

	server.list = &ds.LinkedList{}
	server.db = database.NewMultiDB()

	server.semp = semaphore.NewWeighted(prop.MaxClients)
	server.workChan = make(chan *base.Command)
	server.closeChan = make(chan int64)

	server.pubsubDict = ds.NewDict(128)
	server.pubsubPattern = ds.NewLinkedList()

	server.slave = ds.NewDict(8)

	server.replPingSlavePeriod = 10

	//server.ringbuffer = io.ReadWriteSeeker()

	go server.closeClient()
	return server
}

func (s *RegisServer) closeClient() {
	for {
		id := <-s.closeChan
		log.Debug("server close conn %v", id)
		c := s.list.RemoveFirst(func(conn interface{}) bool {
			//sid := utils.GetConnFd(conn.(*RegisConn).conn)
			sid := conn.(*RegisConn).id
			return sid == id
		})
		_ = c.(*RegisConn).conn.Close()
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
