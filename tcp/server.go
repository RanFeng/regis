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

type replica struct {
	replid string
	slave  *ds.Dict // 用于存储slave的client, ip:port -> *Client
	master *Client  // 用于存储master的client

	masterReplOffset int64 // 当前节点作为master时，发出的offset
	slaveReplOffset  int64 // 当前节点作为slave时，与master的同步offset

	replPingSlavePeriod int // 给slave发心跳包的周期

	ringbuffer io.ReadWriteSeeker
}

type Server struct {
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

func (s *Server) GetReplid() string {
	return s.replid
}

func (s *Server) SetReplid(id string) {
	s.replid = id
}

func (s *Server) GetAddr() string {
	return s.address
}
func (s *Server) GetDB() base.DB {
	return s.db
}
func (s *Server) FlushDB() {
	s.SetDB(database.NewMultiDB())
}
func (s *Server) SetDB(db base.DB) {
	s.db = db
}
func (s *Server) GetWorkChan() <-chan *base.Command {
	return s.workChan
}
func (s *Server) SetWorkChan(ch chan *base.Command) {
	s.workChan = ch
}

func (s *Server) GetPubSub() *ds.Dict {
	return s.pubsubDict
}
func (s *Server) GetPPubSub() *ds.LinkedList {
	return s.pubsubPattern
}

func (s *Server) GetInfo() string {
	serverInfo := `# Server
redis_version:6.2.5
redis_mode:standalone
run_id:%v
tcp_port:%v
`
	port := strings.Split(s.address, ":")[1]
	return fmt.Sprintf(serverInfo, s.replid, port)
}

//func (s *Server)loadRDB() {
//	s.FlushDB()
//	query := file.LoadRDB(conf.Conf.RDBName)
//	for i := range query {
//		client.Send(redis.CmdReply(query[i]))
//		_, _ = client.RecvAll()
//	}
//	//client.Close()
//}

func InitServer(prop *conf.RegisConf) *Server {
	server := &Server{}
	server.replid = utils.GetRandomHexChars(40)
	server.address = fmt.Sprintf("%s:%d", prop.Bind, prop.Port)
	server.maxClients = prop.MaxClients

	server.list = &ds.LinkedList{}

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

func (s *Server) closeClient() {
	for {
		id := <-s.closeChan
		log.Debug("server close conn %v", id)
		c := s.list.RemoveFirst(func(conn interface{}) bool {
			//sid := utils.GetConnFd(conn.(*Connection).conn)
			sid := conn.(*Connection).id
			return sid == id
		})
		_ = c.(*Connection).conn.Close()
		s.semp.Release(1)
	}
}

func (s *Server) addClient(ctx context.Context, conn net.Conn) {
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

func ListenAndServer(server *Server) error {
	address := fmt.Sprintf("%s:%d", conf.Conf.Bind, conf.Conf.Port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	ctx := context.Background()
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		log.Info("accept ok %v", conn.RemoteAddr())
		server.addClient(ctx, conn)
	}
}
