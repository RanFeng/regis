package tcp

import (
	"code/regis/base"
	"code/regis/conf"
	"code/regis/ds"
	log "code/regis/lib"
	"context"
	"fmt"
	"net"
	"sync"

	"golang.org/x/sync/semaphore"
)

type Server struct {
	lock      sync.Mutex          // 对list操作的锁
	semp      *semaphore.Weighted // 用于控制最大客户端连接数量
	list      *ds.LinkedList      // 存储已连接的client
	closeChan chan int64          // 用于监听conn的断连，close时就将client从list中删除se可连接的信号量+1

	// freeze
	// 初始时， freeze = base.WorldNormal
	// 当调用BGSave时， freeze = base.WorldFrozen
	// BGSave结束时， freeze = base.WorldMoving
	// bgDB 里的数据完全转移到 db 中时，freeze = base.WorldNormal
	freeze base.WorldStatus
	// db 是服务端的主数据库
	db base.DB
	// freeze = base.WorldFrozen 时，读命令优先 bgDB，miss后再读 db，写命令直接写入 bgDB，
	// freeze == base.WorldMoving 时，如果 bgDB 里有值，就蚂蚁搬家式地写入 db 中，在这段期间，不允许 BGSave
	bgDB base.DB

	workChan chan *base.Command // 用于给主协程输送命令的

	// 保存所有频道的订阅关系 channel -> list(base.Conn)
	pubsubDict    *ds.Dict
	pubsubPattern *ds.LinkedList

	address    string
	maxClients int64
}

func (s *Server) GetAddr() string {
	return s.address
}
func (s *Server) GetDB() base.DB {
	return s.db
}
func (s *Server) SetDB(db base.DB) {
	s.db = db
}
func (s *Server) GetWorkChan() chan *base.Command {
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

func InitServer(prop *conf.RegisConf) *Server {
	server := &Server{}
	server.address = fmt.Sprintf("%s:%d", prop.Bind, prop.Port)
	server.maxClients = prop.MaxClients

	server.list = &ds.LinkedList{}
	server.semp = semaphore.NewWeighted(prop.MaxClients)
	server.workChan = make(chan *base.Command)
	server.closeChan = make(chan int64)

	server.pubsubDict = ds.NewDict(128)
	server.pubsubPattern = ds.NewLinkedList()
	//server.db = database.NewMultiDB()

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
	c := initClient(conn, s)
	s.lock.Lock()
	defer s.lock.Unlock()
	s.list.Append(c)
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
