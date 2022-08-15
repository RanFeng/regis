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
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	// 当自己又是slave又是master时，说明自己是个slave
	Role   RoleType // RoleMaster -> master, RoleSlave -> slave
	Replid string
	Slave  map[int64]*RegisConn // 是 RegisServer.Who 的子集用于存储slave的connection, RegisConn.ID -> *RegisConn

	// 用于存储master的client, 如果为nil，表示当前不是slave
	// 如果与master断开链接，并不能将 Master 置为 nil !!
	// 除非不想要主从同步了
	Master *RegisClient

	// 当前节点往缓冲区存进去的字节数
	// 如果不是主从结构，写命令来时不会增加
	// 如果是主从结构，且自己是master，将用户命令写入缓冲区并进行计数，
	// 如果是slave，且master传来了写命令，也要写入缓冲区中并进行计数
	//	如果一个slave来要求全量同步，发出 fullsync replid masterReplOffset，并发出rdb文件
	//	表示："我发给你的rdb文件同步进度是 masterReplOffset"
	MasterReplOffset int64

	// 当前节点作为slave时，与master的同步offset
	// 收到 fullsync ID offset 之后，会收到一个rdb文件，将rdb load到db中之后
	// 令 SlaveReplOffset = offset, MasterReplOffset = offset
	// 表示 "我收到的rdb文件的同步进度是 slaveReplOffset"
	SlaveReplOffset int64

	ReplPingSlavePeriod  int // 给slave发心跳包的周期
	ReplPingMasterPeriod int // 给master发心跳包的周期

	ReplBacklog *ds.RingBuffer
}

type safety struct {
	Lock       sync.Mutex // 对 RegisServer.Who, RegisServer.Slave 操作的锁
	Monopolist string
	semp       *semaphore.Weighted  // 用于控制最大客户端连接数量
	Who        map[int64]*RegisConn // 存储已连接的connection，RegisConn.ID -> *RegisConn
}

type RegisServer struct {
	Address    string
	maxClients int64

	replica
	safety

	// DB 是服务端的主数据库
	DB base.DB

	workChan chan *Command // 用于给主协程输送命令的

	// PubsubDict 保存所有频道的订阅关系 channel -> RegisConn.ID -> *RegisConn
	PubsubDict map[string]map[int64]*RegisConn
	//pubsubPattern *ds.LinkedList
}

func (s *RegisServer) PassExec(c *RegisConn) bool {
	if len(s.Monopolist) == 0 {
		return true
	}
	return c.RemoteAddr() == s.Monopolist
}

func (s *RegisServer) LoadRDB(fn string) {
	Client.Send(redis.CmdReply("lock"))
	query := file.LoadRDB(fn)
	for i := range query {
		Client.Send(redis.CmdReply(query[i]...))
		_ = Client.GetReply()
	}
	Client.Send(redis.CmdReply("unlock"))
}

func (s *RegisServer) SyncSlave(msg []byte) {
	atomic.AddInt64(&s.MasterReplOffset, s.ReplBacklog.Write(msg))
	log.Info("SyncSlave now %v", Server.MasterReplOffset)
	if len(s.Slave) == 0 {
		return
	}
	for k := range s.Slave {
		_ = s.Slave[k].Write(msg)
	}
}

// HeartBeatToSlave
// 当自己是master时，用这个来告知slave自己的存活状态。
// 当自己是slave，同时也是别的机器的master时，就不用告知了
//   上面的master会发出ping包，我转发那个ping包就行
func (s *RegisServer) HeartBeatToSlave() {
	for {
		if s.Role == RoleMaster {
			s.SyncSlave(redis.CmdReply("ping").Bytes())
		}
		time.Sleep(time.Duration(s.ReplPingSlavePeriod) * time.Second)
	}
}

// HeartBeatFromSlave
// 当自己是master时，用这个来监听slave是否存活，
//   如果超出时间，slave没有朝我发送心跳包时，就当它没了，要关闭。
// 当自己是slave，同时也是别的机器的master时，也要监听自己的slave的存活状态
func (s *RegisServer) HeartBeatFromSlave() {
	for {
		for key, cli := range s.Slave {
			if time.Since(cli.LastBeat) > time.Minute {
				log.Error("MASTER <-> REPLICA sync timeout")
				delete(s.Slave, key)
				cli.Close()
			}
		}
		time.Sleep(time.Duration(s.ReplPingSlavePeriod) * time.Second)
	}
}

func (s *RegisServer) ReconnectMaster() {
	cli := MustNewClient(s.Master.Addr, s)
	cli.PSync()
}

// HeartBeatToMaster
// 当自己是slave时，定时向master发出心跳包，证明自己存活。
// 当自己是slave和master时，也要向自己的master发心跳包证明自己存活
func (s *RegisServer) HeartBeatToMaster() {
	var ack base.Reply
	for {
		if s.Master != nil {
			//log.Info("ack to master %v %v", s.Replid, s.SlaveReplOffset)
			ack = redis.CmdReply("REPLCONF", "ACK", s.SlaveReplOffset)
			err := s.Master.Write(ack.Bytes())
			if err != nil { // 主从断开了
				s.ReconnectMaster()
			}
		}
		time.Sleep(time.Duration(s.ReplPingMasterPeriod) * time.Second)
	}
}

// HeartBeatFromMaster
// 当自己是slave时，定时向检查master的存活
// 当自己是slave和master时，也要定时向检查master的存活
func (s *RegisServer) HeartBeatFromMaster() {
	for {
		if s.Master != nil {
			if time.Since(s.Master.LastBeat) > time.Minute {
				log.Notice("master conn lost")
				s.ReconnectMaster()
			}
		}
		time.Sleep(time.Duration(s.ReplPingMasterPeriod) * time.Second)
	}
}

func (s *RegisServer) GetWorkChan() <-chan *Command {
	return s.workChan
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
	port := strings.Split(s.Address, ":")[1]
	return fmt.Sprintf(serverInfo, s.Role, len(s.Slave),
		atomic.LoadInt64(&s.MasterReplOffset), atomic.LoadInt64(&s.SlaveReplOffset), s.Replid, port)
}

func (s *RegisServer) CloseConn(ids ...int64) {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	for _, id := range ids {
		c, ok := s.Who[id]
		if !ok {
			continue
		}
		delete(s.Who, id)
		delete(s.Slave, id)
		_ = c.Conn.Close()
		s.semp.Release(1)
	}
}

func (s *RegisServer) addClient(ctx context.Context, conn net.Conn) {
	//log.Debug("wait semp now is %v", s.Who.Len())
	err := s.semp.Acquire(ctx, 1)
	if err != nil {
		log.Error("semp acquire fail %v", err)
	}
	//log.Debug("get semp now is %v", s.Who.Len())
	c := NewConnection(conn, s)
	s.Lock.Lock()
	defer s.Lock.Unlock()
	s.Who[c.ID] = c
	//log.Debug("get client now is %v", s.Who.Len())
}

func ListenAndServer(server *RegisServer) error {
	address := fmt.Sprintf("%s:%d", conf.Conf.Bind, conf.Conf.Port)
	log.Notice("listen in %v", address)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	Client = MustNewClient(Server.Address, Server)
	go Server.LoadRDB(conf.Conf.RDBName)
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		log.Info("accept ok %v", conn.RemoteAddr())
		server.addClient(ctx, conn)
	}
}

func InitServer(prop *conf.RegisConf) *RegisServer {
	server := &RegisServer{}
	server.Replid = utils.GetRandomHexChars(40)
	server.Address = fmt.Sprintf("%s:%d", prop.Bind, prop.Port)
	server.maxClients = prop.MaxClients

	server.Who = make(map[int64]*RegisConn, conf.Conf.MaxClients)
	server.DB = database.NewMultiDB()

	server.semp = semaphore.NewWeighted(prop.MaxClients)
	server.workChan = make(chan *Command)

	server.PubsubDict = make(map[string]map[int64]*RegisConn, 128)
	//server.pubsubPattern = ds.NewLinkedList()

	server.Slave = make(map[int64]*RegisConn, 8)

	server.ReplPingSlavePeriod = 10
	server.ReplPingMasterPeriod = 3

	server.ReplBacklog = ds.NewRingBuffer(conf.Conf.ReplBacklogSize)

	go server.HeartBeatToSlave()
	go server.HeartBeatFromSlave()
	go server.HeartBeatToMaster()
	go server.HeartBeatFromMaster()
	return server
}
