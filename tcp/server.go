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
	"time"

	"golang.org/x/sync/semaphore"
)

var (
	ctx    = context.Background()
	Server *RegisServer
	Client *RegisClient
)

// replicationMaster 这个结构体存放一些master的专有数据
// 就是当自己作为master的身份时，需要用到哪些数据
type replicationMaster struct {
	// 是 RegisServer.Who 的子集用于存储slave的connection, RegisConn.ID -> *RegisConn
	Slave map[int64]*RegisConn

	// 自己的slave的db下标
	SlaveDBIndex int

	ReplPingSlavePeriod int64 // 给slave发心跳包的周期

	ReplBacklogLastBeat time.Time
}

// replicationSlave 这个结构体存放一些slave的专有数据
// 就是当自己作为slave的身份时，需要用到哪些数据
type replicationSlave struct {
	// 用于存储master的client, 如果为nil，表示当前不是slave
	// 如果与master断开链接，并不能将 Master 置为 nil !!
	// 除非不想要主从同步了
	Master *RegisClient

	// 用于存储master的地址
	MasterAddr string

	// 上一次向master发握手包的时间
	LastBeatToMaster time.Time

	// 当自己是个slave的时候，自己现在的状态
	SlaveState base.MeSlaveState

	// 给master发心跳包的周期
	ReplPingMasterPeriod int
}

type replication struct {
	Replid  string
	Replid2 string

	replicationMaster
	replicationSlave

	// 当前节点往缓冲区存进去的字节数
	// 如果不是主从结构，写命令来时不会增加
	// 如果是主从结构，且自己是master，将用户命令写入缓冲区并进行计数，
	// 如果是slave，且master传来了写命令，也要写入缓冲区中并进行计数
	//	如果一个slave来要求全量同步，发出 fullsync replid masterReplOffset，并发出rdb文件
	//	表示："我发给你的rdb文件同步进度是 masterReplOffset"
	// 当前节点作为slave时，与master的同步offset
	// 收到 fullsync ID offset 之后，会收到一个rdb文件，将rdb load到db中之后
	// 令 MasterReplOffset = offset, MasterReplOffset = offset
	// 表示 "我收到的rdb文件的同步进度是 slaveReplOffset"
	MasterReplOffset  int64
	MasterReplOffset2 int64

	// 表示上一次进行BGSave的时候，本机的 MasterReplOffset 是多少？
	// 根据slave中有没有 base.SlaveStateWaitBGSaveEnd 状态的slave，
	// 可以确认当前服务正在BGSave的rdb是否可以用于传给slave全量同步。
	// 显然，当 LastBGSaveOffset = 0 的时候，有两种情况，
	// - 第一种：Server 还是单机的时候，自发的BGSave，
	//   因为单机时，即使在BGSave的过程中发生了新的写入，也不会增加 MasterReplOffset，
	//   这时候 LastBGSaveOffset 一直都是0。这种情况，不允许使用本次BGSave生成的rdb来给slave全量同步。
	// - 第二种：Server 接到首个slave时，存下的 MasterReplOffset ，
	//   因为是首个，所以此时 LastBGSaveOffset 也为0，但是只要找slave中有没有 base.SlaveStateWaitBGSaveEnd 状态的slave，
	//   如果有，就说明当前的这个BGSave就是他们创建的，那就可以用这个rdb来同步给当前的slave
	// 上述只使用 base.SlaveStateWaitBGSaveEnd 而不使用 base.SlaveStateNeedBGSave，是为了解决这个边界case：
	//   如果BGSave的CronJob开始了，然后来了一条写命令，然后来了一个slave A要求同步，这时候slave A只能挂起等待下一次BGSave，
	//   如果这时候来了个slave B要求同步，发现 LastBGSaveOffset == 0，但是有个slave A在 base.SlaveStateNeedBGSave
	//   以为刚刚的BGSave是slave A发起的，然后进行同步，这时候就有问题了。
	//   所以当 LastBGSaveOffset == 0的时候，必须用 base.SlaveStateWaitBGSaveEnd 来判断是否可以用正在BGSave的rdb。
	//   另外，一旦slave A连接了，master自己就会开始ping slave A，这样 MasterReplOffset 就会自增，
	//   这样等下一次不管是CronJob BGSave还是slave C进来，都会发现...
	LastBGSaveOffset int64

	ReplBacklog *ds.RingBuffer
}

type safety struct {
	Lock       sync.Mutex // 对 RegisServer.Who, RegisServer.Slave 操作的锁
	Monopolist string
	clientSema *semaphore.Weighted  // 用于控制最大客户端连接数量
	Who        map[int64]*RegisConn // 存储已连接的connection，RegisConn.ID -> *RegisConn
}

type RegisServer struct {
	Address    string
	maxClients int64

	replication
	safety

	// DB 是服务端的主数据库
	DB base.DB

	workChan chan *Command // 用于给主协程输送命令的

	// 上一次BGSave的状态
	//LastBGSaveStatus int

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
	_ = Client.GetReply()
	query := file.LoadRDB(fn)
	for i := range query {
		Client.Send(redis.CmdReply(query[i]...))
		_ = Client.GetReply()
	}
	Client.Send(redis.CmdReply("unlock"))
	_ = Client.GetReply()
}

func SaveRDB() error {
	Server.LastBGSaveOffset = Server.MasterReplOffset
	//log.Debug("save RDB offset %v %v", Server.LastBGSaveOffset, Server.MasterReplOffset)
	err := file.SaveRDB(Server.DB.SaveRDB)
	if err != nil {
		log.Error("save RDB error %v", err)
		Server.LastBGSaveOffset = -1
		// TODO bgsave出错，断开所有在 base.SlaveStateNeedBGSave 的slave
		waits := make([]int64, 0, len(Server.Slave))
		for s := range Server.Slave {
			if Server.Slave[s].State <= base.SlaveStateWaitBGSaveEnd {
				waits = append(waits, s)
			}
		}
		Server.CloseConn(waits...)
		return err
	}

	// 完成一次bgsave，对于那些等待rdb同步的slave，传递rdb
	Server.SlaveDBIndex = -1
	for s := range Server.Slave {
		//log.Debug("slave now is  %v", Server.Slave[s].State)
		if Server.Slave[s].State == base.SlaveStateWaitBGSaveEnd {
			_ = Server.Slave[s].Write(redis.InlineIReply("FULLRESYNC", Server.Replid, Server.LastBGSaveOffset).Bytes())
			Server.Slave[s].State = base.SlaveStateSendingRDB
			// 发出全量RDB
			file.SendRDB(conf.Conf.RDBName, Server.Slave[s].Conn)
			Server.Slave[s].State = base.SlaveStateOnline
		}
	}
	return nil
}

func (s *RegisServer) GetWorkChan() <-chan *Command {
	return s.workChan
}

func (s *RegisServer) GetInfo() string {
	serverInfo := `# RegisServer
tcp_port:%s
run_id:%s
role:%s
connected_slaves:%d
master_repl_offset:%d
repl_backlog_active:%v
repl_backlog_size:%d
repl_backlog_first_byte_offset:%v
repl_backlog_histlen:%v
`
	port := strings.Split(s.Address, ":")[1]
	serverInfo += fmt.Sprintf("tcp_port:%v\n", port)
	serverInfo += fmt.Sprintf("run_id:%v\n", s.Replid)
	serverInfo += fmt.Sprintf("role:%v\n", utils.IF(s.Master == nil, "master", "slave"))
	serverInfo += fmt.Sprintf("connected_slaves:%v\n", len(s.Slave))
	serverInfo += fmt.Sprintf("master_repl_offset:%v\n", s.MasterReplOffset)
	serverInfo += fmt.Sprintf("repl_backlog_active:%v\n", s.ReplBacklog != nil && s.ReplBacklog.Active)
	if s.ReplBacklog != nil {
		serverInfo += fmt.Sprintf("repl_backlog_size:%v\n", s.ReplBacklog.Size)
		serverInfo += fmt.Sprintf("repl_backlog_first_byte_offset:%v\n", s.ReplBacklog.StartPtr)
		serverInfo += fmt.Sprintf("repl_backlog_histlen:%v\n", s.ReplBacklog.HistLen)
	}
	return serverInfo
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
		s.clientSema.Release(1)
	}
}

func (s *RegisServer) addClient(ctx context.Context, conn net.Conn) {
	//log.Debug("wait clientSema now is %v", s.Who.Len())
	err := s.clientSema.Acquire(ctx, 1)
	if err != nil {
		log.Error("clientSema acquire fail %v", err)
	}
	//log.Debug("get clientSema now is %v", s.Who.Len())
	c := NewConnection(conn)
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
	Client = MustNewClient(Server.Address)
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
	server.Replid = utils.GetRandomHexChars(base.ConfigRunIDSize)
	server.Address = fmt.Sprintf("%s:%d", prop.Bind, prop.Port)
	server.maxClients = prop.MaxClients

	server.Who = make(map[int64]*RegisConn, conf.Conf.MaxClients)
	server.DB = database.NewMultiDB()

	server.clientSema = semaphore.NewWeighted(prop.MaxClients)
	server.workChan = make(chan *Command)

	server.PubsubDict = make(map[string]map[int64]*RegisConn, 128)
	//server.pubsubPattern = ds.NewLinkedList()

	server.Slave = make(map[int64]*RegisConn, 8)

	server.ReplPingSlavePeriod = 10
	server.ReplPingMasterPeriod = 3

	server.ReplBacklog = nil

	return server
}
