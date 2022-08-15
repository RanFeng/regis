package tcp

import (
	"bufio"
	"code/regis/base"
	"code/regis/conf"
	"code/regis/file"
	log "code/regis/lib"
	"code/regis/lib/utils"
	"code/regis/redis"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// RegisClient
// - fake client，用于加载rdb
// - master client，用于与主库交流
// RegisClient 与 RegisConn 的区别是：
// RegisClient 是本地任意端口连接其他redis服务器，所以远端主库都在 RegisClient
// RegisConn 是远端任意端口连接本redis服务器，所以远端从库都在 RegisConn
type RegisClient struct {
	ID     int64
	server *RegisServer
	Conn   net.Conn

	Addr     string // remote server Addr
	LastBeat time.Time
}

func (cli *RegisClient) RemoteAddr() string {
	return cli.Conn.RemoteAddr().String()
}

func (cli *RegisClient) LocalAddr() string {
	return cli.Conn.LocalAddr().String()
}

func (cli *RegisClient) Send(reply base.Reply) {
	_, _ = cli.Conn.Write(reply.Bytes())
}

func (cli *RegisClient) Write(msg []byte) error {
	_, err := cli.Conn.Write(msg)
	if err != nil {
		cli.Close()
	}
	return err
}

func (cli *RegisClient) Close() {
	log.Info("client close")
	_ = cli.Conn.Close()
}

// Recv 暂时先不处理错误，反正这是假client
func (cli *RegisClient) Recv() base.Reply {
	reader := bufio.NewReader(cli.Conn)
	for {
		msg, err := reader.ReadBytes('\n')
		if err != nil {
			log.Error("client err %v", err)
			cli.Close()
			return redis.NilReply
		}
		switch msg[0] {
		case redis.PrefixErr[0]:
			log.Error("%v", string(msg[:len(msg)-2]))
		case redis.PrefixStr[0], redis.PrefixInt[0]:
			log.Debug("%v", string(msg[:len(msg)-2]))
		case redis.PrefixArray[0], redis.PrefixBulk[0]:
			log.Debug("bluk or array")
		default:
			log.Debug("len: %v, msg: %v", len(msg), msg)
		}
	}
}

func (cli *RegisClient) GetReply() base.Reply {
	return redis.Parse2Reply(cli.Conn)
}

func (cli *RegisClient) RecvN(buf []byte) (n int, err error) {
	return io.ReadAtLeast(cli.Conn, buf, 1)
}

func (cli *RegisClient) RecvAll() (buf []byte, err error) {
	//io.ReadAtLeast()
	return io.ReadAll(cli.Conn)
}

// PartSync 自己是slave，从远端master 增量同步到自己
func (cli *RegisClient) PartSync() {
	//selfClient := MustNewClient(cli.server.GetAddr(), cli.server)
	//defer selfClient.Close()
	r := bufio.NewReader(cli.Conn)
	for {
		log.Info("I'm listen master cmd %v", cli.Conn.LocalAddr())
		n, query, err := redis.Parse2Inline(r)
		log.Info("PartSync ing, %v", query)
		cli.LastBeat = time.Now()
		if err != nil {
			log.Error("PartSync err %v", err)
			return
		}
		atomic.AddInt64(&Server.SlaveReplOffset, int64(n))
		//log.Info("PartSync add MasterReplOffset %v %v", Server.SlaveReplOffset, n)
		Client.Send(redis.CmdSReply(query...))
		_ = Client.GetReply()
		//reply := selfClient.GetReply()
		//log.Debug("master cmd is done %v", utils.BytesViz(reply.Bytes()))
	}
}

// FullSync 自己是slave，要拉远端master同步
func (cli *RegisClient) FullSync(replid string, offset int64) {
	// 接下来master传递一个bulk字符串，用于传输rdb
	// 先传递一个$509\r\n，其中509表示rdb大小
	log.Notice("Full resync from master: %v:%v", replid, offset)
	reader := bufio.NewReader(cli.Conn)
	msg, err := reader.ReadBytes('\n')
	log.Info("read msg %v", utils.BytesViz(msg))
	if err != nil {
		return
	}
	rdbSize, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return
	}
	err = file.SaveFile(conf.Conf.RDBName, reader, int(rdbSize))
	if err != nil {
		log.Error("get rdb fail, err %v", err)
	}

	// 清掉自己所有的历史数据
	cli.server.DB.Flush()
	cli.server.ReplBacklog.Reset(offset)

	// set false 让load rdb的命令不至于写入到ReplBacklog中
	cli.server.ReplBacklog.Active = false

	// 同步地load rdb，也就是说这个函数退出时，rdb就已经完全load完毕
	cli.server.LoadRDB(conf.Conf.RDBName)

	cli.server.Replid = replid
	atomic.SwapInt64(&cli.server.MasterReplOffset, offset)
	atomic.SwapInt64(&cli.server.SlaveReplOffset, offset)

	for k := range cli.server.Slave {
		cli.server.CloseConn(k)
	}
	log.Notice("MASTER <-> REPLICA sync: Finished with success")
	//log.Notice("Synchronization with replica %v succeeded", cli.RemoteAddr())
}

// PSync 自己是slave，要拉远端master同步
func (cli *RegisClient) PSync() {
	log.Notice("MASTER <-> REPLICA sync started")
	cli.Send(redis.CmdReply("ping")) // TODO 此处发出的ping会被master认为是master的master发来的
	if !redis.Equal(cli.GetReply(), redis.StrReply("PONG")) {
		return
	}
	cli.Send(redis.CmdReply("REPLCONF", "listening-port", conf.Conf.Port))
	if !redis.Equal(cli.GetReply(), redis.OkReply) {
		return
	}
	cli.Send(redis.CmdReply("REPLCONF", "capa", "PSYNC"))
	if !redis.Equal(cli.GetReply(), redis.OkReply) {
		return
	}
	//if cli.server.Master == nil {
	//	cli.Send(redis.CmdReply("PSYNC", "?", -1))
	//} else {
	log.Notice("Trying a partial resynchronization (request %v:%v).",
		cli.server.Replid, cli.server.SlaveReplOffset+1)
	cli.Send(redis.CmdReply("PSYNC", cli.server.Replid, cli.server.SlaveReplOffset+1))
	//}
	reply := redis.GetString(cli.GetReply())
	log.Info("get master reply, %v", reply)
	syncInfo := strings.Split(reply, " ")
	if len(syncInfo) == 3 { // 是 full sync
		tag := strings.ToUpper(syncInfo[0])
		switch tag {
		case "FULLRESYNC":
			offset, err := strconv.ParseInt(syncInfo[2], 10, 64)
			if err != nil {
				log.Error("get syncInfo offset fail, err %v", syncInfo)
			}
			if offset < 0 {
				offset = 0
			}
			cli.FullSync(syncInfo[1], offset)
		}
	}

	go cli.PartSync()
	cli.server.Master = cli
	cli.LastBeat = time.Now()
	cli.server.ReplBacklog.Active = true
	log.Info("slave to %v %v %v", cli.server.Replid, cli.RemoteAddr(), cli.server.SlaveReplOffset)
}

func NewClient(addr string, server *RegisServer) (*RegisClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	cli := &RegisClient{
		ID:     utils.GetConnFd(conn),
		server: server,
		Conn:   conn,
		Addr:   addr,
	}
	return cli, nil
}

func MustNewClient(addr string, server *RegisServer) *RegisClient {
	cli := &RegisClient{
		server: server,
		Addr:   addr,
	}
	var err error
	for {
		cli.Conn, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
		log.Error("Error condition on socket for SYNC: RegisConn refused %v", addr)
		time.Sleep(time.Second)
	}
	cli.ID = utils.GetConnFd(cli.Conn)
	return cli
}
