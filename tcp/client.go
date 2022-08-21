package tcp

import (
	"bufio"
	"code/regis/base"
	"code/regis/conf"
	"code/regis/ds"
	"code/regis/file"
	log "code/regis/lib"
	"code/regis/lib/utils"
	"code/regis/redis"
	"net"
	"strconv"
	"strings"
	"time"
)

// RegisClient
// - fake client，用于加载rdb
// - master client，用于与主库交流
// RegisClient 与 RegisConn 的区别是：
// RegisClient 是本地任意端口连接其他redis服务器，所以远端主库都在 RegisClient，还有一种可能是load rdb的fake client
// RegisConn 是远端任意端口连接本redis服务器，所以远端从库都在 RegisConn
type RegisClient struct {
	ID int64

	Conn net.Conn

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
	log.Info("client close %v %v", cli.ID, cli.Addr)
	_ = cli.Conn.Close()
}

// Recv 暂时先不处理错误，反正这是假client
//func (cli *RegisClient) Recv() base.Reply {
//	reader := bufio.NewReader(cli.Conn)
//	for {
//		msg, err := reader.ReadBytes('\n')
//		if err != nil {
//			log.Error("client err %v", err)
//			cli.Close()
//			return redis.NilReply
//		}
//		switch msg[0] {
//		case redis.PrefixErr[0]:
//			log.Error("%v", string(msg[:len(msg)-2]))
//		case redis.PrefixStr[0], redis.PrefixInt[0]:
//			log.Debug("%v", string(msg[:len(msg)-2]))
//		case redis.PrefixArray[0], redis.PrefixBulk[0]:
//			log.Debug("bluk or array")
//		default:
//			log.Debug("len: %v, msg: %v", len(msg), msg)
//		}
//	}
//}

func (cli *RegisClient) GetReply() base.Reply {
	return redis.Parse2Reply(cli.Conn)
}

// PartSync 自己是slave，从远端master 增量同步到自己
func (cli *RegisClient) PartSync() {
	//selfClient := MustNewClient(Server.GetAddr(), Server)
	//defer selfClient.Close()
	r := bufio.NewReader(cli.Conn)
	for {
		log.Debug("I'm listen master cmd %v", cli.Conn.LocalAddr())
		_, query, err := redis.Parse2Inline(r)
		log.Debug("PartSync ing, %v", query)
		cli.LastBeat = time.Now()
		if err != nil {
			log.Error("PartSync err %v", err)
			cli.Close()
			return
		}
		cmdBytes := redis.CmdSReply(query...)
		add := Server.ReplBacklog.Write(cmdBytes.Bytes())
		Server.MasterReplOffset += add
		Client.Send(cmdBytes)
		_ = Client.GetReply()
	}
}

// FullSync 自己是slave，要拉远端master同步
func (cli *RegisClient) FullSync(offset int64) {

	// 接下来master传递一个bulk字符串，用于传输rdb
	// 先传递一个$509\r\n，其中509表示rdb大小
	reader := bufio.NewReader(cli.Conn)
	msg, err := reader.ReadBytes('\n')
	//msg, err := cli.Conn.Read('\n')
	log.Info("read msg %v %v", utils.BytesViz(msg), err)
	if err != nil {
		return
	}
	rdbSize, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		log.Error("can not parseInt, %v", err)
		return
	}
	err = file.SaveFile(conf.Conf.RDBName, reader, int(rdbSize))
	if err != nil {
		log.Error("get rdb fail, err %v", err)
	}

	// 清掉自己所有的历史数据
	Server.DB.Flush()
	if Server.ReplBacklog != nil {
		Server.ReplBacklog.Reset(offset)

		// set false 让load rdb的命令不至于写入到ReplBacklog中
		Server.ReplBacklog.Active = false
	}

	// 同步地load rdb，也就是说这个函数退出时，rdb就已经完全load完毕
	Server.LoadRDB(conf.Conf.RDBName)

	log.Notice("MASTER <-> REPLICA sync: Finished with success")
	if Server.ReplBacklog == nil {
		Server.ReplBacklog = ds.NewRingBuffer(conf.Conf.ReplBacklogSize)
	}
	if !Server.ReplBacklog.Active {
		Server.ReplBacklog.Active = true
	}
	Server.MasterReplOffset = offset
	Server.Replid2 = strings.Repeat("0", base.ConfigRunIDSize)
	Server.SlaveState = base.ReplStateConnected
	cli.PartSync()
}

func NewClient(addr string) (*RegisClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	cli := &RegisClient{
		ID:   utils.GetConnFd(conn),
		Conn: conn,
		Addr: addr,
	}
	return cli, nil
}

func MustNewClient(addr string) *RegisClient {
	cli := &RegisClient{
		Addr: addr,
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
