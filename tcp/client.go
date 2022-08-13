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
	"time"
)

type RegisClient struct {
	server *RegisServer
	conn   net.Conn
	addr   string // server addr
}

func (cli *RegisClient) GetConn() net.Conn {
	return cli.conn
}

func (cli *RegisClient) Send(reply base.Reply) {
	_, _ = cli.conn.Write(reply.Bytes())
}

func (cli *RegisClient) Close() {
	log.Info("client close")
	_ = cli.conn.Close()
}

// Recv 暂时先不处理错误，反正这是假client
func (cli *RegisClient) Recv() base.Reply {
	reader := bufio.NewReader(cli.conn)
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

func (cli *RegisClient) Reply() base.Reply {
	return redis.ParseReply(cli.conn)
}

func (cli *RegisClient) RecvN(buf []byte) (n int, err error) {
	return io.ReadAtLeast(cli.conn, buf, 1)
}

func (cli *RegisClient) RecvAll() (buf []byte, err error) {
	//io.ReadAtLeast()
	return io.ReadAll(cli.conn)
}

func (cli *RegisClient) Handler() {
	cli.Send(redis.CmdReply("ping"))
	if !redis.Equal(cli.Reply(), redis.StrReply("PONG")) {
		return
	}
	cli.Send(redis.CmdReply("REPLCONF", "listening-port", conf.Conf.Port))
	if !redis.Equal(cli.Reply(), redis.OkReply) {
		return
	}
	cli.Send(redis.CmdReply("REPLCONF", "capa", "PSYNC"))
	if !redis.Equal(cli.Reply(), redis.OkReply) {
		return
	}
	cli.Send(redis.CmdReply("PSYNC", "?", -1))
	syncInfo := strings.Split(redis.GetString(cli.Reply()), " ")
	// 接下来master传递一个bulk字符串，用于传输rdb
	// 先传递一个$509\r\n，其中509表示rdb大小
	reader := bufio.NewReader(cli.conn)
	msg, err := reader.ReadBytes('\n')
	if err != nil {
		return
	}
	rdbSize, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return
	}
	log.Info("begin to save %v %v", rdbSize, syncInfo)
	err = file.SaveFile("dump.rdb", reader, int(rdbSize))
	if err != nil {
		log.Error("get rdb fail, err", err)
	}
	LoadRDB(cli.server, nil, nil)
	cli.server.replid = syncInfo[1]
	cli.server.role = RoleSlave
	log.Info("slave to %v", cli.server.replid)
	//cli.server.addClient(ctx, cli.conn)
	//log.Info("size %v", rdbSize)

	// 1. 阻塞读conn中的信息
	// 2. 解析conn中的信息为payload
	// 3. 一旦形成一个payload，将其转化为Command传入workChan
	// 4. 等主线程完成再回到1

	// 1. 2. 3. 解析客户端的命令并放入workChan中
	pC := redis.Parse(cli.conn)
	selfClient := MustNewClient(cli.server.GetAddr(), cli.server)
	defer selfClient.Close()
	for {
		// 阻塞获取payload
		pc := <-pC
		if pc.Err != nil {
			log.Error("connection err %v", pc.Err)
			cli.Close()
			return
		}
		Server.slaveReplOffset += int64(pc.Size)
		selfClient.Send(redis.CmdSReply(pc.Query...))
		reply := selfClient.Reply()
		//cli.conn.Write(cmd.Reply.Bytes())
		log.Debug("master cmd is done %v", utils.BytesViz(reply.Bytes()))
		//_, _ = cli.conn.Write(reply)
		cli.Send(redis.CmdReply("REPLCONF", "ACK", Server.slaveReplOffset))

	}
}
func NewClient(addr string, server *RegisServer) (*RegisClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	cli := &RegisClient{
		server: server,
		conn:   conn,
		addr:   addr,
	}
	return cli, nil
}

func MustNewClient(addr string, server *RegisServer) *RegisClient {
	cli := &RegisClient{
		server: server,
		addr:   addr,
	}
	var err error
	for {
		cli.conn, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
		log.Error("Error condition on socket for SYNC: RegisConn refused %v", addr)
		time.Sleep(time.Second)
	}
	return cli
}
