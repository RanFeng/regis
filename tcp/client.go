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

type Client struct {
	server *Server
	conn   net.Conn
	addr   string // server addr
}

func (cli *Client) GetConn() net.Conn {
	return cli.conn
}

func (cli *Client) Send(reply base.Reply) {
	_, _ = cli.conn.Write(reply.Bytes())
}

func (cli *Client) Close() {
	log.Info("client close")
	_ = cli.conn.Close()
}

// Recv 暂时先不处理错误，反正这是假client
func (cli *Client) Recv() base.Reply {
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

func (cli *Client) Reply() base.Reply {
	return redis.ParseReply(cli.conn)
}

func (cli *Client) RecvN(buf []byte) (n int, err error) {
	return io.ReadAtLeast(cli.conn, buf, 1)
}

func (cli *Client) RecvAll() (buf []byte, err error) {
	//io.ReadAtLeast()
	return io.ReadAll(cli.conn)
}

func (cli *Client) Handler() {
	defer func() {
		cli.Close()
	}()
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
	cli.server.replid = syncInfo[1]

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
	//log.Info("size %v", rdbSize)
	//go func() {
	for {
		buf := make([]byte, 40)
		n, err := cli.RecvN(buf)
		if err != nil {
			log.Error("%v", err)
		}
		log.Info("%v %v", n, utils.BytesViz(buf))
	}
}
func NewClient(addr string, server *Server) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	cli := &Client{
		server: server,
		conn:   conn,
		addr:   addr,
	}
	return cli, nil
}

func MustNewClient(addr string, server *Server) *Client {
	cli := &Client{
		server: server,
		addr:   addr,
	}
	var err error
	for {
		cli.conn, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
		log.Error("Error condition on socket for SYNC: Connection refused %v", addr)
		time.Sleep(time.Second)
	}
	return cli
}
