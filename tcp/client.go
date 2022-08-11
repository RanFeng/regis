package tcp

import (
	"bufio"
	"code/regis/base"
	log "code/regis/lib"
	"code/regis/redis"
	"io"
	"net"
	"time"
)

type Client struct {
	conn net.Conn
	addr string // server addr
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

func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	cli := &Client{
		conn: conn,
		addr: addr,
	}
	return cli, nil
}

func MustNewClient(addr string) *Client {
	cli := &Client{
		addr: addr,
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
