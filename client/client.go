package client

import (
	"bufio"
	"code/regis/base"
	log "code/regis/lib"
	"code/regis/redis"
	"net"
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
func (cli *Client) Recv() {
	reader := bufio.NewReader(cli.conn)
	for {
		msg, err := reader.ReadBytes('\n')
		if err != nil {
			log.Error("client err %v", err)
			cli.Close()
			return
		}
		if msg[0] == redis.PrefixErr[0] {
			log.Error("%v", string(msg[:len(msg)-2]))
		} else {
			log.Debug("%v", string(msg[:len(msg)-2]))
		}
	}
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
