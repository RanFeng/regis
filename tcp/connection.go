package tcp

import (
	"code/regis/base"
	"code/regis/ds"
	log "code/regis/lib"
	"code/regis/lib/utils"
	"code/regis/redis"
	"net"
)

type RegisConn struct {
	id   int64
	conn net.Conn

	server *RegisServer

	//name      string // 客户端名字
	selectedDB int // 客户端连上的db_index

	//closeChan chan<- int64
	//workChan  chan<- *base.Command
	doneChan chan *base.Command

	// 存储客户端订阅的频道
	pubsubList    *ds.LinkedList
	pubsubPattern *ds.LinkedList
}

func (c *RegisConn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *RegisConn) GetID() int64 {
	return c.id
}

func (c *RegisConn) GetDBIndex() int {
	return c.selectedDB
}

func (c *RegisConn) SetDBIndex(i int) {
	c.selectedDB = i
}

func (c *RegisConn) Close() {
	log.Info("connection close")
	UnSubscribe(c.server, c, []string{"unsubscribe"})
	c.server.closeChan <- utils.GetConnFd(c.conn)
}

func (c *RegisConn) GetConn() net.Conn {
	return c.conn
}

func (c *RegisConn) Write(b []byte) {
	_, err := c.conn.Write(b)
	if err != nil {
		c.Close()
	}
}

func (c *RegisConn) Reply(reply base.Reply) {
	if reply == nil {
		return
	}
	_, err := c.conn.Write(reply.Bytes())
	if err != nil {
		c.Close()
	}
}

func (c *RegisConn) Handle() {
	// 1. 阻塞读conn中的信息
	// 2. 解析conn中的信息为payload
	// 3. 一旦形成一个payload，将其转化为Command传入workChan
	// 4. 等主线程完成再回到1

	// 1. 2. 3. 解析客户端的命令并放入workChan中
	pC := redis.Parse(c.conn)
	for {
		// 阻塞获取payload
		pc := <-pC
		if pc.Err != nil {
			log.Error("connection err %v", pc.Err)
			c.Close()
			return
		}
		cmd := &base.Command{
			Conn:  c,
			Query: pc.Query,
		}
		// 将Command放入工作队列中，等待主协程完成
		c.server.workChan <- cmd
		// 4. 阻塞等待cmd完成
		doneCMD := <-c.doneChan
		if doneCMD.Err != nil {
			log.Error("connection err %v", doneCMD.Err)
			c.Close()
			return
		}
		//log.Debug("cmd is done %v", cmd.Reply.Bytes())
		c.Reply(doneCMD.Reply)
		//_, _ = c.conn.Write(doneCMD.Reply.Bytes())
		//c.Reply(doneCMD.Reply)
	}
}

func (c *RegisConn) CmdDone(cmd *base.Command) {
	c.doneChan <- cmd
}

func (c *RegisConn) NSubChannel() base.LList {
	return c.pubsubList
}
func (c *RegisConn) PSubChannel() base.LList {
	return c.pubsubPattern
}

func initConnection(conn net.Conn, server *RegisServer) *RegisConn {
	log.Debug("get conn client %v", utils.GetConnFd(conn))
	c := &RegisConn{
		id:   utils.GetConnFd(conn),
		conn: conn,

		server: server,
		//closeChan: server.closeChan,
		//workChan:  server.workChan,
		doneChan: make(chan *base.Command),

		pubsubList:    ds.NewLinkedList(),
		pubsubPattern: ds.NewLinkedList(),
	}
	go c.Handle()
	return c
}
