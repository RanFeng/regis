package tcp

import (
	"code/regis/base"
	"code/regis/ds"
	log "code/regis/lib"
	"code/regis/lib/utils"
	"code/regis/redis"
	"net"
	"time"
)

type Command struct {
	Conn  *RegisConn
	Query []string
	Reply base.Reply
	Err   error
}

// RegisConn
// - 用于管理客户端来的连接
// RegisClient 与 RegisConn 的区别是：
// RegisClient 是本地任意端口连接其他redis服务器，所以主库都在 RegisClient
// RegisConn 是远端任意端口连接本redis服务器，所以从库都在 RegisConn
type RegisConn struct {
	ID   int64
	Conn net.Conn

	// 一般来说都是 base.ConnNormal 的状态
	// 当 RegisConn 作为从库，且正在与主库进行rdb同步时，状态为 base.ConnSync
	Status int

	server *RegisServer

	//name      string // 客户端名字
	DBIndex int // 客户端连上的db_index

	doneChan chan *Command

	LastBeat time.Time

	// 存储客户端订阅的频道
	PubsubList    *ds.LinkedList
	PubsubPattern *ds.LinkedList
}

func (c *RegisConn) RemoteAddr() string {
	return c.Conn.RemoteAddr().String()
}

func (c *RegisConn) Close() {
	log.Info("connection close")
	c.UnSubscribeAll()
	c.server.CloseConn(utils.GetConnFd(c.Conn))
}

func (c *RegisConn) UnSubscribeAll() {
	dict := c.server.GetPubSub()
	args := make([]string, 0, c.PubsubList.Len())

	ch := make(chan struct{})
	for val := range c.PubsubList.Range(ch) {
		args = append(args, val.(string))
	}

	for i := 1; i < len(args); i++ {
		// 获取server的订阅dict
		if val, ok := dict.Get(args[i]); ok {
			// 将conn从server的订阅list中删除
			val.(base.LList).RemoveFirst(func(conn interface{}) bool {
				return c.ID == conn.(*RegisConn).ID
			})
		}

		// conn自己更新自己的订阅list，取消订阅该频道
		c.PubsubList.RemoveFirst(func(s interface{}) bool {
			return args[i] == s.(string)
		})
	}
}

func (c *RegisConn) Write(b []byte) error {
	_, err := c.Conn.Write(b)
	if err != nil {
		c.Close()
	}
	return err
}

func (c *RegisConn) Reply(reply base.Reply) {
	if reply == nil {
		return
	}
	_, err := c.Conn.Write(reply.Bytes())
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
	pC := redis.Parse(c.Conn)
	for {
		// 阻塞获取payload
		pc := <-pC
		if pc.Err != nil {
			log.Error("connection err %v", pc.Err)
			c.Close()
			return
		}
		c.LastBeat = time.Now()
		cmd := &Command{
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
		c.Reply(doneCMD.Reply)
	}
}

func (c *RegisConn) CmdDone(cmd *Command) {
	c.doneChan <- cmd
}

func NewConnection(conn net.Conn, server *RegisServer) *RegisConn {
	log.Debug("get Conn client %v", utils.GetConnFd(conn))
	c := &RegisConn{
		ID:            utils.GetConnFd(conn),
		Conn:          conn,
		server:        server,
		doneChan:      make(chan *Command),
		PubsubList:    ds.NewLinkedList(),
		PubsubPattern: ds.NewLinkedList(),
	}
	go c.Handle()
	return c
}
