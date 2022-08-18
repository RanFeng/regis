package tcp

import (
	"code/regis/base"
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

type replicaForRegisConn struct {
	// 作为slave的状态
	State base.MySlaveState

	AckOffset int64

	LastBeat time.Time
}

// RegisConn
// - 用于管理客户端来的连接
// RegisClient 与 RegisConn 的区别是：
// RegisClient 是本地任意端口连接其他redis服务器，所以主库都在 RegisClient, master都是在 RegisClient的远端
// RegisConn 是远端任意端口连接本redis服务器，所以从库都在 RegisConn, slave都是在 RegisConn 的远端
type RegisConn struct {
	ID   int64
	Conn net.Conn

	//name      string // 客户端名字
	DBIndex int // 客户端连上的db_index

	doneChan chan *Command

	// 存储客户端订阅的频道 channel -> struct{}
	PubsubList map[string]struct{}
	//PubsubPattern *ds.LinkedList

	replicaForRegisConn
}

func (c *RegisConn) RemoteAddr() string {
	return c.Conn.RemoteAddr().String()
}

func (c *RegisConn) Close() {
	log.Info("connection close")
	c.UnSubscribeAll()
	Server.CloseConn(c.ID)
}

func (c *RegisConn) UnSubscribeAll() {
	for key := range c.PubsubList {
		// 获取server的订阅dict
		if subs, ok := Server.PubsubDict[key]; ok {
			// 将conn从server的订阅list中删除
			delete(subs, c.ID)
		}

		// conn自己更新自己的订阅list，取消订阅该频道
		delete(c.PubsubList, key)
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
	pC := redis.Parse2Payload(c.Conn)
	for {
		// 阻塞获取payload
		pc := <-pC
		if pc.Err != nil {
			log.Error("connection err %v %v %v", pc.Err, c.ID, c.RemoteAddr())
			c.Close()
			return
		}
		c.LastBeat = time.Now()
		cmd := &Command{
			Conn:  c,
			Query: pc.Query,
		}
		// 将Command放入工作队列中，等待主协程完成
		Server.workChan <- cmd
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

func NewConnection(conn net.Conn) *RegisConn {
	c := &RegisConn{
		ID:         utils.GetConnFd(conn),
		Conn:       conn,
		doneChan:   make(chan *Command),
		PubsubList: make(map[string]struct{}),
		//PubsubPattern: ds.NewLinkedList(),
	}
	log.Debug("get Conn client %v", c.ID)
	go c.Handle()
	return c
}
