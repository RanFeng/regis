package command

import (
	"code/regis/base"
	"code/regis/conf"
	"code/regis/ds"
	"code/regis/file"
	log "code/regis/lib"
	"code/regis/redis"
	"code/regis/tcp"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	_sub   = "subscribe"
	_msg   = "message"
	_unsub = "unsubscribe"
)

func Ping(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	if len(args) >= 3 {
		return redis.ArgNumErrReply(args[0])
	}
	if len(args) == 2 {
		return redis.StrReply(args[1])
	}
	return redis.StrReply("PONG")
}

func Select(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	i, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return redis.ErrReply("ERR value is not an integer or out of range")
	}
	index := int(i)
	if index >= conf.Conf.Databases || index < 0 {
		return redis.ErrReply("ERR DB index is out of range")
	}
	conn.DBIndex = index
	return redis.OkReply
}

func Save(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	if server.DB.GetStatus() != base.WorldNormal {
		return redis.ErrReply("ERR can not save in bgsave")
	}
	server.DB.SetStatus(base.WorldStopped)
	defer server.DB.SetStatus(base.WorldNormal)

	ch := make(chan struct{})
	defer close(ch)

	err := file.SaveRDB(server.DB.SaveRDB)
	if err != nil {
		return redis.ErrReply("ERR in save")
	}
	return redis.OkReply
}

func BGSave(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	if server.DB.GetStatus() != base.WorldNormal {
		return redis.ErrReply("ERR can not save in bgsave")
	}
	server.DB.SetStatus(base.WorldFrozen)

	go file.SaveRDB(server.DB.SaveRDB) //nolint:errcheck

	return redis.BulkStrReply("Background saving started")
}

func Publish(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	dict := server.GetPubSub()
	val, ok := dict.Get(args[1])
	if !ok {
		return redis.IntReply(0)
	}
	list := val.(*ds.LinkedList)
	reply := redis.ArrayReply([]interface{}{
		_msg, args[1], args[2],
	})

	ch := make(chan struct{})
	defer close(ch)
	for item := range list.Range(ch) {
		item.(*tcp.RegisConn).Reply(reply)
	}

	return redis.IntReply(int(list.Len()))
}

func Subscribe(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	dict := server.GetPubSub()
	ret := make([]interface{}, 0, len(args)*3)
	for i := 1; i < len(args); i++ {
		// 获取server的订阅dict
		val, ok := dict.Get(args[i])
		if !ok {
			val = ds.NewLinkedList()
		}
		list := val.(base.LList)
		// 将conn加入server的订阅dict
		list.PushTail(conn)
		dict.Put(args[i], list)

		// conn自己更新自己的订阅dict
		conn.PubsubList.PushTail(args[i])

		ret = append(ret, _sub, args[i], i-1)
	}

	return redis.ArrayReply(ret)
}

func UnSubscribe(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	dict := server.GetPubSub()
	if len(args) == 1 {
		ch := make(chan struct{})
		for val := range conn.PubsubList.Range(ch) {
			args = append(args, val.(string))
		}
	}
	ret := make([]interface{}, 0, len(args)*3)
	for i := 1; i < len(args); i++ {
		// 获取server的订阅dict
		if val, ok := dict.Get(args[i]); ok {
			// 将conn从server的订阅list中删除
			val.(base.LList).RemoveFirst(func(c interface{}) bool {
				return conn.ID == c.(*tcp.RegisConn).ID
			})
		}

		// conn自己更新自己的订阅list，取消订阅该频道
		conn.PubsubList.RemoveFirst(func(s interface{}) bool {
			return args[i] == s.(string)
		})

		ret = append(ret, _unsub, args[i], i-1)
	}
	return redis.ArrayReply(ret)
}

func ReplicaOf(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	_, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return redis.ErrReply("ERR value is not an integer or out of range")
	}

	server.Master = tcp.MustNewClient(fmt.Sprintf("%v:%v", args[1], args[2]), server)
	go server.Master.Handler()

	return redis.OkReply
}

func Info(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	sInfo := server.GetInfo()
	return redis.StrReply(sInfo)
}

func ReplConf(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	subCmd := strings.ToLower(args[1])
	switch subCmd {
	case "listening-port":
		//ip, _ := utils.ParseAddr(conn.RemoteAddr())
		//cli := tcp.MustNewClient(fmt.Sprintf("%v:%v", ip, args[2]), server)
		//server.Slave.Put(conn.RemoteAddr(), cli)
		//go server.HeartBeatFromSlave()
		return redis.OkReply
	case "capa":
		return redis.OkReply
	case "ack":
		return nil
	}
	return redis.OkReply
}

func PSync(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	if args[1] != server.Replid {
		go func() {
			conn.Status = base.ConnSync
			go func() {
				for conn.Status == base.ConnSync {
					err := conn.Write([]byte{'\n'})
					if err != nil {
						return
					}
					time.Sleep(time.Duration(server.ReplPingSlavePeriod) * time.Second)
				}
			}()
			file.SaveRDB(server.DB.SaveRDB) //nolint:errcheck
			file.SendRDB("dump.rdb", conn.Conn)
			server.Slave.Put(conn.RemoteAddr(), conn)
			conn.Status = base.ConnNormal
		}()
		log.Info("not replid, need full sync")
		return redis.StrReply(fmt.Sprintf("FULLRESYNC %v %v", server.Replid, server.MasterReplOffset))
	}
	sInfo := server.GetInfo()
	return redis.StrReply(sInfo)
}

func LoadRDB(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	server.FlushDB()
	query := file.LoadRDB(conf.Conf.RDBName)
	//payload := redis.Payload{
	//	Query: nil,
	//	Err: nil,
	//}
	for i := range query {
		//payload.Query = query[i]
		//server.workChan <- payload
		tcp.Client.Send(redis.CmdReply(query[i]...))
		//Client.Recv()
	}
	//client.Close()
	return redis.OkReply
}
