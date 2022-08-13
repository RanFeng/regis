package tcp

import (
	"code/regis/base"
	"code/regis/conf"
	"code/regis/ds"
	"code/regis/file"
	log "code/regis/lib"
	"code/regis/lib/utils"
	"code/regis/redis"
	"fmt"
	"strconv"
	"strings"
)

const (
	_sub   = "subscribe"
	_msg   = "message"
	_unsub = "unsubscribe"
)

type ServerRealFunc func(s *RegisServer, c base.Conn, args []string) base.Reply

func GetServerReal(exec interface{}) ServerRealFunc {
	return exec.(func(s *RegisServer, c base.Conn, args []string) base.Reply)
}

func Ping(server *RegisServer, conn base.Conn, args []string) base.Reply {
	if len(args) >= 3 {
		return redis.ArgNumErrReply(args[0])
	}
	if len(args) == 2 {
		return redis.StrReply(args[1])
	}
	return redis.StrReply("PONG")
}

func Select(server *RegisServer, conn base.Conn, args []string) base.Reply {
	i, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return redis.ErrReply("ERR value is not an integer or out of range")
	}
	index := int(i)
	if index >= conf.Conf.Databases || index < 0 {
		return redis.ErrReply("ERR DB index is out of range")
	}
	conn.SetDBIndex(index)
	return redis.OkReply
}

func Save(server *RegisServer, conn base.Conn, args []string) base.Reply {
	if server.GetDB().GetStatus() != base.WorldNormal {
		return redis.ErrReply("ERR can not save in bgsave")
	}
	server.GetDB().SetStatus(base.WorldStopped)
	defer server.GetDB().SetStatus(base.WorldNormal)

	ch := make(chan struct{})
	defer close(ch)

	err := file.SaveRDB(server.GetDB().SaveRDB)
	if err != nil {
		return redis.ErrReply("ERR in save")
	}
	return redis.OkReply
}

func BGSave(server *RegisServer, conn base.Conn, args []string) base.Reply {
	if server.GetDB().GetStatus() != base.WorldNormal {
		return redis.ErrReply("ERR can not save in bgsave")
	}
	server.GetDB().SetStatus(base.WorldFrozen)

	go file.SaveRDB(server.GetDB().SaveRDB) //nolint:errcheck

	return redis.BulkStrReply("Background saving started")
}

func Publish(server *RegisServer, conn base.Conn, args []string) base.Reply {
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
		item.(base.Conn).Reply(reply)
	}

	return redis.IntReply(int(list.Len()))
}

func Subscribe(server *RegisServer, conn base.Conn, args []string) base.Reply {
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
		conn.NSubChannel().PushTail(args[i])

		ret = append(ret, _sub, args[i], i-1)
	}

	return redis.ArrayReply(ret)
}

func UnSubscribe(server *RegisServer, conn base.Conn, args []string) base.Reply {
	dict := server.GetPubSub()
	if len(args) == 1 {
		list := conn.NSubChannel()
		ch := make(chan struct{})
		for val := range list.Range(ch) {
			args = append(args, val.(string))
		}
	}
	ret := make([]interface{}, 0, len(args)*3)
	for i := 1; i < len(args); i++ {
		// 获取server的订阅dict
		if val, ok := dict.Get(args[i]); ok {
			// 将conn从server的订阅list中删除
			val.(base.LList).RemoveFirst(func(c interface{}) bool {
				return conn.GetID() == c.(base.Conn).GetID()
			})
		}

		// conn自己更新自己的订阅list，取消订阅该频道
		conn.NSubChannel().RemoveFirst(func(s interface{}) bool {
			return args[i] == s.(string)
		})

		ret = append(ret, _unsub, args[i], i-1)
	}
	return redis.ArrayReply(ret)
}

func ReplicaOf(server *RegisServer, conn base.Conn, args []string) base.Reply {
	_, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return redis.ErrReply("ERR value is not an integer or out of range")
	}

	server.master = MustNewClient(fmt.Sprintf("%v:%v", args[1], args[2]), server)
	go server.master.Handler()

	return redis.OkReply
}

func Info(server *RegisServer, conn base.Conn, args []string) base.Reply {
	sInfo := server.GetInfo()
	return redis.StrReply(sInfo)
}

func ReplConf(server *RegisServer, conn base.Conn, args []string) base.Reply {
	subCmd := strings.ToLower(args[1])
	switch subCmd {
	case "listening-port":
		ip, _ := utils.ParseAddr(conn.RemoteAddr())
		cli := MustNewClient(fmt.Sprintf("%v:%v", ip, args[2]), server)
		server.slave.Put(conn.RemoteAddr(), cli)
	case "capa":
		return redis.OkReply
	case "ack":
		return nil
	}
	return redis.OkReply
}

func PSync(server *RegisServer, conn base.Conn, args []string) base.Reply {
	if args[1] != server.replid {
		// todo begin bgsave
		go func() {
			file.SaveRDB(server.GetDB().SaveRDB)
			file.SendRDB("dump.rdb", conn.GetConn())
		}()
		log.Info("not replid, need full sync")
		return redis.StrReply(fmt.Sprintf("FULLRESYNC %v %v", server.replid, server.masterReplOffset))
	}
	sInfo := server.GetInfo()
	return redis.StrReply(sInfo)
}

func LoadRDB(server *RegisServer, conn base.Conn, args []string) base.Reply {
	server.FlushDB()
	query := file.LoadRDB(conf.Conf.RDBName)
	//payload := redis.Payload{
	//	Query: nil,
	//	Err: nil,
	//}
	for i := range query {
		//payload.Query = query[i]
		//server.workChan <- payload
		Client.Send(redis.CmdReply(query[i]...))
		//Client.Recv()
	}
	//client.Close()
	return redis.OkReply
}
