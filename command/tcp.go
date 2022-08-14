package command

import (
	"code/regis/base"
	"code/regis/conf"
	"code/regis/ds"
	"code/regis/file"
	log "code/regis/lib"
	"code/regis/lib/utils"
	"code/regis/redis"
	"code/regis/tcp"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
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

// ReplicaOf 自己是slave，向master要同步
func ReplicaOf(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	if args[1] == "no" && args[2] == "one" {
		if server.Master != nil {
			server.Role = tcp.RoleMaster
			server.Master.Close()
			server.Master = nil
		}
		return redis.OkReply
	}
	_, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return redis.ErrReply("ERR value is not an integer or out of range")
	}

	addr := fmt.Sprintf("%v:%v", args[1], args[2])
	if server.Master != nil { // 只要master在，就不允许继续同步
		return redis.ErrReply("ERR already slave")
	}
	cli, err := tcp.NewClient(addr, server)
	if err != nil {
		return redis.ErrReply("ERR conn fail, check input")
	}
	go cli.PSync()

	return redis.OkReply
}

func Lock(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	server.Lock.Lock()
	defer server.Lock.Unlock()
	if len(server.Monopolist) == 0 {
		server.Monopolist = conn.RemoteAddr()
		return redis.OkReply
	}
	return redis.ErrReply(fmt.Sprintf("ERR monopolized by %v", server.Monopolist))
}

func UnLock(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	server.Lock.Lock()
	defer server.Lock.Unlock()
	if len(server.Monopolist) == 0 || server.Monopolist == conn.RemoteAddr() {
		server.Monopolist = ""
		return redis.OkReply
	}

	return redis.ErrReply(fmt.Sprintf("ERR monopolized by %v", server.Monopolist))
}

func Info(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	sInfo := server.GetInfo()
	return redis.StrReply(sInfo)
}

func FlushALl(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	server.DB.Flush()
	return redis.OkReply
}

func ReplConf(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	subCmd := strings.ToLower(args[1])
	switch subCmd {
	case "listening-port":
		log.Notice("Replica %v asks for synchronization", conn.RemoteAddr())
		return redis.OkReply
	case "capa":
		return redis.OkReply
	case "ack":
		return nil
	}
	return redis.OkReply
}

// PSync 自己是master，给slave进行同步
func PSync(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	offset, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return redis.IntErrReply
	}
	offset -= 1

	if args[1] == server.Replid && server.ReplBacklog.IsActive() {
		log.Info("try partial sync")
		bs, err := server.ReplBacklog.Read(offset)
		if err == nil {
			_ = conn.Write(redis.InlineSReply("CONTINUE").Bytes())
			_ = conn.Write(bs)
			server.Slave.Put(conn.ID, conn)
			return nil
		}
	}

	log.Notice("Full resync requested by replica %v", conn.RemoteAddr())
	if server.DB.GetStatus() != base.WorldNormal {
		return redis.ErrReply("ERR can not save in bgsave")
	}
	server.DB.SetStatus(base.WorldFrozen)

	server.ReplBacklog.SetStatus(true)

	_ = conn.Write(redis.InlineIReply("FULLRESYNC", server.Replid, atomic.LoadInt64(&server.MasterReplOffset)).Bytes())

	go func() {
		log.Notice("Starting BGSAVE for SYNC with target: disk")
		offsetBak := atomic.LoadInt64(&server.MasterReplOffset)
		conn.Status = base.ConnSync
		//go func() {
		//	// 防止在传输rdb过程中，长时间不给slave发心跳包，导致slave以为master挂了
		//	for conn.Status == base.ConnSync {
		//		err := conn.Write([]byte{'\n'})
		//		if err != nil {
		//			return
		//		}
		//		time.Sleep(time.Duration(server.ReplPingSlavePeriod) * time.Second)
		//	}
		//	//log.Info("go routine close safety")
		//}()
		_ = file.SaveRDB(server.DB.SaveRDB)
		file.SendRDB(conf.Conf.RDBName, conn.Conn)
		server.Slave.Put(conn.ID, conn)
		conn.Status = base.ConnNormal
		// 对增量进行同步
		if bs, err := server.ReplBacklog.Read(offsetBak); err == nil {
			_, _ = conn.Conn.Write(bs)
		}
	}()
	return nil
}

func Debug(server *tcp.RegisServer, conn *tcp.RegisConn, args []string) base.Reply {
	sub := strings.ToLower(args[1])
	switch sub {
	case "reload":
		Save(server, nil, nil)
		server.DB.Flush()
		server.LoadRDB(conf.Conf.RDBName)
		return redis.OkReply
	case "object":
		if len(args) < 3 {
			return redis.ArgNumErrReply(args[0])
		}
		v, ok := server.DB.GetSDB(conn.DBIndex).GetData(args[2])
		if !ok {
			return redis.ErrReply("ERR no such key")
		}
		return redis.StrReply(fmt.Sprintf("Value at:%p %v", &v, v))
	case "db":
		ret := []interface{}{fmt.Sprintf("mdb have %v SDBs, now is %v", server.DB.GetSpaceNum(), server.DB.GetStatus())}
		for i := 0; i < server.DB.GetSpaceNum(); i++ {
			sdb := server.DB.GetSDB(i)
			ret = append(ret, fmt.Sprintf("sdb %2d is %v have %v keys, %v is bgsave",
				i, sdb.GetStatus(), sdb.Size(), sdb.ShadowSize()))
		}
		return redis.ArrayReply(ret)
	case "populate":
		if len(args) < 3 {
			return redis.ArgNumErrReply(args[0])
		}
		num, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return redis.ErrReply("ERR value is out of range, must be positive")
		}
		var i int64
		for i = 0; i < num; i++ {
			key := fmt.Sprintf("key:%d", i)
			val := fmt.Sprintf("value:%d", i)
			_ = server.DB.GetSDB(conn.DBIndex).PutData(key, base.RString(val))
		}
		return redis.OkReply
	case "sleep":
		if len(args) < 3 {
			return redis.ArgNumErrReply(args[0])
		}
		num, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return redis.ErrReply("ERR value is out of range, must be positive")
		}
		time.Sleep(time.Duration(num) * time.Second)
		return redis.OkReply
	case "error":
		if len(args) < 3 {
			return redis.ArgNumErrReply(args[0])
		}
		return redis.ErrReply(args[2])
	case "buffer":
		if len(args) < 3 {
			return redis.ArgNumErrReply(args[0])
		}
		if server.ReplBacklog.IsActive() {
			return redis.NilReply
		}
		num, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return redis.ErrReply("ERR value is out of range, must be positive")
		}
		val, err := server.ReplBacklog.Read(num)
		if err != nil {
			return redis.ErrReply(err.Error())
		}
		return redis.StrReply(utils.BytesViz(val))
	case "who":
		ret := []interface{}{}
		ch := make(chan struct{})
		defer close(ch)
		for kv := range server.Who.RangeKV(ch) {
			val := kv.Val.(*tcp.RegisConn)
			_, ok := server.Slave.Get(val.ID)
			info := fmt.Sprintf("id: %v, addr: %v, db_index: %v, is_slave: %v, last_at: %v",
				val.ID, val.RemoteAddr(), val.DBIndex, ok, val.LastBeat)
			ret = append(ret, info)
		}
		if server.Master != nil {
			info := fmt.Sprintf("master: id: %v, addr: %v, last_at: %v",
				server.Master.ID, server.Master.RemoteAddr(), server.Master.LastBeat)
			ret = append(ret, info)
		}
		return redis.InterfacesReply(ret)
	case "slave":
		ret := []interface{}{}
		ch := make(chan struct{})
		defer close(ch)
		for v := range server.Slave.RangeKV(ch) {
			val := v.Val.(*tcp.RegisConn)
			info := fmt.Sprintf("id: %v, db_index: %v status：%v, addr: %v",
				val.ID, val.DBIndex, val.Status, val.RemoteAddr())
			ret = append(ret, info)
		}
		return redis.InterfacesReply(ret)
	case "close":
		if len(args) < 3 {
			return redis.ArgNumErrReply(args[0])
		}
		server.CloseConn(args[2])
		return redis.OkReply
	}
	return redis.ErrReply(fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for '%v'. Try DEBUG HELP.", args[1]))
}
