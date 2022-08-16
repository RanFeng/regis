package command

import (
	"code/regis/base"
	"code/regis/conf"
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

func Ping(conn *tcp.RegisConn, args []string) base.Reply {
	if len(args) >= 3 {
		return redis.ArgNumErrReply(args[0])
	}
	if len(args) == 2 {
		return redis.StrReply(args[1])
	}
	return redis.StrReply("PONG")
}

func Select(conn *tcp.RegisConn, args []string) base.Reply {
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

func Save(conn *tcp.RegisConn, args []string) base.Reply {
	if tcp.Server.DB.GetStatus() != base.WorldNormal {
		return redis.ErrReply("ERR can not save in bgsave")
	}
	tcp.Server.DB.SetStatus(base.WorldStopped)
	defer tcp.Server.DB.SetStatus(base.WorldNormal)

	err := file.SaveRDB(tcp.Server.DB.SaveRDB)

	if err != nil {
		return redis.ErrReply("ERR in save")
	}
	return redis.OkReply
}

func BGSave(conn *tcp.RegisConn, args []string) base.Reply {
	if tcp.Server.DB.GetStatus() != base.WorldNormal {
		return redis.ErrReply("ERR can not save in bgsave")
	}
	tcp.Server.DB.SetStatus(base.WorldFrozen)

	go file.SaveRDB(tcp.Server.DB.SaveRDB) //nolint:errcheck

	return redis.BulkStrReply("Background saving started")
}

func Publish(conn *tcp.RegisConn, args []string) base.Reply {
	subs, ok := tcp.Server.PubsubDict[args[1]]
	if !ok {
		return redis.IntReply(0)
	}

	reply := redis.ArrayReply([]interface{}{
		_msg, args[1], args[2],
	})

	for k := range subs {
		subs[k].Reply(reply)
	}

	return redis.IntReply(len(subs))
}

func Subscribe(conn *tcp.RegisConn, args []string) base.Reply {
	ret := make([]interface{}, 0, len(args)*3)
	for i := 1; i < len(args); i++ {
		// 获取server的订阅dict
		subs, ok := tcp.Server.PubsubDict[args[i]]
		if !ok {
			subs = make(map[int64]*tcp.RegisConn, 16)
		}
		// 将conn加入server的订阅dict
		subs[conn.ID] = conn
		tcp.Server.PubsubDict[args[i]] = subs

		// conn自己更新自己的订阅dict
		conn.PubsubList[args[i]] = struct{}{}

		ret = append(ret, _sub, args[i], i-1)
	}

	return redis.ArrayReply(ret)
}

func UnSubscribe(conn *tcp.RegisConn, args []string) base.Reply {
	if len(args) == 1 {
		for key := range conn.PubsubList {
			args = append(args, key)
		}
	}
	ret := make([]interface{}, 0, len(args)*3)
	for i := range args[1:] {
		// 获取server的订阅dict
		if subs, ok := tcp.Server.PubsubDict[args[i]]; ok {
			// 将conn从server的订阅list中删除
			delete(subs, conn.ID)
		}

		// conn自己更新自己的订阅list，取消订阅该频道
		delete(conn.PubsubList, args[i])

		ret = append(ret, _unsub, args[i], i)
	}
	return redis.ArrayReply(ret)
}

// ReplicaOf 自己是slave，向master要同步
func ReplicaOf(conn *tcp.RegisConn, args []string) base.Reply {
	if args[1] == "no" && args[2] == "one" {
		if tcp.Server.Master != nil {
			tcp.Server.ReplBacklog.Active = false
			tcp.Server.Master.Close()
			tcp.Server.Master = nil
		}
		return redis.OkReply
	}
	_, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return redis.ErrReply("ERR value is not an integer or out of range")
	}

	addr := fmt.Sprintf("%v:%v", args[1], args[2])
	if tcp.Server.Master != nil { // 只要master在，就不允许继续同步
		return redis.ErrReply("ERR already slave")
	}
	cli, err := tcp.NewClient(addr)
	if err != nil {
		return redis.ErrReply("ERR conn fail, check input")
	}
	go cli.PSync()

	return redis.OkReply
}

func Lock(conn *tcp.RegisConn, args []string) base.Reply {
	tcp.Server.Lock.Lock()
	defer tcp.Server.Lock.Unlock()
	if len(tcp.Server.Monopolist) == 0 {
		tcp.Server.Monopolist = conn.RemoteAddr()
		return redis.OkReply
	}
	return redis.ErrReply(fmt.Sprintf("ERR monopolized by %v", tcp.Server.Monopolist))
}

func UnLock(conn *tcp.RegisConn, args []string) base.Reply {
	tcp.Server.Lock.Lock()
	defer tcp.Server.Lock.Unlock()
	if len(tcp.Server.Monopolist) == 0 || tcp.Server.Monopolist == conn.RemoteAddr() {
		tcp.Server.Monopolist = ""
		return redis.OkReply
	}

	return redis.ErrReply(fmt.Sprintf("ERR monopolized by %v", tcp.Server.Monopolist))
}

func Info(conn *tcp.RegisConn, args []string) base.Reply {
	sInfo := tcp.Server.GetInfo()
	return redis.StrReply(sInfo)
}

func FlushALl(conn *tcp.RegisConn, args []string) base.Reply {
	tcp.Server.DB.Flush()
	return redis.OkReply
}

func ReplConf(conn *tcp.RegisConn, args []string) base.Reply {
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
func PSync(conn *tcp.RegisConn, args []string) base.Reply {
	offset, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return redis.IntErrReply
	}
	offset -= 1

	if args[1] == tcp.Server.Replid &&
		(args[1] == tcp.Server.Replid2 && offset <= tcp.Server.MasterReplOffset2) &&
		tcp.Server.ReplBacklog.Active {
		log.Info("try partial sync %v", offset)
		bs, err := tcp.Server.ReplBacklog.Read(offset)
		if err == nil {
			log.Info("I'm sending %v !!", utils.BytesViz(bs))
			_ = conn.Write(redis.InlineSReply("CONTINUE").Bytes())
			_ = conn.Write(bs)
			tcp.Server.Slave[conn.ID] = conn
			return nil
		}
	}

	log.Notice("Full resync requested by replica %v", conn.RemoteAddr())
	if tcp.Server.DB.GetStatus() != base.WorldNormal {
		return redis.ErrReply("ERR can not save in bgsave")
	}
	tcp.Server.DB.SetStatus(base.WorldFrozen)

	tcp.Server.ReplBacklog.Active = true

	_ = conn.Write(redis.InlineIReply("FULLRESYNC", tcp.Server.Replid, atomic.LoadInt64(&tcp.Server.MasterReplOffset)).Bytes())

	go func() {
		log.Notice("Starting BGSAVE for SYNC with target: disk")
		offsetBak := atomic.LoadInt64(&tcp.Server.MasterReplOffset)
		//conn.State = base.ConnSync
		//go func() {
		//	// 防止在传输rdb过程中，长时间不给slave发心跳包，导致slave以为master挂了
		//	for conn.Status == base.ConnSync {
		//		err := conn.Write([]byte{'\n'})
		//		if err != nil {
		//			return
		//		}
		//		time.Sleep(time.Duration(tcp.Server.ReplPingSlavePeriod) * time.Second)
		//	}
		//	//log.Info("go routine close safety")
		//}()
		_ = file.SaveRDB(tcp.Server.DB.SaveRDB)
		file.SendRDB(conf.Conf.RDBName, conn.Conn)
		tcp.Server.Slave[conn.ID] = conn
		//conn.State = base.ConnNormal
		// 对增量进行同步
		if bs, err := tcp.Server.ReplBacklog.Read(offsetBak); err == nil {
			_, _ = conn.Conn.Write(bs)
		}
	}()
	return nil
}

func Debug(conn *tcp.RegisConn, args []string) base.Reply {
	sub := strings.ToLower(args[1])
	switch sub {
	case "reload":
		go func() {
			Save(nil, nil)
			tcp.Server.DB.Flush()
			tcp.Server.LoadRDB(conf.Conf.RDBName)
		}()
		return redis.OkReply
	case "object":
		if len(args) < 3 {
			return redis.ArgNumErrReply(args[0])
		}
		v, ok := tcp.Server.DB.GetSDB(conn.DBIndex).GetData(args[2])
		if !ok {
			return redis.ErrReply("ERR no such key")
		}
		return redis.StrReply(fmt.Sprintf("Value at:%p %v", &v, v))
	case "db":
		ret := []interface{}{fmt.Sprintf("mdb have %v SDBs, now is %v", tcp.Server.DB.GetSpaceNum(), tcp.Server.DB.GetStatus())}
		for i := 0; i < tcp.Server.DB.GetSpaceNum(); i++ {
			sdb := tcp.Server.DB.GetSDB(i)
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
			_ = tcp.Server.DB.GetSDB(conn.DBIndex).PutData(key, base.RString(val))
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
		if !tcp.Server.ReplBacklog.Active {
			return redis.NilReply
		}
		num, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return redis.ErrReply("ERR value is out of range, must be positive")
		}
		val, err := tcp.Server.ReplBacklog.Read(num)
		if err != nil {
			return redis.ErrReply(err.Error())
		}
		return redis.StrReply(utils.BytesViz(val))
	case "who":
		ret := []interface{}{}
		ch := make(chan struct{})
		defer close(ch)
		for _, val := range tcp.Server.Who {
			_, ok := tcp.Server.Slave[val.ID]
			info := fmt.Sprintf("id: %v, addr: %v, db_index: %v, is_slave: %v, last_at: %v",
				val.ID, val.RemoteAddr(), val.DBIndex, ok, val.LastBeat)
			ret = append(ret, info)
		}
		if tcp.Server.Master != nil {
			info := fmt.Sprintf("master: id: %v, addr: %v, last_at: %v",
				tcp.Server.Master.ID, tcp.Server.Master.RemoteAddr(), tcp.Server.Master.LastBeat)
			ret = append(ret, info)
		}
		return redis.InterfacesReply(ret)
	case "slave":
		ret := []interface{}{}
		for _, val := range tcp.Server.Slave {
			info := fmt.Sprintf("id: %v, db_index: %v status：%v, addr: %v",
				val.ID, val.DBIndex, val.State, val.RemoteAddr())
			ret = append(ret, info)
		}
		return redis.InterfacesReply(ret)
	case "close":
		if len(args) < 3 {
			return redis.ArgNumErrReply(args[0])
		}
		port, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return redis.ErrReply("ERR value is out of range, must be positive")
		}
		tcp.Server.CloseConn(port)
		return redis.OkReply
	}
	return redis.ErrReply(fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for '%v'. Try DEBUG HELP.", args[1]))
}
