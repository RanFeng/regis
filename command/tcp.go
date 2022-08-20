package command

import (
	"code/regis/base"
	"code/regis/conf"
	"code/regis/ds"
	log "code/regis/lib"
	"code/regis/lib/utils"
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

	err := tcp.SaveRDB()

	if err != nil {
		return redis.ErrReply("ERR in save")
	}
	return redis.OkReply
}

func BGSave(conn *tcp.RegisConn, args []string) base.Reply {
	if tcp.Server.DB.GetStatus() != base.WorldNormal {
		return redis.ErrReply("ERR can not save in bgsave")
	}
	//tcp.Server.DB.SetStatus(base.WorldFrozen)

	go func() { base.NeedSave <- 0 }()

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
	if strings.ToLower(args[1]) == "no" && strings.ToLower(args[2]) == "one" {
		tcp.UnsetMaster()
		return redis.OkReply
	}

	if strings.ToLower(args[1]) == "getack" {
		tcp.HeartBeatToMaster()
		return nil
	}

	// replicaof IP port
	_, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return redis.ErrReply("ERR value is not an integer or out of range")
	}

	addr := fmt.Sprintf("%v:%v", args[1], args[2])
	if tcp.Server.Master != nil && tcp.Server.Master.RemoteAddr() == addr {
		return redis.StrReply("OK Already connected to specified master")
	}
	tcp.UnsetMaster()
	tcp.Server.MasterAddr = addr
	tcp.Server.SlaveState = base.ReplStateConnect
	// 在这里还不直接去连接master，接下来要交给CronJob去连接
	// 因为使用CronJob就会自带retry属性了
	log.Notice("REPLICAOF %v enabled (user request from '%s')", addr, conn.RemoteAddr())
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
		offset, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return nil
		}
		if _, ok := tcp.Server.Slave[conn.ID]; !ok {
			return nil
		}
		conn.AckOffset = utils.IF(conn.AckOffset > offset, conn.AckOffset, offset).(int64)
		conn.LastBeat = time.Now()
		tcp.Server.ReplBacklogLastBeat = time.Now()
		//log.Info("conn state %v", conn.State)
		switch conn.State {
		case base.SlaveStateOnline:
			// 再发出从BGSave开始到现在的所有增量命令
			//bs, err := tcp.Server.ReplBacklog.Read(conn.AckOffset)
			//log.Debug("read back_log %v %v", utils.BytesViz(bs), err)
			if bs, err := tcp.Server.ReplBacklog.Read(conn.AckOffset); err == nil {
				_ = conn.Write(bs)
				conn.AckOffset = tcp.Server.MasterReplOffset
			}
		}
		return nil
	}
	return redis.OkReply
}

// PSync 自己是master，给slave进行同步
func PSync(conn *tcp.RegisConn, args []string) base.Reply {
	offset, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		// 此处不应该出错，试试直接全量同步
		goto fullSync
	}

	// 如果传过来是个 PSYNC id offset， 如果不是本机 Replid 也不是本机 Replid2，或者超出了本机 MasterReplOffset2，都得全量
	if args[1] != tcp.Server.Replid || (args[1] == tcp.Server.Replid2 && offset > tcp.Server.MasterReplOffset2) {
		goto fullSync
	}

	// 本机offset过多，被刷掉了，或者本机是个slave，只有部分的back log，不足以支持部分同步
	if tcp.Server.ReplBacklog == nil ||
		!tcp.Server.ReplBacklog.Active ||
		tcp.Server.ReplBacklog.Readable(offset) != nil {
		goto fullSync
	}

	// 好了，至此应该可以开始部分同步了
	conn.LastBeat = time.Now()
	conn.State = base.SlaveStateOnline
	tcp.Server.Slave[conn.ID] = conn

	// 回复失败，直接退出。
	err = conn.Write(redis.InlineSReply("CONTINUE", tcp.Server.Replid).Bytes())
	if err != nil {
		return nil
	}

	// 开始真正的部分同步
	if tcp.Server.ReplBacklog.Active {
		// 部分同步
		log.Info("try partial sync %v", offset-1)
		bs, _ := tcp.Server.ReplBacklog.Read(offset - 1)
		log.Info("I'm sending %v !!", utils.BytesViz(bs))
		_ = conn.Write(bs)
	}
	return nil

fullSync:
	// 全量同步
	log.Notice("Full resync requested by replica %v %v", conn.RemoteAddr(), offset)
	conn.LastBeat = time.Now()
	conn.State = base.SlaveStateNeedBGSave
	tcp.Server.Slave[conn.ID] = conn

	// 如果我们从一个独立的机器变成master，要做一些初始化
	if len(tcp.Server.Slave) == 1 {
		// 老id过期了，刷新Replid以及Replid2
		tcp.Server.Replid = utils.GetRandomHexChars(base.ConfigRunIDSize)
		tcp.Server.Replid2 = strings.Repeat("0", base.ConfigRunIDSize)

		tcp.Server.MasterReplOffset2 = -1

		if tcp.Server.ReplBacklog == nil {
			tcp.Server.ReplBacklog = ds.NewRingBuffer(conf.Conf.ReplBacklogSize)
			tcp.Server.ReplBacklog.Active = true
			tcp.Server.ReplBacklog.WritePtr = offset - 1 // redis这里是+1，regis这里直接置为offset - 1
		}
	}

	if tcp.Server.DB.GetStatus() == base.WorldFrozen && tcp.Server.LastBGSaveOffset >= 0 {
		// 1. 正在进行BGSave，如果slave中有已经 base.SlaveStateWaitBGSaveEnd 或以上 的，
		// 说明这次的BGSave是可信的，可以传输给slave，并且本次的BGSave生成的rdb的offset就是 tcp.Server.LastBGSaveOffset
		for k := range tcp.Server.Slave {
			if tcp.Server.Slave[k].State >= base.SlaveStateWaitBGSaveEnd {
				// 可信，传！
				conn.State = base.SlaveStateWaitBGSaveEnd
				tcp.Server.SlaveDBIndex = -1
				//_ = conn.Write(redis.InlineIReply("FULLRESYNC", tcp.Server.Replid, tcp.Server.LastBGSaveOffset).Bytes())
				return nil
			}
		}
		// TODO 没有可信的rdb，只能等下一次BGSave咯。
		log.Notice("Can't attach the replica to the current BGSAVE. Waiting for next BGSAVE for SYNC")
	} else {
		// 2. 不在进行BGSave，说明可以自己开始BGSave
		//log.Debug("start new BgSave")
		//tcp.Server.DB.SetStatus(base.WorldFrozen)
		//_ = conn.Write(redis.InlineIReply("FULLRESYNC", tcp.Server.Replid, tcp.Server.LastBGSaveOffset).Bytes())
		go func() {
			base.NeedSave <- 0
			conn.State = base.SlaveStateWaitBGSaveEnd
			tcp.Server.SlaveDBIndex = -1
		}()
	}

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
