package command

import (
	"code/regis/base"
	"code/regis/ds"
	"code/regis/lib/utils"
	"code/regis/redis"
	"code/regis/tcp"
	"strconv"
)

// base.RString 操作

func Set(s *tcp.RegisServer, c *tcp.RegisConn, args []string) base.Reply {
	_ = s.DB.GetSDB(c.DBIndex).PutData(args[1], base.RString(args[2]))
	return redis.OkReply
}

func Get(s *tcp.RegisServer, c *tcp.RegisConn, args []string) base.Reply {
	v, ok := s.DB.GetSDB(c.DBIndex).GetData(args[1])
	if !ok {
		return redis.NilReply
	}
	val, ok := v.(base.RString)
	if !ok {
		return redis.TypeErrReply
	}
	return redis.BulkReply(utils.InterfaceToBytes(val))
}

func MSet(s *tcp.RegisServer, c *tcp.RegisConn, args []string) base.Reply {
	if len(args)%2 == 0 {
		return redis.ArgNumErrReply(args[0])
	}
	db := s.DB.GetSDB(c.DBIndex)
	for i := 1; i+1 < len(args); i += 2 {
		_ = db.PutData(args[i], base.RString(args[i+1]))
	}
	return redis.OkReply
}
func MGet(s *tcp.RegisServer, c *tcp.RegisConn, args []string) base.Reply {
	ret := make([]interface{}, 0, len(args)-1)
	db := s.DB.GetSDB(c.DBIndex)
	for i := 1; i < len(args); i++ {
		val, ok := db.GetData(args[i])
		if !ok {
			ret = append(ret, nil)
		} else {
			ret = append(ret, val)
		}
	}
	return redis.ArrayReply(ret)
}
func Del(s *tcp.RegisServer, c *tcp.RegisConn, args []string) base.Reply {
	ret := s.DB.GetSDB(c.DBIndex).RemoveData(args[1:]...)
	return redis.IntReply(ret)
}

func DBSize(s *tcp.RegisServer, c *tcp.RegisConn, args []string) base.Reply {
	return redis.IntReply(s.DB.GetSDB(c.DBIndex).Size())
}

// base.RList 操作

func LPush(s *tcp.RegisServer, c *tcp.RegisConn, args []string) base.Reply {
	db := s.DB.GetSDB(c.DBIndex)
	v, ok := db.GetData(args[1])
	if !ok {
		v = ds.NewRList()
	}
	val, ok := v.(base.RList)
	if !ok {
		return redis.TypeErrReply
	}
	for i := 2; i < len(args); i++ {
		val.PushHead(args[i])
	}
	db.PutData(args[1], val)
	return redis.Int64Reply(val.Len())
}

func RPush(s *tcp.RegisServer, c *tcp.RegisConn, args []string) base.Reply {
	db := s.DB.GetSDB(c.DBIndex)
	v, ok := db.GetData(args[1])
	if !ok {
		db.PutData(args[1], ds.NewRList(args[2:]...))
		return redis.IntReply(len(args) - 2)
	}
	val, ok := v.(base.RList)
	if !ok {
		return redis.TypeErrReply
	}
	for i := 2; i < len(args); i++ {
		val.PushTail(args[i])
	}
	db.PutData(args[1], val)
	return redis.Int64Reply(val.Len())
}

func LPushX(s *tcp.RegisServer, c *tcp.RegisConn, args []string) base.Reply {
	db := s.DB.GetSDB(c.DBIndex)
	v, ok := db.GetData(args[1])
	if !ok {
		return redis.StrReply("(empty array)")
	}
	val, ok := v.(base.RList)
	if !ok {
		return redis.TypeErrReply
	}
	for i := 2; i < len(args); i++ {
		val.PushHead(args[i])
	}
	db.PutData(args[1], val)
	return redis.Int64Reply(val.Len())
}

func RPushX(s *tcp.RegisServer, c *tcp.RegisConn, args []string) base.Reply {
	db := s.DB.GetSDB(c.DBIndex)
	v, ok := db.GetData(args[1])
	if !ok {
		return redis.StrReply("(empty array)")
	}
	val, ok := v.(base.RList)
	if !ok {
		return redis.TypeErrReply
	}
	for i := 2; i < len(args); i++ {
		val.PushTail(args[i])
	}
	db.PutData(args[1], val)
	return redis.Int64Reply(val.Len())
}

func LRange(s *tcp.RegisServer, c *tcp.RegisConn, args []string) base.Reply {
	db := s.DB.GetSDB(c.DBIndex)
	v, ok := db.GetData(args[1])
	if !ok {
		redis.StrReply("(empty array)")
	}
	val, ok := v.(base.RList)
	if !ok {
		return redis.TypeErrReply
	}
	start, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return redis.IntErrReply
	}
	end, err := strconv.ParseInt(args[3], 10, 64)
	if err != nil {
		return redis.IntErrReply
	}
	ret := val.LRange(start, end)
	return redis.InterfacesReply(ret)
}
