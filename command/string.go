package command

import (
	"code/regis/base"
	"code/regis/database"
	"code/regis/lib/utils"
	"code/regis/redis"
)

func execSet(db *database.SingleDB, args []string) base.Reply {
	_ = db.PutData(args[1], args[2])
	return redis.OkReply
}

func execGet(db *database.SingleDB, args []string) base.Reply {
	val, ok := db.GetData(args[1])
	if !ok {
		return redis.NilReply
	}
	return redis.BulkReply(utils.InterfaceToBytes(val))
}

func execMSet(db *database.SingleDB, args []string) base.Reply {
	if len(args)%2 == 0 {
		return redis.ArgNumErrReply(args[0])
	}
	for i := 1; i+1 < len(args); i += 2 {
		_ = db.PutData(args[i], args[i+1])
	}
	return redis.OkReply
}
func execMGet(db *database.SingleDB, args []string) base.Reply {
	ret := make([]interface{}, 0, len(args)-1)
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
