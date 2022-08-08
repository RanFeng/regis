package database

import (
	"code/regis/base"
	"code/regis/ds"
	"code/regis/redis"
)

const (
	dataDictSize   = 1 << 16
	expireDictSize = 1 << 10
)

type SDBRealFunc func(db *SingleDB, args []string) base.Reply

func GetSDBReal(exec interface{}) SDBRealFunc {
	return exec.(func(db *SingleDB, args []string) base.Reply)
}

type SingleDB struct {
	index int
	// 实际存储的数据，key -> Data
	data *ds.Dict
	// key的过期时间，key -> time.Time
	expire *ds.Dict
}

func (sdb *SingleDB) Exec(cmd *base.Command) base.Reply {
	args := cmd.Query
	handler, _ := base.GetCmdInfo(args[0])
	if !handler.Level(base.CmdLevelSDB) {
		return redis.NilReply
	}
	return GetSDBReal(handler.GetExec())(sdb, args)
}

func (sdb *SingleDB) RangeKV(ch <-chan struct{}) chan base.DBKV {
	kvs := make(chan base.DBKV)
	go func() {
		defer func() {
			close(kvs)
		}()
		for kv := range sdb.data.RangeKV(ch) {
			select {
			case <-ch:
				return
			case kvs <- base.DBKV{DictKV: kv}:
			}
		}
	}()
	return kvs
}

func (sdb *SingleDB) PutData(key string, val interface{}) int {
	return sdb.data.Put(key, val)
}

func (sdb *SingleDB) GetData(key string) (interface{}, bool) {
	return sdb.data.Get(key)
}

func newSDB() *SingleDB {
	sdb := &SingleDB{
		data:   ds.NewDict(dataDictSize),
		expire: ds.NewDict(expireDictSize),
	}
	return sdb
}
