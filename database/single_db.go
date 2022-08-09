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

type core struct {
	// 实际存储的数据，key -> Data
	data *ds.Dict
	// key的过期时间，key -> time.Time
	expire *ds.Dict
}

type SingleDB struct {
	// status
	// 初始时， status = base.WorldNormal
	// 当调用BGSave时， status = base.WorldFrozen
	// BGSave结束时， status = base.WorldMoving
	// bgDB 里的数据完全转移到 db 中时， status = base.WorldNormal
	status base.WorldStatus
	// db 是 SingleDB 的主数据库
	db *core
	// status = base.WorldFrozen 时，读命令优先 bgDB，miss后再读 db，写命令直接写入 bgDB，
	// status == base.WorldMoving 时，如果 bgDB 里有值，就蚂蚁搬家式地写入 db 中，在这段期间，不允许 BGSave
	//bgDB base.DB
	bgDB *core
}

func (sdb *SingleDB) GetDB() *core {
	return sdb.db
}

func (sdb *SingleDB) SetStatus(status base.WorldStatus) {
	sdb.status = status
}

func (sdb *SingleDB) GetStatus() base.WorldStatus {
	return sdb.status
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
		for kv := range sdb.GetDB().data.RangeKV(ch) {
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
	return sdb.GetDB().data.Put(key, val)
}

func (sdb *SingleDB) GetData(key string) (interface{}, bool) {
	return sdb.GetDB().data.Get(key)
}

func (sdb *SingleDB) Size() int {
	return sdb.GetDB().data.Len()
}

func (sdb *SingleDB) TTLSize() int {
	return sdb.GetDB().expire.Len()
}

func newSDB() *SingleDB {
	db := &core{
		data:   ds.NewDict(dataDictSize),
		expire: ds.NewDict(expireDictSize),
	}
	bgDB := &core{
		data:   ds.NewDict(dataDictSize >> 10),
		expire: ds.NewDict(expireDictSize >> 10),
	}
	sdb := &SingleDB{
		status: base.WorldNormal,
		db:     db,
		bgDB:   bgDB,
	}
	return sdb
}
