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

type ReqType int // 获取 SingleDB 的 core 的请求类型

const (
	ReqWrite     ReqType = iota // 写请求
	ReqFirstRead                // 首次读请求
	ReqMissRead                 // 首次读miss，后续的读请求
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

	// 当 status = base.WorldFrozen, base.WorldMoving 时，这时候直接计算 db 的键的数量作为 SingleDB 的键数是有偏差的
	// 因为在这期间，很可能有一部分新的key被写入 bgDB 中了，对于这部分新增的数据，称之为luck Key，对他们的计数称为luckCount
	// 也有一部分luck Key在这期间被删除了，对于这部分以负数计入luckCount中
	// 对于更改的Key，不算luck Key
	luckCount int
}

// GetDB 获取 SingleDB 各种状态下应该读写哪个core
// 分为三种可能：
// 写请求：  	mode == 0
// 第一次读： mode == 1
// 第二次读： mode == 2
// 第二次读是指第一次读的时候miss了，所以需要有第二次读
func (sdb *SingleDB) GetDB(mode ReqType) *core {
	switch sdb.status {
	case base.WorldNormal:
		return sdb.db
	case base.WorldFrozen:
		if mode == 0 || mode == 1 {
			return sdb.bgDB
		} else if mode == 2 {
			return sdb.db
		}
	case base.WorldMoving:
		if mode == 1 {
			return sdb.bgDB
		} else if mode == 0 || mode == 2 {
			return sdb.db
		}
	case base.WorldStopped:
		return sdb.db
	}
	return sdb.db
}

func (sdb *SingleDB) GetOri() *core {
	return sdb.db
}

func (sdb *SingleDB) GetBak() *core {
	return sdb.bgDB
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
		for kv := range sdb.db.data.RangeKV(ch) {
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
	switch sdb.status {
	case base.WorldNormal:
		return sdb.db.data.Put(key, val)
	case base.WorldFrozen:
		_, exists := sdb.db.data.Get(key)
		change := sdb.bgDB.data.Put(key, val)
		if !exists {
			// TODO 此处可以计算size
			return change
		}
		return 0
	case base.WorldMoving:
		_, exists := sdb.bgDB.data.Get(key)
		change := sdb.db.data.Put(key, val)
		if !exists {
			// TODO 此处可以计算size
			return change
		}
		sdb.bgDB.data.Del(key)
		return 0
	case base.WorldStopped:
		return sdb.bgDB.data.Put(key, val)
	}
	return sdb.db.data.Put(key, val)
}

func (sdb *SingleDB) GetData(key string) (interface{}, bool) {
	switch sdb.status {
	case base.WorldNormal:
		return sdb.db.data.Get(key)
	case base.WorldFrozen:
		val, exists := sdb.bgDB.data.Get(key)
		if !exists {
			return sdb.db.data.Get(key)
		}
		return val, exists
	case base.WorldMoving:
		val, exists := sdb.bgDB.data.Get(key)
		if !exists {
			return sdb.db.data.Get(key)
		}
		sdb.db.data.Put(key, val)
		sdb.bgDB.data.Del(key)
	case base.WorldStopped:
		return sdb.db.data.Get(key)
	}
	return sdb.db.data.Get(key)
}

func (sdb *SingleDB) Size() int {
	// TODO 在 bgsave期间，对bgDB写了新的key，这时候数据不准确
	return sdb.GetDB(ReqFirstRead).data.Len()
}

func (sdb *SingleDB) TTLSize() int {
	// TODO 在 bgsave期间，对bgDB写了新的key，这时候数据不准确
	return sdb.GetDB(ReqFirstRead).expire.Len()
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
