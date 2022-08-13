package database

import (
	"code/regis/base"
	"code/regis/ds"
	log "code/regis/lib"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/encoder"
)

const (
	dataDictSize   = 1 << 16
	expireDictSize = 1 << 10
)

type carrier struct {
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
	db *carrier
	// status = base.WorldFrozen 时，读命令优先 bgDB，miss后再读 db，写命令直接写入 bgDB，
	// status == base.WorldMoving 时，如果 bgDB 里有值，就蚂蚁搬家式地写入 db 中，在这段期间，不允许 BGSave
	//bgDB base.DB
	bgDB *carrier

	// 当 status = base.WorldFrozen, base.WorldMoving 时，这时候直接计算 db 的键的数量作为 SingleDB 的键数是有偏差的
	// 因为在这期间，很可能有一部分新的key被写入 bgDB 中了，对于这部分新增的数据，称之为luck Key，对他们的计数称为luckCount
	// 也有一部分luck Key在这期间被删除了，对于这部分以负数计入luckCount中
	// 对于更改的Key，不算luck Key
	luckCount int
}

func (sdb *SingleDB) SetStatus(status base.WorldStatus) {
	sdb.status = status
}

func (sdb *SingleDB) GetStatus() base.WorldStatus {
	return sdb.status
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

func (sdb *SingleDB) SaveRDB(rdb *core.Encoder) error {
	sdb.SetStatus(base.WorldFrozen)
	ch := make(chan struct{})
	var err error
	defer func() {
		close(ch)
		sdb.SetStatus(base.WorldMoving)
		if err != nil {
			log.Error("save to rdb error! %v", err)
		}
	}()
	for kv := range sdb.RangeKV(ch) {
		log.Info("write kvs %v %T", kv, kv.Val)
		//time.Sleep(1 * time.Second)
		var ttlOp interface{}
		if kv.TTL > 0 {
			ttlOp = encoder.WithTTL(uint64(time.Now().Add(time.Duration(kv.TTL*int64(time.Millisecond))).Unix() * 1000))
		}
		switch v := kv.Val.(type) {
		case base.RString:
			err = rdb.WriteStringObject(kv.Key, []byte(v), ttlOp)
		case base.RList:
			ret := make([][]byte, 0, v.Len())
			for k := range v.Range(ch) {
				ret = append(ret, k.([]byte))
			}
			err = rdb.WriteListObject(kv.Key, ret, ttlOp)
		case base.RHash:
			ret := make(map[string][]byte, v.Len())
			for hkv := range v.RangeKV(ch) {
				ret[hkv.Key] = hkv.Val.([]byte)
			}
			err = rdb.WriteHashMapObject(kv.Key, ret, ttlOp)
		case base.RSet:
			// TODO
			//	err = rdb.WriteSetObject("set", [][]byte{
			//		[]byte("123"),
			//		[]byte("abc"),
			//		[]byte("la la la"),
			//	})
		case base.RZSet:
			// TODO
			//err = rdb.WriteZSetObject("list2", []*model.ZSetEntry{
			//	{
			//		Score:  1.234,
			//		Member: "a",
			//	},
			//	{
			//		Score:  2.71828,
			//		Member: "b",
			//	},
			//})
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (sdb *SingleDB) PutData(key string, val interface{}) int {
	luck := 0
	switch sdb.status {
	case base.WorldNormal:
		return sdb.db.data.Put(key, val)
	case base.WorldFrozen:
		_, exists1 := sdb.db.data.Get(key)
		v, exists2 := sdb.bgDB.data.Get(key)
		_, valNull := v.(base.Null)
		if (!exists1 && !exists2) || (exists2 && valNull) {
			luck = 1
			sdb.luckCount++
		}
		sdb.bgDB.data.Put(key, val)
		return luck
	case base.WorldMoving:
		_, exists1 := sdb.db.data.Get(key)
		v, exists2 := sdb.bgDB.data.Get(key)
		_, valNull := v.(base.Null)
		if (!exists1 && !exists2) || (exists2 && valNull) {
			luck = 1
			sdb.luckCount++
		}
		sdb.db.data.Put(key, val)
		sdb.bgDB.data.Del(key)
		return luck
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
		w, exists1 := sdb.db.data.Get(key)
		v, exists2 := sdb.bgDB.data.Get(key)
		_, valNull := v.(base.Null)
		if exists1 && !exists2 {
			return w, true
		} else if exists2 && !valNull {
			return v, true
		} else {
			return nil, false
		}
	case base.WorldMoving:
		w, exists1 := sdb.db.data.Get(key)
		v, exists2 := sdb.bgDB.data.Get(key)
		_, valNull := v.(base.Null)
		sdb.bgDB.data.Del(key)
		if exists1 && !exists2 {
			return w, true
		} else if exists2 && !valNull {
			sdb.db.data.Put(key, v)
			return v, true
		} else {
			return nil, false
		}
	case base.WorldStopped:
		return sdb.db.data.Get(key)
	}
	return sdb.db.data.Get(key)
}

// RemoveData 删除指定keys的值，返回更改的数量
func (sdb *SingleDB) RemoveData(keys ...string) int {
	luck := 0
	for _, key := range keys {
		switch sdb.status {
		case base.WorldNormal:
			luck += sdb.db.data.Del(key)
		case base.WorldFrozen:
			_, exists1 := sdb.db.data.Get(key)
			v, exists2 := sdb.bgDB.data.Get(key)
			_, valNull := v.(base.Null)
			if (exists1 && !exists2) || (exists2 && !valNull) {
				luck++
				sdb.bgDB.data.Put(key, base.Null{})
			}
		case base.WorldMoving:
			_, exists1 := sdb.db.data.Get(key)
			v, exists2 := sdb.bgDB.data.Get(key)
			_, valNull := v.(base.Null)
			if (exists1 && !exists2) || (exists2 && !valNull) {
				luck++
			}
			sdb.db.data.Del(key)
			sdb.bgDB.data.Del(key)
		case base.WorldStopped:
			luck += sdb.bgDB.data.Put(key, base.Null{})
		}
	}
	switch sdb.status {
	case base.WorldNormal:

	case base.WorldFrozen:
		sdb.luckCount -= luck
	case base.WorldMoving:
		sdb.luckCount -= luck
	case base.WorldStopped:
		sdb.luckCount -= luck
	}

	return luck
}

func (sdb *SingleDB) NotifyMoving(i int) {
	for sdb.bgDB.data.Len() > 0 {
		base.NeedMoving <- i
	}
	sdb.status = base.WorldNormal
}

func (sdb *SingleDB) Flush() {
	sdb.db = &carrier{
		data:   ds.NewDict(dataDictSize),
		expire: ds.NewDict(expireDictSize),
	}
	sdb.bgDB = &carrier{
		data:   ds.NewDict(dataDictSize >> 10),
		expire: ds.NewDict(expireDictSize >> 10),
	}
	sdb.status = base.WorldNormal
}

// MoveData 在bgsave存储完成之后，将 sdb.bgDB 的数据转移到 sdb.db中
func (sdb *SingleDB) MoveData() {
	// 每次转移两个Key
	batch := 2
	for _, key := range sdb.bgDB.data.RandomKey(batch) {
		_, exists1 := sdb.db.data.Get(key)
		v, _ := sdb.bgDB.data.Get(key)
		_, valNull := v.(base.Null)

		if exists1 && valNull {
			sdb.luckCount++
			sdb.db.data.Del(key)
		} else if exists1 && !valNull {
			sdb.db.data.Put(key, v)
		} else if !exists1 && valNull {
		} else if !exists1 && !valNull {
			sdb.luckCount--
			sdb.db.data.Put(key, v)
		}
		sdb.bgDB.data.Del(key)
	}
	if sdb.bgDB.data.Len() == 0 {
		sdb.status = base.WorldNormal
	}
}

func (sdb *SingleDB) Size() int {
	switch sdb.status {
	case base.WorldNormal:
		return sdb.db.data.Len()
	case base.WorldFrozen:
		return sdb.db.data.Len() + sdb.luckCount
	case base.WorldMoving:
		return sdb.db.data.Len() + sdb.luckCount
	case base.WorldStopped:
		return sdb.db.data.Len() + sdb.luckCount
	}
	return sdb.db.data.Len() + sdb.luckCount
}

func (sdb *SingleDB) ShadowSize() int {
	return sdb.db.data.Len()
}

func (sdb *SingleDB) TTLSize() int {
	// TODO 在 bgsave期间，对bgDB写了新的key，这时候数据不准确
	return sdb.db.expire.Len()
}

func newSDB() *SingleDB {
	db := &carrier{
		data:   ds.NewDict(dataDictSize),
		expire: ds.NewDict(expireDictSize),
	}
	bgDB := &carrier{
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
