package file

import (
	"code/regis/base"
	log "code/regis/lib"
	"os"
	"time"

	"github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"

	"github.com/hdt3213/rdb/parser"
)

func LoadRDB(fn string) [][]interface{} {
	start := time.Now().UnixNano()
	defer func() {
		end := time.Now().UnixNano()
		log.Info("DB loaded from disk: %.3f seconds", float64(end-start)/float64(time.Second))
	}()
	rdbFile, err := os.Open(fn)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		panic("open dump.rdb failed")
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	decoder := parser.NewDecoder(rdbFile)
	query := make([][]interface{}, 0, 100)

	err = decoder.Parse(func(o parser.RedisObject) bool {
		//log.Debug("database index: %v", o.GetDBIndex())
		query = append(query, []interface{}{"select", o.GetDBIndex()})

		switch val := o.(type) {
		case *parser.StringObject:
			query = append(query, []interface{}{"set", val.Key, string(val.Value)})
			//case *parser.SetObject:

			//case *parser.ListObject:
			//	for i := range val.Values {
			//		println(o.GetType(), val.Key, i, string(val.Values[i]))
			//	}
			//case *parser.HashObject:
			//	for k, v := range val.Hash {
			//		println(o.GetType(), val.Key, k, string(v))
			//	}
			//case *parser.ZSetObject:
			//	println(o.GetType(), val.Key, val.Entries)
		}
		return true
	})
	if err != nil {
		panic(err)
	}
	return query
}

func SaveRDB(db base.DB) error {
	fn := "dump.rdb"
	rdbFile, err := os.Create(fn)
	if err != nil {
		return err
	}
	defer rdbFile.Close()
	defer func() {
		log.Error("get err %v", err)
	}()

	enc := encoder.NewEncoder(rdbFile)
	err = enc.WriteHeader()
	if err != nil {
		return err
	}
	auxMap := map[string]string{
		"redis-ver":    "6.2.5",
		"redis-bits":   "64",
		"aof-preamble": "0",
	}
	for k, v := range auxMap {
		err = enc.WriteAux(k, v)
		if err != nil {
			return err
		}
	}

	dbNum := db.GetSpaceNum()
	ch := make(chan struct{})
	for i := 0; i < dbNum; i++ {
		sdb := db.GetSDB(i)
		if sdb.Size() == 0 {
			sdb.SetStatus(base.WorldMoving)
			continue
		}
		err = enc.WriteDBHeader(uint(i), uint64(sdb.Size()), uint64(sdb.TTLSize()))
		if err != nil {
			return err
		}
		for kv := range sdb.RangeKV(ch) {
			log.Info("write kvs %v %T", kv, kv.Val)
			var ttlOp interface{}
			if kv.TTL > 0 {
				ttlOp = encoder.WithTTL(uint64(time.Now().Add(time.Duration(kv.TTL*int64(time.Millisecond))).Unix() * 1000))
			}
			switch v := kv.Val.(type) {
			case base.String:
				err = enc.WriteStringObject(kv.Key, []byte(v), ttlOp)
			case base.List:
				ch := make(chan struct{})
				ret := make([][]byte, 0, v.Len())
				for k := range v.Range(ch) {
					ret = append(ret, k.([]byte))
				}
				close(ch)
				err = enc.WriteListObject(kv.Key, ret, ttlOp)
			case base.Hash:
				ch := make(chan struct{})
				ret := make(map[string][]byte, v.Len())
				for hkv := range v.RangeKV(ch) {
					ret[hkv.Key] = hkv.Val.([]byte)
				}
				close(ch)
				err = enc.WriteHashMapObject(kv.Key, ret, ttlOp)
			case base.Set:
				// TODO
				err = enc.WriteSetObject("set", [][]byte{
					[]byte("123"),
					[]byte("abc"),
					[]byte("la la la"),
				})
			case base.Zset:
				// TODO
				err = enc.WriteZSetObject("list2", []*model.ZSetEntry{
					{
						Score:  1.234,
						Member: "a",
					},
					{
						Score:  2.71828,
						Member: "b",
					},
				})
			}
			if err != nil {
				return err
			}
		}
		sdb.SetStatus(base.WorldMoving)
	}

	err = enc.WriteEnd()
	if err != nil {
		return err
	}
	return nil
}
