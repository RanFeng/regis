package file

import (
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

func SaveRDB(fn string) {
	//rdbFile, err := os.OpenFile(fn)
	rdbFile, err := os.Create(fn)
	if err != nil {
		panic(err)
	}
	defer rdbFile.Close()
	enc := encoder.NewEncoder(rdbFile)
	err = enc.WriteHeader()
	if err != nil {
		panic(err)
	}
	auxMap := map[string]string{
		"redis-ver":    "6.2.5",
		"redis-bits":   "64",
		"aof-preamble": "0",
	}
	for k, v := range auxMap {
		err = enc.WriteAux(k, v)
		if err != nil {
			panic(err)
		}
	}

	err = enc.WriteDBHeader(0, 5, 1)
	if err != nil {
		panic(err)
	}

	expirationMs := uint64(time.Now().Add(time.Hour*8).Unix() * 1000)
	err = enc.WriteStringObject("hello", []byte("world"), encoder.WithTTL(expirationMs))
	if err != nil {
		panic(err)
	}
	err = enc.WriteListObject("list1", [][]byte{
		[]byte("123"),
		[]byte("abc"),
		[]byte("la la la"),
	})
	if err != nil {
		panic(err)
	}
	err = enc.WriteSetObject("set", [][]byte{
		[]byte("123"),
		[]byte("abc"),
		[]byte("la la la"),
	})
	if err != nil {
		panic(err)
	}
	err = enc.WriteHashMapObject("list3", map[string][]byte{
		"1":  []byte("123"),
		"a":  []byte("abc"),
		"la": []byte("la la la"),
	})
	if err != nil {
		panic(err)
	}
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
	if err != nil {
		panic(err)
	}

	err = enc.WriteDBHeader(1, 1, 0)
	if err != nil {
		panic(err)
	}
	err = enc.WriteStringObject("hello", []byte("world22222"), encoder.WithTTL(expirationMs))
	if err != nil {
		panic(err)
	}
	err = enc.WriteEnd()
	if err != nil {
		panic(err)
	}
}
