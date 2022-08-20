package file

import (
	"code/regis/conf"
	log "code/regis/lib"
	"code/regis/redis"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/hdt3213/rdb/core"

	"github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/parser"
)

func LoadRDB(fn string) (query [][]interface{}) {
	start := time.Now().UnixNano()
	defer func() {
		end := time.Now().UnixNano()
		log.Info("DB loaded from %v: %.3f seconds, %d keys", fn, float64(end-start)/float64(time.Second), len(query))
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
	query = make([][]interface{}, 0, 100)

	err = decoder.Parse(func(o parser.RedisObject) bool {
		//log.Debug("database index: %v", o.GetDBIndex())
		query = append(query, []interface{}{"select", o.GetDBIndex()})

		switch val := o.(type) {
		case *parser.StringObject:
			query = append(query, []interface{}{"set", val.Key, string(val.Value)})
		case *parser.SetObject:

		case *parser.ListObject:
		//	for i := range val.Values {
		//		println(o.GetType(), val.Key, i, string(val.Values[i]))
		//	}
		case *parser.HashObject:
		//	for k, v := range val.Hash {
		//		println(o.GetType(), val.Key, k, string(v))
		//	}
		case *parser.ZSetObject:
			//	println(o.GetType(), val.Key, val.Entries)
		}
		return true
	})
	if err != nil {
		panic(err)
	}
	return query
}

func SaveRDB(WriteMDB func(rdb *core.Encoder) error) error {
	fn := conf.Conf.RDBName
	log.Debug("save RDB error %v", fn)
	var err error
	rdbFile, err := os.Create(fn)
	if err != nil {
		return err
	}
	rdb := encoder.NewEncoder(rdbFile)

	err = WriteHeader(rdb)
	if err != nil {
		return err
	}

	err = WriteMDB(rdb)
	if err != nil {
		return err
	}

	err = rdb.WriteEnd()
	if err != nil {
		return err
	}
	return nil
}

func WriteHeader(rdb *core.Encoder) error {
	var err error
	err = rdb.WriteHeader()
	if err != nil {
		return err
	}
	auxMap := map[string]string{
		"redis-ver":    "6.2.5",
		"redis-bits":   "64",
		"aof-preamble": "0",
	}
	for k, v := range auxMap {
		err = rdb.WriteAux(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func SaveFile(fn string, conn io.Reader, size int) error {
	file, err := os.Create(fn)
	if err != nil {
		log.Error("os.Create()函数执行错误，错误为:%v", err)
		return err
	}
	defer file.Close()

	//从网络中读数据，写入本地文件
	for size > 0 {
		buf := make([]byte, 20)
		n, err := conn.Read(buf)

		//写入本地文件，读多少，写多少
		file.Write(buf[:n])
		if err != nil {
			if err == io.EOF {
				//log.Info("接收文件完成")
				return nil
			}
			log.Error("conn.Read()方法执行出错，错误为:%v\n", err)
			return err
		}
		size -= n
	}
	//log.Info("接收文件完成")
	return nil
}

func SendRDB(fn string, conn net.Conn) {
	start := time.Now().UnixNano()
	defer func() {
		end := time.Now().UnixNano()
		log.Info("rdb send from %v: %.3f seconds", fn, float64(end-start)/float64(time.Second))
	}()
	rdbFile, err := os.Open(fn)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		panic("open dump.rdb failed")
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	fp, _ := rdbFile.Stat()
	head := fmt.Sprintf("%v%v%v", redis.PrefixBulk, fp.Size(), redis.CRLF)
	_, err = conn.Write([]byte(head))
	if err != nil {
		fmt.Println("conn.Write err:", err)
		return
	}
	buf := make([]byte, 4096)
	for {
		n, err := rdbFile.Read(buf)
		if err == io.EOF {
			fmt.Println("文件读取完毕")
			return
		}
		if err != nil {
			fmt.Println("file.Read err:", err)
			return
		}
		_, err = conn.Write(buf[:n])
		if err != nil {
			fmt.Println("conn.Write err:", err)
			return
		}
	}
}
