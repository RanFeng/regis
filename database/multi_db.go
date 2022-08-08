package database

import (
	"code/regis/base"
	"code/regis/conf"
	"code/regis/redis"
)

const (
	DefaultSDBNum = 16
)

type MultiDB struct {
	SDB []*SingleDB
}

func (md *MultiDB) Exec(cmd *base.Command) base.Reply {
	ci, _ := base.GetCmdInfo(cmd.Query[0])
	if ci.Level(base.CmdLevelMDB) {
		return redis.NilReply
	}
	sdb := md.GetSDB(cmd.Conn.GetDBIndex())
	return sdb.Exec(cmd)
}

func (md *MultiDB) RangeKV(ch <-chan struct{}) chan base.DBKV {
	kvs := make(chan base.DBKV)
	go func() {
		defer func() {
			close(kvs)
		}()
		for i := range md.SDB {
			for kv := range md.SDB[i].RangeKV(ch) {
				kv.Index = i
				select {
				case <-ch: // c被close或者传入信号时，都会触发，此时就要结束该协程
					return
				case kvs <- kv:
				}
			}
		}
	}()
	return kvs
}

func (md *MultiDB) GetSDB(i int) *SingleDB {
	if i >= len(md.SDB) || i <= 0 {
		return md.SDB[0]
	}
	return md.SDB[i]
}

func NewMultiDB() *MultiDB {
	if conf.Conf.Databases == 0 {
		conf.Conf.Databases = DefaultSDBNum
	}
	db := &MultiDB{
		SDB: make([]*SingleDB, conf.Conf.Databases),
	}
	for i := 0; i < conf.Conf.Databases; i++ {
		sdb := newSDB()
		sdb.index = i
		db.SDB[i] = sdb
	}
	return db
}
