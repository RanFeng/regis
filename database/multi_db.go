package database

import (
	"code/regis/base"
	"code/regis/conf"

	"github.com/hdt3213/rdb/core"
)

const (
	DefaultSDBNum = 16
)

type MultiDB struct {
	// status
	// 初始时， status = base.WorldNormal
	// 当调用BGSave时， status = base.WorldFrozen，此时server不接受外界的新的BGSave
	// 当调用Save时， status = base.WorldStopped，此时server不接受外界的新的请求
	status base.WorldStatus

	sDB []*SingleDB
}

//func (md *MultiDB) Exec(cmd *base.Command) base.Reply {
//	ci, _ := base.GetCmdInfo(cmd.Query[0])
//	if ci.Level(base.CmdLevelMDB) {
//		return redis.NilReply
//	}
//	sdb := md.GetSDB(cmd.Conn.GetDBIndex())
//	return sdb.Exec(cmd)
//}

func (md *MultiDB) SetStatus(status base.WorldStatus) {
	md.status = status
	for i := range md.sDB {
		md.sDB[i].SetStatus(status)
	}
}

func (md *MultiDB) GetStatus() base.WorldStatus {
	return md.status
}

func (md *MultiDB) GetSpaceNum() int {
	return len(md.sDB)
}

func (md *MultiDB) GetSDB(i int) base.SDB {
	if i >= len(md.sDB) || i <= 0 {
		return md.sDB[0]
	}
	return md.sDB[i]
}

func (md *MultiDB) FreshNormal() {
	for i := range md.sDB {
		if md.sDB[i].GetStatus() != base.WorldNormal {
			return
		}
	}
	md.status = base.WorldNormal
}

//func (md *MultiDB) Flush() {
//
//}

func (md *MultiDB) SaveRDB(rdb *core.Encoder) error {
	var err error
	for i := range md.sDB {
		if md.sDB[i].ShadowSize() == 0 {
			md.sDB[i].SetStatus(base.WorldNormal)
			md.FreshNormal()
			continue
		}
		err = rdb.WriteDBHeader(uint(i), uint64(md.sDB[i].ShadowSize()), uint64(md.sDB[i].TTLSize()))
		if err != nil {
			return err
		}
		err = md.sDB[i].SaveRDB(rdb)
		if err != nil {
			return err
		}

		// 让主线程知道这个sdb可以被moving
		go md.sDB[i].NotifyMoving(i)
	}
	return nil
}

func NewMultiDB() *MultiDB {
	if conf.Conf.Databases == 0 {
		conf.Conf.Databases = DefaultSDBNum
	}
	db := &MultiDB{
		sDB: make([]*SingleDB, conf.Conf.Databases),
	}
	for i := 0; i < conf.Conf.Databases; i++ {
		sdb := newSDB()
		db.sDB[i] = sdb
	}
	return db
}
