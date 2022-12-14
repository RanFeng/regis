package base

import (
	"github.com/hdt3213/rdb/core"
)

type WorldStatus int

func (ws WorldStatus) String() string {
	switch ws {
	case WorldNormal:
		return "normal"
	case WorldFrozen:
		return "frozen"
	case WorldMoving:
		return "moving"
	case WorldStopped:
		return "stopped"
	}
	return "unknown"
}

const (
	WorldNormal  WorldStatus = iota // 正常读写server.db
	WorldFrozen                     // 已发生BGSave等命令，写命令进入server.bgDB，读命令先读server.bgDB，miss再读server.db
	WorldMoving                     // 此时BGSave命令刚刚完成，正在蚂蚁搬家式地将server.bgDB中的内容写入server.db中
	WorldStopped                    // 此时主线下不再执行任何命令，比如save命令发生时
)

const (
	ConnNormal = iota
	ConnSync
)

// DB 面向server的DB模型
type DB interface {
	SetStatus(status WorldStatus)
	GetStatus() WorldStatus
	GetSpaceNum() int
	GetSDB(i int) SDB
	FreshNormal()
	SaveRDB(rdb *core.Encoder) error
	Flush()
}

// SDB 面向命令的DB模型
type SDB interface {
	SetStatus(status WorldStatus)
	GetStatus() WorldStatus
	RangeKV(ch <-chan struct{}) chan DBKV
	PutData(key string, val interface{}) int
	GetData(key string) (interface{}, bool)
	RemoveData(keys ...string) int
	NotifyMoving(i int)
	Flush()
	MoveData()
	Size() int
	ShadowSize() int
	TTLSize() int
}

type DBKV struct {
	//Index int
	DictKV
	TTL int64
}

type DictKV struct {
	Key string
	Val interface{}
}
