package base

import (
	"strings"

	"github.com/hdt3213/rdb/core"
)

const (
	CmdLevelSDB     = iota // SingleDB 下运行的命令
	CmdLevelMDB            // MultiDB 下运行的命令
	CmdLevelServer         // Server 下运行的命令
	CmdLevelSenti          // Senti 下运行的命令
	CmdLevelCluster        // Cluster 下运行的命令

)

const (
	CmdWrite         = 0x0001
	CmdReadOnly      = 0x0002
	CmdDenyOom       = 0x0004
	CmdAdmin         = 0x0008
	CmdPubSub        = 0x0010
	CmdNoScript      = 0x0020
	CmdRandom        = 0x0040
	CmdSortForScript = 0x0080
	CmdLoading       = 0x0100
	CmdStale         = 0x0200
	CmdSkipMonitor   = 0x0400
	CmdAsking        = 0x0800
	CmdFast          = 0x1000
)

var (
	NeedMoving = make(chan int) // 让主线程知道某个sdb可以被moving
)

// DB 面向server的DB模型
type DB interface {
	Exec(cmd *Command) Reply
	SetStatus(status WorldStatus)
	GetStatus() WorldStatus
	GetSpaceNum() int
	GetSDB(i int) SDB
	FreshNormal()
	SaveRDB(rdb *core.Encoder) error
}

// SDB 面向命令的DB模型
type SDB interface {
	Exec(cmd *Command) Reply
	SetStatus(status WorldStatus)
	GetStatus() WorldStatus
	RangeKV(ch <-chan struct{}) chan DBKV
	PutData(key string, val interface{}) int
	GetData(key string) (interface{}, bool)
	RemoveData(keys ...string) int
	NotifyMoving(i int)
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

var CmdTable = make(map[string]*cmdInfo)

type cmdInfo struct {
	name   string
	arity  int // arity > 0 表示该命令的参数数量必须等于arity，arity < 0表示该命令的参数数量至少是arity
	level  int // 表明是在什么级别下运行的命令，只有相同才可以运行
	sflags int
	exec   interface{}
}

func RegCmdInfo(name string, arity, level int, sflags int, exec interface{}) {
	name = strings.ToLower(name)
	CmdTable[name] = &cmdInfo{
		name:   name,
		arity:  arity,
		level:  level,
		sflags: sflags,
		exec:   exec,
	}
}

func GetCmdInfo(name string) (*cmdInfo, bool) {
	name = strings.ToLower(name)
	a, b := CmdTable[name]
	return a, b
}

func (cmd *cmdInfo) Validate(cmdArgs []string) bool {
	argNum := len(cmdArgs)
	if cmd.arity >= 0 {
		return argNum == cmd.arity
	}
	return argNum >= -cmd.arity
}

// IsAttr 判断 cmd中有没有 attr 属性，
// 要求满足attr中所有的属性，有一个不符合就返回 false
func (cmd *cmdInfo) IsAttr(attr ...int) bool {
	for _, a := range attr {
		if a&cmd.sflags == 0 {
			return false
		}
	}
	return true
}

// HasAttr 判断 cmd中有没有 attr 属性
// 要求满足attr其中一个即可，有一个符合就返回 true
func (cmd *cmdInfo) HasAttr(attr ...int) bool {
	for _, a := range attr {
		if a&cmd.sflags == a {
			return true
		}
	}
	return false
}

func (cmd *cmdInfo) Level(level int) bool {
	return cmd.level == level
}

func (cmd *cmdInfo) GetExec() interface{} {
	return cmd.exec
}
