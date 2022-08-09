package base

import "strings"

const (
	CmdLevelSDB     = iota // SingleDB 下运行的命令
	CmdLevelMDB            // MultiDB 下运行的命令
	CmdLevelServer         // Server 下运行的命令
	CmdLevelSenti          // Senti 下运行的命令
	CmdLevelCluster        // Cluster 下运行的命令

)

// DB 面向server的DB模型
type DB interface {
	Exec(cmd *Command) Reply
	SetStatus(status WorldStatus)
	GetStatus() WorldStatus
	GetSpaceNum() int
	GetSDB(i int) SDB
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
	Size() int
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
	name  string
	arity int // arity > 0 表示该命令的参数数量必须等于arity，arity < 0表示该命令的参数数量至少是arity
	level int // 表明是在什么级别下运行的命令，只有相同才可以运行
	exec  interface{}
}

func RegCmdInfo(name string, arity, level int, exec interface{}) {
	name = strings.ToLower(name)
	CmdTable[name] = &cmdInfo{
		name:  name,
		arity: arity,
		level: level,
		exec:  exec,
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

func (cmd *cmdInfo) Level(level int) bool {
	return cmd.level == level
}

func (cmd *cmdInfo) GetExec() interface{} {
	return cmd.exec
}
