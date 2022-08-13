package command

import (
	"code/regis/base"
	"code/regis/tcp"
	"strings"
)

type ExecFunc func(s *tcp.RegisServer, c *tcp.RegisConn, args []string) base.Reply

var CmdTable = make(map[string]*cmdInfo)

type cmdInfo struct {
	name   string
	arity  int // arity > 0 表示该命令的参数数量必须等于arity，arity < 0表示该命令的参数数量至少是arity
	level  int // 表明是在什么级别下运行的命令，只有相同才可以运行
	sflags int
	exec   ExecFunc
}

func RegCmdInfo(name string, arity, sflags int, exec ExecFunc) {
	name = strings.ToLower(name)
	CmdTable[name] = &cmdInfo{
		name:   name,
		arity:  arity,
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

func (cmd *cmdInfo) Exec(s *tcp.RegisServer, c *tcp.RegisConn, args []string) base.Reply {
	return cmd.exec(s, c, args)
}
