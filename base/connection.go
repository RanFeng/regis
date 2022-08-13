package base

import "net"

type WorldStatus int

const (
	WorldNormal  = iota // 正常读写server.db
	WorldFrozen         // 已发生BGSave等命令，写命令进入server.bgDB，读命令先读server.bgDB，miss再读server.db
	WorldMoving         // 此时BGSave命令刚刚完成，正在蚂蚁搬家式地将server.bgDB中的内容写入server.db中
	WorldStopped        // 此时主线下不再执行任何命令，比如save命令发生时
)

// Conn client与server的连接
type Conn interface {
	Handle()
	Close()
	Write(b []byte)
	Reply(reply Reply)
	GetConn() net.Conn
	GetID() int64
	GetDBIndex() int
	SetDBIndex(int)
	CmdDone(cmd *Command)

	NSubChannel() LList
	PSubChannel() LList

	RemoteAddr() string
}

// Reply server单次返回给client的数据
type Reply interface {
	Bytes() []byte
}

type Command struct {
	Conn  Conn
	Query []string
	Reply Reply
	Err   error
}

func (cmd *Command) Done() {
	cmd.Conn.CmdDone(cmd)
}

//type CMD interface {
//	GetConn() Conn
//	GetArgs() []string
//	GetErr() error
//	GetReply() Reply
//	SetReply(r Reply)
//	SetErr(err error)
//	SetArgs([]string)
//
//	Done()
//	Exec()
//}
