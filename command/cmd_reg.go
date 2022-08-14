package command

import (
	"code/regis/base"
)

func sdbInit() {
	// string
	RegCmdInfo("set", Set, -3, base.CmdWrite|base.CmdDenyOom)
	RegCmdInfo("get", Get, 2, base.CmdReadOnly)
	RegCmdInfo("mset", MSet, -3, base.CmdWrite)
	RegCmdInfo("mget", MGet, -2, base.CmdReadOnly)
	RegCmdInfo("del", Del, -2, base.CmdWrite)
	RegCmdInfo("dbsize", DBSize, 1, base.CmdReadOnly)

	// list
	RegCmdInfo("lpush", LPush, -3, base.CmdWrite)
	RegCmdInfo("rpush", RPush, -3, base.CmdWrite)
	RegCmdInfo("lpushx", LPushX, -3, base.CmdWrite)
	RegCmdInfo("rpushx", RPushX, -3, base.CmdWrite)
	RegCmdInfo("lrange", LRange, 4, base.CmdReadOnly)
}

func mdbInit() {

}

func serverInit() {
	RegCmdInfo("ping", Ping, -1, base.CmdWrite|base.CmdAdmin)
	RegCmdInfo("select", Select, 2, base.CmdWrite|base.CmdLoading)
	RegCmdInfo("save", Save, 1, base.CmdAdmin)
	RegCmdInfo("bgsave", BGSave, 1, base.CmdAdmin)
	RegCmdInfo("publish", Publish, 3, base.CmdPubSub)
	RegCmdInfo("subscribe", Subscribe, -2, base.CmdPubSub)
	RegCmdInfo("unsubscribe", UnSubscribe, -2, base.CmdPubSub)

	// 主从
	RegCmdInfo("replicaof", ReplicaOf, 3, base.CmdAdmin)
	RegCmdInfo("info", Info, -1, base.CmdAdmin)
	RegCmdInfo("flushall", FlushALl, 1, base.CmdWrite|base.CmdAdmin)
	RegCmdInfo("replconf", ReplConf, -3, base.CmdAdmin)
	RegCmdInfo("psync", PSync, 3, base.CmdAdmin)
	RegCmdInfo("debug", Debug, -2, base.CmdAdmin)
	RegCmdInfo("lock", Lock, 1, base.CmdAdmin)
	RegCmdInfo("unlock", UnLock, 1, base.CmdAdmin)

}

func ServerInit() {
	sdbInit()
	mdbInit()
	serverInit()
}
