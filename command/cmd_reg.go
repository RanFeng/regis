package command

import (
	"code/regis/base"
)

func sdbInit() {
	// string
	RegCmdInfo("set", -3, base.CmdWrite|base.CmdDenyOom, Set)
	RegCmdInfo("get", 2, base.CmdReadOnly, Get)
	RegCmdInfo("mset", -3, base.CmdWrite, MSet)
	RegCmdInfo("mget", -2, base.CmdReadOnly, MGet)
	//base.RegCmdInfo("mget", -2, base.CmdLevelSDB, database.MGet)
	RegCmdInfo("del", -2, base.CmdWrite, Del)
	RegCmdInfo("dbsize", 1, base.CmdReadOnly, DBSize)

	// list
	RegCmdInfo("lpush", -3, base.CmdWrite, LPush)
	RegCmdInfo("rpush", -3, base.CmdWrite, RPush)
	RegCmdInfo("lpushx", -3, base.CmdWrite, LPushX)
	RegCmdInfo("rpushx", -3, base.CmdWrite, RPushX)
	RegCmdInfo("lrange", 4, base.CmdReadOnly, LRange)
}

func mdbInit() {

}

func serverInit() {
	RegCmdInfo("ping", -1, base.CmdAdmin, Ping)
	RegCmdInfo("select", 2, base.CmdLoading, Select)
	RegCmdInfo("save", 1, base.CmdAdmin, Save)
	RegCmdInfo("bgsave", 1, base.CmdAdmin, BGSave)
	RegCmdInfo("publish", 3, base.CmdPubSub, Publish)
	RegCmdInfo("subscribe", -2, base.CmdPubSub, Subscribe)
	RegCmdInfo("unsubscribe", -2, base.CmdPubSub, UnSubscribe)

	// 主从
	RegCmdInfo("replicaof", 3, base.CmdAdmin, ReplicaOf)
	RegCmdInfo("info", -1, base.CmdAdmin, Info)
	RegCmdInfo("replconf", -3, base.CmdAdmin, ReplConf)
	RegCmdInfo("psync", 3, base.CmdAdmin, PSync)
}

func ServerInit() {
	sdbInit()
	mdbInit()
	serverInit()
}
