package command

import (
	"code/regis/base"
	"code/regis/database"
	"code/regis/tcp"
)

func sdbInit() {
	// string
	base.RegCmdInfo("set", -3, base.CmdLevelSDB, base.CmdWrite|base.CmdDenyOom, database.Set)
	base.RegCmdInfo("get", 2, base.CmdLevelSDB, base.CmdReadOnly, database.Get)
	base.RegCmdInfo("mset", -3, base.CmdLevelSDB, base.CmdWrite, database.MSet)
	base.RegCmdInfo("mget", -2, base.CmdLevelSDB, base.CmdReadOnly, database.MGet)
	//base.RegCmdInfo("mget", -2, base.CmdLevelSDB, database.MGet)
	base.RegCmdInfo("del", -2, base.CmdLevelSDB, base.CmdWrite, database.Del)
	base.RegCmdInfo("dbsize", 1, base.CmdLevelSDB, base.CmdReadOnly, database.DBSize)

	// list
	base.RegCmdInfo("lpush", -3, base.CmdLevelSDB, base.CmdWrite, database.LPush)
	base.RegCmdInfo("rpush", -3, base.CmdLevelSDB, base.CmdWrite, database.RPush)
	base.RegCmdInfo("lpushx", -3, base.CmdLevelSDB, base.CmdWrite, database.LPushX)
	base.RegCmdInfo("rpushx", -3, base.CmdLevelSDB, base.CmdWrite, database.RPushX)
	base.RegCmdInfo("lrange", 4, base.CmdLevelSDB, base.CmdReadOnly, database.LRange)
}

func mdbInit() {

}

func serverInit() {
	base.RegCmdInfo("ping", -1, base.CmdLevelServer, base.CmdAdmin, tcp.Ping)
	base.RegCmdInfo("select", 2, base.CmdLevelServer, base.CmdLoading, tcp.Select)
	base.RegCmdInfo("save", 1, base.CmdLevelServer, base.CmdAdmin, tcp.Save)
	base.RegCmdInfo("bgsave", 1, base.CmdLevelServer, base.CmdAdmin, tcp.BGSave)
	base.RegCmdInfo("publish", 3, base.CmdLevelServer, base.CmdPubSub, tcp.Publish)
	base.RegCmdInfo("subscribe", -2, base.CmdLevelServer, base.CmdPubSub, tcp.Subscribe)
	base.RegCmdInfo("unsubscribe", -2, base.CmdLevelServer, base.CmdPubSub, tcp.UnSubscribe)

	// 主从
	base.RegCmdInfo("replicaof", 3, base.CmdLevelServer, base.CmdAdmin, tcp.ReplicaOf)
	base.RegCmdInfo("info", -1, base.CmdLevelServer, base.CmdAdmin, tcp.Info)
	base.RegCmdInfo("replconf", -3, base.CmdLevelServer, base.CmdAdmin, tcp.ReplConf)
	base.RegCmdInfo("psync", 3, base.CmdLevelServer, base.CmdAdmin, tcp.PSync)
}

func ServerInit() {
	sdbInit()
	mdbInit()
	serverInit()
}
