package command

import (
	"code/regis/base"
	"code/regis/database"
	"code/regis/tcp"
)

func sdbInit() {
	base.RegCmdInfo("set", -3, base.CmdLevelSDB, database.Set)
	base.RegCmdInfo("get", 2, base.CmdLevelSDB, database.Get)
	base.RegCmdInfo("mset", -3, base.CmdLevelSDB, database.MSet)
	base.RegCmdInfo("mget", -2, base.CmdLevelSDB, database.MGet)
	base.RegCmdInfo("mget", -2, base.CmdLevelSDB, database.MGet)
	base.RegCmdInfo("del", -2, base.CmdLevelSDB, database.Del)
	base.RegCmdInfo("dbsize", 1, base.CmdLevelSDB, database.DBSize)
}

func mdbInit() {

}

func serverInit() {
	base.RegCmdInfo("ping", -1, base.CmdLevelServer, tcp.Ping)
	base.RegCmdInfo("select", 2, base.CmdLevelServer, tcp.Select)
	base.RegCmdInfo("save", 1, base.CmdLevelServer, tcp.Save)
	base.RegCmdInfo("bgsave", 1, base.CmdLevelServer, tcp.BGSave)
	base.RegCmdInfo("publish", 3, base.CmdLevelServer, tcp.Publish)
	base.RegCmdInfo("subscribe", -2, base.CmdLevelServer, tcp.Subscribe)
	base.RegCmdInfo("unsubscribe", -2, base.CmdLevelServer, tcp.UnSubscribe)

	// 主从
	base.RegCmdInfo("replicaof", 3, base.CmdLevelServer, tcp.ReplicaOf)
	base.RegCmdInfo("info", -1, base.CmdLevelServer, tcp.Info)
	base.RegCmdInfo("replconf", -3, base.CmdLevelServer, tcp.ReplConf)
	base.RegCmdInfo("psync", 3, base.CmdLevelServer, tcp.PSync)
}

func ServerInit() {
	sdbInit()
	mdbInit()
	serverInit()
}
