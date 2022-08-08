package command

import (
	"code/regis/base"
	"code/regis/tcp"
)

func sdbInit() {
	base.RegCmdInfo("set", -3, base.CmdLevelSDB, execSet)
	base.RegCmdInfo("get", 2, base.CmdLevelSDB, execGet)
	base.RegCmdInfo("MSet", -3, base.CmdLevelSDB, execMSet)
	base.RegCmdInfo("MGet", -2, base.CmdLevelSDB, execMGet)
}

func mdbInit() {

}

func serverInit() {
	base.RegCmdInfo("ping", -1, base.CmdLevelServer, tcp.Ping)
	base.RegCmdInfo("select", 2, base.CmdLevelServer, tcp.Select)
	base.RegCmdInfo("publish", 3, base.CmdLevelServer, tcp.Publish)
	base.RegCmdInfo("subscribe", -2, base.CmdLevelServer, tcp.Subscribe)
	base.RegCmdInfo("unsubscribe", -2, base.CmdLevelServer, tcp.UnSubscribe)
}

func ServerInit() {
	sdbInit()
	mdbInit()
	serverInit()
}
