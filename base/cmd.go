package base

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
	CmdPropagate     = 0x2000 // 当这个命令被master传过来的要传给下游的，就需要有这个属性，所以写命令几乎都有这个属性
)
