package base

const (
	ConfigRunIDSize = 40
)

// master 中使用的变量

// MySlaveState 本机作为master，我的slave的状态
type MySlaveState int

const (
	SlaveStateWaitBGSaveStart MySlaveState = iota // slave正在等待master的bgsave完成
	SlaveStateWaitBGSaveEnd                       // slave等到了master的bgsave完成
	SlaveStateWaitSendRDB                         // slave正在接受master的rdb
	SlaveStateWaitOnline                          // slave正常在线守候同步
)

// slave 中使用的变量

// MeSlaveState 本机作为slave的状态
type MeSlaveState int

const (
	ReplStateNone MeSlaveState = iota // 不是主从模式
	// 连接阶段开始
	ReplStateConnect    // 将要与master建立连接，一般将自己设置成这个状态，等CronJob来帮自己与master建立连接
	ReplStateConnecting // 正在与master建立连接，已与master建立连接套接字，但是还未交流握手信息

	// 连接阶段结束，握手阶段开始
	ReplStateReceivePong  // 已经与master建立套接字，且发过了ping，等master回复PONG
	ReplStateReceivePSync // 我发出了PSync，等待master的回复

	// 握手阶段结束，传输阶段开始
	ReplStateTransfer  // 正在接收master发来的rdb
	ReplStateConnected // 已经接收完master发来的rdb，完全与master建立主从关系，平时就收收master的增量命令什么的
)
