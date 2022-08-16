package base

const (
	ConfigRunIDSize = 40
)

const (
	SlaveStateWaitBGSaveStart = iota // slave正在等待master的bgsave完成
	SlaveStateWaitBGSaveEnd          // slave等到了master的bgsave完成
)
