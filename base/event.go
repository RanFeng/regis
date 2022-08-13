package base

// regis里的事件 channel

var (
	// NeedMoving 让主线程知道某个sdb可以被moving，
	// 用于在sdb完成bgsave一阶段后，通知主线程将bgDB的数据转移到db中
	NeedMoving = make(chan int)
)
