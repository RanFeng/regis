package base

type CmpFunc func(interface{}) bool

type LList interface {
	Len() int64
	PushTail(val interface{})
	PushHead(val interface{})
	Insert(val interface{}, pos int64)
	RemoveFirst(cmp func(interface{}) bool) interface{}
	Range(ch <-chan struct{}) chan interface{}
	UnLock()
	Lock()
	Clear()
}

type RList interface {
	Len() int64
	PushTail(val interface{})
	PushHead(val interface{})
	InsertBefore(pivot interface{}, val interface{}) int64
	DelEntry(val interface{}, count int64) int64
	InsertAfter(target interface{}, val interface{}) int64
	RemoveFirst(cmp func(interface{}) bool) interface{}
	Range(ch <-chan struct{}) chan interface{}
	LRange(from, to int64) []interface{}
	Clear()
}

type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Put(key string, val interface{}) int
	RangeKey(ch <-chan struct{}) chan string
	RangeKV(ch <-chan struct{}) chan DictKV
	GetAllKeys() []string
	RandomKey(num int) []string
	UnLock()
	Lock()
	Len() int
	Clear()
}

type Set interface {
	Get(key string) (val interface{}, exists bool)
	Put(key string, val interface{}) int
	RangeKey(ch <-chan struct{}) chan string
	RangeKV(ch <-chan struct{}) chan DictKV
	Len() int
	Clear()
}

type ZSet interface {
	Get(key string) (val interface{}, exists bool)
	Put(key string, val interface{}) int
	RangeKey(ch <-chan struct{}) chan string
	Len() int
	Clear()
}

type RString string

//type RList LList
type RHash Dict
type RSet Set
type RZSet ZSet

// Null 当需要删除某个key，但是由于bgsave等命令不能删除的时候，在bgDB中对该key置位为Null
// 注意！该值永远不可能存在于 db 中，只能存在于 bgDB 中！！！
type Null struct{}
