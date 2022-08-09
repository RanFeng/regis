package base

type LList interface {
	Len() int64
	Append(val interface{})
	Insert(val interface{}, pos int64)
	RemoveFirst(cmp func(interface{}) bool) interface{}
	Range(ch <-chan struct{}) chan interface{}
	Clear()
}

type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Put(key string, val interface{}) int
	RangeKey(ch <-chan struct{}) chan string
	RangeKV(ch <-chan struct{}) chan DictKV
	Len() int
	Clear()
}

type String string
type List LList
type Hash Dict
type Set interface{}
type Zset interface{}
