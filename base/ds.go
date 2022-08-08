package base

type LList interface {
	Len() int64
	Append(val interface{})
	Insert(val interface{}, pos int64)
	RemoveFirst(cmp func(interface{}) bool) interface{}
	Range(ch <-chan struct{}) chan interface{}
	Clear()
}
