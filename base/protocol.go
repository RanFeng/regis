package base

// Reply server单次返回给client的数据
type Reply interface {
	Bytes() []byte
}
