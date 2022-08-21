package ds

import (
	log "code/regis/lib"
	"code/regis/lib/utils"
	"fmt"
	"sync"
)

type RingBuffer struct {
	Size int64
	// 记录这个 RingBuffer 是从何处开始写入的，
	// StartPtr == WritePtr 表示该处还没写
	// StartPtr < WritePtr 表示该处写了
	// 注意，这个不是 buffer 的下标，是指绝对的偏移值
	StartPtr int64

	// 记录这个 RingBuffer 目前写到哪儿了， WritePtr 的地方还没写
	// 注意，这个不是 buffer 的下标，是指绝对的偏移值
	WritePtr int64
	buffer   []byte

	// Active
	// false 表示没开启buffer
	// true 表示开启了buffer
	Active bool

	// HistLen
	// 表示有效数据的长度
	// 其实就是 (WritePtr - StartPtr + Size) % Size
	HistLen int64
	lock    sync.Mutex
}

func (rb *RingBuffer) Reset(sptr int64) {
	rb.HistLen = 0
	rb.WritePtr = sptr
	rb.StartPtr = sptr
}

func (rb *RingBuffer) Write(bs []byte) int64 {
	if !rb.Active {
		return 0
	}
	rb.lock.Lock()
	defer rb.lock.Unlock()
	for _, b := range bs {
		rb.buffer[rb.WritePtr%rb.Size] = b
		rb.WritePtr++
		if rb.HistLen < rb.Size {
			rb.HistLen++
		}
	}
	return int64(len(bs))
}

func (rb *RingBuffer) Read(rptr int64) ([]byte, error) {
	rb.lock.Lock()
	defer rb.lock.Unlock()
	if err := rb.Readable(rptr); err != nil {
		return nil, err
	}
	delta := rb.WritePtr - rptr
	ret := make([]byte, delta)
	var i int64
	for i = 0; i < delta; i++ {
		ret[i] = rb.buffer[(rptr+i)%rb.Size]
	}
	return ret, nil
}

func (rb *RingBuffer) Readable(rptr int64) error {
	if rb.WritePtr < rptr {
		return fmt.Errorf("rptr is not less than WritePtr, %v < %v", rb.WritePtr, rptr)
	}
	if rb.WritePtr-rptr > rb.HistLen {
		return fmt.Errorf("data loss rptr = %v, sptr = %v, wptr = %v, size = %v",
			rptr, rb.StartPtr, rb.WritePtr, rb.HistLen)
	}
	return nil
}

func (rb *RingBuffer) Print() {
	if rb == nil {
		return
	}
	log.Info("WritePtr: %v data: %v", rb.WritePtr, utils.BytesViz(rb.buffer))
}

func NewRingBuffer(size int64) *RingBuffer {
	rb := &RingBuffer{
		Active:   false,
		Size:     size,
		StartPtr: 0,
		WritePtr: 0,
		HistLen:  0,
		buffer:   make([]byte, size),
	}
	return rb
}
