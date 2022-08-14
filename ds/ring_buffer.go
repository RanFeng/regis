package ds

import (
	log "code/regis/lib"
	"code/regis/lib/utils"
	"fmt"
	"sync"
)

type RingBuffer struct {
	size   int64
	wptr   int64
	buffer []byte

	// active
	// false 表示没开启buffer
	// true 表示开启了buffer
	active bool

	// histLen
	// 表示有效数据的长度
	histLen int64
	lock    sync.Mutex
}

func (rb *RingBuffer) GetHistLen() int64 {
	return rb.histLen
}

func (rb *RingBuffer) IsActive() bool {
	return rb.active
}

func (rb *RingBuffer) SetStatus(a bool) {
	log.Info("be settttt %v ", a)
	rb.active = a
}

func (rb *RingBuffer) Clear() {
	rb.lock.Lock()
	defer rb.lock.Unlock()
	rb.histLen = 0
}

func (rb *RingBuffer) Write(bs []byte) int64 {
	if !rb.active {
		return 0
	}
	rb.lock.Lock()
	defer rb.lock.Unlock()
	for _, b := range bs {
		rb.buffer[rb.wptr%rb.size] = b
		rb.wptr++
		if rb.histLen < rb.size {
			rb.histLen++
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
	delta := rb.wptr - rptr
	ret := make([]byte, delta)
	var i int64
	for i = 0; i < delta; i++ {
		ret[i] = rb.buffer[(rptr+i)%rb.size]
	}
	return ret, nil
}

func (rb *RingBuffer) Readable(rptr int64) error {
	if rptr >= rb.wptr {
		return fmt.Errorf("rptr is greater than wptr, %v > %v", rptr, rb.wptr)
	}
	if rb.wptr-rptr >= rb.histLen {
		return fmt.Errorf("data loss rptr = %v, wptr = %v, size = %v", rptr, rb.wptr, rb.size)
	}
	return nil
}

func (rb *RingBuffer) Print() {
	if rb == nil {
		return
	}
	log.Info("wptr: %v data: %v", rb.wptr, utils.BytesViz(rb.buffer))
}

func NewRingBuffer(size int64) *RingBuffer {
	rb := &RingBuffer{
		active:  false,
		size:    size,
		wptr:    0,
		histLen: 0,
		buffer:  make([]byte, size),
	}
	return rb
}
