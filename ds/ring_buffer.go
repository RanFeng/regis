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
	lock   sync.Mutex
}

func (rb *RingBuffer) Write(bs []byte) {
	rb.lock.Lock()
	defer rb.lock.Unlock()
	for _, b := range bs {
		rb.buffer[rb.wptr%rb.size] = b
		rb.wptr++
	}
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
	if rb.wptr-rptr >= rb.size {
		return fmt.Errorf("data loss rptr = %v, wptr = %v, size = %v", rptr, rb.wptr, rb.size)
	}
	return nil
}

func (rb *RingBuffer) Print() {
	log.Info("wptr: %v data: %v", rb.wptr, utils.BytesViz(rb.buffer))
}

func NewRingBuffer(size int64) *RingBuffer {
	rb := &RingBuffer{
		size:   size,
		wptr:   0,
		buffer: make([]byte, size),
	}
	return rb
}
