package ds

import (
	"code/regis/base"
	"sync"
)

type Dict struct {
	// enableLock 是否启用锁，如果不启用，下面的锁都不会执行
	enableLock bool

	// singleLock 单个函数时上的锁，
	// 比如想执行一个range函数，就会上singleLock锁，
	// 注意 singleLock 锁是自动上的，一旦enableLock开启了，就会在执行函数上锁，对使用者是透明的
	singleLock sync.Mutex

	// serialLock 执行多个函数时上的锁，
	// 比如想执行一个range函数后再执行del函数，而且希望这期间不被其他调用者抢占
	// 就手动调用 Lock, UnLock 函数来完成
	serialLock sync.Mutex

	m map[string]interface{}
}

func NewDict(size int64, enableLock bool) *Dict {
	return &Dict{
		enableLock: enableLock,
		m:          make(map[string]interface{}, size),
	}
}

func (dict *Dict) Lock() {
	if dict.enableLock {
		dict.serialLock.Lock()
	}
}

func (dict *Dict) UnLock() {
	if dict.enableLock {
		dict.serialLock.Unlock()
	}
}

func (dict *Dict) Get(key string) (val interface{}, exists bool) {
	if dict.enableLock {
		dict.singleLock.Lock()
		defer dict.singleLock.Unlock()
	}
	val, exists = dict.m[key]
	return val, exists
}

func (dict *Dict) RandomKey(num int) []string {
	if dict.enableLock {
		dict.singleLock.Lock()
		defer dict.singleLock.Unlock()
	}
	keys := make([]string, 0, num)
	for k := range dict.m {
		if num == 0 {
			break
		}
		keys = append(keys, k)
		num--
	}
	return keys
}

// Put 插入，返回更改后新增的数量
func (dict *Dict) Put(key string, val interface{}) int {
	if dict.enableLock {
		dict.singleLock.Lock()
		defer dict.singleLock.Unlock()
	}
	_, ok := dict.m[key]
	dict.m[key] = val
	if !ok {
		return 1
	}
	return 0
}

// Del 删除，返回删除后更改的数量
func (dict *Dict) Del(key string) int {
	if dict.enableLock {
		dict.singleLock.Lock()
		defer dict.singleLock.Unlock()
	}
	_, ok := dict.m[key]
	if !ok {
		return 0
	}
	delete(dict.m, key)
	return 1
}

/*RangeKey
 * @author: aijialei
 * @date: 2022-08-07 17:41:17
 * @Description: 返回一个传输key的chan
	调用方式如下：
	ch := make(chan struct{})
	defer close(ch)
	for key := range dict.RangeKey(ch) {
		if key == "aaa7" {
			ch <- struct{}{} // 在这里输入关闭信号，不输入也可以，但是要有上面的 defer close(ch) 兜底
			break
		}
	}
 * @receiver dict
 * @param c 用于控制该协程的关闭，由调用方传入，由调用方负责关闭
 * @return chan 用于传给调用方一个迭代器，由 RangeKey 声明、传出、负责关闭
*/
func (dict *Dict) RangeKey(ch <-chan struct{}) chan string {
	if dict.enableLock {
		dict.singleLock.Lock()
	}
	keys := make(chan string)
	go func() {
		defer func() {
			close(keys)
			if dict.enableLock {
				dict.singleLock.Unlock()
			}
		}()
		for k := range dict.m {
			select {
			case <-ch: // c被close或者传入信号时，都会触发，此时就要结束该协程
				return
			case keys <- k:
			}
		}
	}()
	return keys
}

func (dict *Dict) RangeKV(ch <-chan struct{}) chan base.DictKV {
	if dict.enableLock {
		dict.singleLock.Lock()
	}
	keys := make(chan base.DictKV)
	go func() {
		defer func() {
			close(keys)
			if dict.enableLock {
				dict.singleLock.Unlock()
			}
		}()
		for k, v := range dict.m {
			select {
			case <-ch:
				return
			case keys <- base.DictKV{Key: k, Val: v}:
			}
		}
	}()
	return keys
}

func (dict *Dict) GetAllKeys() []string {
	if dict.enableLock {
		dict.singleLock.Lock()
		defer dict.singleLock.Unlock()
	}
	ret := make([]string, 0, len(dict.m))
	for k := range dict.m {
		ret = append(ret, k)
	}
	return ret
}

func (dict *Dict) Len() int {
	if dict.enableLock {
		dict.singleLock.Lock()
		defer dict.singleLock.Unlock()
	}
	return len(dict.m)
}

func (dict *Dict) Clear() {
	if dict.enableLock {
		dict.singleLock.Lock()
		defer dict.singleLock.Unlock()
	}
	dict.m = map[string]interface{}{}
}
