package ds

import (
	"code/regis/base"
	log "code/regis/lib"
)

type Dict struct {
	m map[string]interface{}
}

func NewDict(size int64) *Dict {
	return &Dict{
		m: make(map[string]interface{}, size),
	}
}

func (dict *Dict) Get(key string) (val interface{}, exists bool) {
	val, exists = dict.m[key]
	return val, exists
}

func (dict *Dict) Have(keys ...string) []bool {
	b := make([]bool, len(keys))
	for i := range keys {
		_, b[i] = dict.m[keys[i]]
	}
	return b
}

// Put 插入，返回更改后新增的数量
func (dict *Dict) Put(key string, val interface{}) int {
	_, ok := dict.m[key]
	dict.m[key] = val
	if !ok {
		return 1
	}
	return 0
}

// Del 删除，返回删除后更改的数量
func (dict *Dict) Del(key string) int {
	_, ok := dict.m[key]
	delete(dict.m, key)
	if !ok {
		return 0
	}
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
	keys := make(chan string)
	go func() {
		defer func() {
			log.Info("close ch")
			close(keys)
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
	keys := make(chan base.DictKV)
	go func() {
		defer func() {
			close(keys)
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

func (dict *Dict) Len() int {
	return len(dict.m)
}

func (dict *Dict) Clear() {
	dict.m = map[string]interface{}{}
}
