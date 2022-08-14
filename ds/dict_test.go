package ds

import (
	log "code/regis/lib"
	"fmt"
	"testing"
	"time"
)

func TestDict_RangeKey(t *testing.T) {
	dict := NewDict(16, false)
	for i := 0; i < 15; i++ {
		dict.Put(fmt.Sprintf("aaa%v", i), fmt.Sprintf("bbb%v", i))
	}
	ch := make(chan struct{})
	defer close(ch)
	for key := range dict.RangeKey(ch) {
		if key == "aaa7" {
			ch <- struct{}{}
			break
		}
	}
	//log.Info("sleeppppp")
	//time.Sleep(1 * time.Second)
	//log.Info("AAA")
}

func TestDict_RangeKV(t *testing.T) {
	dict := NewDict(16, false)
	for i := 0; i < 15; i++ {
		dict.Put(fmt.Sprintf("aaa%v", i), fmt.Sprintf("bbb%v", i))
	}
	ch := make(chan struct{})
	defer close(ch)
	for key := range dict.RangeKV(ch) {
		log.Info("%p", &key)
		log.Info("%p %p", &key.Key, &key.Val)
		if key.Key == "aaa0" {
			//log.Info("%p", &key.Val)
			ch <- struct{}{}
			break
		}
	}

	//log.Info("sleeppppp")
	time.Sleep(1 * time.Second)
	//log.Info("AAA")
}
