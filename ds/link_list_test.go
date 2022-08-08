package ds

import (
	"fmt"
	"testing"
	"time"
)

func TestLinkedList_Print(t *testing.T) {
	var list *LinkedList
	list = NewLinkedList()
	list.Print()
	// 空列表
	list = &LinkedList{
		len:  0,
		head: nil,
		tail: nil,
	}
	list.Print()

	// 单个结点的列表
	node1 := &node{
		prev: nil,
		next: nil,
		val:  123,
	}
	node1.prev = node1
	node1.next = node1
	list = &LinkedList{
		len:  1,
		head: node1,
		tail: node1,
	}
	list.Print()

	// 两个结点
	node2 := &node{
		prev: nil,
		next: nil,
		val:  "asdas",
	}
	node1.prev = node2
	node1.next = node2
	node2.prev = node1
	node2.next = node1
	list = &LinkedList{
		len:  2,
		head: node1,
		tail: node2,
	}
	list.Print()

	// 多个结点
	li := []interface{}{"asdas", "12312", 68127391, "hhhhhh"}
	list = NewLinkedList(li...)
	list.Print()
}

func TestLinkedList_Insert(t *testing.T) {
	var list *LinkedList
	// 多个结点
	li := []interface{}{"asdas", "12312", 68127391, "hhhhhh"}
	list = NewLinkedList(li...)
	list.Print()

	for i := 0; i < 10; i++ {
		list.Insert(fmt.Sprintf("aaaa%v", i), int64(i))
		list.Print()
	}
	list.Insert(fmt.Sprintf("aaaa%v", 1000), 1000)
	list.Print()
}

func TestLinkedList_RemoveFirst(t *testing.T) {
	var list *LinkedList
	// 多个结点
	li := []interface{}{"asdas", "12312", 68127391, "hhhhhh"}
	list = NewLinkedList(li...)
	list.Print()

	for i := 0; i < 10; i++ {
		list.Insert(fmt.Sprintf("aaaa%v", i), int64(i))
		list.Print()
	}
	list.Insert(fmt.Sprintf("aaaa%v", 1000), 1000)
	list.Print()

	cmp := func(val interface{}) bool {
		return val.(string) == "aaaa9"
	}
	list.RemoveFirst(cmp)
	list.Print()
}

func TestLinkedList_Range(t *testing.T) {
	//var list = NewLinkedList("asda", "bbbbb", "cccc")
	var list = NewLinkedList("aa")
	// 多个结点
	//li := []interface{}{"asdas", "12312", 68127391, "hhhhhh"}
	//list = NewLinkedList(li...)
	//list.Print()

	ch := make(chan struct{})
	defer close(ch)
	for item := range list.Range(ch) {
		if item == "bbbbb" {
			ch <- struct{}{}
			break
		}
	}
	list.Print()
	time.Sleep(time.Second)
}
