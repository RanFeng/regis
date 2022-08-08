package ds

import (
	log "code/regis/lib"
	"fmt"
)

type chainOpType int

const (
	insertOp chainOpType = iota
	deleteOp
)

type LinkedList struct {
	len  int64
	head *node
	tail *node
}

// Len 获取链表长度
func (list *LinkedList) Len() int64 {
	return list.len
}

func fix(prev *node, self *node, next *node, op chainOpType) {
	switch op {
	case insertOp: // self插入prev和next中间
		self.next = next
		next.prev = self
		self.prev = prev
		prev.next = self
	case deleteOp: // self从prev和next中删除
		prev.next = next
		next.prev = prev
	}
}

func (list *LinkedList) Print() {
	cursor := list.head
	listStr := "list val: "
	for i := list.Len(); i > 0; i-- {
		listStr += fmt.Sprintf("%v, ", cursor.val)
		cursor = cursor.next
	}
	log.Debug(listStr)
}

func (list *LinkedList) Append(val interface{}) {
	list.insert(&node{val: val}, list.Len())
}

func (list *LinkedList) Insert(val interface{}, pos int64) {
	list.insert(&node{val: val}, pos)
}

func (list *LinkedList) RemoveFirst(cmp func(interface{}) bool) interface{} {
	if list.Len() == 0 {
		return nil
	}

	// 检查是否是head
	cursor := list.head
	if cmp(list.head.val) {
		if list.Len() == 1 {
			list.head = nil
			list.tail = nil
			list.len--
			return cursor.val
		} else {
			fix(list.head.prev, list.head, list.head.next, deleteOp)
			list.head = list.head.next
			list.len--
			return cursor.val
		}
	}

	// 检查是否是tail
	cursor = list.tail
	if cmp(list.tail.val) {
		fix(list.tail.prev, list.tail, list.tail.next, deleteOp)
		list.tail = list.tail.prev
		list.len--
		return cursor.val
	}

	cursor = list.head
	for i := list.Len(); i > 0; i-- {
		if cmp(cursor.val) {
			fix(cursor.prev, cursor, cursor.next, deleteOp)
			list.len--
			return cursor.val
		}
		cursor = cursor.next
	}
	log.Debug("not found in list")
	return nil
}

func (list *LinkedList) Clear() {
	list.head = nil
	list.tail = nil
	list.len = 0
}

/*Range
 * @author: aijialei
 * @date: 2022-08-07 20:08:59
 * @Description: 返回一个传输key的chan
	调用方式如下：
	ch := make(chan struct{})
	defer close(ch)
	for item := range list.Range(ch) {
		if item == "bbbbb" {
			ch <- struct{}{} // 在这里输入关闭信号，不输入也可以，但是要有上面的 defer close(ch) 兜底
			break
		}
	}
 * @receiver list
 * @param c
 * @return chan
*/
func (list *LinkedList) Range(ch <-chan struct{}) chan interface{} {
	vals := make(chan interface{})
	if list.Len() == 0 {
		close(vals)
		return vals
	}
	go func() {
		defer func() {
			close(vals)
		}()
		for cur := list.head; cur != list.tail; cur = cur.next {
			select {
			case <-ch: // c被close或者传入信号时，都会触发，此时就要结束该协程
				return
			case vals <- cur.val:
			}
		}
		select {
		case <-ch: // c被close或者传入信号时，都会触发，此时就要结束该协程
			return
		case vals <- list.tail.val:
		}
	}()
	return vals
}

// Insert 插入链表成为下标为pos的node，如果pos为负数或0，则成为头部，如果pos>=len，则成为尾部
func (list *LinkedList) insert(n *node, pos int64) {
	defer func() { list.len++ }()
	if list.Len() == 0 {
		fix(n, n, n, insertOp)
		list.head = n
		list.tail = n
		return
	}
	if pos <= 0 {
		fix(list.tail, n, list.head, insertOp)
		list.head = n
		return
	}
	if pos >= list.Len() {
		fix(list.tail, n, list.head, insertOp)
		list.tail = n
		return
	}
	var cursor *node
	if pos >= list.Len()/2 { // 从后遍历
		cursor = list.tail
		for pos = list.Len() - pos; pos > 0; pos-- {
			cursor = cursor.prev
		}
		fix(cursor, n, cursor.next, insertOp)
	} else { // 从前遍历
		cursor = list.head
		for ; pos > 0; pos-- {
			cursor = cursor.next
		}
		fix(cursor.prev, n, cursor, insertOp)
	}
}

type node struct {
	prev *node
	next *node
	val  interface{}
}

func NewLinkedList(val ...interface{}) *LinkedList {
	list := &LinkedList{}
	for i := range val {
		list.Append(val[i])
	}
	return list
}
