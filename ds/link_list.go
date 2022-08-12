package ds

import (
	"code/regis/base"
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

func (list *LinkedList) PushTail(val interface{}) {
	list.insert(&node{val: val}, list.Len())
}

func (list *LinkedList) PushHead(val interface{}) {
	list.insert(&node{val: val}, 0)
}

// InsertAfter 当list作为 base.RList 提供出去的时候，这时候list中node的val都是字符串
// 返回-1表示插入失败，否则返回插入后的列表长度
func (list *LinkedList) InsertAfter(pivot interface{}, val interface{}) int64 {
	if list.Len() == 0 {
		return 0
	}
	ps := pivot.(string)
	p := list.find(func(v interface{}) bool {
		return ps == v.(string)
	})
	if p == nil {
		return -1
	}
	n := &node{val: val}
	list.insertAB(n, p, true)
	return list.Len()
}

// InsertBefore 当list作为 base.RList 提供出去的时候，这时候list中node的val都是字符串
// 返回-1表示插入失败，否则返回插入后的列表长度
func (list *LinkedList) InsertBefore(pivot interface{}, val interface{}) int64 {
	if list.Len() == 0 {
		return 0
	}
	ps := pivot.(string)
	p := list.find(func(v interface{}) bool {
		return ps == v.(string)
	})
	if p == nil {
		return -1
	}
	n := &node{val: val}
	list.insertAB(n, p, false)
	return list.Len()
}

func (list *LinkedList) Insert(val interface{}, pos int64) {
	list.insert(&node{val: val}, pos)
}

// find 找出符合的首个节点
func (list *LinkedList) find(cmp func(interface{}) bool) *node {
	if list.Len() == 0 {
		return nil
	}

	for cur := list.head; cur != list.tail; cur = cur.next {
		if cmp(cur.val) {
			return cur
		}
	}

	if cmp(list.tail.val) {
		return list.tail
	}
	log.Debug("not found in list")
	return nil
}

// index 找出下标为pos的节点，pos范围是[0, list.Len()], pos < 0 时，返回head，pos >= list.Len()时，返回tail
func (list *LinkedList) index(pos int64) *node {
	if list.Len() == 0 {
		return nil
	}

	if pos <= 0 {
		return list.head
	}

	if pos >= list.Len() {
		return list.tail
	}

	if pos <= list.Len()/2 { // 从head遍历
		for cur := list.head; cur != list.tail; cur = cur.next {
			if pos == 0 {
				return cur
			}
			pos--
		}
	} else { // 从tail遍历
		pos = list.Len() - pos - 1
		for cur := list.tail; cur != list.head; cur = cur.prev {
			if pos == 0 {
				return cur
			}
			pos--
		}
	}
	return nil
}

// find 找出符合的所有节点
//func (list *LinkedList) findAll(cmp func(interface{}) bool) []*node {
//	ret := make([]*node, 0, list.Len())
//	if list.Len() == 0 {
//		return ret
//	}
//
//	for cur := list.head; cur != list.tail; cur = cur.next {
//		if cmp(cur.val) {
//			ret = append(ret, cur)
//		}
//	}
//
//	if cmp(list.tail.val) {
//		ret = append(ret, list.tail)
//	}
//	return ret
//}

func (list *LinkedList) remove(n *node) {
	if n == nil {
		return
	}
	defer func() { list.len-- }()
	// 就一个结点的话，删了就直接是空list
	if list.Len() == 1 {
		list.head = nil
		list.tail = nil
		return
	}
	fix(n.prev, n, n.next, deleteOp)
	if n == list.head {
		list.head = n.next
	}

	if n == list.tail {
		list.tail = n.prev
	}
}

func (list *LinkedList) RemoveFirst(cmp func(interface{}) bool) interface{} {
	n := list.find(cmp)
	list.remove(n)
	return n.val
}

// DelEntry 用于删除list中的元素，
//	count > 0 : 从表头开始向表尾搜索，移除与 VALUE 相等的元素，数量为 COUNT 。
//	count < 0 : 从表尾开始向表头搜索，移除与 VALUE 相等的元素，数量为 COUNT 的绝对值。
//	count = 0 : 移除表中所有与 VALUE 相等的值。
// 返回删除的数量
func (list *LinkedList) DelEntry(val interface{}, count int64) int64 {
	if list.Len() == 0 {
		return 0
	}
	var ret int64 = 0

	ps := val.(string)
	if count > 0 {
		for cur := list.head; cur != list.tail && ret < count; cur = cur.next {
			if cur.val.(string) == ps {
				list.remove(cur)
				ret++
			}
		}
		if list.tail.val.(string) == ps && ret < count {
			list.remove(list.tail)
			ret++
		}
	} else if count < 0 {
		count = -count
		for cur := list.tail; cur != list.head && ret < count; cur = cur.prev {
			if cur.val.(string) == ps {
				list.remove(cur)
				ret++
			}
		}
		if list.head.val.(string) == ps && ret < count {
			list.remove(list.head)
			ret++
		}
	} else {
		for cur := list.head; cur != list.tail; cur = cur.next {
			if cur.val.(string) == ps {
				list.remove(cur)
				ret++
			}
		}
		if list.tail.val.(string) == ps {
			list.remove(list.tail)
			ret++
		}
	}
	return ret
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

func (list *LinkedList) LRange(from, to int64) []interface{} {
	if from < -list.len {
		from = 0
	}
	if from < 0 {
		from += list.len
	}
	if to < -list.len {
		to = 0
	}
	if to < 0 {
		to += list.len
	}
	num := int(to - from + 1)
	if num <= 0 {
		return nil
	}
	ret := make([]interface{}, num)
	n := list.index(from)
	for i := 0; i < num; i++ {
		ret[i] = n.val
		n = n.next
	}
	return ret
}

// insert 插入链表成为下标为pos的node，如果pos为负数或0，则成为头部，如果pos>=len，则成为尾部
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

// insert 将n插入到p之前或之后，保证p一定是在list中的，如果after为true，表示插入后面，否则插前面
func (list *LinkedList) insertAB(n *node, p *node, after bool) {
	defer func() { list.len++ }()
	if after {
		fix(p, n, p.next, insertOp)
		if p == list.tail {
			list.tail = n
		}
	} else {
		fix(p.prev, n, p, insertOp)
		if p == list.head {
			list.head = n
		}
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
		list.PushTail(val[i])
	}
	return list
}

func NewRList(val ...string) base.RList {
	list := &LinkedList{}
	for i := range val {
		list.PushTail(val[i])
	}
	return list
}
