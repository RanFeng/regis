package redis

import (
	"code/regis/lib/utils"
	"fmt"
	"strconv"
)

const (
	nullBulkReplyBytes = "$-1"
)

// echoReply 用于回显命令的测试reply
type echoReply struct {
	query []string
}

func EchoReply(q []string) *echoReply {
	return &echoReply{query: q}
}

func (r *echoReply) Bytes() []byte {
	if len(r.query) == 0 {
		return []byte(nullBulkReplyBytes)
	}
	ret := fmt.Sprintf("%v%v%v", PrefixArray, len(r.query), CRLF)
	for i := 0; i < len(r.query); i++ {
		ret += fmt.Sprintf("%v%v%v", PrefixBulk, len(r.query[i]), CRLF)
		ret += fmt.Sprintf("%v%v", r.query[i], CRLF)
	}
	return []byte(ret)
}

// BulkReply 用于返回一个字符串
type bulkReply struct {
	Arg []byte
}

func BulkReply(Arg []byte) *bulkReply {
	return &bulkReply{Arg: Arg}
}

func (r *bulkReply) Bytes() []byte {
	if len(r.Arg) == 0 {
		return []byte("$-1\r\n")
	}
	return []byte(string(PrefixBulk) + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

// BulkStrReply 用于返回一个字符串
type bulkStrReply struct {
	msg string
}

func BulkStrReply(msg string) *bulkStrReply {
	return &bulkStrReply{msg: msg}
}

func (r *bulkStrReply) Bytes() []byte {
	if len(r.msg) == 0 {
		return []byte("$-1\r\n")
	}
	return []byte(string(PrefixBulk) + strconv.Itoa(len(r.msg)) + CRLF + r.msg + CRLF)
}

// arrayReply 用于返回多行信息，可以是字符串和int类型的混合
type arrayReply struct {
	msg []interface{}
}

func ArrayReply(q []interface{}) *arrayReply {
	return &arrayReply{msg: q}
}

func (r *arrayReply) Bytes() []byte {
	if len(r.msg) == 0 {
		return []byte(nullBulkReplyBytes)
	}
	ret := fmt.Sprintf("%v%v%v", PrefixArray, len(r.msg), CRLF)
	for i := 0; i < len(r.msg); i++ {
		if r.msg[i] == nil {
			ret += fmt.Sprintf("%v%v%v", PrefixBulk, -1, CRLF)
		} else {
			if msgInt, ok := r.msg[i].(int); ok {
				ret += fmt.Sprintf("%v%v%v", PrefixInt, msgInt, CRLF)
			} else {
				msgS := utils.InterfaceToString(r.msg[i])
				ret += fmt.Sprintf("%v%v%v", PrefixBulk, len(msgS), CRLF)
				ret += fmt.Sprintf("%v%v", msgS, CRLF)
			}
		}
	}
	return []byte(ret)
}

// mulReply 用于返回多行信息，可以是字符串和int类型的混合
type multiReply struct {
	msg []interface{}
}

func MultiReply(q []interface{}) *multiReply {
	return &multiReply{msg: q}
}

func (r *multiReply) Bytes() []byte {
	if len(r.msg) == 0 {
		return []byte(nullBulkReplyBytes)
	}
	ret := fmt.Sprintf("%v%v%v", PrefixArray, len(r.msg), CRLF)
	for i := 0; i < len(r.msg); i++ {
		if r.msg[i] == nil {
			ret += fmt.Sprintf("%v%v%v", PrefixBulk, -1, CRLF)
		} else {
			msgS := utils.InterfaceToString(r.msg[i])
			ret += fmt.Sprintf("%v%v%v", PrefixBulk, len(msgS), CRLF)
			ret += fmt.Sprintf("%v%v", msgS, CRLF)
		}
	}
	return []byte(ret)
}

// argNumErrReply 命令参数数量不对时
type argNumErrReply struct {
	cmd string
}

func ArgNumErrReply(cmd string) *argNumErrReply {
	return &argNumErrReply{cmd: cmd}
}

func (r *argNumErrReply) Bytes() []byte {
	ret := fmt.Sprintf("%vERR wrong number of arguments for '%v' command%v", PrefixErr, r.cmd, CRLF)
	return []byte(ret)
}

// unknownCmdErrReply 未知命令时
type unknownCmdErrReply struct {
	cmd string
}

func UnknownCmdErrReply(cmd string) *unknownCmdErrReply {
	return &unknownCmdErrReply{cmd: cmd}
}

func (r *unknownCmdErrReply) Bytes() []byte {
	ret := fmt.Sprintf("%vERR unknown command '%v'%v", PrefixErr, r.cmd, CRLF)
	return []byte(ret)
}

// OkReply is +OK
type okReply struct{}

var OkReply = &okReply{}

func (r *okReply) Bytes() []byte {
	return []byte("+OK\r\n")
}

// NilReply is (nil)
type nilReply struct{}

var NilReply = &nilReply{}

func (r *nilReply) Bytes() []byte {
	return []byte("$-1\r\n")
}

// IntReply 返回一个数字
type intReply struct {
	num int
}

func IntReply(num int) *intReply {
	return &intReply{
		num: num,
	}
}

func (r *intReply) Bytes() []byte {
	ret := fmt.Sprintf("%v%v%v", PrefixInt, r.num, CRLF)
	return []byte(ret)
}

// StrReply 返回一个字符串
type strReply struct {
	str string
}

func StrReply(str string) *strReply {
	return &strReply{
		str: str,
	}
}

func (r *strReply) Bytes() []byte {
	ret := fmt.Sprintf("%v%v%v", PrefixStr, r.str, CRLF)
	return []byte(ret)
}

// errReply 返回一个错误
type errReply struct {
	msg string
}

func ErrReply(status string) *errReply {
	return &errReply{
		msg: status,
	}
}

func (r *errReply) Bytes() []byte {
	ret := fmt.Sprintf("%v%v%v", PrefixErr, r.msg, CRLF)
	return []byte(ret)
}

// parseReply 返回一个错误
type parseErrReply struct{}

var ParseErrReply = &parseErrReply{}

func (r *parseErrReply) Bytes() []byte {
	return []byte("-ERR parse Error occurred\r\n")
}
