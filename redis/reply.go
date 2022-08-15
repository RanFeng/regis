package redis

import (
	"bytes"
	"code/regis/base"
	"code/regis/lib/utils"
	"fmt"
	"strconv"
	"strings"
)

const (
	nullBulkReplyBytes = "$-1"
)

func Equal(a base.Reply, b base.Reply) bool {
	return bytes.Equal(a.Bytes(), b.Bytes())
}

func GetString(r base.Reply) string {
	rb := r.Bytes()
	return string(rb[1 : len(rb)-2])
}

func GetInline(r base.Reply) []string {
	rb := r.Bytes()
	return strings.Split(string(rb[1:len(rb)-2]), " ")
}

func GetInt(r base.Reply) int {
	rb := r.Bytes()
	rs := string(rb[1 : len(rb)-2])
	ri, _ := strconv.ParseInt(rs, 10, 64)
	return int(ri)
}

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

// interfacesReply 用于返回多行字符串信息
type interfacesReply struct {
	msg []interface{}
}

func InterfacesReply(q []interface{}) *interfacesReply {
	return &interfacesReply{msg: q}
}

func (r *interfacesReply) Bytes() []byte {
	if len(r.msg) == 0 {
		return []byte("+(empty array)\r\n")
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

// mulReply 用于返回多行reply信息
type multiReply struct {
	r []base.Reply
}

func MultiReply(q []base.Reply) *multiReply {
	return &multiReply{r: q}
}

func (r *multiReply) Bytes() []byte {
	if len(r.r) == 0 {
		return []byte(nullBulkReplyBytes)
	}
	ret := fmt.Sprintf("%v%v%v", PrefixArray, len(r.r), CRLF)
	for i := range r.r {
		ret += string(r.r[i].Bytes())
	}
	return []byte(ret)
}

// cmdReply 用于返回一行客户端执行的cmd，也是fake client的命令请求信息
type cmdReply struct {
	cmd []interface{}
}

func CmdReply(cmd ...interface{}) *cmdReply {
	return &cmdReply{cmd: cmd}
}

func (r *cmdReply) Bytes() []byte {
	if len(r.cmd) == 0 {
		return []byte(nullBulkReplyBytes)
	}
	ret := fmt.Sprintf("%v%v%v", PrefixArray, len(r.cmd), CRLF)
	for i := 0; i < len(r.cmd); i++ {
		if r.cmd[i] == nil {
			ret += fmt.Sprintf("%v%v%v", PrefixBulk, -1, CRLF)
		} else {
			msgS := utils.InterfaceToString(r.cmd[i])
			ret += fmt.Sprintf("%v%v%v", PrefixBulk, len(msgS), CRLF)
			ret += fmt.Sprintf("%v%v", msgS, CRLF)
		}
	}
	return []byte(ret)
}

// cmdSReply 用于返回一行客户端执行的cmd，也是fake client的命令请求信息
type cmdSReply struct {
	cmd []string
}

func CmdSReply(cmd ...string) *cmdSReply {
	return &cmdSReply{cmd: cmd}
}

func (r *cmdSReply) Bytes() []byte {
	if len(r.cmd) == 0 {
		return []byte(nullBulkReplyBytes)
	}
	ret := fmt.Sprintf("%v%v%v", PrefixArray, len(r.cmd), CRLF)
	for i := 0; i < len(r.cmd); i++ {
		if len(r.cmd[i]) == 0 {
			ret += fmt.Sprintf("%v%v%v", PrefixBulk, -1, CRLF)
		} else {
			msgS := utils.InterfaceToString(r.cmd[i])
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

// Int64Reply 返回一个数字
type int64Reply struct {
	num int64
}

func Int64Reply(num int64) *int64Reply {
	return &int64Reply{
		num: num,
	}
}

func (r *int64Reply) Bytes() []byte {
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

// inlineSReply 返回一个inline 命令字符串 +set A B\r\n
type inlineSReply struct {
	cmd []string
}

func InlineSReply(cmd ...string) *inlineSReply {
	return &inlineSReply{
		cmd: cmd,
	}
}

func (r *inlineSReply) Bytes() []byte {
	s := strings.Join(r.cmd, " ")
	ret := fmt.Sprintf("%v%v%v", PrefixStr, s, CRLF)
	return []byte(ret)
}

// inlineIReply 返回一个inline 命令字符串 +set A B\r\n
type inlineIReply struct {
	cmd []interface{}
}

func InlineIReply(cmd ...interface{}) *inlineIReply {
	return &inlineIReply{
		cmd: cmd,
	}
}

func (r *inlineIReply) Bytes() []byte {
	cmds := make([]string, len(r.cmd))
	for i := range r.cmd {
		cmds[i] = utils.InterfaceToString(r.cmd[i])
	}
	s := strings.Join(cmds, " ")
	ret := fmt.Sprintf("%v%v%v", PrefixStr, s, CRLF)
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

// typeErrReply 当对某个键操作一个不属于其类型的操作时报错，比如对一个string的key进行lpush
type typeErrReply struct{}

var TypeErrReply = &typeErrReply{}

func (r *typeErrReply) Bytes() []byte {
	return []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}

// intErrReply 当输入一个值，它应当转化为int，但是因为太大或不是int而无法转发时报错
type intErrReply struct{}

var IntErrReply = &intErrReply{}

func (r *intErrReply) Bytes() []byte {
	return []byte("-ERR value is not an integer or out of range\r\n")
}
