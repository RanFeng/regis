package utils

import (
	"code/regis/base"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

func GetConnFd(l net.Conn) int64 {
	v := reflect.ValueOf(l)
	netFD := reflect.Indirect(reflect.Indirect(v).FieldByName("fd"))
	FD := reflect.Indirect(reflect.Indirect(netFD).FieldByName("pfd"))
	fd := FD.FieldByName("Sysfd").Int()
	return fd
}

func GetRandomHexChars(len int) string {
	chars := "0123456789abcdef"
	ret := make([]byte, len)
	for i := 0; i < len; {
		r := rand.Int63()
		for i < len && r >= 16 {
			ret[i] = chars[r&0xF]
			r = r >> 4
			i++
		}
	}
	return *(*string)(unsafe.Pointer(&ret))
}

func ParseAddr(addr string) (ip, port string) {
	s := strings.Split(addr, ":")
	return s[0], s[1]
}

func InterfaceToBytes(val interface{}) []byte {
	return []byte(InterfaceToString(val))
}

func BytesViz(buf []byte) string {
	str := ""
	for _, b := range buf {
		if b >= 32 {
			str += string(b)
			continue
		}
		switch b {
		case '\r':
			str += `\r`
		case '\n':
			str += `\n`
		default:
			str += fmt.Sprintf(`\%d`, b)
		}
	}
	return str
}

func InterfaceToString(value interface{}) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case base.RString:
		return string(v)
	case fmt.Stringer:
		return v.String()
	case fmt.GoStringer:
		return v.GoString()
	case bool:
		if v {
			return "true"
		} else {
			return "false"
		}
	case error:
		return v.Error()

	case int:
		return strconv.FormatInt(int64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)

	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)

	case float32:
		return strconv.FormatFloat(float64(v), 'E', 6, 64)
	case float64:
		return strconv.FormatFloat(float64(v), 'E', 6, 64)
	default:
		bytes, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(bytes)
	}
}
