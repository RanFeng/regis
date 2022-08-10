package utils

import (
	"code/regis/base"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"strconv"
)

func GetConnFd(l net.Conn) int64 {
	v := reflect.ValueOf(l)
	netFD := reflect.Indirect(reflect.Indirect(v).FieldByName("fd"))
	FD := reflect.Indirect(reflect.Indirect(netFD).FieldByName("pfd"))
	fd := FD.FieldByName("Sysfd").Int()
	return fd
}

func InterfaceToBytes(val interface{}) []byte {
	return []byte(InterfaceToString(val))
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
