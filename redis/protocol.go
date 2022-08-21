package redis

import (
	"bufio"
	"code/regis/base"
	log "code/regis/lib"
	"code/regis/lib/utils"
	"errors"
	"io"
	"strconv"
	"strings"
)

const (
	PrefixStr   = "+"
	PrefixErr   = "-"
	PrefixInt   = ":"
	PrefixBulk  = "$"
	PrefixArray = "*"

	CRLF = "\r\n"
)

type Payload struct {
	Size  int
	Query []string
	Err   error
}

func Parse2Inline(r *bufio.Reader) (int, []string, error) {
	for {
		msg, err := r.ReadBytes('\n')
		lens := len(msg)
		if err != nil {
			return lens, nil, err
		}
		//log.Debug("msg is %v", utils.BytesViz(msg))
		switch msg[0] {
		case PrefixArray[0]:
			argsNum, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
			if err != nil {
				log.Error("array parse err")
				return lens, nil, err
			}
			if argsNum <= 0 {
				log.Error("array length is zero")
				return lens, nil, errors.New("array length is zero")
			}
			query := make([]string, argsNum)
			for i := 0; i < int(argsNum); i++ {
				n, q, err := Parse2Inline(r)
				lens += n
				if err != nil {
					return lens, nil, err
				}
				query[i] = q[0]
			}
			return lens, query, nil
		case PrefixBulk[0]:
			bulkLen, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
			if err != nil {
				log.Error("bulk parse err")
				return lens, nil, err
			}
			if bulkLen <= 0 {
				log.Error("bulk is null %v", utils.BytesViz(msg))
				continue
			}
			//log.Debug("bulk len %v", bulkLen)
			buf := make([]byte, bulkLen)
			n, err := io.ReadFull(r, buf)
			lens += n
			if err != nil {
				log.Error("bulk read err")
				return lens, nil, err
			}
			//log.Debug("bulk val %v", string(buf))
			end := make([]byte, len(CRLF))
			n, err = io.ReadFull(r, end)
			lens += n
			if err != nil {
				log.Error("CRLF read err")
				return lens, nil, err
			}
			if string(end) != CRLF {
				log.Error("bulk end is invalid %v", end)
				return lens, nil, errors.New("bulk end is invalid")
			}
			return lens, []string{string(buf)}, nil
		case PrefixErr[0], PrefixStr[0], PrefixInt[0]:
			return lens, []string{string(msg[1 : len(msg)-2])}, nil
		default:
			if len(msg) < 2 {
				continue
			}
			ret := strings.Split(string(msg[:len(msg)-2]), " ")
			if len(ret) == 0 {
				continue
			}
			return lens, ret, nil
		}
	}
}

func Parse2Payload(conn io.Reader) chan Payload {
	cmdChan := make(chan Payload)
	go func(conn io.Reader) {
		r := bufio.NewReader(conn)
		for {
			n, query, err := Parse2Inline(r)
			payload := Payload{
				Size:  n,
				Query: query,
				Err:   err,
			}
			cmdChan <- payload
			if err != nil {
				return
			}
		}
	}(conn)
	//go parseCmd(conn, cmdChan)
	return cmdChan
}

func Parse2Reply(conn io.Reader) base.Reply {
	r := bufio.NewReader(conn)
	msg, err := r.ReadBytes('\n')
	if err != nil {
		return NilReply
	}
	switch msg[0] {
	case PrefixArray[0]:
		argsNum, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
		if err != nil {
			log.Error("array parse err")
			return NilReply
		}
		if argsNum <= 0 {
			log.Error("array length is zero")
			return NilReply
		}
		query := make([]base.Reply, argsNum)
		for i := 0; i < int(argsNum); i++ {
			query[i] = Parse2Reply(conn)
		}
		return MultiReply(query)
	case PrefixBulk[0]:
		bulkLen, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
		if err != nil {
			log.Error("bulk parse err")
			return NilReply
		}
		if bulkLen <= 0 {
			log.Error("bulk is null %v", utils.BytesViz(msg))
			return NilReply
		}
		//log.Debug("bulk len %v", bulkLen)
		buf := make([]byte, bulkLen)
		_, err = io.ReadFull(r, buf)
		if err != nil {
			log.Error("bulk read err")
			return NilReply
		}
		end := make([]byte, len(CRLF))
		_, err = io.ReadFull(r, end)
		if err != nil {
			log.Error("CRLF read err")
			return NilReply
		}
		if string(end) != CRLF {
			log.Error("bulk end is invalid %v", end)
			return NilReply
		}
		return StrReply(string(buf))
	case PrefixErr[0]:
		return ErrReply(string(msg[1 : len(msg)-2]))
	case PrefixStr[0]:
		return StrReply(string(msg[1 : len(msg)-2]))
	case PrefixInt[0]:
		v, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
		if err != nil {
			log.Error("ParseInt err")
			return NilReply
		}
		return IntReply(int(v))
	default:
		if len(msg) == 1 {
			return nil
		}
		ret := strings.Split(string(msg[:len(msg)-2]), " ")
		if len(ret) > 0 {
			return CmdSReply(ret...)
		}
		return nil
	}
}
