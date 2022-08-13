package redis

import (
	"bufio"
	"code/regis/base"
	log "code/regis/lib"
	"code/regis/lib/utils"
	"errors"
	"io"
	"strconv"
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

func parseCmd(conn io.Reader, cmdChan chan Payload) {
	r := bufio.NewReader(conn)
	payload := Payload{}
	var query []string
	var argsNum int64 = 0
	for {
		msg, err := r.ReadBytes('\n')
		if err != nil {
			payload.Err = err
			cmdChan <- payload
			return
		}
		//log.Debug("msg is %v", string(msg[:len(msg)-2]))
		switch msg[0] {
		case PrefixArray[0]:
			argsNum, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
			if err != nil {
				log.Fatal("array parse err")
				payload.Err = err
				cmdChan <- payload
				return
			}
			if argsNum <= 0 {
				log.Fatal("array length is zero")
				payload.Err = errors.New("array length is zero")
				cmdChan <- payload
				return
			}
			query = make([]string, 0, argsNum)
		case PrefixBulk[0]:
			bulkLen, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
			if err != nil {
				log.Fatal("bulk parse err")
				payload.Err = err
				cmdChan <- payload
				return
			}
			if bulkLen <= 0 {
				log.Fatal("bulk length is zero")
				payload.Err = errors.New("bulk length is zero")
				cmdChan <- payload
				return
			}
			//log.Debug("bulk len %v", bulkLen)
			buf := make([]byte, bulkLen)
			_, err = io.ReadFull(r, buf)
			if err != nil {
				log.Fatal("bulk read err")
				payload.Err = err
				cmdChan <- payload
				return
			}
			//log.Debug("bulk val %v", string(buf))
			end := make([]byte, len(CRLF))
			_, err = io.ReadFull(r, end)
			if err != nil {
				log.Fatal("CRLF read err")
				payload.Err = err
				cmdChan <- payload
				return
			}
			if string(end) != CRLF {
				log.Fatal("bulk end is invalid %v", end)
				payload.Err = errors.New("bulk end is invalid")
				cmdChan <- payload
				return
			}
			query = append(query, string(buf))
			argsNum--
		}
		if argsNum == 0 {
			payload.Query = query
			cmdChan <- payload
			payload = Payload{}
			argsNum = 0
		}
	}
}

func parse0(r *bufio.Reader) (int, []string, error) {
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
				log.Fatal("array parse err")
				return lens, nil, err
			}
			if argsNum <= 0 {
				log.Fatal("array length is zero")
				return lens, nil, errors.New("array length is zero")
			}
			query := make([]string, argsNum)
			for i := 0; i < int(argsNum); i++ {
				n, q, err := parse0(r)
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
				log.Fatal("bulk parse err")
				return lens, nil, err
			}
			if bulkLen <= 0 {
				log.Fatal("bulk length is zero")
				return lens, nil, errors.New("bulk length is zero")
			}
			//log.Debug("bulk len %v", bulkLen)
			buf := make([]byte, bulkLen)
			n, err := io.ReadFull(r, buf)
			lens += n
			if err != nil {
				log.Fatal("bulk read err")
				return lens, nil, err
			}
			//log.Debug("bulk val %v", string(buf))
			end := make([]byte, len(CRLF))
			n, err = io.ReadFull(r, end)
			lens += n
			if err != nil {
				log.Fatal("CRLF read err")
				return lens, nil, err
			}
			if string(end) != CRLF {
				log.Fatal("bulk end is invalid %v", end)
				return lens, nil, errors.New("bulk end is invalid")
			}
			return lens, []string{string(buf)}, nil
		case PrefixErr[0], PrefixStr[0], PrefixInt[0]:
			return lens, []string{string(msg[1 : len(msg)-2])}, nil
		default:
			log.Error("maybe binary input stream %v", utils.BytesViz(msg))
		}
	}
}

func Parse(conn io.Reader) chan Payload {
	cmdChan := make(chan Payload)
	go func(conn io.Reader) {
		r := bufio.NewReader(conn)
		for {
			n, query, err := parse0(r)
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

//func reply2Payload(reply base.Reply) *Payload {
//	msg := reply.Bytes()
//	payload := &Payload{}
//	switch msg[0] {
//	case PrefixArray[0]:
//		argsNum, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
//		if err != nil {
//			log.Fatal("array parse err")
//			payload.Err = err
//			return payload
//		}
//		if argsNum <= 0 {
//			log.Fatal("array length is zero")
//			payload.Err = errors.New("array length is zero")
//			return payload
//		}
//		query := make([]string, argsNum)
//		for i := 0; i < int(argsNum); i++ {
//			q, err := parse0(r)
//			if err != nil {
//				payload.Err = err
//				return payload
//			}
//			query[i] = q[0]
//		}
//		return query, nil
//	case PrefixBulk[0]:
//		bulkLen, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
//		if err != nil {
//			log.Fatal("bulk parse err")
//			return nil, err
//		}
//		if bulkLen <= 0 {
//			log.Fatal("bulk length is zero")
//			return nil, errors.New("bulk length is zero")
//		}
//		//log.Debug("bulk len %v", bulkLen)
//		buf := make([]byte, bulkLen)
//		_, err = io.ReadFull(r, buf)
//		if err != nil {
//			log.Fatal("bulk read err")
//			return nil, err
//		}
//		//log.Debug("bulk val %v", string(buf))
//		end := make([]byte, len(CRLF))
//		_, err = io.ReadFull(r, end)
//		if err != nil {
//			log.Fatal("CRLF read err")
//			return nil, err
//		}
//		if string(end) != CRLF {
//			log.Fatal("bulk end is invalid %v", end)
//			return nil, errors.New("bulk end is invalid")
//		}
//		return []string{string(buf)}, nil
//	case PrefixErr[0], PrefixStr[0], PrefixInt[0]:
//		return []string{string(msg[1 : len(msg)-2])}, nil
//	default:
//		log.Error("maybe binary input stream")
//	}
//}

func ParseReply(conn io.Reader) base.Reply {
	r := bufio.NewReader(conn)
	msg, err := r.ReadBytes('\n')
	if err != nil {
		return NilReply
	}
	switch msg[0] {
	case PrefixArray[0]:
		argsNum, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
		if err != nil {
			log.Fatal("array parse err")
			return NilReply
		}
		if argsNum <= 0 {
			log.Fatal("array length is zero")
			return NilReply
		}
		query := make([]base.Reply, argsNum)
		for i := 0; i < int(argsNum); i++ {
			query[i] = ParseReply(conn)
		}
		return MultiReply(query)
	case PrefixBulk[0]:
		bulkLen, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
		if err != nil {
			log.Fatal("bulk parse err")
			return NilReply
		}
		if bulkLen <= 0 {
			log.Fatal("bulk length is zero")
			return NilReply
		}
		buf := make([]byte, bulkLen)
		_, err = io.ReadFull(r, buf)
		if err != nil {
			log.Fatal("bulk read err")
			return NilReply
		}
		end := make([]byte, len(CRLF))
		_, err = io.ReadFull(r, end)
		if err != nil {
			log.Fatal("CRLF read err")
			return NilReply
		}
		if string(end) != CRLF {
			log.Fatal("bulk end is invalid %v", end)
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
			log.Fatal("ParseInt err")
			return NilReply
		}
		return IntReply(int(v))
	}

	return nil
}
