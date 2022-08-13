package ds

import (
	log "code/regis/lib"
	"code/regis/lib/utils"
	"testing"
)

func TestRingBuffer(t *testing.T) {
	rbf := NewRingBuffer(100)
	rbf.Write([]byte(utils.GetRandomHexChars(80)))
	rbf.Print()
	rbf.Write([]byte(utils.GetRandomHexChars(120)))
	rbf.Print()
	rbf.Write([]byte(utils.GetRandomHexChars(20)))
	rbf.Print()
	var bs []byte
	var err error
	bs, err = rbf.Read(140)
	if err != nil {
		log.Error("err %v", err)
	}
	log.Info(utils.BytesViz(bs))
	bs, err = rbf.Read(120)
	if err != nil {
		log.Error("err %v", err)
	}
	log.Info(utils.BytesViz(bs))
}
