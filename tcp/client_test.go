package tcp

import (
	log "code/regis/lib"
	"code/regis/redis"
	"testing"
	"time"
)

func Test_NewClient(t *testing.T) {
	cli, err := NewClient(":6379", nil)
	if err != nil {
		panic(err)
	}
	cmd := []interface{}{"set", "ABCD", "100"}
	cli.Send(redis.ArrayReply(cmd))
	go cli.Recv()
	time.Sleep(3 * time.Second)

	cli.Close()

	time.Sleep(3 * time.Second)
}

func TestTimer(t *testing.T) {
	t1 := time.NewTimer(3 * time.Second)
	t2 := time.NewTimer(2 * time.Second)
	for {
		//timer := time.NewTimer(2 * time.Second)
		select {
		case <-t1.C:
			log.Info("time 3 sec")
			t1.Reset(3 * time.Second)
		case <-t2.C:
			log.Info("time 2 sec")
			t2.Reset(2 * time.Second)
		}
	}
}
