package tcp

import (
	"code/regis/redis"
	"testing"
	"time"
)

func Test_NewClient(t *testing.T) {
	cli, err := NewClient(":6379")
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
