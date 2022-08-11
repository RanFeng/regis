package main

import (
	"code/regis/debug"
	log "code/regis/lib"
	"code/regis/redis"
	"code/regis/tcp"
	"os"
	"testing"
	"time"
)

var (
	cli *tcp.Client
	err error
)

func TestMain(m *testing.M) {
	go main()
	time.Sleep(200 * time.Millisecond)
	cli, err = tcp.NewClient(server.GetAddr())
	if err != nil {
		log.Error("new fake client fail %v", err)
		return
	}
	code := m.Run()
	cli.Close()
	os.Exit(code)
}

func TestSave(t *testing.T) {
	d := debug.NewDebugger(cli, t)
	d.CmdReply(redis.OkReply, "set", "A", "100")
	d.CmdReply(redis.OkReply, "set", "B", "10asjakjsaks0")
	d.CmdReply(redis.BulkStrReply("100"), "get", "A")
	d.CmdReply(redis.IntReply(2), "dbsize")
	d.CmdReply(redis.BulkStrReply("10asjakjsaks0"), "get", "B")
	d.CmdReply(redis.OkReply, "select", 2)
	d.CmdReply(redis.OkReply, "set", "B", "BBBBB")
	d.CmdReply(redis.IntReply(1), "dbsize")

	d.CmdReply(redis.OkReply, "save")

}

func TestBGSave(t *testing.T) {
	d := debug.NewDebugger(cli, t)
	d.CmdReply(redis.OkReply, "set", "A", "100")
	d.CmdReply(redis.OkReply, "set", "B", "10asjakjsaks0")
	d.CmdReply(redis.BulkStrReply("100"), "get", "A")
	d.CmdReply(redis.IntReply(2), "dbsize")
	d.CmdReply(redis.BulkStrReply("10asjakjsaks0"), "get", "B")
	d.CmdReply(redis.OkReply, "select", 2)
	d.CmdReply(redis.OkReply, "set", "B", "BBBBB")
	d.CmdReply(redis.IntReply(1), "dbsize")
	d.CmdReply(redis.OkReply, "set", "D", "DDDD")

	d.CmdReply(redis.IntReply(2), "dbsize")
	d.CmdReply(redis.IntReply(1), "del", "B", "A")

	d.CmdReply(redis.BulkStrReply("Background saving started"), "bgsave")
	d.CmdReply(redis.BulkStrReply("DDDD"), "get", "D")

	d.CmdReply(redis.IntReply(1), "dbsize")
	d.CmdReply(redis.ErrReply("ERR can not save in bgsave"), "save")

	time.Sleep(10 * time.Second)
}

func TestRestart(t *testing.T) {
	d := debug.NewDebugger(cli, t)
	d.CmdReply(redis.BulkStrReply("100"), "get", "A")
	d.CmdReply(redis.IntReply(2), "dbsize")
	d.CmdReply(redis.BulkStrReply("10asjakjsaks0"), "get", "B")
	d.CmdReply(redis.OkReply, "select", 2)
	d.CmdReply(redis.NilReply, "get", "B")
	d.CmdReply(redis.IntReply(1), "dbsize")
}
