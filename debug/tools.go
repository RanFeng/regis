package debug

import (
	"bytes"
	"code/regis/base"
	log "code/regis/lib"
	"code/regis/redis"
	"code/regis/tcp"
	"testing"
)

type Debugger struct {
	T   *testing.T
	Cli *tcp.Client
}

func (d *Debugger) SetCli(c *tcp.Client) {
	d.Cli = c
}

func (d *Debugger) CmdReply(want base.Reply, cmd ...interface{}) {
	log.Info("cmd is %v", cmd)
	d.Cli.Send(redis.CmdReply(cmd))
	wb := want.Bytes()
	buf := make([]byte, len(wb))
	num, err := d.Cli.RecvN(buf)

	if err != nil {
		d.T.Errorf("err %v", err)
		d.T.Fatal()
	}
	if len(wb) != num {
		d.T.Errorf("want len %v, get len %v", len(wb), num)
		d.T.Errorf("want %v, get %v", string(wb), string(buf))
		d.T.Fatal()
	}
	if !bytes.Equal(buf, wb) {
		d.T.Errorf("want %v, get %v", string(wb), string(buf))
		d.T.Fatal()
	}
}
func (d *Debugger) CmdReply_(want base.Reply, cmd ...interface{}) {
	d.Cli.Send(redis.CmdReply(cmd))
	wb := want.Bytes()

	buf, err := d.Cli.RecvAll()

	if err != nil {
		d.T.Errorf("err %v", err)
	}
	if len(wb) != len(buf) {
		d.T.Errorf("want len %v, get len %v", len(wb), len(buf))
		d.T.Errorf("want %v, get %v", wb, buf)
	}
	if !bytes.Equal(buf, wb) {
		d.T.Errorf("want %v, get %v", wb, buf)
	}
	log.Info("cmd is ok %v", cmd)
}

func NewDebugger(c *tcp.Client, t *testing.T) *Debugger {
	d := &Debugger{
		Cli: c,
		T:   t,
	}

	return d
}
