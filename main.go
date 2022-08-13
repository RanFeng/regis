package main

import (
	"code/regis/base"
	"code/regis/command"
	"code/regis/conf"
	log "code/regis/lib"
	"code/regis/redis"
	"code/regis/tcp"
	"time"
)

func Executor() {
	for {
		if tcp.Server.DB.GetStatus() == base.WorldStopped {
			continue
		}

		select {
		case cmd := <-tcp.Server.GetWorkChan():
			log.Info("get %v", cmd.Query)
			if len(cmd.Query) == 0 {
				cmd.Reply = redis.NilReply
			} else {
				cmdInfo, ok := command.GetCmdInfo(cmd.Query[0])
				if !ok {
					log.Error("command not found %v", cmd.Query)
					cmd.Reply = redis.UnknownCmdErrReply(cmd.Query[0])
				} else if !cmdInfo.Validate(cmd.Query) {
					cmd.Reply = redis.ArgNumErrReply(cmd.Query[0])
				} else {
					cmd.Reply = cmdInfo.Exec(tcp.Server, cmd.Conn, cmd.Query)
					if cmdInfo.HasAttr(base.CmdWrite) {
						tcp.Server.SyncSlave(redis.CmdSReply(cmd.Query...).Bytes())
					}
				}
			}
			cmd.Conn.CmdDone(cmd)
			time.Sleep(100 * time.Millisecond)
		case index := <-base.NeedMoving:
			log.Info("moving %v", index)
			if tcp.Server.DB.GetSDB(index).GetStatus() == base.WorldMoving {
				tcp.Server.DB.GetSDB(index).MoveData()
				if tcp.Server.DB.GetSDB(index).GetStatus() == base.WorldNormal {
					log.Info("fresh %v", index)
					tcp.Server.DB.FreshNormal()
				}
			}
		}

	}
}

func main() {
	conf.LoadConf("redis.conf")

	command.ServerInit()
	tcp.Server = tcp.InitServer(conf.Conf)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Fatal("ListenAndServer failed %v\n", r)
			}
		}()
		_ = tcp.ListenAndServer(tcp.Server)
	}()
	//client = tcp.MustNewClient(server.GetAddr())
	go command.LoadRDB(tcp.Server, nil, nil)
	go tcp.Server.LoadRDB(conf.Conf.RDBName)
	//go loadRDB()

	Executor()
}
