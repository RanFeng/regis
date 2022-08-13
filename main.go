package main

import (
	"code/regis/command"
	"code/regis/conf"
	log "code/regis/lib"
	"code/regis/tcp"
)

//func mainRoutine() {
//	for {
//		if tcp.Server.GetDB().GetStatus() == base.WorldStopped {
//			continue
//		}
//
//		select {
//		case cmd := <-tcp.Server.GetWorkChan():
//			log.Info("query %v", cmd.Query)
//			if len(cmd.Query) == 0 {
//				cmd.Reply = redis.NilReply
//			} else {
//				handler, ok := base.GetCmdInfo(cmd.Query[0])
//				if !ok {
//					cmd.Reply = redis.UnknownCmdErrReply(cmd.Query[0])
//				} else if !handler.Validate(cmd.Query) {
//					cmd.Reply = redis.ArgNumErrReply(cmd.Query[0])
//				} else if handler.Level(base.CmdLevelServer) {
//					exec := tcp.GetServerReal(handler.GetExec())
//					cmd.Reply = exec(tcp.Server, cmd.Conn, cmd.Query)
//				} else {
//					cmd.Reply = tcp.Server.GetDB().Exec(cmd)
//				}
//			}
//			switch tcp.Server.GetRole() {
//			case tcp.RoleMaster:
//
//			case tcp.RoleSlave:
//				cmd.Reply = redis.CmdReply("REPLCONF", "ACK", tcp.Server.)
//			}
//			cmd.Done()
//			time.Sleep(100 * time.Millisecond)
//		case index := <-base.NeedMoving:
//			log.Info("moving %v", index)
//			if tcp.Server.GetDB().GetSDB(index).GetStatus() == base.WorldMoving {
//				tcp.Server.GetDB().GetSDB(index).MoveData()
//				if tcp.Server.GetDB().GetSDB(index).GetStatus() == base.WorldNormal {
//					log.Info("fresh %v", index)
//					tcp.Server.GetDB().FreshNormal()
//				}
//			}
//		}
//
//	}
//}

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
	go tcp.LoadRDB(tcp.Server, nil, nil)
	//go loadRDB()

	tcp.Executor()
}
