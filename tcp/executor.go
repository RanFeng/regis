package tcp

import (
	"code/regis/base"
	log "code/regis/lib"
	"code/regis/redis"
	"time"
)

func Executor() {
	for {
		if Server.GetDB().GetStatus() == base.WorldStopped {
			continue
		}

		select {
		case cmd := <-Server.GetWorkChan():
			log.Info("query %v", cmd.Query)
			if len(cmd.Query) == 0 {
				cmd.Reply = redis.NilReply
			} else {
				handler, ok := base.GetCmdInfo(cmd.Query[0])
				if !ok {
					cmd.Reply = redis.UnknownCmdErrReply(cmd.Query[0])
				} else if !handler.Validate(cmd.Query) {
					cmd.Reply = redis.ArgNumErrReply(cmd.Query[0])
				} else if handler.Level(base.CmdLevelServer) {
					exec := GetServerReal(handler.GetExec())
					cmd.Reply = exec(Server, cmd.Conn, cmd.Query)
				} else {
					cmd.Reply = Server.GetDB().Exec(cmd)
				}
			}
			cmd.Done()
			time.Sleep(100 * time.Millisecond)
		case index := <-base.NeedMoving:
			log.Info("moving %v", index)
			if Server.GetDB().GetSDB(index).GetStatus() == base.WorldMoving {
				Server.GetDB().GetSDB(index).MoveData()
				if Server.GetDB().GetSDB(index).GetStatus() == base.WorldNormal {
					log.Info("fresh %v", index)
					Server.GetDB().FreshNormal()
				}
			}
		}

	}
}
