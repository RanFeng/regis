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

var tick = time.NewTimer(time.Second)

func TimeTicker() {
	for {
		select {
		case <-tick.C:
			//log.Debug("tick one second!")
			tcp.ReplicationCron()
			tick.Reset(time.Second)

		}
	}
}

func Executor() {
	for {
		if tcp.Server.DB.GetStatus() == base.WorldStopped {
			continue
		}
		select {
		case cmd := <-tcp.Server.GetWorkChan():
			func() {
				defer cmd.Conn.CmdDone(cmd)
				cmd.Conn.LastBeat = time.Now()
				log.Info("get %v %v", cmd.Query, cmd.Conn.RemoteAddr())
				if !tcp.Server.PassExec(cmd.Conn) {
					log.Notice("regis is monopolised by %v", tcp.Server.Monopolist)
					return
				}

				// 空命令，忽略
				if len(cmd.Query) == 0 {
					cmd.Reply = redis.NilReply
					return
				}

				cmdInfo, ok := command.GetCmdInfo(cmd.Query[0])

				// 未知命令，报错
				if !ok {
					log.Error("command not found %v", cmd.Query)
					cmd.Reply = redis.UnknownCmdErrReply(cmd.Query[0])
					return
				}

				// 命令参数数量不对，报错
				if !cmdInfo.Validate(cmd.Query) {
					cmd.Reply = redis.ArgNumErrReply(cmd.Query[0])
					return
				}

				// 如果自己是slave，只接收master的write命令
				// 而master的write命令又全部由 tcp.Client 来转发
				// 所以，当自己是slave时，写命令只接收 tcp.Client 的
				if tcp.Server.Master != nil &&
					cmdInfo.HasAttr(base.CmdWrite) &&
					cmd.Conn.RemoteAddr() != tcp.Client.LocalAddr() {
					log.Error("I'm slave, %v call me write! 😡 I only accept %v write!😤",
						cmd.Conn.RemoteAddr(), tcp.Client.LocalAddr())
					cmd.Reply = redis.ErrReply("ERR not write when slave")
					return
				}

				cmd.Reply = cmdInfo.Exec(cmd.Conn, cmd.Query)

				if cmdInfo.HasAttr(base.CmdPropagate) {
					//tcp.ReplicationFeedSlavesFromMasterStream()
					cmdBs := redis.CmdSReply(cmd.Query...).Bytes()
					tcp.ReplicationFeedSlaves(cmdBs, cmd.Conn.DBIndex)
				}
			}()

			//time.Sleep(100 * time.Millisecond)
		case index := <-base.NeedMoving:
			log.Info("moving %v", index)
			if tcp.Server.DB.GetSDB(index).GetStatus() == base.WorldMoving {
				tcp.Server.DB.GetSDB(index).MoveData()
			}
			if tcp.Server.DB.GetSDB(index).GetStatus() == base.WorldNormal {
				log.Info("fresh %v", index)
				tcp.Server.DB.FreshNormal()
			}
		case saveMode := <-base.NeedSave:
			switch saveMode {
			case base.SaveModeBGSave:
				log.Debug("start BGSave %v", tcp.Server.DB.GetStatus())
				if tcp.Server.DB.GetStatus() == base.WorldNormal {
					tcp.Server.DB.SetStatus(base.WorldFrozen)
					go tcp.SaveRDB()
				}
			case base.SaveModeSave:
			}
		}

	}
}

func main() {
	conf.LoadConf()

	command.ServerInit()
	tcp.Server = tcp.InitServer(conf.Conf)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("ListenAndServer failed %v\n", r)
			}
		}()
		_ = tcp.ListenAndServer(tcp.Server)
	}()

	go TimeTicker()
	Executor()
}
