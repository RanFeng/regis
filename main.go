package main

import (
	"code/regis/base"
	"code/regis/command"
	"code/regis/conf"
	"code/regis/database"
	log "code/regis/lib"
	"code/regis/redis"
	"code/regis/tcp"
)

var (
	server *tcp.Server
	//client *tcp.Client
)

func mainRoutine() {
	for {
		if server.GetDB().GetStatus() == base.WorldStopped {
			continue
		}

		select {
		case cmd := <-server.GetWorkChan():
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
					exec := tcp.GetServerReal(handler.GetExec())
					cmd.Reply = exec(server, cmd.Conn, cmd.Query)
				} else {
					cmd.Reply = server.GetDB().Exec(cmd)
				}
			}
			cmd.Done()
		case index := <-base.NeedMoving:
			log.Info("moving %v", index)
			if server.GetDB().GetSDB(index).GetStatus() == base.WorldMoving {
				server.GetDB().GetSDB(index).MoveData()
				if server.GetDB().GetSDB(index).GetStatus() == base.WorldNormal {
					log.Info("fresh %v", index)
					server.GetDB().FreshNormal()
				}
			}
		}

	}
}

//func loadRDB() {
//	client := tcp.MustNewClient(server.GetAddr())
//	server.FlushDB()
//	//go client.Recv()
//	query := file.LoadRDB(conf.Conf.RDBName)
//	for i := range query {
//		client.Send(redis.CmdReply(query[i]...))
//	}
//	//client.Close()
//}

func makeServer() *tcp.Server {
	server = tcp.InitServer(conf.Conf)
	server.SetDB(database.NewMultiDB())
	return server
}

func main() {
	conf.LoadConf("redis.conf")

	command.ServerInit()

	server = makeServer()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Fatal("ListenAndServer failed %v\n", r)
			}
		}()
		_ = tcp.ListenAndServer(server)
	}()
	//client = tcp.MustNewClient(server.GetAddr())
	go tcp.LoadRDB(server, nil, nil)
	//go loadRDB()

	mainRoutine()
}
