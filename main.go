package main

import (
	"code/regis/base"
	"code/regis/client"
	"code/regis/command"
	"code/regis/conf"
	"code/regis/database"
	"code/regis/file"
	log "code/regis/lib"
	"code/regis/redis"
	"code/regis/tcp"
)

var (
	server *tcp.Server
)

func mainRoutine() {
	for {
		cmd := <-server.GetWorkChan()
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
	}
}

func loadRDB() {
	query := file.LoadRDB(conf.Conf.RDBName)
	cli, err := client.NewClient(server.GetAddr())
	if err != nil {
		log.Error("new fake client fail %v", err)
		return
	}
	go cli.Recv()
	for i := range query {
		//log.Info("query %v", query[i])
		cli.Send(redis.MultiReply(query[i]))
	}
	cli.Close()
}

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

	go loadRDB()

	mainRoutine()
}
