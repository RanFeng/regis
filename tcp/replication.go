package tcp

import (
	"code/regis/base"
	log "code/regis/lib"
	"code/regis/lib/utils"
	"code/regis/redis"
	"time"
)

var (
	replicationCronLoops int64 = 0

	pingCmd = redis.CmdReply("ping").Bytes()
)

// master 行为函数

// 全量同步

// 部分同步

// ReplicationFeedSlaves
// 当自己是最上层的master时，用户执行写入命令时，往下游slave写
// 当自己不是最上层的master，但是自己下面有slave的时候，用 ReplicationFeedSlavesFromMasterStream 函数
func ReplicationFeedSlaves(msg []byte, dbIndex int) {

	// 如果自己不是最上层的master，不做命令传播
	if Server.Master == nil || (Server.ReplBacklog == nil || !Server.ReplBacklog.Active || len(Server.Slave) == 0) {
		return
	}

	// 如果写命令的db改了，要使用select改一下
	selectCMD := []byte{}
	if Server.SlaveDBIndex != dbIndex {
		selectCMD = redis.CmdReply("SELECT", dbIndex).Bytes()
		Server.SlaveDBIndex = dbIndex
	}

	// 先写入本地backlog
	var appendNum int64 = 0
	if Server.ReplBacklog != nil && Server.ReplBacklog.Active {
		if len(selectCMD) > 0 {
			appendNum += Server.ReplBacklog.Write(selectCMD)
		}
		appendNum += Server.ReplBacklog.Write(msg)
	}

	// master复制偏移增加
	Server.MasterReplOffset += appendNum

	// 再往slave同步
	for k := range Server.Slave {
		// 对于在等待rdb的那部分slave，现在不同步
		if Server.Slave[k].State == base.SlaveStateWaitBGSaveStart {
			continue
		}

		if len(selectCMD) > 0 {
			_ = Server.Slave[k].Write(selectCMD)
		}
		_ = Server.Slave[k].Write(msg)
	}
}

// ReplicationFeedSlavesFromMasterStream
// 当自己是slave的时候，下面还有slave，那就直接转发来自master的命令
func ReplicationFeedSlavesFromMasterStream(msg []byte) {
	// 记录到自己的back log中，并改变偏移量
	if Server.ReplBacklog != nil && Server.ReplBacklog.Active {
		Server.MasterReplOffset += Server.ReplBacklog.Write(msg)
	}

	for k := range Server.Slave {
		// 对于在等待rdb的那部分slave，现在不同步
		if Server.Slave[k].State == base.SlaveStateWaitBGSaveStart {
			continue
		}
		_ = Server.Slave[k].Write(msg)
	}
}

// HeartBeatFromSlave
// 当自己是master时，用这个来监听slave是否存活，
//   如果超出时间，slave没有朝我发送心跳包时，就当它没了，要关闭。
// 当自己是slave，同时也是别的机器的master时，也要监听自己的slave的存活状态
func HeartBeatFromSlave() {
	for {
		for key, cli := range Server.Slave {
			if time.Since(cli.LastBeat) > time.Minute {
				log.Error("MASTER <-> REPLICA sync timeout")
				delete(Server.Slave, key)
				cli.Close()
			}
		}
		time.Sleep(time.Duration(Server.ReplPingSlavePeriod) * time.Second)
	}
}

// slave 行为函数

// UnsetMaster 设置自己为master
func UnsetMaster() {

	// 如果自己是master，直接返回
	if Server.Master == nil {
		return
	}

	// 关闭master
	Server.Master.Close()
	Server.Master = nil

	// 切换自己的offset和replid
	Server.MasterReplOffset2 = Server.MasterReplOffset
	Server.Replid = utils.GetRandomHexChars(base.ConfigRunIDSize)

	// 断开所有的slave，让他们重新获取新的replid
	freeAllSlaves()

	Server.SlaveDBIndex = -1
	Server.ReplBacklogLastBeat = time.Now()
	Server.SlaveState = base.ReplStateNone
}

func freeAllSlaves() {
	waits := make([]int64, 0, len(Server.Slave))
	for s := range Server.Slave {
		waits = append(waits, s)
	}
	Server.CloseConn(waits...)
}

// CancelSlaveHeartBeat
// 如果与master的连接出现问题，终止与master的交流，准备重连
func CancelSlaveHeartBeat() int {
	switch Server.SlaveState {
	case base.ReplStateConnecting, base.ReplStateTransfer:
		Server.Master.Close()
		Server.SlaveState = base.ReplStateConnect
	default:
		return 0
	}

	return 1
}

// HeartBeatToMaster
// 当自己是slave时，定时向master发出心跳包，证明自己存活。
// 当自己是slave和master时，也要向自己的master发心跳包证明自己存活
func HeartBeatToMaster() {
	if Server.Master != nil {
		ack := redis.CmdReply("REPLCONF", "ACK", Server.SlaveReplOffset)
		err := Server.Master.Write(ack.Bytes())
		if err != nil { // 主从断开了
			//CancelSlaveHeartBeat()
			return
		}
		Server.LastBeatToMaster = time.Now()
	}
}

// HeartBeatToSlave
// 当自己是master时，用这个来告知slave自己的存活状态。
// 当自己是slave，同时也是别的机器的master时，就不用告知了
//   上面的master会发出ping包，我转发那个ping包就行
func HeartBeatToSlave() {
	if Server.Master == nil && len(Server.Slave) > 0 {
		ReplicationFeedSlaves(pingCmd, Server.SlaveDBIndex)
	}
}

func masterTimeout() bool {
	return Server.Master != nil && time.Since(Server.Master.LastBeat) > time.Minute
}

// ReplicationCron 每秒调用一次
func ReplicationCron() {
	// master

	// 作为master，定期向slave发送ping包
	if replicationCronLoops%Server.ReplPingSlavePeriod == 0 {
		HeartBeatToSlave()
	}

	// 对于所有在等待RDB或正在传输RDB的slave，为了避免slave等待太长时间而主从机器不交流，导致超时，也得定期心跳一下
	// 对于所有已经建立连接的slave，要定期检查心跳
	// 顺便检查是否有在等待BGSave的slave
	var maxWait time.Duration = 0
	for k := range Server.Slave {
		switch Server.Slave[k].State {
		case base.SlaveStateWaitBGSaveStart:
			wait := time.Since(Server.Slave[k].LastBeat)
			maxWait = utils.IF(maxWait > wait, maxWait, wait).(time.Duration)
			_ = Server.Slave[k].Write([]byte{'\n'})
		case base.SlaveStateWaitBGSaveEnd:
			_ = Server.Slave[k].Write([]byte{'\n'})
		case base.SlaveStateWaitOnline:
			if time.Since(Server.Slave[k].LastBeat) > time.Minute {
				log.Warn("Disconnecting timedout replica (streaming sync): %s", Server.Slave[k].RemoteAddr())
				Server.CloseConn(k)
			}
		}
	}

	// 如果有slave在等待BGSave，且现在可以开BGSave，那我们就开启一个BGSave
	if Server.DB.GetStatus() == base.WorldNormal && maxWait >= time.Second {
		base.NeedSave <- 0
	}

	// 如果一个master长时间没有任何一个slave，但是却一直开着Backlog，这不合适
	// 那么我们就切换成普通的，非主从的机器
	if len(Server.Slave) == 0 && time.Since(Server.ReplBacklogLastBeat) > time.Hour {
		Server.ReplBacklog = nil

		// 切换自己的offset和replid
		Server.MasterReplOffset2 = Server.MasterReplOffset
		Server.Replid = utils.GetRandomHexChars(base.ConfigRunIDSize)

		log.Notice("Replication backlog freed after %v seconds without connected replicas.", time.Since(Server.ReplBacklogLastBeat).Seconds())
	}

	// slave

	// 如果我们是slave，就准备与master进行同步

	// 监听master的心跳，判断master是否心跳超时了
	if masterTimeout() && Server.SlaveState >= base.ReplStateConnecting && Server.SlaveState <= base.ReplStateTransfer {
		switch Server.SlaveState {
		case base.ReplStateTransfer:
			// 在接受master传输rdb的时候，master断开了，或者因为rdb太大导致master心跳超时
			log.Warn("Timeout receiving bulk data from MASTER... " +
				"If the problem persists try to set the 'repl-timeout' " +
				"parameter in redis.conf to a larger value.")
			CancelSlaveHeartBeat()
		case base.ReplStateConnected:
			log.Warn("MASTER timeout: no data nor PING received...")
			Server.CloseConn(Server.Master.ID)
		default:
			// >= base.ReplStateConnecting && <= base.ReplStateReceivePSync
			// 在握手阶段master就心跳超时了
			log.Warn("Timeout connecting to the MASTER...")
			CancelSlaveHeartBeat()
		}
	}

	// 开始建立与master的连接
	if Server.SlaveState == base.ReplStateConnect {
		log.Notice("Connecting to MASTER %v", Server.MasterAddr)
		cli, err := NewClient(Server.MasterAddr)
		if err != nil {
			log.Notice("Unable to connect to MASTER: %s", Server.MasterAddr)
			return
		}
		Server.Master = cli
		log.Notice("MASTER <-> REPLICA sync started")
		Server.Master.LastBeat = time.Now()
		Server.SlaveState = base.ReplStateConnecting
	}

	// 定期向master发送ack
	if Server.SlaveState == base.ReplStateConnected {
		HeartBeatToMaster()
	}

	replicationCronLoops++
}
