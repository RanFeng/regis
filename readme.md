# Regis

- [x] `ping, get, set, mget, mset, select`
- [x] `select, publish, subscribe, unsubscribe`
- [x] `save, bgsave, del, dbsize`
- [x] RDB load, fake client
- [x] RDB save
- [x] list, but not compatibility with bgsave (need read copy)
- [x] ring buffer (so easy)
- [x] redis offset, part, full sync
- [x] boot from conf and shell flags
- [ ] cascade master-slave doing ....
- [ ] slaveof, PSYNC
- [ ] master-slave reconnection
- [ ] set, zset, hash command


- [x] release some key struct dependencies to ds
- [ ] use time event? or multi goroutine?
- [ ] 遗留一个bug，当slave断开一段时间后再连接，尽管master send了断点的部分，但是slave还是收不到，
  redis作为master，regis作为slave的时候，没问题。说明接收没有问题。要试试regis作为master而redis作为slave时的表现。

- [ ] info replication
- [ ] AOF
- [ ] master and slave
- [ ] sentinel
- [ ] cluster
- [ ] expire key

- [x] string -> string
- [x] list -> LinkedList
- [x] hash -> Dict
- [ ] set -> Set
- [ ] zset -> SkipList

# TODO
- [x] BGsave时，有数量bug
- [ ] 留下一个bug，B为了同步masterA而断开slaveC连接的时候，slaveC重连会ping到B这里来，导致B这里的offset有差异，其实此时不应当将这个ping计入offset的
  如果不计入，就接不到master的ping
  如果晚点再允许计入，这个晚点的时间点无法估计，也许有的slave要很晚才重连
  只能根据是否是slave的ping来进行计入。
  而且自己是slave的时候，也不能允许其他人写入，这也待实现
- [ ] server中的workChan也许可以当成全局变量
- [ ] base.Conn 也许可以不用了

