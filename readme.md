# Regis

- [x] `ping, get, set, mget, mset, select`
- [x] `select, publish, subscribe, unsubscribe`
- [x] `save, bgsave, del, dbsize`
- [x] RDB load, fake client
- [x] RDB save
- [x] list, but not compatibility with bgsave (need read copy)
- [x] ring buffer (so easy)
- [x] redis offset, part, full sync
- [ ] boot from conf
- [ ] cascade master-slave
- [ ] slaveof, PSYNC
- [ ] master-slave reconnection
- [ ] set, zset, hash command




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
- [ ] server中的workChan也许可以当成全局变量
- [ ] base.Conn 也许可以不用了

