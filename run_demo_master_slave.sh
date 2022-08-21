redis-server --port 1236 &
redis-server --port 1234 --replicaof 127.0.0.1 1236 &
redis-server --port 1235 --replicaof 127.0.0.1 1236