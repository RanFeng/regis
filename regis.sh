go build -o target/regis-darwin ./ &&
cp conf/redis.conf target/ &&
cd target/ &&
./regis-darwin $*