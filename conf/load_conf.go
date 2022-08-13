package conf

type RegisConf struct {
	Bind            string
	Port            int64
	MaxClients      int64
	Databases       int
	RDBName         string
	ReplBacklogSize int64
}

var (
	Conf = &RegisConf{}
)

func LoadConf(filePath string) {
	Conf = &RegisConf{
		Bind:            "0.0.0.0",
		Port:            6399,
		MaxClients:      16,
		Databases:       16,
		RDBName:         "/Users/admin/go/src/code/regis/dump.rdb",
		ReplBacklogSize: 1048576,
	}
}
