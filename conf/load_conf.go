package conf

import (
	"bufio"
	log "code/regis/lib"
	"flag"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
)

var (
	Conf = &RegisConf{}
)

func loadFlag() {
	flag.Int64Var(&Conf.Port, "port", Conf.Port, "set listen port")
}

type RegisConf struct {
	Bind            string `cfg:"bind"`
	Port            int64  `cfg:"port"`
	MaxClients      int64  `cfg:"maxclients"`
	Databases       int    `cfg:"databases"`
	RDBName         string `cfg:"dbfilename"`
	ReplBacklogSize int64  `cfg:"repl-backlog-size"`
}

func parse(src io.Reader) *RegisConf {
	config := &RegisConf{}

	// read config file
	rawMap := make(map[string]string)
	scanner := bufio.NewScanner(src)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && line[0] == '#' {
			continue
		}
		pivot := strings.IndexAny(line, " ")
		if pivot > 0 && pivot < len(line)-1 { // separator found
			key := line[0:pivot]
			value := strings.Trim(line[pivot+1:], " ")
			rawMap[strings.ToLower(key)] = value
		}
	}
	if err := scanner.Err(); err != nil {
		log.Error("fail load cfg %v", err)
	}

	// parse format
	t := reflect.TypeOf(config)
	v := reflect.ValueOf(config)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok {
			key = field.Name
		}
		value, ok := rawMap[strings.ToLower(key)]
		if ok {
			// fill config
			switch field.Type.Kind() {
			case reflect.String:
				fieldVal.SetString(value)
			case reflect.Int, reflect.Int64, reflect.Int32:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					fieldVal.SetInt(intValue)
				}
			case reflect.Bool:
				boolValue := "yes" == value
				fieldVal.SetBool(boolValue)
			case reflect.Slice:
				if field.Type.Elem().Kind() == reflect.String {
					slice := strings.Split(value, ",")
					fieldVal.Set(reflect.ValueOf(slice))
				}
			}
		}
	}
	return config
}

// LoadConf 加载conf文件
func LoadConf() {
	file, err := os.Open("redis.conf")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	Conf = parse(file)
	loadFlag()
	flag.Parse()
}
