package log

import (
	"fmt"
	"log"
	"runtime"
	"strings"
)

const (
	Reset       = "\033[0m"
	Red         = "\033[31m"
	Green       = "\033[32m"
	Yellow      = "\033[33m"
	Blue        = "\033[34m"
	Magenta     = "\033[35m"
	Cyan        = "\033[36m"
	White       = "\033[37m"
	BlueBold    = "\033[34;1m"
	MagentaBold = "\033[35;1m"
	RedBold     = "\033[31;1m"
	YellowBold  = "\033[33;1m"
)

const (
	LevelDebug = iota
	LevelInfo
	LevelNotice
	LevelError
)

var (
	logPrefix = `%v%v %v` // RedBold, "[E] ", 时间，文件
	logSuffix = fmt.Sprintf("%v", Reset)
	//errorPre  = fmt.Sprintf(logPrefix, RedBold, "[E] ")
	//infoPre   = "%v%v", BlueBold, "[I] "
	//debugPre  = fmt.Sprintf("%v%v", White, "[D] ")
	//noticeLog = log.New(os.Stdout, fmt.Sprintf("%v%v", YellowBold, "[N] "), log.LstdFlags|log.Lshortfile)
	//loggers   = []*log.Logger{errorLog, infoLog}
	//mu        sync.Mutex
)

func logs(level int, format string, v ...interface{}) {
	_, fn, ln, _ := runtime.Caller(2)
	fns := strings.Split(fn, "/")
	fn = fns[len(fns)-1]
	msg := fmt.Sprintf(format, v...)
	switch level {
	case LevelDebug:
		prefix := fmt.Sprintf(logPrefix, White, "[D]", fmt.Sprintf("%v:%v", fn, ln))
		log.Printf("%v %v %v\n", prefix, msg, logSuffix)
	case LevelInfo:
		prefix := fmt.Sprintf(logPrefix, BlueBold, "[I]", fmt.Sprintf("%v:%v", fn, ln))
		log.Printf("%v %v %v\n", prefix, msg, logSuffix)
	case LevelNotice:
		prefix := fmt.Sprintf(logPrefix, Cyan, "[N]", fmt.Sprintf("%v:%v", fn, ln))
		log.Printf("%v %v %v\n", prefix, msg, logSuffix)
	case LevelError:
		prefix := fmt.Sprintf(logPrefix, Red, "[E]", fmt.Sprintf("%v:%v", fn, ln))
		log.Printf("%v %v %v\n", prefix, msg, logSuffix)
	}
}

func Info(format string, v ...interface{}) {
	logs(LevelInfo, format, v...)
}
func Debug(format string, v ...interface{}) {
	logs(LevelDebug, format, v...)
}

func Notice(format string, v ...interface{}) {
	logs(LevelNotice, format, v...)
}
func Error(format string, v ...interface{}) {
	logs(LevelError, format, v...)
}

func Fatal(format string, v ...interface{}) {
	log.Fatalf(format, v...)
}
