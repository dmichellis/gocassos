package gocassos

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"syscall"
)

func (l *Logger) Enabled() bool {
	if LogLevel < l.level {
		return false
	}
	return true
}

const (
	LShowCaller = 1 << iota
	LShowPID
)

var LogLevel int = 99
var logFlags = LShowPID | LShowCaller
var NVM = Logger{prefix: "NVM  ", level: 4}
var BTW = Logger{prefix: "BTW  ", level: 3}
var FYI = Logger{prefix: "FYI  ", level: 2}
var WTF = Logger{prefix: "WTF?!", level: 1}
var FUUU = Logger{prefix: "FUUU!", level: 0}
var FUU = Logger{prefix: "FUUU!", level: 0}

func (l *Logger) Printf(format string, args ...interface{}) {
	if !l.Enabled() {
		return
	}
	prefix := fmt.Sprintf("[%s]", l.prefix)

	if logFlags&(LShowPID) != 0 {
		prefix = fmt.Sprintf("[%5d][%s]", syscall.Getpid(), l.prefix)
	}

	caller := ""
	if logFlags&(LShowCaller) != 0 {
		_, file, line, _ := runtime.Caller(1)
		split := strings.Split(file, "/")
		caller = fmt.Sprintf(" [%s:%d]", split[len(split)-1], line)
	}
	log.Printf("%s %s%s", prefix, fmt.Sprintf(format, args...), caller)
}

func (l *Logger) Fatalf(format string, args ...interface{}) {
	log.Printf("[%s] %s", l.prefix, fmt.Sprintf(format, args...))
	os.Exit(1)
}

func (l *Logger) Level() int {
	return l.level
}

func GetLogLevel() int {
	return LogLevel
}

func SetLogLevel(i int) {
	LogLevel = i
}

func GetLogFlags() int {
	return logFlags
}

func SetLogFlags(i int) {
	logFlags = i
}

func KewlLogLevelNames() {
	NVM.prefix = "NVM  "
	BTW.prefix = "BTW  "
	FYI.prefix = "FYI  "
	WTF.prefix = "WTF?!"
	FUU.prefix = "FUUU!"
	FUUU.prefix = "FUUU!"
}

func BoringLogLevelNames() {
	NVM.prefix = "TRACE"
	BTW.prefix = "DEBUG"
	FYI.prefix = "INFO "
	WTF.prefix = "ERROR"
	FUU.prefix = "FATAL"
	FUUU.prefix = "FATAL"
}
