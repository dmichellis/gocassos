package gocassos

import (
	"fmt"
	"log"
	"os"
)

func (l *Logger) Enabled() bool {
	if LogLevel < l.level {
		return false
	}
	return true
}

func (l *Logger) Printf(format string, args ...interface{}) {
	if !l.Enabled() {
		return
	}
	log.Printf("[%s] %s", l.prefix, fmt.Sprintf(format, args...))
}

func (l *Logger) Fatalf(format string, args ...interface{}) {
	log.Printf("[%s] %s", l.prefix, fmt.Sprintf(format, args...))
	os.Exit(1)
}

func (l *Logger) Level() int {
	return l.level
}

var LogLevel int = 99
var NVM = Logger{prefix: "NVM  ", level: 4}
var BTW = Logger{prefix: "BTW  ", level: 3}
var FYI = Logger{prefix: "FYI  ", level: 2}
var WTF = Logger{prefix: "WTF?!", level: 1}
var FUUU = Logger{prefix: "FUUU!", level: 0}
var FUU = Logger{prefix: "FUUU!", level: 0}

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
