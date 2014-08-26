package logging

import "github.com/dmichellis/gocassos"

// so you can import . github.com/dmichellis/gocassos/logging and use those
var LogLevel = &gocassos.LogLevel
var SetLogFlags = gocassos.SetLogFlags
var GetLogFlags = gocassos.GetLogFlags
var SetLogLevel = gocassos.SetLogLevel
var GetLogLevel = gocassos.GetLogLevel
var KewlLogLevelNames = gocassos.KewlLogLevelNames
var BoringLogLevelNames = gocassos.BoringLogLevelNames

const (
	LShowCaller = gocassos.LShowCaller
	LShowPID    = gocassos.LShowPID
)

var FUUU = &gocassos.FUUU
var FUU = &gocassos.FUUU
var WTF = &gocassos.WTF
var FYI = &gocassos.FYI
var NVM = &gocassos.NVM
var BTW = &gocassos.BTW
