package raft

import "log"

// Debugging
const Debug = True

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
