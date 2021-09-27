package common

import (
	log "github.com/sirupsen/logrus"
	"os"
	"runtime/debug"
)

func PanicHandler() {
	r := recover()
	if r == nil {
		return // no panic underway
	}

	log.Errorf("Panic occurred in Prana %v\n", r)

	// print debug stack
	debug.PrintStack()

	os.Exit(1)
}
