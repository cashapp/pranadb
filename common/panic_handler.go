package common

import (
	"os"
	"runtime/debug"

	log "github.com/sirupsen/logrus"
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
