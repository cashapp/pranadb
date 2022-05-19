package common

import (
	"fmt"
	"os"
	"runtime/debug"
)

func PanicHandler() {
	r := recover()
	if r == nil {
		return // no panic underway
	}

	fmt.Printf("Panic occurred in Prana %v\n", r)

	// print debug stack
	debug.PrintStack()

	os.Exit(1)
}
