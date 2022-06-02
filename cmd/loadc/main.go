package main

import (
	"github.com/alecthomas/kong"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/cmd/loadc/runner"
)

var CLI struct {
	Addr       string `help:"Address of LoadRunner server to connect to." default:"127.0.0.1:6684"`
	ScriptFile string `type:"existingfile" help:"The script to run"`
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("%+v\n", err)
	}
}

func run() error {
	//defer common.PanicHandler()
	kong.Parse(&CLI)
	run := runner.NewRunner(CLI.ScriptFile, CLI.Addr)
	return run.Run()
}
