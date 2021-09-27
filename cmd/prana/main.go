package main

import (
	"github.com/squareup/pranadb/common"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/client"
	"github.com/squareup/pranadb/cmd/prana/commands"

	"github.com/alecthomas/kong"
)

var CLI struct {
	Shell       commands.ShellCommand       `cmd:"" help:"Start a SQL shell for Prana"`
	UploadProto commands.UploadProtoCommand `cmd:"" help:"Upload a protobuf file descriptor set that can be used by Prana to decode sources"`
	Addr        string                      `help:"Address of PranaDB server to connect to." default:"127.0.0.1:6584"`
}

func main() {
	defer common.PanicHandler()
	if err := run(); err != nil {
		log.Fatalf("%+v\n", err)
	}
}

func run() error {
	ctx := kong.Parse(&CLI)

	cl := client.NewClient(CLI.Addr, time.Second*5)
	if err := cl.Start(); err != nil {
		return err
	}
	defer func() {
		if err := cl.Stop(); err != nil {
			log.Errorf("failed to close cli %v", err)
		}
	}()
	return ctx.Run(cl)
}
