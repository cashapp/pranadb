package main

import (
	"github.com/squareup/pranadb/errors"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/client"
	"github.com/squareup/pranadb/cmd/prana/commands"
	"github.com/squareup/pranadb/common"

	"github.com/alecthomas/kong"
)

var CLI struct {
	Shell       commands.ShellCommand       `cmd:"" help:"Start a SQL shell for Prana"`
	UploadProto commands.UploadProtoCommand `cmd:"" help:"Upload a protobuf file descriptor set that can be used by Prana to decode sources"`
	Addr        string                      `help:"Address of PranaDB server to connect to." default:"127.0.0.1:6584"`
	UseHTTPAPI  bool                        `name:"use-http-api" help:"If true then will connect using Prana HTTP API, if false then gRPC API." default:"false"`
	TLSConfig   client.TLSConfig            `help:"TLS client configuration" embed:"" prefix:""`
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("%+v\n", err)
	}
}

func run() error {
	defer common.PanicHandler()
	ctx := kong.Parse(&CLI)
	var cl *client.Client
	if CLI.UseHTTPAPI {
		cl = client.NewClientUsingHTTP(CLI.Addr, CLI.TLSConfig)
	} else {
		cl = client.NewClientUsingGRPC(CLI.Addr, CLI.TLSConfig)
	}
	cl.SetExitOnError(true)
	if err := cl.Start(); err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		if err := cl.Stop(); err != nil {
			log.Errorf("failed to close cli %+v", err)
		}
	}()
	return ctx.Run(cl)
}
