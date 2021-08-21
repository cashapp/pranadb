package main

import (
	"github.com/alecthomas/kong"
	konghcl "github.com/alecthomas/kong-hcl"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/server"
)

var cli struct {
	Config kong.ConfigFlag `help:"Configuration file to load."`
	NodeID int             `help:"Cluster node identifier." default:"0"`
	Bind   string          `help:"Bind address for Prana server." default:"127.0.0.1:6584"`
}

func main() {
	kctx := kong.Parse(&cli, kong.Configuration(konghcl.Loader, "~/.pranadb.conf", "/etc/pranadb.conf"))

	// TODO parse conf file into Config
	psrv, err := server.NewServer(*(conf.NewTestConfig(0)))
	kctx.FatalIfErrorf(err)
	err = psrv.Start()
	kctx.FatalIfErrorf(err)
}
