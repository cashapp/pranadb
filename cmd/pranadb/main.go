package main

import (
	"github.com/squareup/pranadb/conf"
	"log"
	"net"

	"github.com/alecthomas/kong"
	konghcl "github.com/alecthomas/kong-hcl"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/service"
	"github.com/squareup/pranadb/server"
	"github.com/squareup/pranadb/server/wire"
)

var cli struct {
	Config kong.ConfigFlag `help:"Configuration file to load."`
	NodeID int             `help:"Cluster node identifier." default:"0"`
	Bind   string          `help:"Bind address for Prana server." default:"127.0.0.1:6584"`
}

func main() {
	kctx := kong.Parse(&cli, kong.Configuration(konghcl.Loader, "~/.pranadb.conf", "/etc/pranadb.conf"))

	log.Printf("Starting PranaDB server on %s", cli.Bind)

	l, err := net.Listen("tcp", cli.Bind)
	kctx.FatalIfErrorf(err)

	// TODO parse conf file into Config
	psrv, err := server.NewServer(conf.Config{
		NodeID:     0,
		NumShards:  10,
		TestServer: true,
	})
	kctx.FatalIfErrorf(err)
	pgsrv := wire.New(psrv)

	gsrv := grpc.NewServer(wire.RegisterSessionManager())
	reflection.Register(gsrv)
	service.RegisterPranaDBServiceServer(gsrv, pgsrv)
	err = gsrv.Serve(l)
	kctx.FatalIfErrorf(err)
}
