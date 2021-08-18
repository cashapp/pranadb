package main

import (
	"net"

	"github.com/squareup/pranadb/conf"
	"go.uber.org/zap"

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

	logger := conf.NewLogger()
	zap.ReplaceGlobals(logger)
	logger.Info("Starting PranaDB server", zap.String("bind", cli.Bind))

	l, err := net.Listen("tcp", cli.Bind)
	kctx.FatalIfErrorf(err)

	// TODO parse conf file into Config
	psrv, err := server.NewServer(*(conf.NewTestConfig(0, logger)))
	kctx.FatalIfErrorf(err)
	pgsrv := wire.New(psrv, logger)

	gsrv := grpc.NewServer(wire.RegisterSessionManager(logger))
	reflection.Register(gsrv)
	service.RegisterPranaDBServiceServer(gsrv, pgsrv)
	err = gsrv.Serve(l)
	kctx.FatalIfErrorf(err)
}
