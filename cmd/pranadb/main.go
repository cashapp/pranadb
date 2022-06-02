package main

import (
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/squareup/pranadb/errors"

	"github.com/alecthomas/kong"
	konghcl "github.com/alecthomas/kong-hcl/v2"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/conf"
	plog "github.com/squareup/pranadb/log"
	"github.com/squareup/pranadb/server"
)

type arguments struct {
	Config kong.ConfigFlag `help:"Path to config file" type:"existingfile" required:""`
	Log    plog.Config     `help:"Configuration for the logger" embed:"" prefix:"log-"`
	Server conf.Config     `help:"Server configuration" embed:"" prefix:""`
}

func main() {
	//defer common.PanicHandler()
	r := &runner{}
	if err := r.run(os.Args[1:], true); err != nil {
		log.WithError(err).Fatal("startup failed")
	}
	r.waitForShutdown()
}

type runner struct {
	server *server.Server
}

func (r *runner) run(args []string, start bool) error {
	cfg := arguments{}
	parser, err := kong.New(&cfg, kong.Configuration(konghcl.Loader))
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = parser.Parse(args)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := cfg.Log.Configure(); err != nil {
		return errors.WithStack(err)
	}

	s, err := server.NewServer(cfg.Server)
	if err != nil {
		return errors.WithStack(err)
	}
	r.server = s
	if start {
		if err := s.Start(); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (r *runner) waitForShutdown() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		sig := <-signals
		logger := log.WithField("signal", sig)
		logger.Info("Stopping Prana server")
		err := r.server.Stop()
		if err != nil {
			logger.WithError(err).Error("Failed to stop Prana server")
		} else {
			logger.Info("Prana server stopped")
		}
	}()
	wg.Wait()
}

func (r *runner) getServer() *server.Server {
	return r.server
}
