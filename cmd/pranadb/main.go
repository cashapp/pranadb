package main

import (
	"github.com/alecthomas/kong"
	"github.com/alecthomas/kong-hcl/v2"
	"log"
	"os"

	"github.com/squareup/pranadb/conf"
	plog "github.com/squareup/pranadb/log"
	"github.com/squareup/pranadb/server"
)

type cli struct {
	Config kong.ConfigFlag `help:"Path to config file" type:"existingfile" required:""`
	Log    plog.Config     `help:"Configuration for the logger" embed:"" prefix:"log-"`
	Server conf.Config     `help:"Server configuration" embed:"" prefix:""`
}

func main() {
	r := &runner{}
	if err := r.run(os.Args[1:], true); err != nil {
		log.Printf("startup failed %v", err)
	}
	select {} // prevent main exiting
}

type runner struct {
	server *server.Server
}

func (r *runner) run(args []string, start bool) error {
	cfg := cli{}
	parser, err := kong.New(&cfg, kong.Configuration(konghcl.Loader))
	if err != nil {
		return err
	}
	_, err = parser.Parse(args)
	if err != nil {
		return err
	}
	if err := cfg.Log.Configure(); err != nil {
		return err
	}

	s, err := server.NewServer(cfg.Server)
	if err != nil {
		return err
	}
	r.server = s
	if start {
		if err := s.Start(); err != nil {
			return err
		}
	}
	return nil
}

func (r *runner) getServer() *server.Server {
	return r.server
}
