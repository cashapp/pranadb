package main

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/alecthomas/kong"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/conf"
	plog "github.com/squareup/pranadb/log"
	"github.com/squareup/pranadb/server"
	"muzzammil.xyz/jsonc"
)

type cli struct {
	Config string      `help:"Path to config file" type:"existingfile" required:""`
	Node   int         `help:"ID of the node"`
	Log    plog.Config `help:"Configuration for the logger" embed:"" prefix:"log-"`
}

func main() {
	r := &runner{}
	if err := r.run(os.Args[1:], true); err != nil {
		log.Fatal(err.Error())
	}
	select {} // prevent main exiting
}

type runner struct {
	server *server.Server
}

func (r *runner) run(args []string, start bool) error {
	cfg := cli{}
	parser, err := kong.New(&cfg)
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

	b, err := ioutil.ReadFile(cfg.Config)
	if err != nil {
		return err
	}
	serverCfg := conf.Config{}
	// We use jsonc as it supports comments in JSON
	b = jsonc.ToJSON(b)
	if err := json.Unmarshal(b, &serverCfg); err != nil {
		return err
	}
	serverCfg.NodeID = cfg.Node
	s, err := server.NewServer(serverCfg)
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
