package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"strconv"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/perrors"
	"github.com/squareup/pranadb/server"
	"muzzammil.xyz/jsonc"
)

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
	if len(args) != 4 {
		return errors.New("please run with -conf <config_file> -node <node_id>")
	}
	// Happy to use Kong here! Just couldn't figure out quickly how to get it to parse the config file
	sNodeID := args[3]
	nodeID, err := strconv.ParseInt(sNodeID, 10, 32)
	if err != nil {
		return err
	}
	confFile := args[1]
	b, err := ioutil.ReadFile(confFile)
	if err != nil {
		return err
	}
	cfg := conf.Config{}
	// We use jsonc as it supports comments in JSON
	b = jsonc.ToJSON(b)
	if err := json.Unmarshal(b, &cfg); err != nil {
		return err
	}
	cfg.NodeID = int(nodeID)
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := configureLogging(cfg); err != nil {
		return err
	}
	s, err := server.NewServer(cfg)
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

func configureLogging(cfg conf.Config) error {
	if cfg.LogFile != "" && cfg.LogFile != "-" {
		f, err := os.Create(cfg.LogFile)
		if err != nil {
			return err
		}
		log.SetOutput(f)
	}
	if cfg.LogLevel != "" {
		level, err := log.ParseLevel(cfg.LogLevel)
		if err != nil {
			return err
		}
		log.SetLevel(level)
	}
	switch cfg.LogFormat {
	case "", "text":
		// default, do nothing
		break
	case "json":
		log.SetFormatter(&log.JSONFormatter{})
	default:
		return perrors.NewInvalidConfigurationError("log format must be either text or json")
	}
	return nil
}
