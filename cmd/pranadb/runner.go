package main

import (
	"encoding/json"
	"fmt"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/server"
	"io/ioutil"
	"log"
	"muzzammil.xyz/jsonc"
)

type runner struct {
	server *server.Server
}

func (r *runner) run(args []string, start bool) {
	if len(args) != 2 {
		fmt.Println("Please run with -conf <config_file>")
	}
	confFile := args[1]
	b, err := ioutil.ReadFile(confFile)
	if err != nil {
		log.Fatal(err)
	}
	cfg := conf.Config{}
	// We use jsonc as it supports comments in JSON
	b = jsonc.ToJSON(b)
	if err := json.Unmarshal(b, &cfg); err != nil {
		log.Fatal(err)
	}
	s, err := server.NewServer(cfg)
	if err != nil {
		log.Fatal(err)
	}
	r.server = s
	if start {
		if err := s.Start(); err != nil {
			log.Fatal(err)
		}
	}
}

func (r *runner) getServer() *server.Server {
	return r.server
}
