package main

import (
	"encoding/json"
	"fmt"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/server"
	"io/ioutil"
	"log"
	"muzzammil.xyz/jsonc"
	"os"
	"strconv"
)

func main() {
	r := &runner{}
	if r.run(os.Args[1:], true) {
		select {} // prevent main exiting
	}
}

type runner struct {
	server *server.Server
}

func (r *runner) run(args []string, start bool) bool {
	if len(args) != 4 {
		fmt.Println("Please run with -conf <config_file> -node <node_id>")
		return false
	}
	// Happy to use Kong here! Just couldn't figure out quickly how to get it to parse the config file
	sNodeID := args[3]
	nodeID, err := strconv.ParseInt(sNodeID, 10, 32)
	if err != nil {
		log.Fatal(err)
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
	cfg.NodeID = int(nodeID)
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
	return true
}

func (r *runner) getServer() *server.Server {
	return r.server
}
