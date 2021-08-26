package main

import (
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/alecthomas/kong"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/conf"
	plog "github.com/squareup/pranadb/log"
	"github.com/squareup/pranadb/server"
	"muzzammil.xyz/jsonc"
)

type cli struct {
	Config kong.ConfigFlag `help:"Path to config file" type:"existingfile" required:""`
	Log    plog.Config     `help:"Configuration for the logger" embed:"" prefix:"log-"`
	Server conf.Config     `help:"Server configuration" embed:"" prefix:""`
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
	parser, err := kong.New(&cfg, kong.Configuration(JSONCResolver))
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

// JSONCResolver returns a Resolver that retrieves values from a JSONC source.
func JSONCResolver(r io.Reader) (kong.Resolver, error) {
	values := map[string]interface{}{}
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	err = jsonc.Unmarshal(data, &values)
	if err != nil {
		return nil, err
	}
	var f kong.ResolverFunc = func(context *kong.Context, parent *kong.Path, flag *kong.Flag) (interface{}, error) {
		name := strings.ReplaceAll(flag.Name, "-", "_")
		raw, ok := values[name]
		if ok {
			return raw, nil
		}
		raw = values
		for _, part := range strings.Split(name, ".") {
			if values, ok := raw.(map[string]interface{}); ok {
				raw, ok = values[part]
				if !ok {
					return nil, nil
				}
			} else {
				return nil, nil
			}
		}
		return raw, nil
	}

	return f, nil
}
