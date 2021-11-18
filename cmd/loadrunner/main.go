package main

import (
	"github.com/alecthomas/kong"
	konghcl "github.com/alecthomas/kong-hcl/v2"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/loadrunner"
	plog "github.com/squareup/pranadb/log"
	"log"
	"os"
)

type arguments struct {
	Config kong.ConfigFlag   `help:"Path to config file" type:"existingfile" required:""`
	Log    plog.Config       `help:"Configuration for the logger" embed:"" prefix:"log-"`
	Server loadrunner.Config `help:"LoadRunner configuration" embed:"" prefix:""`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
	select {} // prevent main exiting
}

func run(args []string) error {
	cfg := arguments{}
	parser, err := kong.New(&cfg, kong.Configuration(konghcl.Loader))
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = parser.Parse(args)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := cfg.Server.Validate(); err != nil {
		return err
	}
	pr := loadrunner.NewLoadRunner(cfg.Server)
	return pr.Start()
}
