package main

import (
	"log"
	"os"

	"github.com/alecthomas/kong"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/verifier"
)

type Arguments struct {
	GeneratorName string
	NumMessages   int64
	IndexStart    int64
	RandSrc       int64
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	cfg := Arguments{}
	parser, err := kong.New(&cfg)
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = parser.Parse(args)
	if err != nil {
		return errors.WithStack(err)
	}
	v, err := verifier.NewVerifier(cfg.GeneratorName)
	if err != nil {
		return errors.WithStack(err)
	}
	return v.VerifyMessages(cfg.NumMessages, cfg.IndexStart, cfg.RandSrc)
}
