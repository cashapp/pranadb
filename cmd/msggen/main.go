package main

import (
	"log"
	"os"
	"time"

	"github.com/alecthomas/kong"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/msggen"
)

type Arguments struct {
	GeneratorName   string
	TopicName       string
	Partitions      int
	Delay           time.Duration
	NumMessages     int64
	IndexStart      int64
	KafkaProperties map[string]string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	cfg := Arguments{KafkaProperties: make(map[string]string)}
	parser, err := kong.New(&cfg)
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = parser.Parse(args)
	if err != nil {
		return errors.WithStack(err)
	}
	gm, err := msggen.NewGenManager()
	if err != nil {
		return errors.WithStack(err)
	}
	return gm.ProduceMessages(cfg.GeneratorName, cfg.TopicName, cfg.Partitions, cfg.Delay, cfg.NumMessages, cfg.IndexStart, cfg.KafkaProperties)
}
