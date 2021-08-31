package main

import (
	"github.com/alecthomas/kong"
	"github.com/squareup/pranadb/msggen"
	"log"
	"os"
	"time"
)

type arguments struct {
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
	cfg := arguments{KafkaProperties: make(map[string]string)}
	parser, err := kong.New(&cfg)
	if err != nil {
		return err
	}
	_, err = parser.Parse(args)
	if err != nil {
		return err
	}
	gm, err := msggen.NewGenManager()
	if err != nil {
		return err
	}
	return gm.ProduceMessages(cfg.GeneratorName, cfg.TopicName, cfg.Partitions, cfg.Delay, cfg.NumMessages, cfg.IndexStart, cfg.KafkaProperties)
}
