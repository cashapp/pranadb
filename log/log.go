package log

import (
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/perrors"
	"os"
)

// Config contains the configuration for the global logger.
type Config struct {
	Format string `help:"Format to write log lines in" enum:"text,json" default:"text"`
	Level  string `help:"Lowest log level that will be emitted" enum:"trace,debug,info,warn,error" default:"info"`
	File   string `help:"File to direct logs to. If left blank, or '-', logs will go to stdout" default:"-"`
}

// Configure the global logger
func (cfg *Config) Configure() error {
	if cfg.File != "" && cfg.File != "-" {
		f, err := os.Create(cfg.File)
		if err != nil {
			return err
		}
		log.SetOutput(f)
	}
	if cfg.Level != "" {
		level, err := log.ParseLevel(cfg.Level)
		if err != nil {
			return err
		}
		log.SetLevel(level)
	}
	switch cfg.Format {
	case "text":
		// default, do nothing
		break
	case "json":
		log.SetFormatter(&log.JSONFormatter{})
	default:
		return perrors.NewInvalidConfigurationError("log format must be either text or json")
	}
	return nil
}
