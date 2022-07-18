package logadaptor

import (
	"github.com/lni/dragonboat/v3/logger"
	log "github.com/sirupsen/logrus"
)

/*
This adaptor allows us to plug the dragonboat logging into the logrus logger we use in Prana.
*/

func LogrusLogFactory(pkgName string) logger.ILogger {
	return &LogrusILogger{}
}

type LogrusILogger struct {
	level logger.LogLevel
}

func (l *LogrusILogger) SetLevel(level logger.LogLevel) {
	l.level = level
}

func (l *LogrusILogger) Debugf(format string, args ...interface{}) {
	if l.level >= logger.DEBUG {
		log.Debugf(format, args...)
	}
}

func (l *LogrusILogger) Infof(format string, args ...interface{}) {
	if l.level >= logger.INFO {
		log.Infof(format, args...)
	}
}

func (l *LogrusILogger) Warningf(format string, args ...interface{}) {
	if l.level >= logger.WARNING {
		log.Warnf(format, args...)
	}
}

func (l *LogrusILogger) Errorf(format string, args ...interface{}) {
	if l.level >= logger.ERROR {
		log.Errorf(format, args...)
	}
}

func (l *LogrusILogger) Panicf(format string, args ...interface{}) {
	log.Fatalf(format, args...)
}
