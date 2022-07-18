package logadaptor

import (
	"github.com/lni/dragonboat/v3/logger"
	log "github.com/sirupsen/logrus"
	"sync/atomic"
)

/*
This adaptor allows us to plug the dragonboat logging into the logrus logger we use in Prana.
*/

func LogrusLogFactory(pkgName string) logger.ILogger {
	return &LogrusILogger{}
}

type LogrusILogger struct {
	level int64
}

func (l *LogrusILogger) getLevel() logger.LogLevel {
	return logger.LogLevel(atomic.LoadInt64(&l.level))
}

func (l *LogrusILogger) SetLevel(level logger.LogLevel) {
	atomic.StoreInt64(&l.level, int64(level))
}

func (l *LogrusILogger) Debugf(format string, args ...interface{}) {
	if l.getLevel() >= logger.DEBUG {
		log.Debugf(format, args...)
	}
}

func (l *LogrusILogger) Infof(format string, args ...interface{}) {
	if l.getLevel() >= logger.INFO {
		log.Infof(format, args...)
	}
}

func (l *LogrusILogger) Warningf(format string, args ...interface{}) {
	if l.getLevel() >= logger.WARNING {
		log.Warnf(format, args...)
	}
}

func (l *LogrusILogger) Errorf(format string, args ...interface{}) {
	if l.getLevel() >= logger.ERROR {
		log.Errorf(format, args...)
	}
}

func (l *LogrusILogger) Panicf(format string, args ...interface{}) {
	log.Fatalf(format, args...)
}
