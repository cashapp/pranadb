package dragon

import (
	"github.com/lni/dragonboat/v3/logger"
	log "github.com/sirupsen/logrus"
)

/*
This adaptor allows us to plug the dragonboat logging into the logrus logger we use in Prana.
*/

func init() {
	logger.SetLoggerFactory(logrusLogFactory)
}

func logrusLogFactory(pkgName string) logger.ILogger {
	return &logrusILogger{}
}

type logrusILogger struct {
}

func (l *logrusILogger) SetLevel(level logger.LogLevel) {
	switch level {
	case logger.CRITICAL:
		log.SetLevel(log.FatalLevel)
	case logger.ERROR:
		log.SetLevel(log.ErrorLevel)
	case logger.WARNING:
		log.SetLevel(log.WarnLevel)
	case logger.DEBUG:
		log.SetLevel(log.DebugLevel)
	case logger.INFO:
		log.SetLevel(log.InfoLevel)
	}
}

func (l *logrusILogger) Debugf(format string, args ...interface{}) {
	log.Debugf(format, args...)
}

func (l *logrusILogger) Infof(format string, args ...interface{}) {
	log.Infof(format, args...)
}

func (l *logrusILogger) Warningf(format string, args ...interface{}) {
	log.Warnf(format, args...)
}

func (l *logrusILogger) Errorf(format string, args ...interface{}) {
	log.Errorf(format, args...)
}

func (l *logrusILogger) Panicf(format string, args ...interface{}) {
	log.Fatalf(format, args...)
}
