package logger

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm/logger"
)

type GormSlogLogger struct {
	Slogger log.Logger
	config  logger.Config
}

func NewGormLogger(slogger log.Logger, logLevel logger.LogLevel) logger.Interface {
	return &GormSlogLogger{
		Slogger: slogger,
		config: logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logLevel,
			IgnoreRecordNotFoundError: true,
			Colorful:                  false,
		},
	}
}

func (l *GormSlogLogger) LogMode(level logger.LogLevel) logger.Interface {
	newLogger := *l
	newLogger.config.LogLevel = level
	return &newLogger
}

func (l *GormSlogLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	if l.config.LogLevel >= logger.Info {
		l.Slogger.Infof(msg, data...)
	}
}

func (l *GormSlogLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	if l.config.LogLevel >= logger.Warn {
		l.Slogger.Warnf(msg, data...)
	}
}

func (l *GormSlogLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	if l.config.LogLevel >= logger.Error {
		l.Slogger.Errorf(msg, data...)
	}
}

func (l *GormSlogLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.config.LogLevel <= logger.Silent {
		return
	}

	elapsed := time.Since(begin)
	switch {
	case err != nil && l.config.LogLevel >= logger.Error:
		sql, rows := fc()
		l.Slogger.With(log.Fields{
			"error":   err,
			"elapsed": elapsed,
			"rows":    rows,
			"sql":     sql,
		}).Error("Database error")
	case elapsed > l.config.SlowThreshold && l.config.SlowThreshold != 0 && l.config.LogLevel >= logger.Warn:
		sql, rows := fc()
		l.Slogger.With(log.Fields{
			"elapsed":   elapsed,
			"threshold": l.config.SlowThreshold,
			"rows":      rows,
			"sql":       sql,
		}).Warn("Slow query")
	case l.config.LogLevel == logger.Info:
		sql, rows := fc()
		l.Slogger.With(log.Fields{
			"elapsed": elapsed,
			"rows":    rows,
			"sql":     sql,
		}).Debug("Query")
	}
}
