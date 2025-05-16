package logger

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
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
		if loggerFields, ok := ctx.Value(middleware.TemporalSLoggerKey).(log.Fields); ok {
			l.Slogger.WithFields("requestFields", loggerFields).InfoContext(ctx, msg, data...)
		} else {
			l.Slogger.InfoContext(ctx, msg, data...)
		}
	}
}

func (l *GormSlogLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	if l.config.LogLevel >= logger.Warn {
		if loggerFields, ok := ctx.Value(middleware.TemporalSLoggerKey).(log.Fields); ok {
			l.Slogger.WithFields("requestFields", loggerFields).WarnContext(ctx, msg, data...)
		} else {
			l.Slogger.WarnContext(ctx, msg, data...)
		}
	}
}

func (l *GormSlogLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	if l.config.LogLevel >= logger.Error {
		if loggerFields, ok := ctx.Value(middleware.TemporalSLoggerKey).(log.Fields); ok {
			l.Slogger.WithFields("requestFields", loggerFields).ErrorContext(ctx, msg, data...)
		} else {
			l.Slogger.ErrorContext(ctx, msg, data...)
		}
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
		if loggerFields, ok := ctx.Value(middleware.TemporalSLoggerKey).(log.Fields); ok {
			l.Slogger.With(log.Fields{
				"error":         err,
				"elapsed":       elapsed,
				"rows":          rows,
				"sql":           sql,
				"requestFields": loggerFields,
			}).ErrorContext(ctx, "Database error")
		} else {
			l.Slogger.With(log.Fields{
				"error":   err,
				"elapsed": elapsed,
				"rows":    rows,
				"sql":     sql,
			}).ErrorContext(ctx, "Database error")
		}
	case elapsed > l.config.SlowThreshold && l.config.SlowThreshold != 0 && l.config.LogLevel >= logger.Warn:
		sql, rows := fc()
		if loggerFields, ok := ctx.Value(middleware.TemporalSLoggerKey).(log.Fields); ok {
			l.Slogger.With(log.Fields{
				"elapsed":       elapsed,
				"threshold":     l.config.SlowThreshold,
				"rows":          rows,
				"sql":           sql,
				"requestFields": loggerFields,
			}).WarnContext(ctx, "Slow query")
		} else {
			l.Slogger.With(log.Fields{
				"elapsed":   elapsed,
				"threshold": l.config.SlowThreshold,
				"rows":      rows,
				"sql":       sql,
			}).WarnContext(ctx, "Slow query")
		}
	case l.config.LogLevel == logger.Info:
		sql, rows := fc()
		if loggerFields, ok := ctx.Value(middleware.TemporalSLoggerKey).(log.Fields); ok {
			l.Slogger.With(log.Fields{
				"elapsed":       elapsed,
				"rows":          rows,
				"sql":           sql,
				"requestFields": loggerFields,
			}).DebugContext(ctx, "Query")
		} else {
			l.Slogger.With(log.Fields{
				"elapsed": elapsed,
				"rows":    rows,
				"sql":     sql,
			}).DebugContext(ctx, "Query")
		}
	}
}
