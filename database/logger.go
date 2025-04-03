// logger.go
package database

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm/logger"
)

// Logger interface abstracts logging functionality
type Logger interface {
	Debug(msg string, fields ...interface{})
	Info(msg string, fields ...interface{})
	Warn(msg string, fields ...interface{})
	Error(msg string, fields ...interface{})
}

// GormLogger implements gorm's logger.Interface
type GormLogger struct {
	logger Logger
	config logger.Config
}

// NewGormLogger creates a new GORM logger adapter
func NewGormLogger(l Logger) logger.Interface {
	return &GormLogger{
		logger: l,
		config: logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: true,
			Colorful:                  false,
		},
	}
}

func (l *GormLogger) LogMode(level logger.LogLevel) logger.Interface {
	newLogger := *l
	newLogger.config.LogLevel = level
	return &newLogger
}

func (l *GormLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	if l.config.LogLevel >= logger.Info {
		l.logger.Info(fmt.Sprintf(msg, data...))
	}
}

func (l *GormLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	if l.config.LogLevel >= logger.Warn {
		l.logger.Warn(fmt.Sprintf(msg, data...))
	}
}

func (l *GormLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	if l.config.LogLevel >= logger.Error {
		l.logger.Error(fmt.Sprintf(msg, data...))
	}
}

func (l *GormLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.config.LogLevel <= logger.Silent {
		return
	}

	elapsed := time.Since(begin)
	switch {
	case err != nil && l.config.LogLevel >= logger.Error:
		sql, rows := fc()
		l.logger.Error("Database error",
			"error", err,
			"elapsed", elapsed,
			"rows", rows,
			"sql", sql,
		)
	case elapsed > l.config.SlowThreshold && l.config.SlowThreshold != 0 && l.config.LogLevel >= logger.Warn:
		sql, rows := fc()
		l.logger.Warn("Slow query",
			"elapsed", elapsed,
			"threshold", l.config.SlowThreshold,
			"rows", rows,
			"sql", sql,
		)
	case l.config.LogLevel == logger.Info:
		sql, rows := fc()
		l.logger.Debug("Query",
			"elapsed", elapsed,
			"rows", rows,
			"sql", sql,
		)
	}
}
