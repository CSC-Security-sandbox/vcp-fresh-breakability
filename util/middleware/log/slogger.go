package log

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	"go.opentelemetry.io/otel/sdk/log"
)

const (
	requestID            = "x-request-id"
	requestCorrelationID = "x-correlation-id"
)

type LoggerHandlerType string

const (
	LoggerHandlerTypeJSON     LoggerHandlerType = "json"
	LoggerHandlerTypeOtelSlog LoggerHandlerType = "otelslog"
)

func getSlogger(config Config) (*Slogger, error) {
	switch LoggerHandlerType(strings.ToLower(config.HandlerType)) {
	case LoggerHandlerTypeJSON:
		return getSloggerObject(config.LogLevel, config.AddSource), nil
	case LoggerHandlerTypeOtelSlog:
		return getSloggerOtelObject(config.ExporterType)
	default:
		return getSloggerObject(config.LogLevel, config.AddSource), nil
	}
}

type Slogger struct {
	slogger  *slog.Logger
	shutdown func(context.Context) error
}

type LogExporter interface {
	NewExporter() (log.Exporter, error)
}

type StdoutLogExporter struct{}

func (e *StdoutLogExporter) NewExporter() (log.Exporter, error) {
	return stdoutlog.New()
}

func getSloggerOtelObject(exporterType string) (*Slogger, error) {
	shutdown, err := initLoggerProvider(exporterType)
	if err != nil {
		return nil, err
	}
	return &Slogger{
		slogger:  slog.New(otelslog.NewHandler("otelhandler")),
		shutdown: shutdown,
	}, nil
}

// initLoggerProvider initializes a logging exporter to stdout, and configures a log provider for use with a structured logger
func initLoggerProvider(exporterType string) (func(context.Context) error, error) {
	var exporter LogExporter

	switch exporterType {
	case "stdout":
		exporter = &StdoutLogExporter{}
	default:
		return nil, fmt.Errorf("unsupported exporter type: %s", exporterType)
	}

	logExporter, err := exporter.NewExporter()
	if err != nil {
		return nil, err
	}
	loggerProvider := log.NewLoggerProvider(
		log.WithProcessor(log.NewBatchProcessor(logExporter)),
	)
	return loggerProvider.Shutdown, nil
}

func getSloggerObject(loglevel string, addSource bool) *Slogger {
	options := slog.HandlerOptions{
		AddSource: addSource,
		Level:     convertLogLevel(loglevel),
	}
	return &Slogger{
		slogger: slog.New(slog.NewJSONHandler(defaultOutputStream, &options)),
	}
}

// WithFields returns a new logger with the request fields
func (s Slogger) WithFields(fields Fields) Logger {
	return &Slogger{
		slogger: s.slogger.With("fields", fields),
	}
}

func (s *Slogger) Error(args ...interface{}) {
	s.slogger.Error(fmt.Sprint(args...))
}

func (s *Slogger) Errorf(format string, args ...interface{}) {
	s.slogger.Error(fmt.Sprintf(format, args...))
}

func (s *Slogger) Warn(args ...interface{}) {
	s.slogger.Warn(fmt.Sprint(args...))
}

func (s *Slogger) Warnf(format string, args ...interface{}) {
	s.slogger.Warn(fmt.Sprintf(format, args...))
}

func (s *Slogger) Info(args ...interface{}) {
	s.slogger.Info(fmt.Sprint(args...))
}

func (s *Slogger) Infof(format string, args ...interface{}) {
	s.slogger.Info(fmt.Sprintf(format, args...))
}

func (s *Slogger) Debug(args ...interface{}) {
	s.slogger.Debug(fmt.Sprint(args...))
}

func (s *Slogger) Debugf(format string, args ...interface{}) {
	s.slogger.Debug(fmt.Sprintf(format, args...))
}

func (s *Slogger) Shutdown(ctx context.Context) {
	if err := s.shutdown(ctx); err != nil {
		s.slogger.Error(fmt.Sprintf("Failed to shutdown: %v", err))
	}
}

// convertLogLevel retrieves the log level from an environment variable
func convertLogLevel(loglevel string) slog.Level {
	levelMap := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
	}
	if level, exists := levelMap[strings.ToLower(loglevel)]; exists {
		return level
	}
	return slog.LevelInfo
}
