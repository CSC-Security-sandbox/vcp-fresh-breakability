package log

import (
	"context"
	"fmt"
	"runtime"
	"strings"

	"go.opentelemetry.io/otel/trace"
	"log/slog"
)

const (
	RequestID            = "x-request-id"
	RequestCorrelationID = "x-correlation-id"
)

type Slogger struct {
	slogger *slog.Logger
}

type Source struct {
	Function string `json:"function"`
	File     string `json:"file"`
	Line     int    `json:"line"`
}

func getSlogger(config Config) (*Slogger, error) {
	return getSloggerObject(config.LogLevel, config.AddSource), nil
}

func getSloggerObject(loglevel string, addSource bool) *Slogger {
	options := slog.HandlerOptions{
		AddSource:   addSource,
		Level:       convertLogLevel(loglevel),
		ReplaceAttr: replacer,
	}
	jsonHandler := slog.NewJSONHandler(defaultOutputStream, &options)
	logger := slog.New(handlerWithSpanContext(jsonHandler))
	return &Slogger{
		slogger: logger,
	}
}

// replacer customizes log attribute keys to align with Google Cloud Logging conventions.
func replacer(groups []string, a slog.Attr) slog.Attr {
	switch a.Key {
	case slog.LevelKey:
		a.Key = "severity"
		if level := a.Value.Any().(slog.Level); level == slog.LevelWarn {
			a.Value = slog.StringValue("WARNING")
		}
	case slog.TimeKey:
		a.Key = "timestamp"
	case slog.MessageKey:
		a.Key = "message"
	}
	return a
}

// WithFields returns a new logger with the request fields grouped under a specific field name.
// Example: "fieldName": { "key1": "value1", "key2": "value2" }
func (s *Slogger) WithFields(fieldName string, fields Fields) Logger {
	newLogger := s.slogger.With(fieldName, fields)
	return &Slogger{
		slogger: newLogger,
	}
}

// With returns a new logger with the request fields added directly as key-value pairs.
// Example: "key1": "value1", "key2": "value2"
func (s *Slogger) With(fields Fields) Logger {
	attrs := make([]any, 0, len(fields)*2)
	for k, v := range fields {
		attrs = append(attrs, k, v)
	}
	newLogger := s.slogger.With(attrs...)
	return &Slogger{
		slogger: newLogger,
	}
}

func (s *Slogger) Error(format string, args ...any) {
	s.slogger.Error(format, args...)
}

func (s *Slogger) Errorf(format string, args ...any) {
	s.slogger.Error(fmt.Sprintf(format, args...))
}

func (s *Slogger) Warn(format string, args ...any) {
	s.slogger.Warn(format, args...)
}

func (s *Slogger) Warnf(format string, args ...any) {
	s.slogger.Warn(fmt.Sprintf(format, args...))
}

func (s *Slogger) Info(format string, args ...any) {
	s.slogger.Info(format, args...)
}

func (s *Slogger) Infof(format string, args ...any) {
	s.slogger.Info(fmt.Sprintf(format, args...))
}

func (s *Slogger) Debug(format string, args ...any) {
	source := slog.Any("sourceLocation", getSource())
	s.slogger.Debug(format, append(args, source)...)
}

func (s *Slogger) Debugf(format string, args ...any) {
	source := slog.Any("sourceLocation", getSource())
	s.slogger.Debug(fmt.Sprintf(format, args...), source)
}

// getSource retrieves the source information (function name, file, and line number) of the caller.
func getSource() *Source {
	pc, file, line, ok := runtime.Caller(2)
	if !ok {
		return &Source{}
	}
	fn := runtime.FuncForPC(pc)
	functionName := ""
	if fn != nil {
		parts := strings.Split(fn.Name(), "/")
		functionName = parts[len(parts)-1]
	}
	return &Source{
		Function: functionName,
		File:     file,
		Line:     line,
	}
}

// InfoContext logs an informational message with context.
func (s *Slogger) InfoContext(ctx context.Context, msg string, args ...any) {
	s.slogger.InfoContext(ctx, msg, args...)
}

// WarnContext logs a warning message with context.
func (s *Slogger) WarnContext(ctx context.Context, msg string, args ...any) {
	s.slogger.WarnContext(ctx, msg, args...)
}

// ErrorContext logs an error message with context.
func (s *Slogger) ErrorContext(ctx context.Context, msg string, args ...any) {
	s.slogger.ErrorContext(ctx, msg, args...)
}

// DebugContext logs a debug message with context.
func (s *Slogger) DebugContext(ctx context.Context, msg string, args ...any) {
	s.slogger.DebugContext(ctx, msg, args...)
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

// handlerWithSpanContext adds attributes from the span context
func handlerWithSpanContext(h slog.Handler) *spanContextLogHandler {
	return &spanContextLogHandler{next: h}
}

// spanContextLogHandler is a slog.Handler which adds attributes from the
// span context.
type spanContextLogHandler struct {
	next slog.Handler
}

// Handle overrides slog.Handler's Handle method. This adds attributes from the
// span context to the slog.Record.
func (t *spanContextLogHandler) Handle(ctx context.Context, record slog.Record) error {
	// Get the SpanContext from the context.
	if s := trace.SpanContextFromContext(ctx); s.IsValid() {
		// Adding trace context attributes following Cloud Logging structured log format described
		record.AddAttrs(
			slog.Any("logging.googleapis.com/trace", s.TraceID()),
		)
		record.AddAttrs(
			slog.Any("logging.googleapis.com/spanId", s.SpanID()),
		)
		record.AddAttrs(
			slog.Bool("logging.googleapis.com/trace_sampled", s.TraceFlags().IsSampled()),
		)
	}
	return t.next.Handle(ctx, record)
}

func (t *spanContextLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &spanContextLogHandler{next: t.next.WithAttrs(attrs)}
}

func (t *spanContextLogHandler) WithGroup(name string) slog.Handler {
	return &spanContextLogHandler{next: t.next.WithGroup(name)}
}

func (t *spanContextLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return t.next.Enabled(ctx, level)
}
