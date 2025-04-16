package log

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

type Fields map[string]interface{}

type Logger interface {
	WithFields(fields Fields) Logger
	Errorf(format string, args ...interface{})
	Error(args ...interface{})
	Warnf(format string, args ...interface{})
	Warn(args ...interface{})
	Infof(format string, args ...interface{})
	Info(args ...interface{})
	Debugf(format string, args ...interface{})
	Debug(args ...interface{})
}

type contextKey struct {
	name string
}

var (
	TraceKey            = &contextKey{"trace"}
	defaultOutputStream = os.Stdout
	config              Config
)

type Config struct {
	LogLevel     string
	AddSource    bool
	HandlerType  string
	ExporterType string
}

func init() {
	config = Config{
		AddSource:    env.AddSource,
		ExporterType: env.ExporterType,
		HandlerType:  env.SlogHandlerType,
		LogLevel:     env.LogLevel,
	}
}

type LoggerType string

const (
	LoggerTypeSlog LoggerType = "slog"
)

// getLogger function with a switch case for logger types
func getLogger(config Config) Logger {
	var logger Logger
	var err error
	switch LoggerType(strings.ToLower(env.LoggerType)) {
	case LoggerTypeSlog:
		logger, err = getSlogger(config)
	// Add other logger types here if needed
	default:
		logger, err = getSlogger(config)
	}
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger: %v", err))
	}
	return logger
}

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger := NewRequestLogger(r)
		ctx := context.WithValue(r.Context(), TraceKey, logger)
		r = r.WithContext(ctx)
		next.ServeHTTP(w, r)
	})
}

// NewLogger initializes a logger for development purposes
func NewLogger() Logger {
	logger := getLogger(config)
	return logger
}

func NewRequestLogger(r *http.Request) Logger {
	return extractFieldsFromHttpRequest(r)
}

func extractFieldsFromHttpRequest(r *http.Request) Logger {
	correlationID := r.Header.Get(requestCorrelationID)
	logger := NewLogger().WithFields(Fields{
		"requestCorrelationID": correlationID,
		"requestID":            r.Header.Get(requestID),
		"traceMethod":          r.Method,
		"traceURL":             r.URL.String(),
	})
	return logger
}
