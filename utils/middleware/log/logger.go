package log

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"net/http"
	"os"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
)

type Fields map[string]interface{}

type Logger interface {
	Errorf(format string, args ...any)
	Error(format string, args ...any)
	Warnf(format string, args ...any)
	Warn(format string, args ...any)
	Infof(format string, args ...any)
	Info(format string, args ...any)
	Debugf(format string, args ...any)
	Debug(format string, args ...any)

	// InfoContext logs the message with context for traceability.
	InfoContext(ctx context.Context, msg string, args ...any)
	WarnContext(ctx context.Context, msg string, args ...any)
	ErrorContext(ctx context.Context, msg string, args ...any)
	DebugContext(ctx context.Context, msg string, args ...any)

	// WithFields returns a new logger with the request fields grouped under a specific field name.
	WithFields(fieldName string, fields Fields) Logger
	// With returns a new logger with the request fields added directly as key-value pairs.
	With(fields Fields) Logger
}

var (
	defaultOutputStream = os.Stdout
	config              Config
	uuidNewString       = uuid.NewString
)

type Config struct {
	LogLevel   string
	AddSource  bool
	LoggerType string
}

func init() {
	config = Config{
		AddSource:  env.AddSource,
		LogLevel:   env.LogLevel,
		LoggerType: env.LoggerType,
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
	switch LoggerType(strings.ToLower(config.LoggerType)) {
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

// LoggingMiddleware injects a logger into the request context
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Populating logger with fields like requestID, correlationID, traceURL, etc
		logger, logFields := NewRequestLogger(r)
		ctx := context.WithValue(r.Context(), middleware.ContextSLoggerKey, logger)
		// Populating the context with fields required for logger
		// creation. These values will be used in context propagation
		// and logger initialization later
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, logFields)
		r = r.WithContext(ctx)
		next.ServeHTTP(w, r)
	})
}

// NewLogger initializes a logger for development purposes
func NewLogger() Logger {
	logger := getLogger(config)
	return logger
}

func NewRequestLogger(r *http.Request) (Logger, Fields) {
	return extractFieldsFromHttpRequest(r)
}

func extractFieldsFromHttpRequest(r *http.Request) (Logger, Fields) {
	correlationID := GetCorrelationID(r)
	requestID := GetRequestID(r)
	fields := Fields{
		"requestCorrelationID": correlationID,
		"requestID":            requestID,
		"traceMethod":          r.Method,
		"traceURL":             r.URL.String(),
	}
	logger := NewLogger().WithFields("requestFields", fields)
	return logger, fields
}

// GetCorrelationID retrieves the correlation ID from the request header.
func GetCorrelationID(req *http.Request) string {
	return GetHeaderID(req, RequestCorrelationID)
}

// GetRequestID retrieves the request ID from the request header.
func GetRequestID(req *http.Request) string {
	return GetHeaderID(req, RequestID)
}

// GetHeaderID retrieves or generates a unique ID for the given header key.
func GetHeaderID(req *http.Request, headerKey string) string {
	id := req.Header.Get(headerKey)
	if id == "" {
		id = uuidNewString()
		req.Header.Set(headerKey, id)
	}
	return id
}
