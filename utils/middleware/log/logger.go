package log

import (
	"context"
	"errors"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"google.golang.org/api/option"
	"net/http"
	"os"
	"strings"

	mexporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric"
	texporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
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
)

type Config struct {
	LogLevel  string
	AddSource bool
}

func init() {
	config = Config{
		AddSource: env.AddSource,
		LogLevel:  env.LogLevel,
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

// LoggingMiddleware injects a logger into the request context
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger := NewRequestLogger(r)
		ctx := context.WithValue(r.Context(), middleware.ContextSLoggerKey, logger)
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
	logger := NewLogger().WithFields("requestFields", Fields{
		"requestCorrelationID": correlationID,
		"requestID":            r.Header.Get(requestID),
		"traceMethod":          r.Method,
		"traceURL":             r.URL.String(),
	})
	return logger
}

// SetupOpenTelemetry sets up the OpenTelemetry SDK and exporters for metrics and
// traces. If it does not return an error, call shutdown for proper cleanup.
func SetupOpenTelemetry(ctx context.Context) (shutdown func(context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error

	// shutdown combines shutdown functions from multiple OpenTelemetry
	// components into a single function.
	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	otel.SetTextMapPropagator(propagation.TraceContext{})

	// If User wants to run this application locally and check the traces in Google Cloud Tracer, both these environment variables need to be set accordingly:
	// - GOOGLE_CLOUD_PROJECT : <name_of_your_consumer_project>
	// - GOOGLE_APPLICATION_CREDENTIALS : <Service_json_consumer_project>
	traceExporter, err := texporter.New(texporter.WithProjectID(env.OtelGoogleProjectID),
		texporter.WithTraceClientOptions([]option.ClientOption{option.WithTelemetryDisabled()}), // Disables default telemetry data sent by the Google Cloud Trace client.
	)
	if err != nil {
		err = errors.Join(err, shutdown(ctx))
		return
	}
	traceProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(env.ServiceName),
		)),
	)
	shutdownFuncs = append(shutdownFuncs, traceProvider.Shutdown)
	otel.SetTracerProvider(traceProvider)

	metricExporter, err := mexporter.New(mexporter.WithProjectID(env.OtelGoogleProjectID),
		mexporter.WithMonitoringClientOptions(option.WithTelemetryDisabled()), // Disables default telemetry data sent by the Google Cloud Trace client.
	)
	if err != nil {
		err = errors.Join(err, shutdown(ctx))
		return
	}
	metricProvider := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(metricExporter)),
		metric.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(env.ServiceName),
		)),
	)
	shutdownFuncs = append(shutdownFuncs, metricProvider.Shutdown)
	otel.SetMeterProvider(metricProvider)

	return shutdown, nil
}

// Secret is a type that represents a secret value, such as a password
type Secret string

// PasswordMask defines the mask used when logging out a password
const PasswordMask = "******************"

// Secret defines a type that outputs the password mask when called with String()
func (s Secret) String() string {
	return PasswordMask
}
