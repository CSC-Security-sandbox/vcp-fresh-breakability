package log

import (
	"context"
	"errors"
	"strings"

	texporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/api/option"
)

var (
	prometheusExporter = prometheus.New
	traceExporterFunc  = texporter.New
)

// SetupOpenTelemetry sets up the OpenTelemetry SDK and exporters for metrics and
// traces. If it does not return an error, call shutdown for proper cleanup.
func SetupOpenTelemetry(ctx context.Context) (shutdown func(context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error
	logger := NewLogger()

	// shutdown combines shutdown functions from multiple OpenTelemetry components into a single function.
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
	// - OTEL_GOOGLE_PROJECT_ID : <Google_Project_ID>
	// - GOOGLE_APPLICATION_CREDENTIALS : <Path_to_Service_Account_Json_Credentials> (Without this variable, the application cannot authenticate and interact with Google Cloud APIs.)

	otelProjectID := env.OtelGoogleProjectID
	var traceError error
	if otelProjectID != "" {
		traceExporter, traceErr := traceExporterFunc(texporter.WithProjectID(otelProjectID),
			texporter.WithTraceClientOptions([]option.ClientOption{option.WithTelemetryDisabled()}), // Disables default telemetry data sent by the Google Cloud Trace client.
		)
		traceError = traceErr
		if traceError == nil {
			traceProvider := trace.NewTracerProvider(
				trace.WithBatcher(traceExporter),
				trace.WithResource(resource.NewWithAttributes(
					semconv.SchemaURL,
					semconv.ServiceNameKey.String(env.ServiceName),
				)),
			)
			shutdownFuncs = append(shutdownFuncs, traceProvider.Shutdown)
			otel.SetTracerProvider(traceProvider)
			logger.Info("Trace exporter set up successful")
		} else {
			logger.Error("Failed to set up trace exporter", "error", traceError)
		}
	}

	// Setting up the metric exporter.
	promExporter, metricError := prometheusExporter()
	if metricError == nil {
		metricProvider := metric.NewMeterProvider(
			metric.WithReader(promExporter),
			metric.WithResource(resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String(env.ServiceName),
			)),
			metric.WithView(
				metric.NewView(
					metric.Instrument{Name: "*"}, // Empty: applies to all instruments
					metric.Stream{
						AttributeFilter: func(kv attribute.KeyValue) bool {
							// Skip metrics with unresolved template values
							if strings.Contains(kv.Value.AsString(), "{") {
								return false
							}
							return true
						},
					},
				),
			),
		)
		shutdownFuncs = append(shutdownFuncs, metricProvider.Shutdown)
		otel.SetMeterProvider(metricProvider)
		logger.Info("Prometheus metrics will be available at /metrics")
	} else {
		logger.Error("Failed to set up metric exporter", "error", metricError)
	}

	if traceError != nil && metricError != nil {
		err = errors.Join(traceError, metricError)
	}

	return shutdown, err
}
