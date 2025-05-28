package log

import (
	"context"
	"errors"
	"testing"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
)

// TestSetupOpenTelemetry tests the SetupOpenTelemetry function
func TestSetupOpenTelemetry(t *testing.T) {
	ctx := context.Background()

	t.Run("Verify TextMapPropagator is set", func(t *testing.T) {
		otel.SetTextMapPropagator(propagation.TraceContext{})
		assert.IsType(t, propagation.TraceContext{}, otel.GetTextMapPropagator())
	})

	t.Run("When OtelGoogleProjectID is empty and Trace Exporter is not setting up", func(t *testing.T) {
		traceExporterCalled := false
		MockTraceExporter := func(opts ...trace.Option) (*trace.Exporter, error) {
			traceExporterCalled = true
			return nil, errors.New("trace Exporter error")
		}

		originalTraceExporter := traceExporterFunc
		traceExporterFunc = MockTraceExporter

		originalProjectID := env.OtelGoogleProjectID
		env.OtelGoogleProjectID = ""

		defer func() {
			traceExporterFunc = originalTraceExporter
			env.OtelGoogleProjectID = originalProjectID
		}()

		shutdown, err := SetupOpenTelemetry(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, shutdown)
		assert.False(t, traceExporterCalled, "traceExporterFunc should not be called when OtelGoogleProjectID is empty")
		if err := shutdown(ctx); err != nil {
			t.Errorf("shutdown failed: %v", err)
		}
	})

	t.Run("When Trace Exporter fails and returns an error", func(t *testing.T) {
		MockTraceExporter := func(opts ...trace.Option) (*trace.Exporter, error) {
			return nil, errors.New("trace Exporter error")
		}

		originalTraceExporter := traceExporterFunc
		traceExporterFunc = MockTraceExporter

		originalProjectID := env.OtelGoogleProjectID
		env.OtelGoogleProjectID = "12345678"

		defer func() {
			traceExporterFunc = originalTraceExporter
			env.OtelGoogleProjectID = originalProjectID
		}()

		shutdown, err := SetupOpenTelemetry(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, shutdown)
		if err := shutdown(ctx); err != nil {
			t.Errorf("shutdown failed: %v", err)
		}
	})

	t.Run("When Metric Exporter fails and returns an error", func(t *testing.T) {
		MockMetricsExporter := func(opts ...prometheus.Option) (*prometheus.Exporter, error) {
			return nil, errors.New("prometheus Exporter error")
		}

		originalPrometheusExporter := prometheusExporter
		prometheusExporter = MockMetricsExporter

		originalProjectID := env.OtelGoogleProjectID
		env.OtelGoogleProjectID = ""

		defer func() {
			env.OtelGoogleProjectID = originalProjectID
			prometheusExporter = originalPrometheusExporter
		}()

		shutdown, err := SetupOpenTelemetry(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, shutdown)
		if err := shutdown(ctx); err != nil {
			t.Errorf("shutdown failed: %v", err)
		}
	})

	t.Run("When Both Trace and Metric Exporter fails", func(t *testing.T) {
		MockTraceExporter := func(opts ...trace.Option) (*trace.Exporter, error) {
			return nil, errors.New("trace Exporter error")
		}
		MockMetricsExporter := func(opts ...prometheus.Option) (*prometheus.Exporter, error) {
			return nil, errors.New("metrics Exporter error")
		}

		originalTraceExporter := traceExporterFunc
		traceExporterFunc = MockTraceExporter

		originalPrometheusExporter := prometheusExporter
		prometheusExporter = MockMetricsExporter

		originalProjectID := env.OtelGoogleProjectID
		env.OtelGoogleProjectID = "12345678"

		defer func() {
			traceExporterFunc = originalTraceExporter
			env.OtelGoogleProjectID = originalProjectID
			prometheusExporter = originalPrometheusExporter
		}()

		shutdown, err := SetupOpenTelemetry(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "trace Exporter error")
		assert.Contains(t, err.Error(), "metrics Exporter error")
		assert.NotNil(t, shutdown)
		if err := shutdown(ctx); err != nil {
			t.Errorf("shutdown failed: %v", err)
		}
	})
}
