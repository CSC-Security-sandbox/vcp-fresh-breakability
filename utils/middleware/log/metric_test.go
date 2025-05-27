package log

import (
	"context"
	"errors"
	"testing"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"github.com/stretchr/testify/assert"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
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

func TestAddLabelerAttributesWithMockedLabeler(t *testing.T) {
	type labelerTestContextKey struct{}

	labelerFromContextTest := func(ctx context.Context) (*gcpserver.Labeler, bool) {
		if l, ok := ctx.Value(labelerTestContextKey{}).(*gcpserver.Labeler); ok {
			return l, true
		}
		return &gcpserver.Labeler{}, false
	}

	originalLabelerFromContext := gcpgenserverLabelerFromContext
	gcpgenserverLabelerFromContext = labelerFromContextTest
	defer func() { gcpgenserverLabelerFromContext = originalLabelerFromContext }()

	t.Run("Valid context with loggerFields and traceURL", func(t *testing.T) {
		ctx := context.Background()
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, Fields{
			"traceURL": "https://netapp.com/trace",
		})
		labeler := &gcpserver.Labeler{}
		ctx = context.WithValue(ctx, labelerTestContextKey{}, labeler)

		AddLabelerAttributes(ctx, "12345", "us-central1")
		labeler, _ = labelerFromContextTest(ctx)
		attrSet := labeler.AttributeSet()

		httpRouteValue, _ := attrSet.Value("http.route")
		locationvalue, _ := attrSet.Value("locationID")
		projectNumber, _ := attrSet.Value("projectNumber")
		assert.EqualValues(t, "https://netapp.com/trace", httpRouteValue.AsString())
		assert.EqualValues(t, "us-central1", locationvalue.AsString())
		assert.EqualValues(t, "12345", projectNumber.AsString())
	})

	t.Run("Valid context without loggerFields", func(t *testing.T) {
		ctx := context.Background()
		labeler := &gcpserver.Labeler{}
		ctx = context.WithValue(ctx, labelerTestContextKey{}, labeler)

		AddLabelerAttributes(ctx, "67890", "europe-west1")
		labeler, _ = labelerFromContextTest(ctx)
		attrSet := labeler.AttributeSet()

		httpRouteValue, _ := attrSet.Value("http.route")
		locationvalue, _ := attrSet.Value("locationID")
		projectNumber, _ := attrSet.Value("projectNumber")
		assert.EqualValues(t, "", httpRouteValue.AsString())
		assert.EqualValues(t, "europe-west1", locationvalue.AsString())
		assert.EqualValues(t, "67890", projectNumber.AsString())
	})
}
