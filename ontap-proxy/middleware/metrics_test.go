package middleware

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	promclient "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
)

var (
	// Global variable to track the current test meter provider for cleanup
	testMeterProvider   *metric.MeterProvider
	testMeterProviderMu sync.Mutex
)

// resetMetricsState resets the metrics state for testing
// This ensures each test starts with a clean state
func resetMetricsState(t *testing.T) {
	t.Helper()

	// Shutdown previous meter provider if it exists
	testMeterProviderMu.Lock()
	if testMeterProvider != nil {
		_ = testMeterProvider.Shutdown(context.Background())
		testMeterProvider = nil
	}
	testMeterProviderMu.Unlock()

	// Reset the sync.Once to allow re-initialization
	initOnce = sync.Once{}

	// Reset metric variables
	ontapProxyRequestsTotal = nil
	ontapProxyRequestDurationSeconds = nil
	ontapProxyErrorsTotal = nil
	ontapProxyBackendRequestsTotal = nil
	ontapProxyBackendRequestDurationSeconds = nil
	ontapProxyBackendErrorsTotal = nil
	meter = nil

	// Set a noop meter provider to clear global state
	otel.SetMeterProvider(metric.NewMeterProvider())
}

// setupTestMetrics sets up OpenTelemetry with Prometheus exporter for testing
// Uses a custom Prometheus registry per test to avoid conflicts
func setupTestMetrics(t *testing.T) promclient.Gatherer {
	t.Helper()

	// Reset metrics state (shuts down previous provider)
	resetMetricsState(t)

	// Create a custom Prometheus registry for this test to avoid conflicts
	testRegistry := promclient.NewRegistry()

	// Create Prometheus exporter with custom registry
	// This prevents metrics from being registered in the default registry
	// WithoutTargetInfo() prevents the target_info metric that causes duplicates
	promExporter, err := otelprom.New(
		otelprom.WithRegisterer(testRegistry),
		otelprom.WithoutTargetInfo(),
	)
	require.NoError(t, err, "Failed to create Prometheus exporter for testing")

	// Set up meter provider with Prometheus exporter
	metricProvider := metric.NewMeterProvider(
		metric.WithReader(promExporter),
	)

	// Store provider for cleanup
	testMeterProviderMu.Lock()
	testMeterProvider = metricProvider
	testMeterProviderMu.Unlock()

	otel.SetMeterProvider(metricProvider)

	// Return the custom registry as gatherer
	return testRegistry
}

func TestClassifyBackendError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{"nil", nil, BackendErrorUnknown},
		{"connection refused", &errWithMessage{"connection refused"}, BackendErrorTransport},
		{"context canceled", &errWithMessage{"context canceled"}, BackendErrorTransport},
		{"deadline exceeded", &errWithMessage{"context deadline exceeded"}, BackendErrorTransport},
		{"timeout", &errWithMessage{"read timeout"}, BackendErrorTransport},
		{"no such host", &errWithMessage{"no such host"}, BackendErrorTransport},
		{"tls handshake", &errWithMessage{"tls: handshake failure"}, BackendErrorTransport},
		{"other", &errWithMessage{"something else"}, BackendErrorTransport},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyBackendError(tt.err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestBackendErrorCodeForMetric(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		err        error
		expected   string
	}{
		{"http 404 wins over err", 404, &errWithMessage{"connection refused"}, "404"},
		{"http 500", 500, nil, "500"},
		{"http 400", 400, nil, "400"},
		{"no response, transport err", 0, &errWithMessage{"connection refused"}, BackendErrorTransport},
		{"no response, nil err", 0, nil, ""},
		{"success 200 no err", 200, nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BackendErrorCodeForMetric(tt.statusCode, tt.err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestStatusClass(t *testing.T) {
	tests := []struct {
		statusCode int
		expected   string
	}{
		{200, "2xx"},
		{201, "2xx"},
		{204, "2xx"},
		{299, "2xx"},
		{400, "4xx"},
		{401, "4xx"},
		{404, "4xx"},
		{499, "4xx"},
		{500, "5xx"},
		{502, "5xx"},
		{503, "5xx"},
		{599, "5xx"},
		{0, "unknown"},
		{199, "unknown"},
		{300, "unknown"},
		{600, "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.expected+"_"+strconv.Itoa(tt.statusCode), func(t *testing.T) {
			got := StatusClass(tt.statusCode)
			assert.Equal(t, tt.expected, got)
		})
	}
}

type errWithMessage struct{ msg string }

func (e *errWithMessage) Error() string { return e.msg }

func TestGetBackendMetricsFromContext(t *testing.T) {
	t.Run("empty context returns unknown", func(t *testing.T) {
		p, po, path := GetBackendMetricsFromContext(context.Background())
		assert.Equal(t, "unknown", p)
		assert.Equal(t, "unknown", po)
		assert.Equal(t, "unknown", path)
	})
	t.Run("values from context", func(t *testing.T) {
		ctx := context.Background()
		ctx = AddBackendMetricsToContext(ctx, "proj-1", "pool-1", "/api/storage/volumes")
		p, po, path := GetBackendMetricsFromContext(ctx)
		assert.Equal(t, "proj-1", p)
		assert.Equal(t, "pool-1", po)
		assert.Equal(t, "/api/storage/volumes", path)
	})
	t.Run("empty values become unknown", func(t *testing.T) {
		ctx := AddBackendMetricsToContext(context.Background(), "", "", "")
		p, po, path := GetBackendMetricsFromContext(ctx)
		assert.Equal(t, "unknown", p)
		assert.Equal(t, "unknown", po)
		assert.Equal(t, "unknown", path)
	})
}

func TestAddBackendRequestStartToContext_GetBackendRequestStartFromContext(t *testing.T) {
	t.Run("empty context returns false", func(t *testing.T) {
		_, ok := GetBackendRequestStartFromContext(context.Background())
		assert.False(t, ok)
	})
	t.Run("value set in context is returned", func(t *testing.T) {
		start := time.Now()
		ctx := AddBackendRequestStartToContext(context.Background(), start)
		got, ok := GetBackendRequestStartFromContext(ctx)
		assert.True(t, ok)
		assert.False(t, got.IsZero())
		assert.True(t, got.Equal(start))
	})
	t.Run("wrong type in context returns false", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), ontapMetricsBackendStartKey, "not-a-time")
		_, ok := GetBackendRequestStartFromContext(ctx)
		assert.False(t, ok)
	})
}

// TestRecordBackendRequest_Duration_Error_WithMetrics verifies RecordBackendRequest, RecordBackendDuration,
// and RecordBackendError record when metrics are initialized (covers Add/Record bodies and status_code branch).
func TestRecordBackendRequest_Duration_Error_WithMetrics(t *testing.T) {
	gatherer := setupTestMetrics(t)
	require.NoError(t, InitMetrics())

	ctx := context.Background()
	method := "GET"
	projectID := "proj-1"
	poolID := "pool-1"
	path := "/api/storage/volumes"

	RecordBackendRequest(ctx, method, projectID, poolID, path, 200)
	RecordBackendDuration(ctx, 0.1, method, projectID, poolID, path, 200)
	RecordBackendRequest(ctx, method, projectID, poolID, path, 404)
	RecordBackendDuration(ctx, 0.2, method, projectID, poolID, path, 404)
	RecordBackendError(ctx, method, projectID, poolID, path, "404")

	mfs, err := gatherer.Gather()
	require.NoError(t, err)

	var reqMF, durMF, errMF *dto.MetricFamily
	for _, mf := range mfs {
		switch mf.GetName() {
		case "ontap_proxy_backend_requests_total":
			reqMF = mf
		case "ontap_proxy_backend_request_duration_seconds":
			durMF = mf
		case "ontap_proxy_backend_errors_total":
			errMF = mf
		}
	}
	require.NotNil(t, reqMF)
	require.NotNil(t, durMF)
	require.NotNil(t, errMF)

	// Should have 2xx and 4xx in requests (status_code branch: exact codes 200, 404)
	var has200, has404 bool
	for _, m := range reqMF.Metric {
		for _, lp := range m.Label {
			if *lp.Name == "status_code" {
				if *lp.Value == "200" {
					has200 = true
				}
				if *lp.Value == "404" {
					has404 = true
				}
			}
		}
	}
	assert.True(t, has200)
	assert.True(t, has404)
	assert.GreaterOrEqual(t, len(errMF.Metric), 1)
}

// TestRecordBackendRequest_Duration_Error_WhenNil ensures Record* no-op when metrics are not initialized (nil).
func TestRecordBackendRequest_Duration_Error_WhenNil(t *testing.T) {
	resetMetricsState(t)
	// Do not call InitMetrics so backend metric vars remain nil.

	ctx := context.Background()
	RecordBackendRequest(ctx, "GET", "p", "pool", "/path", 200)
	RecordBackendDuration(ctx, 0.5, "GET", "p", "pool", "/path", 500)
	RecordBackendError(ctx, "POST", "p", "pool", "/path", BackendErrorTransport)
	// No panic; early return when metric is nil. error_code for transport errors is "500".
}

// TestProcessResponseAndRecordBackendMetrics_RecordsMetrics verifies that ProcessResponseAndRecordBackendMetrics
// records backend_requests_total, backend_request_duration_seconds, and backend_errors_total for 4xx
// when the response has request context with start time and backend metrics labels (simulates ModifyResponse path).
func TestProcessResponseAndRecordBackendMetrics_RecordsMetrics(t *testing.T) {
	gatherer := setupTestMetrics(t)
	require.NoError(t, InitMetrics())

	start := time.Now().Add(-100 * time.Millisecond) // 100ms ago
	ctx := context.Background()
	ctx = AddBackendRequestStartToContext(ctx, start)
	ctx = AddBackendMetricsToContext(ctx, "proj-1", "pool-1", "/api/storage/volumes")
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req = req.WithContext(ctx)
	resp := &http.Response{
		StatusCode: http.StatusNotFound,
		Request:    req,
		Body:       io.NopCloser(bytes.NewBufferString("")),
	}

	err := ProcessResponseAndRecordBackendMetrics(resp)
	require.NoError(t, err)

	mfs, err := gatherer.Gather()
	require.NoError(t, err)

	var reqMF, errMF *dto.MetricFamily
	for _, mf := range mfs {
		switch mf.GetName() {
		case "ontap_proxy_backend_requests_total":
			reqMF = mf
		case "ontap_proxy_backend_errors_total":
			errMF = mf
		}
	}
	require.NotNil(t, reqMF)
	require.NotNil(t, errMF)

	// Should have one request with status_code 404
	var countReq float64
	for _, m := range reqMF.Metric {
		for _, lp := range m.Label {
			if *lp.Name == "status_code" && *lp.Value == "404" {
				if m.Counter != nil {
					countReq += m.Counter.GetValue()
				}
				break
			}
		}
	}
	assert.GreaterOrEqual(t, countReq, 1.0)
	assert.GreaterOrEqual(t, len(errMF.Metric), 1)
}

// TestProcessResponseAndRecordBackendMetrics_WhenNoStartInContext_DoesNotRecord verifies that when the
// response's request context has no start time, ProcessResponseAndRecordBackendMetrics returns without
// recording (so no panic and no new backend metrics from this call).
func TestProcessResponseAndRecordBackendMetrics_WhenNoStartInContext_DoesNotRecord(t *testing.T) {
	gatherer := setupTestMetrics(t)
	require.NoError(t, InitMetrics())

	// Context has backend labels but no start time
	ctx := AddBackendMetricsToContext(context.Background(), "proj-1", "pool-1", "/path")
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req = req.WithContext(ctx)
	resp := &http.Response{
		StatusCode: 200,
		Request:    req,
		Body:       io.NopCloser(bytes.NewBufferString("")),
	}

	err := ProcessResponseAndRecordBackendMetrics(resp)
	require.NoError(t, err)

	// Gather and ensure we have no backend_requests_total for this request (we didn't add start, so nothing recorded)
	mfs, _ := gatherer.Gather()
	var totalBackendReqs float64
	for _, mf := range mfs {
		if mf.GetName() == "ontap_proxy_backend_requests_total" {
			for _, m := range mf.Metric {
				if m.Counter != nil {
					totalBackendReqs += m.Counter.GetValue()
				}
			}
			break
		}
	}
	assert.Equal(t, 0.0, totalBackendReqs)
}

func TestNormalizePath(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Normalize basic API path",
			input:    "/v1beta/projects/81821054389/locations/australia-southeast1-b/pools/pool-123/ontap/api/storage/volumes",
			expected: "/api/storage/volumes",
		},
		{
			name:     "Normalize UUID in path",
			input:    "/v1beta/projects/123/locations/us-central1/pools/550e8400-e29b-41d4-a716-446655440000/ontap/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000",
			expected: "/api/storage/volumes/{uuid}",
		},
		{
			name:     "Normalize numeric IDs in API path",
			input:    "/v1beta/projects/123/locations/us-central1/pools/pool-123/ontap/api/storage/volumes/12345",
			expected: "/api/storage/volumes/{id}",
		},
		{
			name:     "Normalize multiple UUIDs",
			input:    "/v1beta/projects/123/locations/us-central1/pools/550e8400-e29b-41d4-a716-446655440000/ontap/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000/files/660e8400-e29b-41d4-a716-446655440001",
			expected: "/api/storage/volumes/{uuid}/files/{uuid}",
		},
		{
			name:     "Path without /api/ segment",
			input:    "/v1beta/projects/123/locations/us-central1/pools/pool-123/ontap/some/other/path",
			expected: "/some/other/path",
		},
		{
			name:     "Path with numeric ID before /api/",
			input:    "/v1beta/projects/123/locations/us-central1/pools/pool-123/ontap/api/storage/volumes/12345/snapshots/67890",
			expected: "/api/storage/volumes/{id}/snapshots/{id}",
		},
		{
			name:     "Path with multiple numeric IDs",
			input:    "/v1beta/projects/123/locations/us-central1/pools/pool-123/ontap/api/storage/volumes/12345/snapshots/67890/restore/99999",
			expected: "/api/storage/volumes/{id}/snapshots/{id}/restore/{id}",
		},
		{
			name:     "Path with mixed UUIDs and numeric IDs",
			input:    "/v1beta/projects/123/locations/us-central1/pools/pool-123/ontap/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000/snapshots/12345",
			expected: "/api/storage/volumes/{uuid}/snapshots/{id}",
		},
		{
			name:     "Path without UUIDs or numeric IDs - aggregates",
			input:    "/v1beta/projects/123456789/locations/us-central1/pools/pool-123/ontap/api/storage/aggregates",
			expected: "/api/storage/aggregates",
		},
		{
			name:     "Path without UUIDs or numeric IDs - svms",
			input:    "/v1beta/projects/123456789/locations/us-central1/pools/pool-123/ontap/api/svm/svms",
			expected: "/api/svm/svms",
		},
		{
			name:     "Fallback to normalizeFullPath when ontap path cannot be extracted - no ontap segment",
			input:    "/some/invalid/path/without/ontap",
			expected: "/some/invalid/path/without/ontap",
		},
		{
			name:     "Fallback path with project number normalization",
			input:    "/v1beta/projects/123456789/something",
			expected: "/v1beta/projects/{project}/something",
		},
		{
			name:     "Fallback path with location normalization",
			input:    "/v1beta/locations/australia-southeast1-b/path",
			expected: "/v1beta/locations/{location}/path",
		},
		{
			name:     "Fallback path with pool normalization",
			input:    "/v1beta/pools/pool-123/path",
			expected: "/v1beta/pools/{pool}/path",
		},
		{
			name:     "Fallback path with UUID normalization",
			input:    "/v1beta/path/550e8400-e29b-41d4-a716-446655440000",
			expected: "/v1beta/path/{uuid}",
		},
		{
			name:     "Fallback path with /api/ and numeric ID (no ontap segment)",
			input:    "/v1beta/projects/123/locations/us/pools/pool/api/storage/volumes/12345",
			expected: "/v1beta/projects/{project}/locations/{location}/pools/{pool}/api/storage/volumes/{id}",
		},
		{
			name:     "Fallback when ontap is at the end (no path after)",
			input:    "/v1beta/projects/123/locations/us/pools/pool/ontap",
			expected: "/v1beta/projects/{project}/locations/{location}/pools/{pool}/ontap",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizePath(tt.input)
			assert.Equal(t, tt.expected, result, "Path normalization failed")
		})
	}
}

func TestExtractProjectID(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Valid path with project ID",
			input:    "/v1beta/projects/123456789/locations/us-central1/pools/pool-123/ontap/api/storage/volumes",
			expected: "123456789",
		},
		{
			name:     "Valid path with project name",
			input:    "/v1beta/projects/my-project-123/locations/us-central1/pools/pool-123/ontap/api/storage/volumes",
			expected: "my-project-123",
		},
		{
			name:     "Path without v1beta prefix",
			input:    "/api/storage/volumes",
			expected: "unknown",
		},
		{
			name:     "Path with insufficient segments",
			input:    "/v1beta/projects",
			expected: "unknown",
		},
		{
			name:     "Path with empty project ID",
			input:    "/v1beta/projects//locations/us-central1/pools/pool-123/ontap/api",
			expected: "unknown",
		},
		{
			name:     "Valid path with UUID project ID",
			input:    "/v1beta/projects/550e8400-e29b-41d4-a716-446655440000/locations/us-central1/pools/pool-123/ontap/api/storage/volumes",
			expected: "550e8400-e29b-41d4-a716-446655440000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractProjectID(tt.input)
			assert.Equal(t, tt.expected, result, "Project ID extraction failed")
		})
	}
}

func TestExtractOntapPathForMetrics(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Valid path with ontap segment",
			input:    "/v1beta/projects/123/locations/us-central1/pools/pool-123/ontap/api/storage/volumes",
			expected: "/api/storage/volumes",
		},
		{
			name:     "Path with ontap at end",
			input:    "/v1beta/projects/123/locations/us-central1/pools/pool-123/ontap",
			expected: "",
		},
		{
			name:     "Path without ontap segment",
			input:    "/v1beta/projects/123/locations/us-central1/pools/pool-123/api/storage/volumes",
			expected: "",
		},
		{
			name:     "Path with ontap and trailing content",
			input:    "/v1beta/projects/123/locations/us-central1/pools/pool-123/ontap/api/storage/volumes/12345",
			expected: "/api/storage/volumes/12345",
		},
		{
			name:     "Empty path",
			input:    "",
			expected: "",
		},
		{
			name:     "Path with multiple ontap segments (first one wins)",
			input:    "/v1beta/ontap/first/api/storage/ontap/second",
			expected: "/first/api/storage/ontap/second",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractOntapPathForMetrics(tt.input)
			assert.Equal(t, tt.expected, result, "extractOntapPathForMetrics failed")
		})
	}
}

func TestNormalizeFullPath(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Normalize project number",
			input:    "/v1beta/projects/123456789/locations/us-central1/pools/pool-123/ontap/api/storage/volumes",
			expected: "/v1beta/projects/{project}/locations/{location}/pools/{pool}/ontap/api/storage/volumes",
		},
		{
			name:     "Normalize location",
			input:    "/v1beta/projects/123/locations/australia-southeast1-b/pools/pool-123/ontap/api/storage/volumes",
			expected: "/v1beta/projects/{project}/locations/{location}/pools/{pool}/ontap/api/storage/volumes",
		},
		{
			name:     "Normalize pool ID",
			input:    "/v1beta/projects/123/locations/us-central1/pools/pool-123/ontap/api/storage/volumes",
			expected: "/v1beta/projects/{project}/locations/{location}/pools/{pool}/ontap/api/storage/volumes",
		},
		{
			name:     "Normalize UUID (pool ID UUID becomes {pool}, path UUID becomes {uuid})",
			input:    "/v1beta/projects/123/locations/us-central1/pools/550e8400-e29b-41d4-a716-446655440000/ontap/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000",
			expected: "/v1beta/projects/{project}/locations/{location}/pools/{pool}/ontap/api/storage/volumes/{uuid}",
		},
		{
			name:     "Normalize numeric ID in API path",
			input:    "/v1beta/projects/123/locations/us-central1/pools/pool-123/ontap/api/storage/volumes/12345",
			expected: "/v1beta/projects/{project}/locations/{location}/pools/{pool}/ontap/api/storage/volumes/{id}",
		},
		{
			name:     "Normalize multiple numeric IDs in API path",
			input:    "/v1beta/projects/123/locations/us-central1/pools/pool-123/ontap/api/storage/volumes/12345/snapshots/67890",
			expected: "/v1beta/projects/{project}/locations/{location}/pools/{pool}/ontap/api/storage/volumes/{id}/snapshots/{id}",
		},
		{
			name:     "Path without /api/ segment",
			input:    "/v1beta/projects/123/locations/us-central1/pools/pool-123/ontap/some/other/path",
			expected: "/v1beta/projects/{project}/locations/{location}/pools/{pool}/ontap/some/other/path",
		},
		{
			name:     "Path with /api/ but not at expected position",
			input:    "/v1beta/api/projects/123/locations/us-central1/pools/pool-123",
			expected: "/v1beta/api/projects/{project}/locations/{location}/pools/{pool}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeFullPath(tt.input)
			assert.Equal(t, tt.expected, result, "normalizeFullPath failed")
		})
	}
}

func TestExtractPoolID(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Valid path with pool ID",
			input:    "/v1beta/projects/123456789/locations/us-central1/pools/pool-123/ontap/api/storage/volumes",
			expected: "pool-123",
		},
		{
			name:     "Valid path with UUID pool ID",
			input:    "/v1beta/projects/123456789/locations/us-central1/pools/550e8400-e29b-41d4-a716-446655440000/ontap/api/storage/volumes",
			expected: "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:     "Path without v1beta prefix",
			input:    "/api/storage/volumes",
			expected: "unknown",
		},
		{
			name:     "Path with insufficient segments",
			input:    "/v1beta/projects/123",
			expected: "unknown",
		},
		{
			name:     "Path with empty pool ID",
			input:    "/v1beta/projects/123/locations/us-central1/pools//ontap/api",
			expected: "unknown",
		},
		{
			name:     "Valid path with long pool ID",
			input:    "/v1beta/projects/123456789/locations/us-central1/pools/9b65c998-75b9-f8da-1118-500a2d588474/ontap/api/storage/volumes",
			expected: "9b65c998-75b9-f8da-1118-500a2d588474",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractPoolID(tt.input)
			assert.Equal(t, tt.expected, result, "Pool ID extraction failed")
		})
	}
}

func TestMetricsMiddleware_PassthroughRoutes(t *testing.T) {
	tests := []struct {
		name           string
		path           string
		method         string
		statusCode     int
		shouldTrack    bool
		shouldBeError  bool
		expectedPoolID string
	}{
		{
			name:           "Track GET request with 200",
			path:           "/v1beta/projects/123456789/locations/us-central1/pools/pool-123/ontap/api/storage/volumes",
			method:         "GET",
			statusCode:     200,
			shouldTrack:    true,
			shouldBeError:  false,
			expectedPoolID: "pool-123",
		},
		{
			name:           "Track POST request with 201",
			path:           "/v1beta/projects/123456789/locations/us-central1/pools/pool-123/ontap/api/storage/volumes",
			method:         "POST",
			statusCode:     201,
			shouldTrack:    true,
			shouldBeError:  false,
			expectedPoolID: "pool-123",
		},
		{
			name:           "Track 401 auth failure",
			path:           "/v1beta/projects/123456789/locations/us-central1/pools/pool-123/ontap/api/storage/volumes",
			method:         "GET",
			statusCode:     401,
			shouldTrack:    true,
			shouldBeError:  true,
			expectedPoolID: "pool-123",
		},
		{
			name:           "Track 404 not found",
			path:           "/v1beta/projects/123456789/locations/us-central1/pools/pool-123/ontap/api/storage/volumes/123",
			method:         "GET",
			statusCode:     404,
			shouldTrack:    true,
			shouldBeError:  true,
			expectedPoolID: "pool-123",
		},
		{
			name:           "Track 500 server error",
			path:           "/v1beta/projects/123456789/locations/us-central1/pools/pool-123/ontap/api/storage/volumes",
			method:         "GET",
			statusCode:     500,
			shouldTrack:    true,
			shouldBeError:  true,
			expectedPoolID: "pool-123",
		},
		{
			name:           "Track 503 service unavailable",
			path:           "/v1beta/projects/123456789/locations/us-central1/pools/pool-123/ontap/api/storage/volumes",
			method:         "GET",
			statusCode:     503,
			shouldTrack:    true,
			shouldBeError:  true,
			expectedPoolID: "pool-123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up OpenTelemetry with Prometheus exporter for testing
			gatherer := setupTestMetrics(t)

			// Create a handler that returns the test status code
			nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
			})

			// Create request
			req := httptest.NewRequest(tt.method, tt.path, nil)
			w := httptest.NewRecorder()

			// Use the actual MetricsMiddleware (uses OTEL metrics)
			middleware := MetricsMiddleware()
			middleware(nextHandler).ServeHTTP(w, req)

			// Verify status code was captured
			assert.Equal(t, tt.statusCode, w.Code)

			if tt.shouldTrack {
				// Gather metrics from Prometheus exporter
				mfs, err := gatherer.Gather()
				require.NoError(t, err)

				// Find our metrics
				var requestsMetric, errorsMetric *dto.MetricFamily
				for _, mf := range mfs {
					switch mf.GetName() {
					case "ontap_proxy_requests_total":
						requestsMetric = mf
					case "ontap_proxy_errors_total":
						errorsMetric = mf
					}
				}

				require.NotNil(t, requestsMetric, "requests_total metric should be present")
				assert.Greater(t, len(requestsMetric.Metric), 0, "Should have at least one metric sample")

				// Check if error metric was recorded
				if tt.shouldBeError {
					require.NotNil(t, errorsMetric, "errors_total metric should be present for error status codes")
					assert.Greater(t, len(errorsMetric.Metric), 0, "Should have at least one error metric sample")
				}
			}
		})
	}
}

func TestMetricsMiddleware_NonPassthroughRoutes(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		shouldTrack bool
	}{
		{
			name:        "Skip /health endpoint",
			path:        "/health",
			shouldTrack: false,
		},
		{
			name:        "Skip /metrics endpoint",
			path:        "/metrics",
			shouldTrack: false,
		},
		{
			name:        "Skip /v1/expertMode endpoint",
			path:        "/v1/expertMode/something",
			shouldTrack: false,
		},
		{
			name:        "Skip path without /v1beta prefix",
			path:        "/api/storage/volumes",
			shouldTrack: false,
		},
		{
			name:        "Track valid passthrough route",
			path:        "/v1beta/projects/123/locations/us-central1/pools/pool-123/ontap/api/storage/volumes",
			shouldTrack: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up OpenTelemetry with Prometheus exporter for testing
			gatherer := setupTestMetrics(t)

			// Create handler
			nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})

			req := httptest.NewRequest("GET", tt.path, nil)
			w := httptest.NewRecorder()

			// Use the actual MetricsMiddleware (uses OTEL metrics)
			middleware := MetricsMiddleware()
			middleware(nextHandler).ServeHTTP(w, req)

			// Gather metrics from Prometheus exporter
			mfs, err := gatherer.Gather()
			require.NoError(t, err)

			// Check if metrics were recorded
			var requestsMetric *dto.MetricFamily
			for _, mf := range mfs {
				if mf.GetName() == "ontap_proxy_requests_total" {
					requestsMetric = mf
					break
				}
			}

			if tt.shouldTrack {
				require.NotNil(t, requestsMetric, "requests_total metric should be present for passthrough routes")
				assert.Greater(t, len(requestsMetric.Metric), 0, "Should have metric samples")
			} else {
				// For non-passthrough routes, metrics should not be recorded
				// (or the metric family might exist but be empty)
				if requestsMetric != nil {
					assert.Equal(t, 0, len(requestsMetric.Metric), "Should not have metric samples for non-passthrough routes")
				}
			}
		})
	}
}

func TestResponseWriter(t *testing.T) {
	t.Run("Capture status code", func(t *testing.T) {
		w := httptest.NewRecorder()
		rw := &responseWriter{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		// Write header with different status code
		rw.WriteHeader(http.StatusNotFound)

		// Verify status code was captured
		assert.Equal(t, http.StatusNotFound, rw.statusCode)
		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("Default status code", func(t *testing.T) {
		w := httptest.NewRecorder()
		rw := &responseWriter{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		// Verify default status code
		assert.Equal(t, http.StatusOK, rw.statusCode)
	})
}

func TestMetricsMiddleware_Duration(t *testing.T) {
	// Set up OpenTelemetry with Prometheus exporter for testing
	gatherer := setupTestMetrics(t)

	// Create handler that takes some time
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest("GET", "/v1beta/projects/123/locations/us-central1/pools/pool-123/ontap/api/storage/volumes", nil)
	w := httptest.NewRecorder()

	// Use the actual MetricsMiddleware (uses OTEL metrics)
	middleware := MetricsMiddleware()
	middleware(nextHandler).ServeHTTP(w, req)

	// Gather metrics from Prometheus exporter
	mfs, err := gatherer.Gather()
	require.NoError(t, err)

	// Find duration metric
	var durationMetric *dto.MetricFamily
	for _, mf := range mfs {
		if mf.GetName() == "ontap_proxy_request_duration_seconds" {
			durationMetric = mf
			break
		}
	}

	require.NotNil(t, durationMetric, "duration metric should be present")
	assert.Greater(t, len(durationMetric.Metric), 0, "Should have duration samples")

	// Check that duration was recorded (should be >= 10ms)
	for _, m := range durationMetric.Metric {
		if histogram := m.GetHistogram(); histogram != nil {
			assert.Greater(t, histogram.GetSampleSum(), float64(0.01), "Duration should be at least 10ms")
		}
	}
}

// TestMetricsMiddleware_ActualFunction tests the actual MetricsMiddleware function
// This ensures we have coverage for the real implementation, not just the test helper
func TestMetricsMiddleware_ActualFunction(t *testing.T) {
	tests := []struct {
		name           string
		path           string
		method         string
		statusCode     int
		shouldTrack    bool
		expectedPoolID string
	}{
		{
			name:           "Track passthrough route with 200",
			path:           "/v1beta/projects/123456789/locations/us-central1/pools/pool-123/ontap/api/storage/volumes",
			method:         "GET",
			statusCode:     200,
			shouldTrack:    true,
			expectedPoolID: "pool-123",
		},
		{
			name:           "Track passthrough route with 401",
			path:           "/v1beta/projects/123456789/locations/us-central1/pools/pool-123/ontap/api/storage/volumes",
			method:         "GET",
			statusCode:     401,
			shouldTrack:    true,
			expectedPoolID: "pool-123",
		},
		{
			name:           "Track passthrough route with 500",
			path:           "/v1beta/projects/123456789/locations/us-central1/pools/pool-123/ontap/api/storage/volumes",
			method:         "POST",
			statusCode:     500,
			shouldTrack:    true,
			expectedPoolID: "pool-123",
		},
		{
			name:        "Skip non-passthrough route",
			path:        "/health",
			method:      "GET",
			statusCode:  200,
			shouldTrack: false,
		},
		{
			name:        "Skip path without /v1beta",
			path:        "/api/storage/volumes",
			method:      "GET",
			statusCode:  200,
			shouldTrack: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a handler that returns the test status code
			nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
			})

			// Create request
			req := httptest.NewRequest(tt.method, tt.path, nil)
			w := httptest.NewRecorder()

			// Use the actual MetricsMiddleware function
			middleware := MetricsMiddleware()
			middleware(nextHandler).ServeHTTP(w, req)

			// Verify status code was captured correctly
			assert.Equal(t, tt.statusCode, w.Code)

			if tt.shouldTrack {
				// Set up OpenTelemetry with Prometheus exporter for testing
				gatherer := setupTestMetrics(t)

				// Re-run the request to record metrics
				req2 := httptest.NewRequest(tt.method, tt.path, nil)
				w2 := httptest.NewRecorder()
				middleware2 := MetricsMiddleware()
				middleware2(nextHandler).ServeHTTP(w2, req2)

				// Gather metrics from Prometheus exporter
				mfs, err := gatherer.Gather()
				require.NoError(t, err)

				// Extract expected attributes
				expectedProjectID := extractProjectID(tt.path)
				expectedPoolID := extractPoolID(tt.path)
				normalizedPath := normalizePath(tt.path)
				expectedStatusCode := strconv.Itoa(tt.statusCode)

				// Find metrics and verify they have the expected attributes
				var requestsMetric, errorsMetric, durationMetric *dto.MetricFamily
				for _, mf := range mfs {
					switch mf.GetName() {
					case "ontap_proxy_requests_total":
						requestsMetric = mf
					case "ontap_proxy_errors_total":
						errorsMetric = mf
					case "ontap_proxy_request_duration_seconds":
						durationMetric = mf
					}
				}

				// Verify requests metric was recorded
				require.NotNil(t, requestsMetric, "requests_total metric should be present")
				assert.Greater(t, len(requestsMetric.Metric), 0, "Should have at least one metric sample")

				// Verify duration metric was recorded
				require.NotNil(t, durationMetric, "duration metric should be present")
				assert.Greater(t, len(durationMetric.Metric), 0, "Should have duration samples")

				// Verify attributes match expected values
				foundRequest := false
				for _, m := range requestsMetric.Metric {
					attrs := make(map[string]string)
					for _, label := range m.Label {
						attrs[*label.Name] = *label.Value
					}
					if attrs["method"] == tt.method &&
						attrs["status_code"] == expectedStatusCode &&
						attrs["project_id"] == expectedProjectID &&
						attrs["pool_id"] == expectedPoolID &&
						attrs["path"] == normalizedPath {
						foundRequest = true
						assert.Greater(t, m.GetCounter().GetValue(), float64(0), "Counter value should be greater than 0")
						break
					}
				}
				assert.True(t, foundRequest, "Should find metric with expected attributes")

				// Check error metric for error status codes
				if tt.statusCode >= 400 {
					require.NotNil(t, errorsMetric, "errors_total metric should be present for error status codes")
					assert.Greater(t, len(errorsMetric.Metric), 0, "Should have error metric samples")
				}
			}
		})
	}
}

// TestMetricsMiddleware_ResponseWriterWrapper tests the responseWriter wrapper behavior
// This ensures status codes are captured correctly in various scenarios
func TestMetricsMiddleware_ResponseWriterWrapper(t *testing.T) {
	tests := []struct {
		name           string
		handlerFunc    func(http.ResponseWriter, *http.Request)
		expectedStatus int
	}{
		{
			name: "WriteHeader sets status code",
			handlerFunc: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(404)
			},
			expectedStatus: 404,
		},
		{
			name: "Write without WriteHeader defaults to 200",
			handlerFunc: func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte("OK"))
			},
			expectedStatus: 200,
		},
		{
			name: "WriteHeader then Write",
			handlerFunc: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(503)
				_, _ = w.Write([]byte("Error"))
			},
			expectedStatus: 503,
		},
		{
			name: "Multiple WriteHeader calls (first one wins)",
			handlerFunc: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(400)
				w.WriteHeader(500) // HTTP ignores this, metrics should capture first (400)
			},
			expectedStatus: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := "/v1beta/projects/123/locations/us-central1/pools/pool-123/ontap/api/storage/volumes"
			req := httptest.NewRequest("GET", path, nil)
			w := httptest.NewRecorder()

			// Use the actual MetricsMiddleware function
			middleware := MetricsMiddleware()
			middleware(http.HandlerFunc(tt.handlerFunc)).ServeHTTP(w, req)

			// Verify HTTP response status code (what client receives)
			assert.Equal(t, tt.expectedStatus, w.Code, "HTTP status code should match expected value")

			// For the multiple WriteHeader test, verify metrics recorded the first status code
			if tt.name == "Multiple WriteHeader calls (first one wins)" {
				// Set up OpenTelemetry with Prometheus exporter for testing
				gatherer := setupTestMetrics(t)

				// Re-run the request to record metrics
				req2 := httptest.NewRequest("GET", path, nil)
				w2 := httptest.NewRecorder()
				middleware2 := MetricsMiddleware()
				middleware2(http.HandlerFunc(tt.handlerFunc)).ServeHTTP(w2, req2)

				// Gather metrics from Prometheus exporter
				mfs, err := gatherer.Gather()
				require.NoError(t, err)

				// Extract expected attributes
				expectedProjectID := extractProjectID(path)
				expectedPoolID := extractPoolID(path)
				normalizedPath := normalizePath(path)
				expectedStatusCode := strconv.Itoa(tt.expectedStatus) // Should be 400, not 500

				// Find metrics
				var requestsMetric, errorsMetric *dto.MetricFamily
				for _, mf := range mfs {
					switch mf.GetName() {
					case "ontap_proxy_requests_total":
						requestsMetric = mf
					case "ontap_proxy_errors_total":
						errorsMetric = mf
					}
				}

				// Verify requests metric recorded the first status code (400), not the second (500)
				require.NotNil(t, requestsMetric, "requests_total metric should be present")
				found400 := false
				found500 := false
				for _, m := range requestsMetric.Metric {
					attrs := make(map[string]string)
					for _, label := range m.Label {
						attrs[*label.Name] = *label.Value
					}
					if attrs["status_code"] == expectedStatusCode &&
						attrs["method"] == "GET" &&
						attrs["project_id"] == expectedProjectID &&
						attrs["pool_id"] == expectedPoolID &&
						attrs["path"] == normalizedPath {
						found400 = true
						assert.Greater(t, m.GetCounter().GetValue(), float64(0), "Counter should be incremented for status 400")
					}
					if attrs["status_code"] == "500" {
						found500 = true
					}
				}
				assert.True(t, found400, "Should find metric with status code 400")
				assert.False(t, found500, "Should NOT find metric with status code 500")

				// Verify error metric was recorded (400 is an error)
				require.NotNil(t, errorsMetric, "errors_total metric should be present")
				assert.Greater(t, len(errorsMetric.Metric), 0, "Should have error metric samples")
			}
		})
	}
}
