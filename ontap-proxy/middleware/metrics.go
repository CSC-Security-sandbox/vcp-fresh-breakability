package middleware

import (
	"context"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Context keys for backend metrics (set by CredentialMiddleware, read by reverseproxy).
type ontapMetricsContextKey string

const (
	ontapMetricsProjectIDKey  ontapMetricsContextKey = "ontap_metrics_project_id"
	ontapMetricsPoolIDKey     ontapMetricsContextKey = "ontap_metrics_pool_id"
	ontapMetricsNormalizedKey ontapMetricsContextKey = "ontap_metrics_normalized_path"
	ontapMetricsBackendStartKey ontapMetricsContextKey = "ontap_metrics_backend_start"
)

var (
	initOnce sync.Once
	initErr  error

	// OpenTelemetry meter for creating metrics
	meter metric.Meter

	// ontapProxyRequestsTotal counts the total number of HTTP requests to the reverse proxy
	ontapProxyRequestsTotal metric.Int64Counter

	// ontapProxyRequestDurationSeconds measures the latency of HTTP requests
	ontapProxyRequestDurationSeconds metric.Float64Histogram

	// ontapProxyErrorsTotal counts HTTP errors (4xx and 5xx status codes)
	ontapProxyErrorsTotal metric.Int64Counter

	// Backend metrics (requests sent to the pool's backend cluster, their duration, and backend errors)
	ontapProxyBackendRequestsTotal          metric.Int64Counter
	ontapProxyBackendRequestDurationSeconds metric.Float64Histogram
	ontapProxyBackendErrorsTotal             metric.Int64Counter

	// Numeric ID pattern (for volume IDs, etc.)
	numericIDPattern = regexp.MustCompile(`/\d+`)
	// Project number pattern (long numeric IDs in projects segment)
	projectNumberPattern = regexp.MustCompile(`/projects/\d+`)
	// Location pattern (locations segment)
	locationPattern = regexp.MustCompile(`/locations/[^/]+`)
	// Pool ID pattern (pools segment - already handled by UUID pattern, but keep for clarity)
	poolIDPattern = regexp.MustCompile(`/pools/[^/]+`)
)

// InitMetrics initializes OpenTelemetry metrics for the ontap-proxy middleware.
// This should be called at application startup, after SetupOpenTelemetry.
// Uses sync.Once to ensure metrics are only initialized once, even if called multiple times.
// Returns an error if metric initialization fails.
func InitMetrics() error {
	initOnce.Do(func() {
		meter = otel.GetMeterProvider().Meter("ontap-proxy")

		var err error

		// Initialize requests counter
		ontapProxyRequestsTotal, err = meter.Int64Counter(
			"ontap_proxy_requests_total",
			metric.WithDescription("Total number of HTTP requests to the ONTAP reverse proxy"),
			metric.WithUnit("{request}"),
		)
		if err != nil {
			initErr = err
			return
		}

		// Initialize duration histogram
		// Default buckets: .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10 seconds
		ontapProxyRequestDurationSeconds, err = meter.Float64Histogram(
			"ontap_proxy_request_duration_seconds",
			metric.WithDescription("HTTP request latency in seconds for the ONTAP reverse proxy"),
			metric.WithUnit("s"),
		)
		if err != nil {
			initErr = err
			return
		}

		// Initialize errors counter
		ontapProxyErrorsTotal, err = meter.Int64Counter(
			"ontap_proxy_errors_total",
			metric.WithDescription("Total number of HTTP errors (4xx and 5xx) from the ONTAP reverse proxy"),
			metric.WithUnit("{error}"),
		)
		if err != nil {
			initErr = err
			return
		}

		// Backend: requests sent to the pool's backend cluster
		ontapProxyBackendRequestsTotal, err = meter.Int64Counter(
			"ontap_proxy_backend_requests_total",
			metric.WithDescription("Total number of HTTP requests sent to the pool backend (ONTAP cluster)"),
			metric.WithUnit("{request}"),
		)
		if err != nil {
			initErr = err
			return
		}

		// Backend: duration of each backend round-trip
		ontapProxyBackendRequestDurationSeconds, err = meter.Float64Histogram(
			"ontap_proxy_backend_request_duration_seconds",
			metric.WithDescription("Backend (pool cluster) request latency in seconds"),
			metric.WithUnit("s"),
		)
		if err != nil {
			initErr = err
			return
		}

		// Backend: errors from the backend (transport or HTTP 4xx/5xx)
		ontapProxyBackendErrorsTotal, err = meter.Int64Counter(
			"ontap_proxy_backend_errors_total",
			metric.WithDescription("Total number of errors from the pool backend (transport errors or HTTP status; label error_code has actual status e.g. 404/500 or 500 for transport failures)"),
			metric.WithUnit("{error}"),
		)
		if err != nil {
			initErr = err
			return
		}
	})

	return initErr
}

// AddBackendMetricsToContext adds project_id, pool_id, and normalized path to ctx for backend metrics.
// Call from CredentialMiddleware so reverseproxy can use the same labels as top-level metrics.
func AddBackendMetricsToContext(ctx context.Context, projectID, poolID, normalizedPath string) context.Context {
	if projectID == "" {
		projectID = "unknown"
	}
	if poolID == "" {
		poolID = "unknown"
	}
	if normalizedPath == "" {
		normalizedPath = "unknown"
	}
	ctx = context.WithValue(ctx, ontapMetricsProjectIDKey, projectID)
	ctx = context.WithValue(ctx, ontapMetricsPoolIDKey, poolID)
	ctx = context.WithValue(ctx, ontapMetricsNormalizedKey, normalizedPath)
	return ctx
}

// GetBackendMetricsFromContext returns project_id, pool_id, and normalized path from ctx for backend metrics.
// Returns "unknown" for any value not set (e.g. when context was not set by CredentialMiddleware).
func GetBackendMetricsFromContext(ctx context.Context) (projectID, poolID, normalizedPath string) {
	if v := ctx.Value(ontapMetricsProjectIDKey); v != nil {
		projectID, _ = v.(string)
	}
	if v := ctx.Value(ontapMetricsPoolIDKey); v != nil {
		poolID, _ = v.(string)
	}
	if v := ctx.Value(ontapMetricsNormalizedKey); v != nil {
		normalizedPath, _ = v.(string)
	}
	if projectID == "" {
		projectID = "unknown"
	}
	if poolID == "" {
		poolID = "unknown"
	}
	if normalizedPath == "" {
		normalizedPath = "unknown"
	}
	return projectID, poolID, normalizedPath
}

// AddBackendRequestStartToContext stores the backend request start time in ctx so
// metrics can be recorded after response processing (ModifyResponse) with correct duration.
func AddBackendRequestStartToContext(ctx context.Context, start time.Time) context.Context {
	return context.WithValue(ctx, ontapMetricsBackendStartKey, start)
}

// GetBackendRequestStartFromContext returns the backend request start time if set.
// Used by the ModifyResponse path to compute duration when recording metrics after ProcessResponseModification.
func GetBackendRequestStartFromContext(ctx context.Context) (time.Time, bool) {
	v := ctx.Value(ontapMetricsBackendStartKey)
	if v == nil {
		return time.Time{}, false
	}
	t, ok := v.(time.Time)
	return t, ok
}

// StatusClass returns the HTTP status class ("2xx", "4xx", "5xx"). Kept for tests and optional grouping;
// backend metrics (RecordBackendRequest, RecordBackendDuration) use exact status_code instead.
// Returns "unknown" for invalid or unexpected status codes.
func StatusClass(statusCode int) string {
	switch {
	case statusCode >= 200 && statusCode < 300:
		return "2xx"
	case statusCode >= 400 && statusCode < 500:
		return "4xx"
	case statusCode >= 500 && statusCode < 600:
		return "5xx"
	default:
		return "unknown"
	}
}

// RecordBackendRequest records one backend request (same labels as top-level for correlation).
// statusCode is the HTTP status from the backend response; recorded as exact status_code label (e.g. "200", "404").
func RecordBackendRequest(ctx context.Context, method, projectID, poolID, path string, statusCode int) {
	if ontapProxyBackendRequestsTotal == nil {
		return
	}
	attrs := backendAttrs(method, projectID, poolID, path, strconv.Itoa(statusCode))
	ontapProxyBackendRequestsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordBackendDuration records backend request duration in seconds.
// statusCode is the HTTP status from the backend response; recorded as exact status_code label (e.g. "200", "404").
func RecordBackendDuration(ctx context.Context, durationSec float64, method, projectID, poolID, path string, statusCode int) {
	if ontapProxyBackendRequestDurationSeconds == nil {
		return
	}
	attrs := backendAttrs(method, projectID, poolID, path, strconv.Itoa(statusCode))
	ontapProxyBackendRequestDurationSeconds.Record(ctx, durationSec, metric.WithAttributes(attrs...))
}

// RecordBackendError records a backend error with error_code: for HTTP 4xx/5xx the actual status code (e.g. "404", "500"); for transport errors "500".
func RecordBackendError(ctx context.Context, method, projectID, poolID, path, errorCode string) {
	if ontapProxyBackendErrorsTotal == nil {
		return
	}
	attrs := backendAttrs(method, projectID, poolID, path, "") // no status_code for error path
	attrs = append(attrs, attribute.String("error_code", errorCode))
	ontapProxyBackendErrorsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func backendAttrs(method, projectID, poolID, path, statusCode string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("method", method),
		attribute.String("project_id", projectID),
		attribute.String("pool_id", poolID),
		attribute.String("path", path),
	}
	if statusCode != "" {
		attrs = append(attrs, attribute.String("status_code", statusCode))
	}
	return attrs
}

// BackendErrorCode constants for transport/connection errors (no HTTP response).
// All transport failures (connection refused, timeout, host not found, TLS, etc.) are server-side/reachability errors → use 500.
// For HTTP 4xx/5xx, we pass the actual status code as string (e.g. "404", "500") from the backend.
const (
	BackendErrorTransport = "500"   // all transport/connection errors (no response from backend)
	BackendErrorUnknown   = "unknown" // only when err is nil (callers should not record in that case)
)

// ClassifyBackendError maps any transport/connection error to an error_code for ontap_proxy_backend_errors_total.
// All such errors (connection refused, timeout, host not found, TLS, etc.) are server-side → we use "500".
// Use when no HTTP response is received (RoundTrip error, handleProxyError path). For HTTP 4xx/5xx use the actual status code instead.
func ClassifyBackendError(err error) string {
	if err == nil {
		return BackendErrorUnknown
	}
	return BackendErrorTransport
}

// BackendErrorCodeForMetric returns the error_code we record: backend HTTP status (e.g. "404", "500") when we have a response,
// or a 5xx-style code (500) when we don't. Never raw error messages.
// Returns empty string when there is no error to record.
func BackendErrorCodeForMetric(statusCode int, err error) string {
	if statusCode >= 400 {
		return strconv.Itoa(statusCode)
	}
	if err != nil {
		return ClassifyBackendError(err)
	}
	return ""
}

// initMetrics is a private wrapper that ensures metrics are initialized.
// It's called lazily by MetricsMiddleware as a safety check.
func initMetrics() error {
	if meter == nil {
		return InitMetrics()
	}
	return nil
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	if rw.statusCode == http.StatusOK {
		rw.statusCode = code
	}
	rw.ResponseWriter.WriteHeader(code)
}

// normalizePath normalizes the path to reduce cardinality by replacing UUIDs and IDs with placeholders
// This is critical to prevent high cardinality in Prometheus metrics
// Returns only the ONTAP API path portion (after /ontap/) to keep the path label short
// since pool_id is already captured as a separate label
func normalizePath(path string) string {
	// Extract only the ONTAP API path portion (after /ontap/)
	// This reduces path length significantly since we already have pool_id as a separate label
	// Example: /v1beta/projects/.../pools/pool-123/ontap/api/storage/volumes/{uuid}
	//          -> /api/storage/volumes/{uuid}
	ontapPath := extractOntapPathForMetrics(path)
	if ontapPath == "" {
		// Fallback: if we can't extract ONTAP path, normalize the full path
		// This should rarely happen for passthrough routes
		return normalizeFullPath(path)
	}

	// Normalize the ONTAP API path only
	normalized := ontapPath

	// Step 1: Replace UUIDs with {uuid}
	normalized = uuidPattern.ReplaceAllString(normalized, "/{uuid}")

	// Step 2: Replace numeric IDs in API paths with {id}
	normalized = numericIDPattern.ReplaceAllString(normalized, "/{id}")

	return normalized
}

// extractOntapPathForMetrics extracts the ONTAP API path from the full request path
// Returns the path after /ontap/ segment, or empty string if not found
func extractOntapPathForMetrics(fullPath string) string {
	parts := strings.Split(fullPath, "/")
	ontapIndex := -1
	for i, part := range parts {
		if part == "ontap" {
			ontapIndex = i
			break
		}
	}
	if ontapIndex == -1 || ontapIndex >= len(parts)-1 {
		return ""
	}
	// Return path starting with / and including everything after /ontap/
	return "/" + strings.Join(parts[ontapIndex+1:], "/")
}

// normalizeFullPath is a fallback that normalizes the full path
// Used when we can't extract the ONTAP path portion
func normalizeFullPath(path string) string {
	normalized := path

	// Replace project numbers: /projects/123456789 -> /projects/{project}
	normalized = projectNumberPattern.ReplaceAllString(normalized, "/projects/{project}")

	// Replace locations: /locations/australia-southeast1-b -> /locations/{location}
	normalized = locationPattern.ReplaceAllString(normalized, "/locations/{location}")

	// Replace pool IDs: /pools/pool-123 -> /pools/{pool}
	normalized = poolIDPattern.ReplaceAllString(normalized, "/pools/{pool}")

	// Replace UUIDs with {uuid}
	normalized = uuidPattern.ReplaceAllString(normalized, "/{uuid}")

	// Replace numeric IDs in API paths with {id}
	if strings.Contains(normalized, "/api/") {
		parts := strings.Split(normalized, "/api/")
		if len(parts) == 2 {
			apiPath := parts[1]
			apiPath = numericIDPattern.ReplaceAllString(apiPath, "/{id}")
			normalized = parts[0] + "/api/" + apiPath
		}
	}

	return normalized
}

// extractProjectID extracts project ID from the request path
func extractProjectID(path string) string {
	// Path format: /v1beta/projects/{projectId}/locations/{locationId}/pools/{poolId}/ontap/...
	parts := strings.Split(path, "/")
	if len(parts) >= 4 && parts[1] == "v1beta" && parts[2] == "projects" && parts[3] != "" {
		return parts[3] // projectId is at index 3
	}
	return "unknown"
}

// extractPoolID extracts pool ID from the request path
func extractPoolID(path string) string {
	// Path format: /v1beta/projects/{projectId}/locations/{locationId}/pools/{poolId}/ontap/...
	parts := strings.Split(path, "/")
	if len(parts) >= 8 && parts[1] == "v1beta" && parts[7] != "" {
		return parts[7] // poolId is at index 7
	}
	return "unknown"
}

// MetricsMiddleware records OpenTelemetry metrics for reverse proxy requests
// Only tracks passthrough routes (/v1beta/...) - other routes are automatically excluded
// Note: /health endpoint is already tracked by ogen metrics, /metrics is the Prometheus endpoint itself
// Metrics are exported to Prometheus via the OpenTelemetry Prometheus exporter
// Note: Metrics should be initialized at startup via InitMetrics() for proper error handling.
// This middleware includes a safety check to initialize lazily if InitMetrics() wasn't called.
func MetricsMiddleware() func(http.Handler) http.Handler {
	// Safety check: ensure metrics are initialized (should already be done at startup)
	_ = initMetrics()

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path := r.URL.Path
			ctx := r.Context()

			// Only track passthrough routes (paths starting with /v1beta/)
			// Other routes (e.g., /health, /metrics, /v1/expertMode) are automatically excluded
			isPassthroughRoute := strings.HasPrefix(path, "/v1beta/")

			if !isPassthroughRoute {
				// Not a passthrough route, skip metrics and continue
				next.ServeHTTP(w, r)
				return
			}

			start := time.Now()

			// Wrap response writer to capture status code
			rw := &responseWriter{
				ResponseWriter: w,
				statusCode:     http.StatusOK, // Default status code
			}

			// Process request
			next.ServeHTTP(rw, r)

			// Calculate duration
			duration := time.Since(start).Seconds()

			// Extract attributes (labels in Prometheus terms)
			method := r.Method
			statusCode := strconv.Itoa(rw.statusCode)
			projectID := extractProjectID(path)
			poolID := extractPoolID(path)
			normalizedPath := normalizePath(path)

			// Build attributes for all metrics
			attrs := []attribute.KeyValue{
				attribute.String("method", method),
				attribute.String("status_code", statusCode),
				attribute.String("project_id", projectID),
				attribute.String("pool_id", poolID),
				attribute.String("path", normalizedPath),
			}

			// Record metrics using OpenTelemetry API
			if ontapProxyRequestsTotal != nil {
				ontapProxyRequestsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
			}

			if ontapProxyRequestDurationSeconds != nil {
				ontapProxyRequestDurationSeconds.Record(ctx, duration, metric.WithAttributes(attrs...))
			}

			// Record errors (4xx and 5xx)
			if rw.statusCode >= 400 && ontapProxyErrorsTotal != nil {
				ontapProxyErrorsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
			}
		})
	}
}
