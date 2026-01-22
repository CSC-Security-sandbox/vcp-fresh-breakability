package middleware

import (
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
	})

	return initErr
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
