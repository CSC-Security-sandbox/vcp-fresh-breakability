package monitoring

import (
	"net/http"
	"strconv"
	"time"
)

// MetricsMiddlewareWithRecorder creates middleware using the provided MetricsRecorder
func MetricsMiddlewareWithRecorder(recorder MetricsRecorder) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			endpoint := r.URL.Path
			method := r.Method
			// Wrap response writer to capture status code
			wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

			// Process request
			next.ServeHTTP(wrapped, r)

			// Record latency
			duration := time.Since(start).Seconds()
			apiParams := &MetricRecorderParams{
				EndPoint:        endpoint,
				Method:          method,
				StatusCode:      strconv.Itoa(wrapped.statusCode),
				LatencyDuration: duration,
			}
			recorder.RecordAPILatency(apiParams)

			// Record request count
			recorder.RecordAPIRequest(apiParams)
		})
	}
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
