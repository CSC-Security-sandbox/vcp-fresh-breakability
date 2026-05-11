package middleware

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/oci-proxy/metrics"
	utilsmiddleware "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/httphelpers"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

const metricsPath = "/metrics"

// WrapWithOCIAndLogging wires the OCI HTTP stack (outer → inner):
//  1. ociMetricsMiddleware — Prometheus counter + duration histogram; outermost so latency
//     covers the full middleware chain (request-prep, logging, handler).
//  2. ociPrepareRequestMiddleware — opc-request-id (or generate), x-correlation-id = opc,
//     HeaderContextKey, ContextSLoggerKey and TemporalSLoggerKey share the same requestFields map,
//     opc-request-id on response.
//  3. httphelpers.LoggingHttpHandler — access logs (default logger; does not read ContextSLoggerKey).
//
// Apply auth and recover outside this wrapper.
func WrapWithOCIAndLogging(api http.Handler) http.Handler {
	api = httphelpers.LoggingHttpHandler(api)
	api = ociPrepareRequestMiddleware(api)
	api = ociMetricsMiddleware(api)
	return api
}

// ociPrepareRequestMiddleware sets OPC / correlation headers, attaches HeaderContextKey, wires the
// request logger (ContextSLoggerKey), and propagates the same fields on TemporalSLoggerKey for workflows.
func ociPrepareRequestMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == metricsPath {
			next.ServeHTTP(w, r)
			return
		}

		opcID := r.Header.Get(string(utilsmiddleware.OPCRequestIDHeaderName))
		if opcID == "" {
			opcID = uuid.NewString()
			r.Header.Set(string(utilsmiddleware.OPCRequestIDHeaderName), opcID)
		}
		// Align with GCNV: x-correlation-id is the same as opc-request-id for tracing / GetCoRelationIDFromContext.
		r.Header.Set(log.RequestCorrelationID, opcID)

		requestFields := log.Fields{
			"requestCorrelationID": opcID,
			"traceMethod":          r.Method,
			"traceURL":             r.URL.String(),
		}
		logger := log.NewLogger().WithFields("requestFields", requestFields)
		ctx := r.Context()
		ctx = context.WithValue(ctx, utilsmiddleware.HeaderContextKey, r.Header)
		ctx = context.WithValue(ctx, utilsmiddleware.ContextSLoggerKey, logger)
		ctx = context.WithValue(ctx, utilsmiddleware.TemporalSLoggerKey, requestFields)

		rw := &opcRequestIDResponseWriter{
			ResponseWriter: w,
			opcID:          opcID,
		}
		next.ServeHTTP(rw, r.WithContext(ctx))
	})
}

// opcRequestIDResponseWriter sets opc-request-id on the HTTP response (OCI contract) when the handler
// commits headers via WriteHeader or Write.
type opcRequestIDResponseWriter struct {
	http.ResponseWriter
	opcID       string
	headerAdded bool
}

func (w *opcRequestIDResponseWriter) ensureHeader() {
	if w.headerAdded {
		return
	}
	w.ResponseWriter.Header().Set(string(utilsmiddleware.OPCRequestIDHeaderName), w.opcID)
	w.headerAdded = true
}

func (w *opcRequestIDResponseWriter) WriteHeader(code int) {
	w.ensureHeader()
	w.ResponseWriter.WriteHeader(code)
}

func (w *opcRequestIDResponseWriter) Write(b []byte) (int, error) {
	w.ensureHeader()
	return w.ResponseWriter.Write(b)
}

func ociMetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == metricsPath {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		wrapped := &statusCapturingWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		endpoint := metrics.NormalizeRoute(r.URL.Path)
		method := r.Method
		region := metrics.Region()
		statusCode := strconv.Itoa(wrapped.statusCode)
		metrics.APIRequestsTotal.WithLabelValues(endpoint, method, statusCode, region).Inc()
		metrics.APIRequestDurationSeconds.WithLabelValues(method, endpoint, region).Observe(time.Since(start).Seconds())
	})
}

type statusCapturingWriter struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
}

func (w *statusCapturingWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.statusCode = code
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusCapturingWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}
