package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/oci-proxy/metrics"
	utilsmiddleware "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// opcRequestIDHTTPHeader is the wire name for OPCRequestIDHeaderName (ContextString);
// net/http Header methods require string keys.
var opcRequestIDHTTPHeader = string(utilsmiddleware.OPCRequestIDHeaderName)

func TestOciPrepareRequestMiddleware_EchoesClientOPC(t *testing.T) {
	const clientID = "client-opc-id-123"
	h := ociPrepareRequestMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/test", nil)
	req.Header.Set(opcRequestIDHTTPHeader, clientID)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, clientID, rr.Header().Get(opcRequestIDHTTPHeader))
	require.Equal(t, clientID, req.Header.Get(log.RequestCorrelationID))
}

func TestOciPrepareRequestMiddleware_GeneratesOPCWhenMissing(t *testing.T) {
	var seen string
	h := ociPrepareRequestMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get(opcRequestIDHTTPHeader)
		require.NotEmpty(t, seen)
		require.Equal(t, seen, r.Header.Get(log.RequestCorrelationID))
		w.WriteHeader(http.StatusCreated)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/test", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusCreated, rr.Code)
	require.NotEmpty(t, rr.Header().Get(opcRequestIDHTTPHeader))
	require.Equal(t, seen, rr.Header().Get(opcRequestIDHTTPHeader))
}

func TestOciPrepareRequestMiddleware_SkipsMetrics(t *testing.T) {
	h := ociPrepareRequestMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, "", rr.Header().Get(opcRequestIDHTTPHeader))
}

func TestOciPrepareRequestMiddleware_DoesNotSetXRequestID(t *testing.T) {
	h := ociPrepareRequestMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Empty(t, r.Header.Get(log.RequestID))
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodPost, "/v1/test", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestOciPrepareRequestMiddleware_SetsContextLogger(t *testing.T) {
	const clientOPC = "cccccccc-cccc-4ccc-cccc-cccccccccccc"
	h := ociPrepareRequestMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v := r.Context().Value(utilsmiddleware.ContextSLoggerKey)
		require.NotNil(t, v)
		_, ok := v.(log.Logger)
		require.True(t, ok, "ContextSLoggerKey should hold log.Logger")
		hdr, ok := r.Context().Value(utilsmiddleware.HeaderContextKey).(http.Header)
		require.True(t, ok)
		require.Equal(t, clientOPC, hdr.Get(opcRequestIDHTTPHeader))
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1beta/pools", nil)
	req.Header.Set(opcRequestIDHTTPHeader, clientOPC)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, clientOPC, rr.Header().Get(opcRequestIDHTTPHeader))
}

func TestOciPrepareRequestMiddleware_SetsTemporalFields(t *testing.T) {
	const opc = "11111111-1111-1111-1111-111111111111"
	var temporal log.Fields
	h := ociPrepareRequestMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, ok := r.Context().Value(utilsmiddleware.TemporalSLoggerKey).(log.Fields)
		require.True(t, ok)
		temporal = v
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1beta/pools", nil)
	req.Header.Set(opcRequestIDHTTPHeader, opc)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, opc, temporal["requestCorrelationID"])
	require.Equal(t, http.MethodPost, temporal["traceMethod"])
	require.Equal(t, "/v1beta/pools", temporal["traceURL"])
}

func TestWrapWithOCIAndLogging_InnerHandlerSeesTemporal(t *testing.T) {
	const opc = "22222222-2222-2222-2222-222222222222"
	var temporal log.Fields
	var ctxLoggerOK bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, ok := r.Context().Value(utilsmiddleware.TemporalSLoggerKey).(log.Fields)
		require.True(t, ok)
		temporal = v
		_, ctxLoggerOK = r.Context().Value(utilsmiddleware.ContextSLoggerKey).(log.Logger)
		w.WriteHeader(http.StatusOK)
	})
	h := WrapWithOCIAndLogging(inner)

	req := httptest.NewRequest(http.MethodPost, "/v1beta/pools", nil)
	req.Header.Set(opcRequestIDHTTPHeader, opc)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, opc, temporal["requestCorrelationID"])
	require.True(t, ctxLoggerOK, "inner handler should see ContextSLoggerKey from ociPrepareRequestMiddleware")
}

// --- ociMetricsMiddleware tests ---

func TestOciMetricsMiddleware_IncrementsCounterOnSuccess(t *testing.T) {
	region := metrics.Region()
	before := testutil.ToFloat64(metrics.APIRequestsTotal.WithLabelValues("/v1beta/pools", "POST", "200", region))

	h := ociMetricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodPost, "/v1beta/pools", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	after := testutil.ToFloat64(metrics.APIRequestsTotal.WithLabelValues("/v1beta/pools", "POST", "200", region))
	assert.Equal(t, before+1, after)
}

func TestOciMetricsMiddleware_NormalizesPoolOCIDPath(t *testing.T) {
	region := metrics.Region()
	normalized := "/v1beta/pools/{poolOCID}"
	before := testutil.ToFloat64(metrics.APIRequestsTotal.WithLabelValues(normalized, "DELETE", "404", region))

	h := ociMetricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	req := httptest.NewRequest(http.MethodDelete, "/v1beta/pools/ocid1.pool.oc1..abc", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	after := testutil.ToFloat64(metrics.APIRequestsTotal.WithLabelValues(normalized, "DELETE", "404", region))
	assert.Equal(t, before+1, after)
}

func TestOciMetricsMiddleware_Captures5xxStatus(t *testing.T) {
	region := metrics.Region()
	before := testutil.ToFloat64(metrics.APIRequestsTotal.WithLabelValues("/v1beta/pools", "POST", "500", region))

	h := ociMetricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	req := httptest.NewRequest(http.MethodPost, "/v1beta/pools", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	after := testutil.ToFloat64(metrics.APIRequestsTotal.WithLabelValues("/v1beta/pools", "POST", "500", region))
	assert.Equal(t, before+1, after)
}

func TestOciMetricsMiddleware_DefaultsTo200WhenNoWriteHeader(t *testing.T) {
	region := metrics.Region()
	before := testutil.ToFloat64(metrics.APIRequestsTotal.WithLabelValues("/v1beta/pools", "GET", "200", region))

	h := ociMetricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1beta/pools", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	after := testutil.ToFloat64(metrics.APIRequestsTotal.WithLabelValues("/v1beta/pools", "GET", "200", region))
	assert.Equal(t, before+1, after)
}

func TestOciMetricsMiddleware_SkipsMetricsPath(t *testing.T) {
	region := metrics.Region()
	before := testutil.ToFloat64(metrics.APIRequestsTotal.WithLabelValues("/metrics", "GET", "200", region))

	h := ociMetricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	after := testutil.ToFloat64(metrics.APIRequestsTotal.WithLabelValues("/metrics", "GET", "200", region))
	assert.Equal(t, before, after)
}

func TestOciMetricsMiddleware_RecordsDuration(t *testing.T) {
	beforeCount := testutil.CollectAndCount(metrics.APIRequestDurationSeconds)

	h := ociMetricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	req := httptest.NewRequest(http.MethodPost, "/v1beta/pools", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	afterCount := testutil.CollectAndCount(metrics.APIRequestDurationSeconds)
	assert.GreaterOrEqual(t, afterCount, beforeCount)
}

func TestOciMetricsMiddleware_MultipleRequestsAccumulate(t *testing.T) {
	region := metrics.Region()
	before := testutil.ToFloat64(metrics.APIRequestsTotal.WithLabelValues("/v1beta/pools", "GET", "200", region))

	h := ociMetricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodGet, "/v1beta/pools", nil)
		h.ServeHTTP(httptest.NewRecorder(), req)
	}

	after := testutil.ToFloat64(metrics.APIRequestsTotal.WithLabelValues("/v1beta/pools", "GET", "200", region))
	assert.Equal(t, before+3, after)
}

// --- statusCapturingWriter tests ---

func TestStatusCapturingWriter_CapturesFirstWriteHeader(t *testing.T) {
	rr := httptest.NewRecorder()
	w := &statusCapturingWriter{ResponseWriter: rr, statusCode: http.StatusOK}

	w.WriteHeader(http.StatusNotFound)
	w.WriteHeader(http.StatusInternalServerError)

	assert.Equal(t, http.StatusNotFound, w.statusCode)
	assert.True(t, w.wroteHeader)
}

func TestStatusCapturingWriter_DefaultsTo200(t *testing.T) {
	rr := httptest.NewRecorder()
	w := &statusCapturingWriter{ResponseWriter: rr, statusCode: http.StatusOK}

	assert.Equal(t, http.StatusOK, w.statusCode)
	assert.False(t, w.wroteHeader)
}

func TestStatusCapturingWriter_WriteSetsFlagButKeepsDefault(t *testing.T) {
	rr := httptest.NewRecorder()
	w := &statusCapturingWriter{ResponseWriter: rr, statusCode: http.StatusOK}

	n, err := w.Write([]byte("hello"))

	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.True(t, w.wroteHeader)
	assert.Equal(t, http.StatusOK, w.statusCode)
}

func TestStatusCapturingWriter_WriteAfterWriteHeaderKeepsOriginal(t *testing.T) {
	rr := httptest.NewRecorder()
	w := &statusCapturingWriter{ResponseWriter: rr, statusCode: http.StatusOK}

	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte("body"))

	assert.Equal(t, http.StatusCreated, w.statusCode)
}

func TestOciMetricsMiddleware_DurationUsesNormalizedEndpoint(t *testing.T) {
	h := ociMetricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))

	req := httptest.NewRequest(http.MethodDelete, "/v1beta/pools/ocid-op-test", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	region := metrics.Region()
	assert.NotPanics(t, func() {
		metrics.APIRequestDurationSeconds.WithLabelValues("DELETE", "/v1beta/pools/{poolOCID}", region)
	})
	assert.GreaterOrEqual(t, testutil.CollectAndCount(metrics.APIRequestDurationSeconds), 1)
}
