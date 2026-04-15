package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
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
