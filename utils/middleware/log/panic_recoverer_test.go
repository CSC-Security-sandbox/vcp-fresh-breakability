package log

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	utilsmiddleware "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
)

func TestRecoverMiddleware(t *testing.T) {
	defer func() {
		if recover() != nil {
			t.Fail()
		}
	}()

	// Mock logger to capture logs
	var buf bytes.Buffer
	trace := &Slogger{
		slogger: slog.New(slog.NewJSONHandler(&buf, nil)),
	}

	// Wrap the handler with RecoverMiddleware
	handler := RecoverMiddleware(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		doPanic()
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://testing", nil)

	// Add the mock logger to the request context
	ctx := req.Context()
	ctx = context.WithValue(ctx, utilsmiddleware.ContextSLoggerKey, trace)
	req = req.WithContext(ctx)

	handler.ServeHTTP(rec, req)

	// Validate the log output
	logOutput := buf.String()
	assert.Contains(t, logOutput, `"msg":"Recovered from panic"`)
	assert.Contains(t, logOutput, `"panic":"Panic! I'm panicking!!"`)

	// Validate the response
	if value, present := rec.Header()[("Content-Type")]; !present {
		t.Error("Content-Type missing from header")
	} else {
		if len(value) != 1 {
			t.Error("Content-Type header value does not match expected one")
		} else {
			if value[0] != "application/json; charset=utf-8" {
				t.Errorf("Content-Type header value '%s' does not match expected 'application/json; charset=utf-8'", value[0])
			}
		}
	}
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("Response code %v does not match expected %v", rec.Code, http.StatusInternalServerError)
	}
	if !strings.HasPrefix(strings.TrimSpace(rec.Body.String()), `{"code":500,"message":"Internal server error - please contact support"`) {
		t.Errorf("Response body '%s' does not match expected one", rec.Body.String())
	}
}

func TestRecoverAndExecHttpServer(t *testing.T) {
	t.Run("WhenNormalError", func(tt *testing.T) {
		var buf bytes.Buffer
		trace := &Slogger{
			slogger: slog.New(slog.NewJSONHandler(&buf, nil)),
		}
		defer func() {
			if recover() != nil {
				tt.Fail()
			}
		}()

		called := false
		doPanicAndRecoverAndExecHTTPServer(trace, func(r interface{}) { called = true }, "some panic stuff")
		logOutput := buf.String()
		assert.True(tt, called)
		assert.Contains(tt, logOutput, `"msg":"Recovered from panic"`)
		assert.Contains(tt, logOutput, `"panic":"some panic stuff"`)
		assert.Contains(tt, logOutput, `"stack":`)
	})

	t.Run("WhenConnectionReset", func(tt *testing.T) {
		var buf bytes.Buffer
		trace := &Slogger{
			slogger: slog.New(slog.NewJSONHandler(&buf, nil)),
		}
		defer func() {
			if recover() != nil {
				tt.Fail()
			}
		}()

		called := false
		doPanicAndRecoverAndExecHTTPServer(trace, func(r interface{}) { called = true }, syscall.ECONNRESET)
		logOutput := buf.String()
		assert.False(tt, called)
		assert.Contains(tt, logOutput, `"msg":"Caller hung up"`)
		assert.Contains(tt, logOutput, `"error":"connection reset by peer"`)
		assert.Contains(tt, logOutput, `"stack":`)
	})
}

func TestNoRecover(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fail()
		}
	}()

	doPanic()
}

// TestRecoverFunction tests the Recover function directly
func TestRecoverFunction(t *testing.T) {
	t.Run("Recover function handles panic", func(t *testing.T) {
		var buf bytes.Buffer
		trace := &Slogger{
			slogger: slog.New(slog.NewJSONHandler(&buf, nil)),
		}

		// Test that Recover function can handle panics
		defer func() {
			if recover() != nil {
				t.Fail()
			}
		}()

		// This should trigger the Recover function
		func() {
			defer Recover(trace)
			panic("test panic for Recover function")
		}()

		// Verify that the panic was logged
		logOutput := buf.String()
		assert.Contains(t, logOutput, `"msg":"Recovered from panic"`)
		assert.Contains(t, logOutput, `"panic":"test panic for Recover function"`)
		assert.Contains(t, logOutput, `"stack":`)
	})
}

// TestRecoverAndExecHTTPServerWithNilRecoveryFunc tests the case where recovery function is nil
func TestRecoverAndExecHTTPServerWithNilRecoveryFunc(t *testing.T) {
	t.Run("nil recovery function is handled", func(t *testing.T) {
		var buf bytes.Buffer
		trace := &Slogger{
			slogger: slog.New(slog.NewJSONHandler(&buf, nil)),
		}

		defer func() {
			if recover() != nil {
				t.Fail()
			}
		}()

		// Test with nil recovery function
		func() {
			defer RecoverAndExecHTTPServer(trace, nil)
			panic("test panic with nil recovery function")
		}()

		// Verify that the panic was logged but no recovery function was called
		logOutput := buf.String()
		assert.Contains(t, logOutput, `"msg":"Recovered from panic"`)
		assert.Contains(t, logOutput, `"panic":"test panic with nil recovery function"`)
		assert.Contains(t, logOutput, `"stack":`)
	})
}

// TestRecoverAndExecHTTPServerWithPanickingRecoveryFunc tests the case where recovery function itself panics
func TestRecoverAndExecHTTPServerWithPanickingRecoveryFunc(t *testing.T) {
	t.Run("panicking recovery function is handled", func(t *testing.T) {
		var buf bytes.Buffer
		trace := &Slogger{
			slogger: slog.New(slog.NewJSONHandler(&buf, nil)),
		}

		defer func() {
			if recover() != nil {
				t.Fail()
			}
		}()

		// Test with a recovery function that panics
		panickingRecoveryFunc := func(r interface{}) {
			panic("recovery function panicked")
		}

		func() {
			defer RecoverAndExecHTTPServer(trace, panickingRecoveryFunc)
			panic("test panic with panicking recovery function")
		}()

		// Verify that both panics were logged
		logOutput := buf.String()
		assert.Contains(t, logOutput, `"msg":"Recovered from panic"`)
		assert.Contains(t, logOutput, `"panic":"test panic with panicking recovery function"`)
		assert.Contains(t, logOutput, `"panic":"recovery function panicked"`)
		assert.Contains(t, logOutput, `"stack":`)
	})
}

func doPanicAndRecoverAndExecHTTPServer(traceLog Logger, f RecoveryFunc, panicParam interface{}) {
	defer RecoverAndExecHTTPServer(traceLog, f)
	panic(panicParam)
}

func doPanic() {
	panic("Panic! I'm panicking!!")
}

// TestRecoverMiddlewareJSONEncodingError tests the error handling when JSON encoding fails
// This covers lines 25-27 in the panic_recoverer.go file
func TestRecoverMiddlewareJSONEncodingError(t *testing.T) {
	defer func() {
		if recover() != nil {
			t.Fail()
		}
	}()

	// Mock logger to capture logs
	var buf bytes.Buffer
	trace := &Slogger{
		slogger: slog.New(slog.NewJSONHandler(&buf, nil)),
	}

	// Create a custom response writer that will fail during Write operations
	// This will trigger the JSON encoding error path
	failingWriter := &failingResponseWriter{
		ResponseWriter: httptest.NewRecorder(),
		shouldFail:     true,
	}

	// Wrap the handler with RecoverMiddleware
	handler := RecoverMiddleware(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// This will cause a panic that should trigger the recovery function
		panic("test panic for JSON encoding error")
	}))

	req := httptest.NewRequest("GET", "http://testing", nil)

	// Add the mock logger to the request context
	ctx := req.Context()
	ctx = context.WithValue(ctx, utilsmiddleware.ContextSLoggerKey, trace)
	req = req.WithContext(ctx)

	handler.ServeHTTP(failingWriter, req)

	// Validate the log output - should contain both the panic recovery and the JSON encoding error
	logOutput := buf.String()
	assert.Contains(t, logOutput, `"msg":"Recovered from panic"`)
	assert.Contains(t, logOutput, `"panic":"test panic for JSON encoding error"`)

	// The JSON encoding error should also be logged
	assert.Contains(t, logOutput, `"msg":"Error encoding JSON response: write failed"`)

	// Validate the response - should fall back to http.Error when JSON encoding fails
	if failingWriter.ResponseWriter.(*httptest.ResponseRecorder).Code != http.StatusInternalServerError {
		t.Errorf("Response code %v does not match expected %v", failingWriter.ResponseWriter.(*httptest.ResponseRecorder).Code, http.StatusInternalServerError)
	}
}

// failingResponseWriter is a custom response writer that fails during Write operations
type failingResponseWriter struct {
	http.ResponseWriter
	shouldFail bool
}

func (f *failingResponseWriter) Write(data []byte) (int, error) {
	if f.shouldFail {
		return 0, errors.New("write failed")
	}
	return f.ResponseWriter.Write(data)
}

func (f *failingResponseWriter) WriteHeader(statusCode int) {
	f.ResponseWriter.WriteHeader(statusCode)
}

func (f *failingResponseWriter) Header() http.Header {
	return f.ResponseWriter.Header()
}

// TestRecoverMiddlewareWithCustomError tests the error handling with a custom error type
// that might cause JSON encoding issues
func TestRecoverMiddlewareWithCustomError(t *testing.T) {
	defer func() {
		if recover() != nil {
			t.Fail()
		}
	}()

	// Mock logger to capture logs
	var buf bytes.Buffer
	trace := &Slogger{
		slogger: slog.New(slog.NewJSONHandler(&buf, nil)),
	}

	// Wrap the handler with RecoverMiddleware
	handler := RecoverMiddleware(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// This will cause a panic that should trigger the recovery function
		panic("custom error panic")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://testing", nil)

	// Add the mock logger to the request context
	ctx := req.Context()
	ctx = context.WithValue(ctx, utilsmiddleware.ContextSLoggerKey, trace)
	req = req.WithContext(ctx)

	handler.ServeHTTP(rec, req)

	// Validate the log output
	logOutput := buf.String()
	assert.Contains(t, logOutput, `"msg":"Recovered from panic"`)
	assert.Contains(t, logOutput, `"panic":"custom error panic"`)

	// Validate the response
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("Response code %v does not match expected %v", rec.Code, http.StatusInternalServerError)
	}
}
