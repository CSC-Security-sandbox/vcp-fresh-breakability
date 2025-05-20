package log

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
)

// TestNewLogger tests the NewLogger function
func TestNewLogger(t *testing.T) {
	t.Run("Initialize the new logger", func(t *testing.T) {
		logger := NewLogger()
		assert.IsType(t, &Slogger{}, logger)
	})
}

// TestLoggingMiddleware tests the LoggingMiddleware function
func TestLoggingMiddleware(t *testing.T) {
	t.Run("Injecting logger into request context", func(t *testing.T) {
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger := r.Context().Value(middleware.ContextSLoggerKey)
			assert.NotNil(t, logger, "Logger should not be nil in context")
			assert.IsType(t, &Slogger{}, logger, "Logger should be of type Slogger")
		})

		logmiddleware := LoggingMiddleware(nextHandler)
		req, _ := http.NewRequest("GET", "/", nil)
		rr := httptest.NewRecorder()

		logmiddleware.ServeHTTP(rr, req)
	})

	t.Run("Missing logger in context", func(t *testing.T) {
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger := r.Context().Value(middleware.ContextSLoggerKey)
			assert.Nil(t, logger, "Logger should be nil if not injected")
		})

		req, _ := http.NewRequest("GET", "/", nil)
		rr := httptest.NewRecorder()

		nextHandler.ServeHTTP(rr, req)
	})

	t.Run("Missing headers in HTTP request", func(t *testing.T) {
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger := r.Context().Value(middleware.ContextSLoggerKey)
			assert.NotNil(t, logger, "Logger should not be nil in context")
			assert.IsType(t, &Slogger{}, logger, "Logger should be of type Slogger")
		})

		logmiddleware := LoggingMiddleware(nextHandler)
		req, _ := http.NewRequest("GET", "/", nil)
		rr := httptest.NewRecorder()

		logmiddleware.ServeHTTP(rr, req)
	})

	t.Run("Inject logger into request context with headers", func(t *testing.T) {
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger := r.Context().Value(middleware.ContextSLoggerKey)
			assert.NotNil(t, logger, "Logger should not be nil in context")
			assert.IsType(t, &Slogger{}, logger, "Logger should be of type Slogger")
		})

		logmiddleware := LoggingMiddleware(nextHandler)
		req, _ := http.NewRequest("GET", "/", nil)
		req.Header.Set("X-Custom-Header", "CustomValue")
		req.Header.Set(RequestCorrelationID, "test-correlation-id")
		req.Header.Set(requestID, "test-request-id")
		rr := httptest.NewRecorder()

		logmiddleware.ServeHTTP(rr, req)
	})
}

// TestExtractFieldsFromHttpRequest tests the extractFieldsFromHttpRequest function
func TestExtractFieldsFromHttpRequest(t *testing.T) {
	t.Run("Extract fields from HTTP request", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/", nil)
		req.Header.Set(RequestCorrelationID, "test-correlation-id")
		req.Header.Set(requestID, "test-request-id")

		logger, fields := extractFieldsFromHttpRequest(req)
		assert.IsType(t, &Slogger{}, logger)
		assert.NotNil(t, fields)
		assert.Equal(t, "test-correlation-id", fields["requestCorrelationID"])
		assert.Equal(t, "test-request-id", fields["requestID"])
	})

	t.Run("Missing headers in HTTP request", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/", nil)

		logger, fields := extractFieldsFromHttpRequest(req)
		assert.IsType(t, &Slogger{}, logger)
		assert.NotNil(t, fields)
		assert.NotEmpty(t, fields["requestCorrelationID"]) // Should generate a new correlation ID
		assert.Empty(t, fields["requestID"])
	})
}

// TestGetLogger tests the getLogger function
func TestGetLogger(t *testing.T) {
	t.Run("Logger Type as slog", func(t *testing.T) {
		config := Config{
			LoggerType: "slog",
		}
		logger := getLogger(config)
		assert.IsType(t, &Slogger{}, logger)
	})

	t.Run("Invalid Logger Type", func(t *testing.T) {
		config := Config{
			LoggerType: "unknown",
		}
		logger := getLogger(config)
		assert.IsType(t, &Slogger{}, logger)
	})

	t.Run("Logger Type is Empty", func(t *testing.T) {
		config := Config{
			LoggerType: "",
		}
		logger := getLogger(config)
		assert.IsType(t, &Slogger{}, logger)
	})
}

// TestSecretString tests the Secret String method
func TestSecretString(t *testing.T) {
	t.Run("mask secret string", func(t *testing.T) {
		secret := Secret("mysecret")
		assert.Equal(t, PasswordMask, secret.String())
	})

	t.Run("empty secret string", func(t *testing.T) {
		secret := Secret("")
		assert.Equal(t, PasswordMask, secret.String())
	})

	t.Run("special characters", func(t *testing.T) {
		secret := Secret("!@#$%^&*()")
		assert.Equal(t, PasswordMask, secret.String())
	})

	t.Run("long strings", func(t *testing.T) {
		longSecret := Secret("this is a very long password that exceeds typical password length")
		assert.Equal(t, PasswordMask, longSecret.String())
	})

	t.Run("Numeric values", func(t *testing.T) {
		numericSecret := Secret("1234567890")
		assert.Equal(t, PasswordMask, numericSecret.String())
	})
}

func TestSetupOpenTelemetry(t *testing.T) {
	ctx := context.Background()
	t.Run("Trace and Metrics exporter creation failure", func(t *testing.T) {
		// Simulating failure in trace exporter creation
		originalProjectID := env.OtelGoogleProjectID
		env.OtelGoogleProjectID = ""
		defer func() { env.OtelGoogleProjectID = originalProjectID }()

		shutdown, err := SetupOpenTelemetry(ctx)
		assert.Error(t, err)
		assert.NotNil(t, shutdown)

		// Calling shutdown to ensure cleanup happens even on error
		err = shutdown(ctx)
		assert.NoError(t, err)
	})

	t.Run("Service Name is Empty", func(t *testing.T) {
		// Simulating failure when service name is empty
		originalServiceName := env.ServiceName
		env.ServiceName = ""
		defer func() { env.ServiceName = originalServiceName }()

		shutdown, err := SetupOpenTelemetry(ctx)
		assert.Error(t, err)
		assert.NotNil(t, shutdown)

		// Calling shutdown to ensure cleanup happens even on error
		err = shutdown(ctx)
		assert.NoError(t, err)
	})

	t.Run("Missing Google Cloud credentials", func(t *testing.T) {
		// Simulate missing Google Cloud credentials
		originalGoogleProjectID := env.OtelGoogleProjectID
		env.OtelGoogleProjectID = "invalid-project-id"
		defer func() { env.OtelGoogleProjectID = originalGoogleProjectID }()

		shutdown, err := SetupOpenTelemetry(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "could not find default credentials")
		assert.NotNil(t, shutdown)

		// Calling shutdown to ensure cleanup happens even on error
		err = shutdown(ctx)
		assert.NoError(t, err)
	})
}
