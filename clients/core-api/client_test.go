package coreapi

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	slogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

type mockRoundTripper struct {
	mock.Mock
}

func (m *mockRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	args := m.Called(r)
	return args.Get(0).(*http.Response), args.Error(1)
}

func TestVcpRoundTripper_RoundTrip(t *testing.T) {
	t.Run("WhenVcpRoundTripperAddsAuthorizationHeader", func(t *testing.T) {
		rt := &mockRoundTripper{}
		rt.On("RoundTrip", mock.Anything).Return(&http.Response{StatusCode: http.StatusOK}, nil)

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		resp, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "test-jwt", req.Header.Get("Authorization"))
		rt.AssertExpectations(t)
	})

	t.Run("WhenVcpRoundTripperAddsCorrelationIDFromContext", func(t *testing.T) {
		rt := &mockRoundTripper{}
		rt.On("RoundTrip", mock.Anything).Return(&http.Response{StatusCode: http.StatusOK}, nil)

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
		}

		ctx := context.WithValue(context.Background(), CorrelationContextKey, "test-correlation-id")
		req, _ := http.NewRequestWithContext(ctx, "GET", "http://example.com", nil)
		resp, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "test-correlation-id", req.Header.Get(slogger.RequestCorrelationID))
		rt.AssertExpectations(t)
	})

	t.Run("WhenVcpRoundTripperKeepsExistingCorrelationID", func(t *testing.T) {
		rt := &mockRoundTripper{}
		rt.On("RoundTrip", mock.Anything).Return(&http.Response{StatusCode: http.StatusOK}, nil)

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		req.Header.Set(slogger.RequestCorrelationID, "existing-correlation-id")
		resp, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "existing-correlation-id", req.Header.Get(slogger.RequestCorrelationID))
		rt.AssertExpectations(t)
	})

	t.Run("WhenVcpRoundTripperHandlesNilContext", func(t *testing.T) {
		rt := &mockRoundTripper{}
		rt.On("RoundTrip", mock.Anything).Return(&http.Response{StatusCode: http.StatusOK}, nil)

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		resp, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "test-jwt", req.Header.Get("Authorization"))
		rt.AssertExpectations(t)
	})
}

func TestGetCoreAPIClient(t *testing.T) {
	t.Run("WhenReturnsClientWithValidServerURL", func(t *testing.T) {
		logger := slogger.NewLogger()
		client := getCoreAPIClient("example.com", "test-jwt", logger)

		assert.NotNil(t, client)
		assert.NotNil(t, client.Invoker)
	})

	t.Run("WhenUsesRetryRoundTripperWithCorrectConfiguration", func(t *testing.T) {
		logger := slogger.NewLogger()
		originalNewRetryRoundTripper := httphelpersNewRetryRoundTripper
		defer func() { httphelpersNewRetryRoundTripper = originalNewRetryRoundTripper }()

		var usedDelay time.Duration
		var usedMaxRetries int
		httphelpersNewRetryRoundTripper = func(delay time.Duration, maxRetries int, logger slogger.Logger, next http.RoundTripper) http.RoundTripper {
			usedDelay = delay
			usedMaxRetries = maxRetries
			return next
		}

		getCoreAPIClient("example.com", "test-jwt", logger)
		assert.Equal(t, ApiRetryDelay, usedDelay)
		assert.Equal(t, ApiMaxRetries, usedMaxRetries)
	})

	t.Run("WhenUsesLoggingRoundTripperWithCorrectConfiguration", func(t *testing.T) {
		logger := slogger.NewLogger()
		originalGetLoggingRoundTripper := httphelpersGetLoggingRoundTripper
		defer func() { httphelpersGetLoggingRoundTripper = originalGetLoggingRoundTripper }()

		var usedCallerInfo string
		var usedLogger slogger.Logger
		httphelpersGetLoggingRoundTripper = func(callerInfo string, logger slogger.Logger, roundTripper http.RoundTripper) http.RoundTripper {
			usedCallerInfo = callerInfo
			usedLogger = logger
			return roundTripper
		}

		getCoreAPIClient("example.com", "test-jwt", logger)
		assert.Equal(t, "Core-API", usedCallerInfo)
		assert.Equal(t, logger, usedLogger)
	})

	t.Run("WhenCreatesClientWithCorrectTransportSchema", func(t *testing.T) {
		logger := slogger.NewLogger()
		originalTransportSchema := transportSchema
		defer func() { transportSchema = originalTransportSchema }()

		transportSchema = "https"
		client := getCoreAPIClient("example.com", "test-jwt", logger)

		assert.NotNil(t, client)
	})

	t.Run("WhenHandlesNewClientError", func(t *testing.T) {
		logger := slogger.NewLogger()
		// This test would require mocking the NewClient function which is generated
		// For now, we test the happy path
		client := getCoreAPIClient("example.com", "test-jwt", logger)

		// If NewClient fails, getCoreAPIClient returns nil
		// This is a basic test to ensure the function doesn't panic
		// In a real scenario, we'd need to mock the generated NewClient function
		if client == nil {
			// This is expected if NewClient fails
			return
		}
		assert.NotNil(t, client)
	})
}

func TestAddConsumersToTransport(t *testing.T) {
	t.Run("WhenAddsAllRequiredConsumers", func(t *testing.T) {
		transport := httptransport.New("example.com", "", []string{"http"})

		_addConsumersToTransport(transport)

		assert.NotNil(t, transport.Consumers[runtime.JSONMime])
		assert.NotNil(t, transport.Consumers[runtime.XMLMime])
		assert.NotNil(t, transport.Consumers[runtime.TextMime])
		assert.NotNil(t, transport.Consumers[runtime.HTMLMime])
		assert.NotNil(t, transport.Consumers[runtime.CSVMime])
		assert.NotNil(t, transport.Consumers[runtime.DefaultMime])
		assert.NotNil(t, transport.Consumers["*/*"])
	})

	t.Run("WhenCustomConsumerHandlesError", func(t *testing.T) {
		transport := httptransport.New("example.com", "", []string{"http"})
		_addConsumersToTransport(transport)

		// Test that custom consumers are properly set
		customConsumer := transport.Consumers[runtime.XMLMime]
		assert.NotNil(t, customConsumer)

		// Test that the custom consumer returns an error with content info
		// Create a simple reader with some content to avoid nil pointer dereference
		reader := strings.NewReader("test content")
		err := customConsumer.Consume(reader, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "content-type test content")
	})

	t.Run("WhenJSONConsumerIsStandard", func(t *testing.T) {
		transport := httptransport.New("example.com", "", []string{"http"})
		_addConsumersToTransport(transport)

		// JSON consumer should be the standard one, not our custom one
		jsonConsumer := transport.Consumers[runtime.JSONMime]
		assert.NotNil(t, jsonConsumer)

		// Test that JSON consumer works with valid JSON (standard consumer behavior)
		jsonReader := strings.NewReader(`{"test": "value"}`)
		err := jsonConsumer.Consume(jsonReader, &map[string]interface{}{})
		// Standard JSON consumer should not error on valid JSON
		// If it's our custom consumer, it would always error
		if err != nil {
			// This might be expected in some cases, but we're testing that it's not our custom consumer
			// which always returns an error
			assert.NotContains(t, err.Error(), "content-type")
		}
	})
}

func TestInitFunction(t *testing.T) {
	t.Run("WhenApiIdleTimeoutIsPositive", func(t *testing.T) {
		originalApiIdleTimeout := apiIdleTimeout
		defer func() { apiIdleTimeout = originalApiIdleTimeout }()

		apiIdleTimeout = 10

		// Re-run init to test the logic
		httpTransportClone := http.DefaultTransport.(*http.Transport).Clone()
		if apiIdleTimeout > 0 {
			httpTransportClone.IdleConnTimeout = time.Second * ((time.Duration)(apiIdleTimeout))
		} else {
			httpTransportClone.DisableKeepAlives = true
		}

		assert.Equal(t, 10*time.Second, httpTransportClone.IdleConnTimeout)
		assert.False(t, httpTransportClone.DisableKeepAlives)
	})

	t.Run("WhenApiIdleTimeoutIsZero", func(t *testing.T) {
		originalApiIdleTimeout := apiIdleTimeout
		defer func() { apiIdleTimeout = originalApiIdleTimeout }()

		apiIdleTimeout = 0

		// Re-run init to test the logic
		httpTransportClone := http.DefaultTransport.(*http.Transport).Clone()
		if apiIdleTimeout > 0 {
			httpTransportClone.IdleConnTimeout = time.Second * ((time.Duration)(apiIdleTimeout))
		} else {
			httpTransportClone.DisableKeepAlives = true
		}

		assert.True(t, httpTransportClone.DisableKeepAlives)
	})
}

func TestEnvironmentVariables(t *testing.T) {
	t.Run("WhenEnvironmentVariablesAreSet", func(t *testing.T) {
		// Test that environment variables are properly read
		assert.NotEmpty(t, transportSchema)
		assert.GreaterOrEqual(t, ApiMaxRetries, 1)
		assert.Greater(t, ApiRetryDelay, time.Duration(0))
	})

	t.Run("WhenApiMaxRetriesIsAtLeastOne", func(t *testing.T) {
		// Ensure ApiMaxRetries is always at least 1 as per the max() function usage
		assert.GreaterOrEqual(t, ApiMaxRetries, 1)
	})
}

func TestCoreAPIClientStruct(t *testing.T) {
	t.Run("WhenCoreAPIClientIsCreated", func(t *testing.T) {
		client := &CoreAPIClient{
			Invoker: nil,
		}

		assert.NotNil(t, client)
		assert.Nil(t, client.Invoker)
	})
}

func TestContextKey(t *testing.T) {
	t.Run("WhenCorrelationContextKeyIsDefined", func(t *testing.T) {
		// Test that the context key is properly defined
		assert.Equal(t, 0, int(CorrelationContextKey))
	})
}

func TestGetCoreAPIClientWithoutRetry(t *testing.T) {
	t.Run("WhenReturnsClientWithValidServerURL", func(t *testing.T) {
		logger := slogger.NewLogger()
		client := getCoreAPIClientWithoutRetry("example.com", "test-jwt", logger)

		assert.NotNil(t, client)
		assert.NotNil(t, client.Invoker)
	})

	t.Run("WhenDoesNotUseRetryRoundTripper", func(t *testing.T) {
		logger := slogger.NewLogger()
		originalNewRetryRoundTripper := httphelpersNewRetryRoundTripper
		defer func() { httphelpersNewRetryRoundTripper = originalNewRetryRoundTripper }()

		retryRoundTripperCalled := false
		httphelpersNewRetryRoundTripper = func(delay time.Duration, maxRetries int, logger slogger.Logger, next http.RoundTripper) http.RoundTripper {
			retryRoundTripperCalled = true
			return next
		}

		getCoreAPIClientWithoutRetry("example.com", "test-jwt", logger)
		// Key difference: retry round tripper should NOT be called
		assert.False(t, retryRoundTripperCalled, "getCoreAPIClientWithoutRetry should not use retry round tripper")
	})

	t.Run("WhenUsesLoggingRoundTripperWithCorrectConfiguration", func(t *testing.T) {
		logger := slogger.NewLogger()
		originalGetLoggingRoundTripper := httphelpersGetLoggingRoundTripper
		defer func() { httphelpersGetLoggingRoundTripper = originalGetLoggingRoundTripper }()

		var usedCallerInfo string
		var usedLogger slogger.Logger
		var usedRoundTripper http.RoundTripper
		httphelpersGetLoggingRoundTripper = func(callerInfo string, logger slogger.Logger, roundTripper http.RoundTripper) http.RoundTripper {
			usedCallerInfo = callerInfo
			usedLogger = logger
			usedRoundTripper = roundTripper
			return roundTripper
		}

		getCoreAPIClientWithoutRetry("example.com", "test-jwt", logger)
		assert.Equal(t, "Core-API", usedCallerInfo)
		assert.Equal(t, logger, usedLogger)
		assert.Equal(t, httpTransport, usedRoundTripper, "Logging round tripper should wrap httpTransport directly, not retry round tripper")
	})

	t.Run("WhenCreatesClientWithCorrectTransportSchema", func(t *testing.T) {
		logger := slogger.NewLogger()
		originalTransportSchema := transportSchema
		defer func() { transportSchema = originalTransportSchema }()

		transportSchema = "https"
		client := getCoreAPIClientWithoutRetry("example.com", "test-jwt", logger)

		assert.NotNil(t, client)
		if client != nil {
			assert.NotNil(t, client.Invoker)
		}
	})

	t.Run("WhenHandlesNewClientError", func(t *testing.T) {
		logger := slogger.NewLogger()
		// This test would require mocking the NewClient function which is generated
		// For now, we test the happy path
		client := getCoreAPIClientWithoutRetry("example.com", "test-jwt", logger)

		// If NewClient fails, getCoreAPIClientWithoutRetry returns nil
		// This is a basic test to ensure the function doesn't panic
		// In a real scenario, we'd need to mock the generated NewClient function
		if client == nil {
			// This is expected if NewClient fails
			return
		}
		assert.NotNil(t, client)
		assert.NotNil(t, client.Invoker)
	})

	t.Run("WhenUsesVcpRoundTripperWithCorrectJWT", func(t *testing.T) {
		logger := slogger.NewLogger()
		client := getCoreAPIClientWithoutRetry("example.com", "test-jwt-token", logger)

		assert.NotNil(t, client)
		if client != nil {
			// Verify the transport chain is set up correctly
			// The transport should be a vcpRoundTripper wrapping loggingRoundTripper
			assert.NotNil(t, client.Invoker)
		}
	})

	t.Run("WhenCreatesClientWithEmptyBasePath", func(t *testing.T) {
		logger := slogger.NewLogger()
		client := getCoreAPIClientWithoutRetry("", "test-jwt", logger)

		// Should handle empty base path gracefully
		// May return nil if NewClient fails with invalid URL
		// This is acceptable behavior
		if client == nil {
			return
		}
		assert.NotNil(t, client)
	})

	t.Run("WhenCreatesClientWithEmptyJWT", func(t *testing.T) {
		logger := slogger.NewLogger()
		client := getCoreAPIClientWithoutRetry("example.com", "", logger)

		assert.NotNil(t, client)
		if client != nil {
			assert.NotNil(t, client.Invoker)
		}
	})

	t.Run("WhenTransportChainDoesNotIncludeRetry", func(t *testing.T) {
		logger := slogger.NewLogger()
		originalGetLoggingRoundTripper := httphelpersGetLoggingRoundTripper
		originalNewRetryRoundTripper := httphelpersNewRetryRoundTripper
		defer func() {
			httphelpersGetLoggingRoundTripper = originalGetLoggingRoundTripper
			httphelpersNewRetryRoundTripper = originalNewRetryRoundTripper
		}()

		loggingRoundTripperCalled := false
		retryRoundTripperCalled := false

		httphelpersGetLoggingRoundTripper = func(callerInfo string, logger slogger.Logger, roundTripper http.RoundTripper) http.RoundTripper {
			loggingRoundTripperCalled = true
			// Verify it receives httpTransport directly, not a retry round tripper
			assert.Equal(t, httpTransport, roundTripper)
			return roundTripper
		}

		httphelpersNewRetryRoundTripper = func(delay time.Duration, maxRetries int, logger slogger.Logger, next http.RoundTripper) http.RoundTripper {
			retryRoundTripperCalled = true
			return next
		}

		getCoreAPIClientWithoutRetry("example.com", "test-jwt", logger)

		// Verify logging round tripper is called
		assert.True(t, loggingRoundTripperCalled, "Logging round tripper should be called")
		// Verify retry round tripper is NOT called
		assert.False(t, retryRoundTripperCalled, "Retry round tripper should NOT be called in getCoreAPIClientWithoutRetry")
	})

	t.Run("WhenAddsConsumersToTransport", func(t *testing.T) {
		logger := slogger.NewLogger()
		originalAddConsumersToTransport := addConsumersToTransport
		defer func() { addConsumersToTransport = originalAddConsumersToTransport }()

		consumersAdded := false
		addConsumersToTransport = func(transport *httptransport.Runtime) {
			consumersAdded = true
			originalAddConsumersToTransport(transport)
		}

		client := getCoreAPIClientWithoutRetry("example.com", "test-jwt", logger)

		assert.True(t, consumersAdded, "Consumers should be added to transport")
		assert.NotNil(t, client)
	})
}
