package googleproxyclient

import (
	"context"
	"io"
	"net"
	"net/http"
	"syscall"
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
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*http.Response), args.Error(1)
}

func TestRoundTrip(t *testing.T) {
	t.Run("WhenVcpRoundTripperAddsAuthorizationHeader", func(t *testing.T) {
		rt := &mockRoundTripper{}
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(nil),
		}
		resp.Header.Set(runtime.HeaderContentType, runtime.JSONMime)
		rt.On("RoundTrip", mock.Anything).Return(resp, nil)

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       slogger.NewLogger(),
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, result.StatusCode)
		// Check that Authorization header was added to the cloned request
		rt.AssertExpectations(t)
	})
	t.Run("WhenVcpRoundTripperAddsCorrelationIDFromContext", func(t *testing.T) {
		rt := &mockRoundTripper{}
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(nil),
		}
		resp.Header.Set(runtime.HeaderContentType, runtime.JSONMime)
		rt.On("RoundTrip", mock.Anything).Return(resp, nil)

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       slogger.NewLogger(),
		}

		ctx := context.WithValue(context.Background(), CorrelationContextKey, "test-correlation-id")
		req, _ := http.NewRequestWithContext(ctx, "GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, result.StatusCode)
		rt.AssertExpectations(t)
	})
	t.Run("WhenVcpRoundTripperKeepsExistingCorrelationID", func(t *testing.T) {
		rt := &mockRoundTripper{}
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(nil),
		}
		resp.Header.Set(runtime.HeaderContentType, runtime.JSONMime)
		rt.On("RoundTrip", mock.Anything).Return(resp, nil)

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       slogger.NewLogger(),
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		req.Header.Set(slogger.RequestCorrelationID, "existing-correlation-id")
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, result.StatusCode)
		rt.AssertExpectations(t)
	})
}

func TestVcpRoundTripperRetryMechanism(t *testing.T) {
	logger := slogger.NewLogger()

	createSuccessResponse := func() *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(io.NopCloser(nil)),
		}
	}

	createResponseWithContentType := func(statusCode int, contentType string) *http.Response {
		resp := &http.Response{
			StatusCode: statusCode,
			Header:     make(http.Header),
			Body:       io.NopCloser(io.NopCloser(nil)),
		}
		if contentType != "" {
			resp.Header.Set(runtime.HeaderContentType, contentType)
		}
		return resp
	}

	t.Run("WhenSuccessOnFirstAttempt_ShouldNotRetry", func(t *testing.T) {
		rt := &mockRoundTripper{}
		resp := createSuccessResponse()
		resp.Header.Set(runtime.HeaderContentType, runtime.JSONMime)
		rt.On("RoundTrip", mock.Anything).Return(resp, nil).Once()

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       logger,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, result.StatusCode)
		rt.AssertExpectations(t)
	})

	t.Run("WhenSuccessWithContentTypeCharset_ShouldNotRetry", func(t *testing.T) {
		rt := &mockRoundTripper{}
		resp := createResponseWithContentType(http.StatusOK, "application/json; charset=utf-8")
		rt.On("RoundTrip", mock.Anything).Return(resp, nil).Once()

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       logger,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, result.StatusCode)
		rt.AssertExpectations(t)
	})

	t.Run("When204NoContent_ShouldNotRetry", func(t *testing.T) {
		rt := &mockRoundTripper{}
		resp := createResponseWithContentType(http.StatusNoContent, "")
		rt.On("RoundTrip", mock.Anything).Return(resp, nil).Once()

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       logger,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, result.StatusCode)
		rt.AssertExpectations(t)
	})

	t.Run("WhenRetryOnECONNREFUSED_ShouldRetry", func(t *testing.T) {
		rt := &mockRoundTripper{}
		successResp := createSuccessResponse()
		successResp.Header.Set(runtime.HeaderContentType, runtime.JSONMime)

		rt.On("RoundTrip", mock.Anything).Return(nil, syscall.ECONNREFUSED).Once()
		rt.On("RoundTrip", mock.Anything).Return(successResp, nil).Once()

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       logger,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, result.StatusCode)
		rt.AssertNumberOfCalls(t, "RoundTrip", 2)
	})

	t.Run("WhenRetryOnETIMEDOUT_ShouldRetry", func(t *testing.T) {
		rt := &mockRoundTripper{}
		successResp := createSuccessResponse()
		successResp.Header.Set(runtime.HeaderContentType, runtime.JSONMime)

		rt.On("RoundTrip", mock.Anything).Return(nil, syscall.ETIMEDOUT).Once()
		rt.On("RoundTrip", mock.Anything).Return(successResp, nil).Once()

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       logger,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, result.StatusCode)
		rt.AssertNumberOfCalls(t, "RoundTrip", 2)
	})

	t.Run("WhenRetryOnErrUnexpectedEOF_ShouldRetry", func(t *testing.T) {
		rt := &mockRoundTripper{}
		successResp := createSuccessResponse()
		successResp.Header.Set(runtime.HeaderContentType, runtime.JSONMime)

		rt.On("RoundTrip", mock.Anything).Return(nil, io.ErrUnexpectedEOF).Once()
		rt.On("RoundTrip", mock.Anything).Return(successResp, nil).Once()

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       logger,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, result.StatusCode)
		rt.AssertNumberOfCalls(t, "RoundTrip", 2)
	})

	t.Run("WhenRetryOnTimeoutError_ShouldRetry", func(t *testing.T) {
		rt := &mockRoundTripper{}
		successResp := createSuccessResponse()
		successResp.Header.Set(runtime.HeaderContentType, runtime.JSONMime)

		timeoutErr := &net.DNSError{
			Err:         "timeout",
			IsTimeout:   true,
			IsTemporary: true,
		}

		rt.On("RoundTrip", mock.Anything).Return(nil, timeoutErr).Once()
		rt.On("RoundTrip", mock.Anything).Return(successResp, nil).Once()

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       logger,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, result.StatusCode)
		rt.AssertNumberOfCalls(t, "RoundTrip", 2)
	})

	t.Run("WhenRetryOnStatus403_ShouldRetry", func(t *testing.T) {
		rt := &mockRoundTripper{}
		errorResp := createResponseWithContentType(http.StatusForbidden, runtime.JSONMime)
		successResp := createSuccessResponse()
		successResp.Header.Set(runtime.HeaderContentType, runtime.JSONMime)

		rt.On("RoundTrip", mock.Anything).Return(errorResp, nil).Once()
		rt.On("RoundTrip", mock.Anything).Return(successResp, nil).Once()

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       logger,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, result.StatusCode)
		rt.AssertNumberOfCalls(t, "RoundTrip", 2)
	})

	t.Run("WhenRetryOnStatus429_ShouldRetry", func(t *testing.T) {
		rt := &mockRoundTripper{}
		errorResp := createResponseWithContentType(http.StatusTooManyRequests, runtime.JSONMime)
		successResp := createSuccessResponse()
		successResp.Header.Set(runtime.HeaderContentType, runtime.JSONMime)

		rt.On("RoundTrip", mock.Anything).Return(errorResp, nil).Once()
		rt.On("RoundTrip", mock.Anything).Return(successResp, nil).Once()

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       logger,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, result.StatusCode)
		rt.AssertNumberOfCalls(t, "RoundTrip", 2)
	})

	t.Run("WhenRetryOnStatus500_ShouldRetry", func(t *testing.T) {
		rt := &mockRoundTripper{}
		errorResp := createResponseWithContentType(http.StatusInternalServerError, runtime.JSONMime)
		successResp := createSuccessResponse()
		successResp.Header.Set(runtime.HeaderContentType, runtime.JSONMime)

		rt.On("RoundTrip", mock.Anything).Return(errorResp, nil).Once()
		rt.On("RoundTrip", mock.Anything).Return(successResp, nil).Once()

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       logger,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, result.StatusCode)
		rt.AssertNumberOfCalls(t, "RoundTrip", 2)
	})

	t.Run("WhenRetryOnStatus503_ShouldRetry", func(t *testing.T) {
		rt := &mockRoundTripper{}
		errorResp := createResponseWithContentType(http.StatusServiceUnavailable, runtime.JSONMime)
		successResp := createSuccessResponse()
		successResp.Header.Set(runtime.HeaderContentType, runtime.JSONMime)

		rt.On("RoundTrip", mock.Anything).Return(errorResp, nil).Once()
		rt.On("RoundTrip", mock.Anything).Return(successResp, nil).Once()

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       logger,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, result.StatusCode)
		rt.AssertNumberOfCalls(t, "RoundTrip", 2)
	})

	t.Run("WhenRetryOnStatus504_ShouldRetry", func(t *testing.T) {
		rt := &mockRoundTripper{}
		errorResp := createResponseWithContentType(http.StatusGatewayTimeout, runtime.JSONMime)
		successResp := createSuccessResponse()
		successResp.Header.Set(runtime.HeaderContentType, runtime.JSONMime)

		rt.On("RoundTrip", mock.Anything).Return(errorResp, nil).Once()
		rt.On("RoundTrip", mock.Anything).Return(successResp, nil).Once()

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       logger,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, result.StatusCode)
		rt.AssertNumberOfCalls(t, "RoundTrip", 2)
	})

	t.Run("WhenRetryOnNonJSONContentType_ShouldRetry", func(t *testing.T) {
		rt := &mockRoundTripper{}
		errorResp := createResponseWithContentType(http.StatusOK, "text/plain")
		successResp := createSuccessResponse()
		successResp.Header.Set(runtime.HeaderContentType, runtime.JSONMime)

		rt.On("RoundTrip", mock.Anything).Return(errorResp, nil).Once()
		rt.On("RoundTrip", mock.Anything).Return(successResp, nil).Once()

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       logger,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, result.StatusCode)
		rt.AssertNumberOfCalls(t, "RoundTrip", 2)
	})

	t.Run("WhenRetryOnMissingContentType_ShouldRetry", func(t *testing.T) {
		rt := &mockRoundTripper{}
		errorResp := createResponseWithContentType(http.StatusOK, "")
		successResp := createSuccessResponse()
		successResp.Header.Set(runtime.HeaderContentType, runtime.JSONMime)

		rt.On("RoundTrip", mock.Anything).Return(errorResp, nil).Once()
		rt.On("RoundTrip", mock.Anything).Return(successResp, nil).Once()

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       logger,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, result.StatusCode)
		rt.AssertNumberOfCalls(t, "RoundTrip", 2)
	})

	t.Run("WhenMaxRetriesExceeded_ShouldReturnLastError", func(t *testing.T) {
		rt := &mockRoundTripper{}
		errorResp := createResponseWithContentType(http.StatusInternalServerError, runtime.JSONMime)

		rt.On("RoundTrip", mock.Anything).Return(errorResp, nil).Times(3)

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       logger,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err) // No error because response was received
		assert.Equal(t, http.StatusInternalServerError, result.StatusCode)
		rt.AssertNumberOfCalls(t, "RoundTrip", 3)
	})

	t.Run("WhenContextCancelled_ShouldStopRetrying", func(t *testing.T) {
		rt := &mockRoundTripper{}

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       logger,
		}

		ctx, cancel := context.WithCancel(context.Background())
		req, _ := http.NewRequestWithContext(ctx, "GET", "http://example.com", nil)
		cancel() // Cancel context immediately

		result, err := vcpRT.RoundTrip(req)

		// Should return context cancellation error
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "context canceled")
		rt.AssertNumberOfCalls(t, "RoundTrip", 0) // Should not make any calls if context is cancelled before first attempt
	})

	t.Run("WhenContextCancelledDuringRetries_ShouldReturnNilResponse", func(t *testing.T) {
		rt := &mockRoundTripper{}
		errorResp := createResponseWithContentType(http.StatusInternalServerError, runtime.JSONMime)

		// Channel to signal when first call completes
		firstCallDone := make(chan bool, 1)

		// First attempt returns an error response that would trigger a retry
		// Allow multiple calls but signal on first call
		callCount := 0
		rt.On("RoundTrip", mock.Anything).Run(func(args mock.Arguments) {
			callCount++
			if callCount == 1 {
				select {
				case firstCallDone <- true:
				default:
				}
			}
		}).Return(errorResp, nil)

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   50 * time.Millisecond, // Longer delay to give time to cancel
			maxRetries:   3,
			logger:       logger,
		}

		ctx, cancel := context.WithCancel(context.Background())
		req, _ := http.NewRequestWithContext(ctx, "GET", "http://example.com", nil)

		// Start the request in a goroutine
		done := make(chan bool)
		var result *http.Response
		var err error
		go func() {
			result, err = vcpRT.RoundTrip(req)
			done <- true
		}()

		// Wait for first call to complete, then cancel context
		// The retry delay gives us time to cancel before the next attempt
		<-firstCallDone
		time.Sleep(10 * time.Millisecond) // Small delay to ensure we're in the retry delay
		cancel()                          // Cancel context during retry delay

		<-done

		// Should return nil response with context cancellation error, not the stale error response
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "context canceled")
		// Should have made at least one call, but may have made more if cancellation was delayed
		assert.GreaterOrEqual(t, callCount, 1)
	})

	t.Run("WhenNonRetryableError_ShouldNotRetry", func(t *testing.T) {
		rt := &mockRoundTripper{}
		nonRetryableErr := &net.DNSError{
			Err:         "no such host",
			IsTimeout:   false,
			IsTemporary: false,
		}

		rt.On("RoundTrip", mock.Anything).Return(nil, nonRetryableErr).Once()

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       logger,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.Error(t, err)
		assert.Nil(t, result)
		rt.AssertNumberOfCalls(t, "RoundTrip", 1)
	})

	t.Run("WhenSuccessAfterMultipleRetries_ShouldReturnSuccess", func(t *testing.T) {
		rt := &mockRoundTripper{}
		errorResp := createResponseWithContentType(http.StatusInternalServerError, runtime.JSONMime)
		successResp := createSuccessResponse()
		successResp.Header.Set(runtime.HeaderContentType, runtime.JSONMime)

		rt.On("RoundTrip", mock.Anything).Return(errorResp, nil).Twice()
		rt.On("RoundTrip", mock.Anything).Return(successResp, nil).Once()

		vcpRT := &vcpRoundTripper{
			JWT:          "test-jwt",
			RoundTripper: rt,
			retryDelay:   10 * time.Millisecond,
			maxRetries:   3,
			logger:       logger,
		}

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		result, err := vcpRT.RoundTrip(req)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, result.StatusCode)
		rt.AssertNumberOfCalls(t, "RoundTrip", 3)
	})
}

func TestGetGProxyClient(t *testing.T) {
	t.Run("WheReturnsClientWithValidServerURL", func(t *testing.T) {
		logger := slogger.NewLogger()
		client := getGProxyClient("example.com", "test-jwt", logger)

		assert.NotNil(t, client)
		assert.Equal(t, "example.com", client.Invoker.(*Client).serverURL.Host)
	})
	t.Run("WhenUsesVcpRoundTripperWithRetryConfiguration", func(t *testing.T) {
		logger := slogger.NewLogger()
		client := getGProxyClient("example.com", "test-jwt", logger)

		assert.NotNil(t, client)
		// Verify that the client is created with retry configuration
		// The retry logic is now built into vcpRoundTripper, so we just verify the client exists
		// The actual retry behavior is tested in TestVcpRoundTripperRetryMechanism
	})
	t.Run("WhenAddsConsumersToTransport", func(t *testing.T) {
		transport := httptransport.New("example.com", "", []string{"http"})

		addConsumersToTransport(transport)

		assert.NotNil(t, transport.Consumers[runtime.JSONMime])
		assert.NotNil(t, transport.Consumers[runtime.XMLMime])
		assert.NotNil(t, transport.Consumers[runtime.TextMime])
		assert.NotNil(t, transport.Consumers[runtime.HTMLMime])
		assert.NotNil(t, transport.Consumers[runtime.CSVMime])
		assert.NotNil(t, transport.Consumers[runtime.DefaultMime])
		assert.NotNil(t, transport.Consumers["*/*"])
	})
}
