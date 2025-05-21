package googleproxyclient

import (
	"context"
	"net/http"
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

func TestRoundTrip(t *testing.T) {
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
	})
}

func TestGetGProxyClient(t *testing.T) {
	t.Run("WheReturnsClientWithValidServerURL", func(t *testing.T) {
		logger := slogger.NewLogger()
		client := getGProxyClient("example.com", "test-jwt", logger)

		assert.NotNil(t, client)
		assert.Equal(t, "example.com", client.serverURL.Host)
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

		getGProxyClient("example.com", "test-jwt", logger)
		assert.Equal(t, ApiRetryDelay, usedDelay)
		assert.Equal(t, ApiMaxRetries, usedMaxRetries)
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
