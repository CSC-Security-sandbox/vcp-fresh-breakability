package httphelpers

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	logpkg "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestRetryRoundTripperRetriesOnRetryableStatus(t *testing.T) {
	originalSleep := timeSleep
	timeSleep = func(time.Duration) {}
	t.Cleanup(func() {
		timeSleep = originalSleep
	})

	firstBody := &trackingReadCloser{ReadCloser: io.NopCloser(strings.NewReader("retryable"))}
	stub := &stubRoundTripper{
		t: t,
		responses: []roundTripResult{
			{
				resp: &http.Response{
					StatusCode: http.StatusServiceUnavailable,
					Body:       firstBody,
				},
			},
			{
				resp: &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("ok")),
				},
			},
		},
	}

	req, err := http.NewRequest(http.MethodGet, "http://example.com/foo", io.NopCloser(strings.NewReader("payload")))
	require.NoError(t, err)

	rt := NewRetryRoundTripper(0, 3, noopLogger{}, stub)

	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, 2, stub.calls)
	require.True(t, firstBody.closed)
}

func TestRetryRoundTripperDoesNotRetryOnNonRetryableStatus(t *testing.T) {
	originalSleep := timeSleep
	timeSleep = func(time.Duration) {}
	t.Cleanup(func() {
		timeSleep = originalSleep
	})

	stub := &stubRoundTripper{
		t: t,
		responses: []roundTripResult{
			{
				resp: &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       io.NopCloser(strings.NewReader("bad request")),
				},
			},
		},
	}

	req, err := http.NewRequest(http.MethodPost, "http://example.com/bar", io.NopCloser(strings.NewReader("payload")))
	require.NoError(t, err)

	rt := NewRetryRoundTripper(0, 3, noopLogger{}, stub)

	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, 1, stub.calls)
}

type stubRoundTripper struct {
	t         *testing.T
	calls     int
	responses []roundTripResult
}

type roundTripResult struct {
	resp *http.Response
	err  error
}

func (s *stubRoundTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	if s.calls >= len(s.responses) {
		s.t.Fatalf("unexpected round trip call %d", s.calls+1)
	}
	result := s.responses[s.calls]
	s.calls++
	return result.resp, result.err
}

type trackingReadCloser struct {
	io.ReadCloser
	closed bool
}

func (t *trackingReadCloser) Close() error {
	t.closed = true
	if t.ReadCloser != nil {
		return t.ReadCloser.Close()
	}
	return nil
}

type noopLogger struct{}

func (noopLogger) Errorf(string, ...any) {}
func (noopLogger) Error(string, ...any)  {}
func (noopLogger) Warnf(string, ...any)  {}
func (noopLogger) Warn(string, ...any)   {}
func (noopLogger) Infof(string, ...any)  {}
func (noopLogger) Info(string, ...any)   {}
func (noopLogger) Debugf(string, ...any) {}
func (noopLogger) Debug(string, ...any)  {}

func (noopLogger) InfoContext(context.Context, string, ...any)  {}
func (noopLogger) WarnContext(context.Context, string, ...any)  {}
func (noopLogger) ErrorContext(context.Context, string, ...any) {}
func (noopLogger) DebugContext(context.Context, string, ...any) {}

func (noopLogger) WithFields(string, logpkg.Fields) logpkg.Logger { return noopLogger{} }
func (noopLogger) With(logpkg.Fields) logpkg.Logger               { return noopLogger{} }
