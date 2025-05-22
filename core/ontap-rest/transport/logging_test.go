package transport

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestNewLoggingRoundTripper(t *testing.T) {
	logger := log.NewLogger().(*log.Slogger)
	mrt := NewMockRoundTripper(t)

	t.Run("WhenOAuthEnabled_ThenAuthStyleIsOauth", func(tt *testing.T) {
		ontapRestOAuthEnabled = true
		defer func() { ontapRestOAuthEnabled = false }()
		lrt := NewLoggingRoundTripper(logger, true, true, mrt)
		assert.Equal(tt, "oauth", lrt.authStyle)
	})

	t.Run("WhenOAuthDisabledAndUseCertTrue_ThenAuthStyleIsCert", func(tt *testing.T) {
		ontapRestOAuthEnabled = false
		lrt := NewLoggingRoundTripper(logger, true, true, mrt)
		assert.Equal(tt, "cert", lrt.authStyle)
	})

	t.Run("WhenOAuthDisabledAndUseCertFalse_ThenAuthStyleIsBasic", func(tt *testing.T) {
		ontapRestOAuthEnabled = false
		lrt := NewLoggingRoundTripper(logger, false, false, mrt)
		assert.Equal(tt, "basic", lrt.authStyle)
	})

	t.Run("WhenLoggingDisabled_ThenLogVerboseIsFalse", func(tt *testing.T) {
		ontapRestOAuthEnabled = false
		lrt := NewLoggingRoundTripper(logger, false, false, mrt)
		assert.False(tt, lrt.logVerbose)
	})

	t.Run("WhenLoggingEnabled_ThenLogVerboseIsTrue", func(tt *testing.T) {
		ontapRestOAuthEnabled = false
		lrt := NewLoggingRoundTripper(logger, true, false, mrt)
		assert.True(tt, lrt.logVerbose)
	})

	t.Run("WhenRoundTripperIsSet_ThenRoundTripperIsNotNil", func(tt *testing.T) {
		ontapRestOAuthEnabled = false
		lrt := NewLoggingRoundTripper(logger, false, false, mrt)
		assert.Equal(tt, mrt, lrt.roundTripper)
	})

	t.Run("WhenTraceIsSet_ThenTraceIsNotNil", func(tt *testing.T) {
		ontapRestOAuthEnabled = false
		lrt := NewLoggingRoundTripper(logger, false, false, mrt)
		assert.NotNil(tt, lrt.trace)
	})
}

func Test_removeWhiteSpaceButNotBetweenDoubleQuotes(t *testing.T) {
	assert.Equal(t, removeWhiteSpaceButNotBetweenDoubleQuotes("{ \"error\" :{\"message\":\"entry doesn't exist\",\"code\":\"4\",\"target\":\"name\"}}"), "{\"error\":{\"message\":\"entry doesn't exist\",\"code\":\"4\",\"target\":\"name\"}}")
	assert.Equal(t, removeWhiteSpaceButNotBetweenDoubleQuotes("{ \"error\" \n :{\"message\":\"entry \n doesn't exist\",\"code\":\"4\",\"target\":\"name\"}}"), "{\"error\":{\"message\":\"entry \n doesn't exist\",\"code\":\"4\",\"target\":\"name\"}}")
}

func mustParseURL(raw string) *url.URL {
	u, err := url.ParseRequestURI(raw)
	if err != nil {
		panic(err)
	}
	return u
}

func Test_prettify(t *testing.T) {
	tests := []struct {
		name   string
		input  map[string][]string
		expect string
	}{
		{
			name:   "WhenMapIsEmpty_ThenReturnEmptyString",
			input:  map[string][]string{},
			expect: "",
		},
		{
			name:   "WhenSingleKeySingleValue_ThenReturnKeyEqualsValue",
			input:  map[string][]string{"foo": {"bar"}},
			expect: "foo=bar",
		},
		{
			name:   "WhenSingleKeyMultipleValues_ThenReturnKeyEqualsList",
			input:  map[string][]string{"foo": {"bar", "baz"}},
			expect: "foo=[bar,baz]",
		},
		{
			name:   "WhenMultipleKeys_ThenReturnSortedKeys",
			input:  map[string][]string{"b": {"2"}, "a": {"1"}},
			expect: "a=1,b=2",
		},
		{
			name: "WhenSensitiveKeysPresent_ThenSkipSensitiveKeys",
			input: map[string][]string{
				"authorization":    {"secret"},
				"foo":              {"bar"},
				"traceparent":      {"abc"},
				"tracestate":       {"xyz"},
				"www-authenticate": {"auth"},
			},
			expect: "foo=bar",
		},
		{
			name:   "WhenKeyWithEmptyValue_ThenReturnKeyEqualsEmpty",
			input:  map[string][]string{"foo": {}},
			expect: "foo=",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, _prettify(tt.input))
		})
	}
}

func TestLoggingRoundTripper_RoundTrip(t *testing.T) {
	// Patch timeNow and ioReadAll for deterministic tests
	oldTimeNow := timeNow
	oldIoReadAll := ioReadAll
	defer func() {
		timeNow = oldTimeNow
		ioReadAll = oldIoReadAll
	}()
	timeNow = func() time.Time { return time.Unix(0, 0) }
	ioReadAll = func(r io.Reader) ([]byte, error) {
		return io.ReadAll(r)
	}

	t.Run("WhenRequestBodyIsNil_ThenNoBodyFieldInRequestFields", func(tt *testing.T) {
		mockRT := NewMockRoundTripper(tt)
		mockLogger := log.NewMockLogger(tt)
		req := &http.Request{
			Method: "GET",
			URL:    mustParseURL("https://host/path"),
			Header: http.Header{},
			Body:   nil,
		}
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       io.NopCloser(bytes.NewBufferString("ok")),
		}
		mockRT.On("RoundTrip", mock.Anything).Return(resp, nil)
		mockLogger.On("With", mock.Anything).Return(mockLogger)
		mockLogger.On("InfoContext", mock.Anything, mock.Anything).Return(mockLogger)

		lrt := &LoggingRoundTripper{
			trace:        mockLogger,
			logVerbose:   true,
			authStyle:    "basic",
			roundTripper: mockRT,
		}
		gotResp, err := lrt.RoundTrip(req)
		assert.NoError(tt, err)
		assert.Equal(tt, resp, gotResp)
	})

	t.Run("WhenRequestBodyReadFails_ThenWarnAndErrorReturned", func(tt *testing.T) {
		mockRT := NewMockRoundTripper(tt)
		mockLogger := log.NewMockLogger(tt)
		req := &http.Request{
			Method: "POST",
			URL:    mustParseURL("https://host/path"),
			Header: http.Header{},
			Body:   io.NopCloser(badReader{}),
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return nil, assert.AnError
		}
		mockLogger.On("With", mock.Anything).Return(mockLogger)
		mockLogger.On("WarnContext", mock.Anything, mock.Anything).Return(mockLogger)

		lrt := &LoggingRoundTripper{
			trace:        mockLogger,
			logVerbose:   true,
			authStyle:    "basic",
			roundTripper: mockRT,
		}
		resp, err := lrt.RoundTrip(req)
		assert.Nil(tt, resp)
		assert.Error(tt, err)
	})

	t.Run("WhenRoundTripperFails_ThenWarnAndErrorReturned", func(tt *testing.T) {
		oldIoReadAll := ioReadAll
		defer func() { ioReadAll = oldIoReadAll }()
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return io.ReadAll(r)
		}

		mockRT := NewMockRoundTripper(tt)
		mockLogger := log.NewMockLogger(tt)
		body := `{"foo":"bar"}`
		req := &http.Request{
			Method: "POST",
			URL:    mustParseURL("https://host/path"),
			Header: http.Header{},
			Body:   io.NopCloser(bytes.NewBufferString(body)),
		}
		mockRT.On("RoundTrip", mock.Anything).Return(nil, assert.AnError)
		mockLogger.On("With", mock.Anything).Return(mockLogger)
		mockLogger.On("InfoContext", mock.Anything, mock.Anything).Return(mockLogger)
		mockLogger.On("WarnContext", mock.Anything, mock.Anything).Return(mockLogger)

		lrt := &LoggingRoundTripper{
			trace:        mockLogger,
			logVerbose:   true,
			authStyle:    "basic",
			roundTripper: mockRT,
		}
		resp, err := lrt.RoundTrip(req)
		assert.Nil(tt, resp)
		assert.Error(tt, err)
	})

	t.Run("WhenResponseBodyReadFails_ThenWarnAndErrorReturned", func(tt *testing.T) {
		mockRT := NewMockRoundTripper(tt)
		mockLogger := log.NewMockLogger(tt)
		body := `{"foo":"bar"}`
		req := &http.Request{
			Method: "POST",
			URL:    mustParseURL("https://host/path"),
			Header: http.Header{},
			Body:   io.NopCloser(bytes.NewBufferString(body)),
		}
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       io.NopCloser(badReader{}),
		}
		mockRT.On("RoundTrip", mock.Anything).Return(resp, nil)
		mockLogger.On("With", mock.Anything).Return(mockLogger)
		mockLogger.On("InfoContext", mock.Anything, mock.Anything).Return(mockLogger)
		mockLogger.On("WarnContext", mock.Anything, mock.Anything).Return(mockLogger)
		ioReadAll = func(r io.Reader) ([]byte, error) {
			if _, ok := r.(badReader); ok {
				return nil, assert.AnError
			}
			return io.ReadAll(r)
		}

		lrt := &LoggingRoundTripper{
			trace:        mockLogger,
			logVerbose:   true,
			authStyle:    "basic",
			roundTripper: mockRT,
		}
		resp2, err := lrt.RoundTrip(req)
		assert.Nil(tt, resp2)
		assert.Error(tt, err)
	})

	t.Run("WhenSlowResponse_ThenSlowFieldSet", func(tt *testing.T) {
		mockRT := NewMockRoundTripper(tt)
		mockLogger := log.NewMockLogger(tt)
		body := `{"foo":"bar"}`
		req := &http.Request{
			Method: "POST",
			URL:    mustParseURL("https://host/path"),
			Header: http.Header{},
			Body:   io.NopCloser(bytes.NewBufferString(body)),
		}
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       io.NopCloser(bytes.NewBufferString("ok")),
		}
		mockRT.On("RoundTrip", mock.Anything).Return(resp, nil)
		mockLogger.On("With", mock.Anything).Return(mockLogger)
		mockLogger.On("InfoContext", mock.Anything, mock.Anything).Return(mockLogger)
		calls := 0
		timeNow = func() time.Time {
			calls++
			if calls == 1 {
				return time.Unix(0, 0)
			}
			return time.Unix(20, 0) // 20s later
		}

		lrt := &LoggingRoundTripper{
			trace:        mockLogger,
			logVerbose:   true,
			authStyle:    "basic",
			roundTripper: mockRT,
		}
		_, err := lrt.RoundTrip(req)
		assert.NoError(tt, err)
	})

	t.Run("WhenJobFailure_ThenWarnContextCalled", func(tt *testing.T) {
		mockRT := NewMockRoundTripper(tt)
		mockLogger := log.NewMockLogger(tt)
		body := `{"foo":"bar"}`
		req := &http.Request{
			Method: "POST",
			URL:    mustParseURL("https://host/api/cluster/jobs/123"),
			Header: http.Header{},
			Body:   io.NopCloser(bytes.NewBufferString(body)),
		}
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       io.NopCloser(bytes.NewBufferString(`{"result":"failure"}`)),
		}
		mockRT.On("RoundTrip", mock.Anything).Return(resp, nil)
		mockLogger.On("With", mock.Anything).Return(mockLogger)
		mockLogger.On("InfoContext", mock.Anything, mock.Anything).Return(mockLogger)
		mockLogger.On("WarnContext", mock.Anything, mock.Anything).Return(mockLogger)

		lrt := &LoggingRoundTripper{
			trace:        mockLogger,
			logVerbose:   false,
			authStyle:    "basic",
			roundTripper: mockRT,
		}
		_, err := lrt.RoundTrip(req)
		assert.NoError(tt, err)
	})

	t.Run("WhenStatusAccepted_ThenInfoContextCalled", func(tt *testing.T) {
		mockRT := NewMockRoundTripper(tt)
		mockLogger := log.NewMockLogger(tt)
		body := `{"foo":"bar"}`
		req := &http.Request{
			Method: "POST",
			URL:    mustParseURL("https://host/api/cluster/jobs/123"),
			Header: http.Header{},
			Body:   io.NopCloser(bytes.NewBufferString(body)),
		}
		resp := &http.Response{
			StatusCode: http.StatusAccepted,
			Header:     http.Header{},
			Body:       io.NopCloser(bytes.NewBufferString(`{"result":"ok"}`)),
		}
		mockRT.On("RoundTrip", mock.Anything).Return(resp, nil)
		mockLogger.On("With", mock.Anything).Return(mockLogger)
		mockLogger.On("InfoContext", mock.Anything, mock.Anything).Return(mockLogger)

		lrt := &LoggingRoundTripper{
			trace:        mockLogger,
			logVerbose:   false,
			authStyle:    "basic",
			roundTripper: mockRT,
		}
		_, err := lrt.RoundTrip(req)
		assert.NoError(tt, err)
	})

	t.Run("WhenStatusBadRequest_ThenWarnContextCalled", func(tt *testing.T) {
		mockRT := NewMockRoundTripper(tt)
		mockLogger := log.NewMockLogger(tt)
		body := `{"foo":"bar"}`
		req := &http.Request{
			Method: "POST",
			URL:    mustParseURL("https://host/path"),
			Header: http.Header{},
			Body:   io.NopCloser(bytes.NewBufferString(body)),
		}
		resp := &http.Response{
			StatusCode: http.StatusBadRequest,
			Header:     http.Header{},
			Body:       io.NopCloser(bytes.NewBufferString(`{"result":"fail"}`)),
		}
		mockRT.On("RoundTrip", mock.Anything).Return(resp, nil)
		mockLogger.On("With", mock.Anything).Return(mockLogger)
		mockLogger.On("InfoContext", mock.Anything, mock.Anything).Return(mockLogger)
		mockLogger.On("WarnContext", mock.Anything, mock.Anything).Return(mockLogger)

		lrt := &LoggingRoundTripper{
			trace:        mockLogger,
			logVerbose:   false,
			authStyle:    "basic",
			roundTripper: mockRT,
		}
		_, err := lrt.RoundTrip(req)
		assert.NoError(tt, err)
	})

	t.Run("WhenSvmNameHeaderPresent_ThenHeaderCopiedToResponse", func(tt *testing.T) {
		mockRT := NewMockRoundTripper(tt)
		mockLogger := log.NewMockLogger(tt)
		body := `{"foo":"bar"}`
		req := &http.Request{
			Method: "POST",
			URL:    mustParseURL("https://host/path"),
			Header: http.Header{xDotSvmNameHeaderKey: []string{"svm1"}},
			Body:   io.NopCloser(bytes.NewBufferString(body)),
		}
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       io.NopCloser(bytes.NewBufferString(`{"result":"ok"}`)),
		}
		mockRT.On("RoundTrip", mock.Anything).Return(resp, nil)
		mockLogger.On("With", mock.Anything).Return(mockLogger)
		mockLogger.On("InfoContext", mock.Anything, mock.Anything).Return(mockLogger)

		lrt := &LoggingRoundTripper{
			trace:        mockLogger,
			logVerbose:   true,
			authStyle:    "basic",
			roundTripper: mockRT,
		}
		gotResp, err := lrt.RoundTrip(req)
		assert.NoError(tt, err)
		assert.Equal(tt, "svm1", gotResp.Header.Get(xDotSvmNameHeaderKey))
	})
}

// Helper for simulating io.Reader error
type badReader struct{}

func (badReader) Read(_ []byte) (int, error) { return 0, assert.AnError }
