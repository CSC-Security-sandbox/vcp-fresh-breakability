package transport

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestRetryTransportSubmit(t *testing.T) {
	orgRetries := maxRetries
	maxRetries = 2

	defaultWaitLongRetryOld := defaultWaitLongRetry
	defaultWaitLongRetry = 2

	defaultWaitSecOld := defaultWait
	defaultWait = 1

	defer func() {
		maxRetries = orgRetries

		defaultWaitLongRetry = defaultWaitLongRetryOld
		defaultWait = defaultWaitSecOld
	}()

	op := &runtime.ClientOperation{
		ID:          "TestOperation",
		Method:      "GET",
		PathPattern: "/api/test",
		Params:      &fakeParams{Param1: "test", Param2: 0},
	}

	t.Run("WhenErrorFromOntap", func(tt *testing.T) {
		transport := NewRetryTransport(log.NewLogger().(*log.Slogger), &fakeTransport{
			result: nil,
			err:    &fakeOntapResult{_statusCode: 666, FakeErrorMessage: "some error occurred", Payload: &models.ErrorResponse{Error: &models.ReturnedError{Message: nillable.ToPointer("some error occurred"), Code: nillable.ToPointer("zeCode")}}}})
		result, err := transport.Submit(op)
		assert.EqualError(tt, err, "some error occurred")
		assert.Nil(tt, result)
	})
	t.Run("WhenRetriableHTTPCode", func(tt *testing.T) {
		httpCodes := []int{
			http.StatusRequestTimeout,
			http.StatusTooManyRequests,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout,
		}
		defer func() { submit = _submit }()
		submitCallCount := 0
		submit = func(ft *fakeTransport, op *runtime.ClientOperation) (interface{}, error) {
			submitCallCount++
			return ft.result, ft.err
		}

		for _, code := range httpCodes {
			submitCallCount = 0
			transport := NewRetryTransport(log.NewLogger().(*log.Slogger), &fakeTransport{
				result: "some result",
				err:    &fakeOntapResult{_statusCode: code, FakeErrorMessage: "err message", Payload: &models.ErrorResponse{Error: &models.ReturnedError{Message: nillable.ToPointer("123"), Code: nillable.ToPointer("zeCode")}}},
			})
			result, err := transport.Submit(op)
			assert.EqualError(tt, err, "Retries exhausted when attempting to reach the storage server")
			assert.Equal(tt, 2, submitCallCount)
			assert.Nil(tt, result)
		}
	})
	t.Run("WhenRetriableOntapCode", func(tt *testing.T) {
		ontapCodes := []string{"7", "8", "262160"}
		submitCallCount := 0
		submit = func(ft *fakeTransport, op *runtime.ClientOperation) (interface{}, error) {
			submitCallCount++
			return ft.result, ft.err
		}

		for _, code := range ontapCodes {
			submitCallCount = 0
			transport := NewRetryTransport(log.NewLogger().(*log.Slogger), &fakeTransport{
				result: "some result",
				err:    &fakeOntapResult{_statusCode: http.StatusOK, FakeErrorMessage: "err message", Payload: &models.ErrorResponse{Error: &models.ReturnedError{Message: nillable.ToPointer("123"), Code: &code}}},
			})
			result, err := transport.Submit(op)
			assert.EqualError(tt, err, "Retries exhausted when attempting to reach the storage server")
			assert.Equal(tt, 2, submitCallCount)
			assert.Nil(tt, result)
		}
	})
	t.Run("WhenMaximumRetriesReachedShouldReturnTimeoutError", func(tt *testing.T) {
		transport := NewRetryTransport(log.NewLogger(), &fakeTransport{
			result: "some result",
			err:    &fakeOntapResult{_statusCode: http.StatusTooManyRequests, FakeErrorMessage: "err message", Payload: &models.ErrorResponse{Error: &models.ReturnedError{Message: nillable.ToPointer("123"), Code: nillable.ToPointer("zeCode")}}},
		})
		result, err := transport.Submit(op)
		assert.EqualError(tt, err, "Retries exhausted when attempting to reach the storage server")
		assert.True(tt, errors.IsTimeoutErr(err))
		assert.Nil(tt, result)
	})
	t.Run("WhenMaximumRetriesReachedShouldReturnTimeoutErrorWithAuth", func(tt *testing.T) {
		ontapRestOAuthEnabledOld := ontapRestOAuthEnabled
		ontapRestOAuthEnabled = true
		defer func() {
			ontapRestOAuthEnabled = ontapRestOAuthEnabledOld
		}()

		transport := NewRetryTransport(log.NewLogger(), &fakeTransport{
			result: "some result",
			err:    &fakeOntapResult{_statusCode: http.StatusTooManyRequests, FakeErrorMessage: "err message", Payload: &models.ErrorResponse{Error: &models.ReturnedError{Message: nillable.ToPointer("123"), Code: nillable.ToPointer("6691623")}}},
		})
		result, err := transport.Submit(op)
		assert.EqualError(tt, err, "Internal server error")
		assert.True(tt, errors.IsTimeoutErr(err))
		assert.Nil(tt, result)
	})
	t.Run("WhenTransportLayerError", func(tt *testing.T) {
		transport := NewRetryTransport(log.NewLogger().(*log.Slogger), &fakeTransport{
			err: &url.Error{Op: "POST", URL: "some broken url", Err: errors.New("some erroring")},
		})
		result, err := transport.Submit(op)
		assert.EqualError(tt, err, "Retries exhausted when attempting to reach the storage server")
		assert.Nil(tt, result)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		submitCallCount := 0
		defer func() { submit = _submit }()
		submit = func(ft *fakeTransport, op *runtime.ClientOperation) (interface{}, error) {
			submitCallCount++
			return ft.result, ft.err
		}
		transport := NewRetryTransport(log.NewLogger().(*log.Slogger), &fakeTransport{
			result: "some result",
			err:    nil,
		})

		result, err := transport.Submit(op)
		assert.NoError(tt, err)
		assert.Equal(tt, 1, submitCallCount)
		assert.Equal(tt, "some result", result)
	})
	t.Run("WhenSuccessTimeoutPatched", func(tt *testing.T) {
		opx := &runtime.ClientOperation{
			ID:          "TestOperation",
			Method:      "GET",
			PathPattern: "/api/test",
			Params:      &fakeParams{Param1: "test", Param2: 0},
		}

		submitCallCount := 0
		defer func() { submit = _submit }()

		ontapTransportTimeoutOld := ontapTransportTimeout
		ontapTransportTimeout = 10
		defer func() {
			ontapTransportTimeout = ontapTransportTimeoutOld
		}()

		submit = func(ft *fakeTransport, op *runtime.ClientOperation) (interface{}, error) {
			submitCallCount++
			return ft.result, ft.err
		}
		transport := NewRetryTransport(log.NewLogger().(*log.Slogger), &fakeTransport{
			result: "some result",
			err:    nil,
		})

		result, err := transport.Submit(opx)
		assert.NoError(tt, err)
		assert.Equal(tt, 1, submitCallCount)
		assert.Equal(tt, "some result", result)
		assert.Equal(tt, opx.Params.(*fakeParams).duration, ontapTransportTimeout)
	})
}

type mockTemporaryErr struct {
}

func (e mockTemporaryErr) Error() string {
	return "error"
}

func (e mockTemporaryErr) Temporary() bool {
	return true
}

type mockTimeoutErr struct {
}

func (e mockTimeoutErr) Error() string {
	return "error"
}

func (e mockTimeoutErr) Timeout() bool {
	return true
}

type mockNetErr struct {
}

func (e mockNetErr) Error() string {
	return "error"
}

func (e mockNetErr) Timeout() bool {
	return true
}

func (e mockNetErr) Temporary() bool {
	return true
}

type mockOntapError struct {
	_statusCode int
	Payload     *models.ErrorResponse
}

func (m mockOntapError) Error() string {
	return ""
}

func (m mockOntapError) GetPayload() *models.ErrorResponse {
	return m.Payload
}

func TestIsRetriableError(t *testing.T) {
	t.Run("WhenUrlErrorAndCertificateVerificationError_ThenNotRetriable", func(tt *testing.T) {
		certErr := &tls.CertificateVerificationError{}
		err := &url.Error{
			Err: certErr,
		}
		waitTime, ok := isRetriableError(err)
		assert.Equal(tt, defaultWait, waitTime.waitDuration)
		assert.False(tt, ok)
	})

	t.Run("WhenUrlErrorAndConnectionRefused_ThenRetriableWithLongWait", func(tt *testing.T) {
		err := &url.Error{
			Err: os.NewSyscallError("connection refused", errors.New("?")),
		}
		waitTime, ok := isRetriableError(err)
		assert.Equal(tt, defaultWaitLongRetry, waitTime.waitDuration)
		assert.True(tt, ok)
	})

	t.Run("WhenUrlErrorAndOther_ThenRetriableWithDefaultWait", func(tt *testing.T) {
		err := &url.Error{
			Err: errors.New("some other error"),
		}
		waitTime, ok := isRetriableError(err)
		assert.Equal(tt, defaultWait, waitTime.waitDuration)
		assert.True(tt, ok)
	})

	t.Run("WhenNetError_ThenRetriableWithDefaultWait", func(tt *testing.T) {
		err := &mockNetErr{}
		waitTime, ok := isRetriableError(err)
		assert.Equal(tt, defaultWait, waitTime.waitDuration)
		assert.True(tt, ok)
	})

	t.Run("WhenTimeoutError_ThenRetriableWithDefaultWait", func(tt *testing.T) {
		err := &mockTimeoutErr{}
		waitTime, ok := isRetriableError(err)
		assert.Equal(tt, defaultWait, waitTime.waitDuration)
		assert.True(tt, ok)
	})

	t.Run("WhenTemporaryError_ThenRetriableWithDefaultWait", func(tt *testing.T) {
		err := &mockTemporaryErr{}
		waitTime, ok := isRetriableError(err)
		assert.Equal(tt, defaultWait, waitTime.waitDuration)
		assert.True(tt, ok)
	})

	t.Run("WhenRetriableHTTPCode_ThenRetriableWithDefaultWait", func(tt *testing.T) {
		httpCodes := []int{
			http.StatusRequestTimeout,
			http.StatusTooManyRequests,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout,
		}
		for _, code := range httpCodes {
			err := &mockOntapError{_statusCode: code}
			waitTime, ok := isRetriableError(err)
			assert.Equal(tt, defaultWait, waitTime.waitDuration)
			assert.True(tt, ok)
		}
	})

	t.Run("WhenRetriableOntapCode_ThenRetriableWithDefaultWait", func(tt *testing.T) {
		ontapCodes := []string{"7", "8", "262160", "6619715", "1967171", "1967177", "13434889", "65536968", "65537564"}
		for _, code := range ontapCodes {
			err := &mockOntapError{
				_statusCode: 200,
				Payload:     &models.ErrorResponse{Error: &models.ReturnedError{Message: nillable.ToPointer("123"), Code: nillable.ToPointer(code)}},
			}
			waitTime, ok := isRetriableError(err)
			assert.Equal(tt, defaultWait, waitTime.waitDuration)
			assert.True(tt, ok)
		}
	})

	t.Run("WhenRetriableOntapCodeWithLongWait_ThenRetriableWithLongWait", func(tt *testing.T) {
		ontapLongerWaitCodes := []string{"13434894", "393271", "524424", "13"}
		for _, code := range ontapLongerWaitCodes {
			err := &mockOntapError{
				_statusCode: 200,
				Payload:     &models.ErrorResponse{Error: &models.ReturnedError{Message: nillable.ToPointer("123"), Code: nillable.ToPointer(code)}},
			}
			waitTime, ok := isRetriableError(err)
			assert.Equal(tt, defaultWaitLongRetry, waitTime.waitDuration)
			assert.True(tt, ok)
		}
	})

	t.Run("WhenRetriableOntapAuthCode_ThenRetriableWithLongWaitAndAuthError", func(tt *testing.T) {
		err := &mockOntapError{
			_statusCode: 200,
			Payload:     &models.ErrorResponse{Error: &models.ReturnedError{Message: nillable.ToPointer("123"), Code: nillable.ToPointer("6691623")}},
		}
		waitTime, ok := isRetriableError(err)
		assert.Equal(tt, defaultWaitLongRetry, waitTime.waitDuration)
		assert.True(tt, ok == ontapRestOAuthEnabled)
		assert.True(tt, waitTime.isAuthError)
	})

	t.Run("WhenRetriableOntapCode458753WithExpectedMessage_ThenRetriableWithLongWait", func(tt *testing.T) {
		msg := "[Job 900592] Job failed: \\nA required service (secd) is not yet available; try again later\\nWait a few minutes, and then try the Vserver delete operation again. If the error persists, contact technical support for assistance."
		err := &mockOntapError{
			_statusCode: 200,
			Payload:     &models.ErrorResponse{Error: &models.ReturnedError{Message: nillable.ToPointer(msg), Code: nillable.ToPointer("458753")}},
		}
		waitTime, ok := isRetriableError(err)
		assert.Equal(tt, defaultWaitLongRetry, waitTime.waitDuration)
		assert.True(tt, ok)
	})

	t.Run("WhenNotRetriableOntapCode_ThenNotRetriable", func(tt *testing.T) {
		err := &mockOntapError{
			_statusCode: 200,
			Payload:     &models.ErrorResponse{Error: &models.ReturnedError{Message: nillable.ToPointer("123"), Code: nillable.ToPointer("666")}},
		}
		waitTime, ok := isRetriableError(err)
		assert.Equal(tt, time.Duration(0), waitTime.waitDuration)
		assert.False(tt, ok)
	})

	t.Run("WhenNotRetriableOntapCode458753WithOtherMessage_ThenNotRetriable", func(tt *testing.T) {
		err := &mockOntapError{
			_statusCode: 200,
			Payload:     &models.ErrorResponse{Error: &models.ReturnedError{Message: nillable.ToPointer("Destination and gateway must belong to the same address family."), Code: nillable.ToPointer("458753")}},
		}
		waitTime, ok := isRetriableError(err)
		assert.Equal(tt, time.Duration(0), waitTime.waitDuration)
		assert.False(tt, ok)
	})

	t.Run("WhenNotRetriableStatusCode_ThenNotRetriable", func(tt *testing.T) {
		err := &mockOntapError{
			_statusCode: http.StatusTeapot,
			Payload:     &models.ErrorResponse{Error: nil},
		}
		waitTime, ok := isRetriableError(err)
		assert.Equal(tt, time.Duration(0), waitTime.waitDuration)
		assert.False(tt, ok)
	})
}

type fakeTransport struct {
	runtime.ClientTransport
	result interface{}
	err    error
}

func (ft *fakeTransport) Submit(op *runtime.ClientOperation) (interface{}, error) {
	return submit(ft, op)
}

var submit = _submit

func _submit(ft *fakeTransport, _ *runtime.ClientOperation) (interface{}, error) {
	return ft.result, ft.err
}

type fakeParams struct {
	Param1   string
	Param2   int
	duration time.Duration
}

func (fp *fakeParams) WriteToRequest(runtime.ClientRequest, strfmt.Registry) error {
	return nil
}

func (fp *fakeParams) SetTimeout(duration time.Duration) {
	fp.duration = duration
}

type fakeOntapResult struct {
	_statusCode      int
	Payload          *models.ErrorResponse
	FakeErrorMessage string
}

func (f *fakeOntapResult) Error() string {
	return f.FakeErrorMessage
}

func (f *fakeOntapResult) GetPayload() *models.ErrorResponse {
	return f.Payload
}
