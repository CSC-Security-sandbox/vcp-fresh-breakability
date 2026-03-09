package active_directory_activities

import (
	stderrors "errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"go.temporal.io/sdk/temporal"
)

type mockCvpApiError struct {
	payload *cvpModels.Error
}

func (m *mockCvpApiError) Error() string   { return "mock cvp error" }
func (m *mockCvpApiError) GetPayload() *cvpModels.Error { return m.payload }

func TestOperationError_Error(t *testing.T) {
	tests := []struct {
		name    string
		err     *operationError
		want    string
	}{
		{
			name: "non-empty message",
			err:  &operationError{code: 400, message: "bad request from CVP"},
			want: "bad request from CVP",
		},
		{
			name: "empty message returns default",
			err:  &operationError{code: 500, message: ""},
			want: "operation failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.err.Error())
		})
	}
}

func TestNewOperationError(t *testing.T) {
	tests := []struct {
		name        string
		code        int
		message     string
		wantMessage string
	}{
		{
			name:        "non-empty message preserved",
			code:        404,
			message:     "resource not found",
			wantMessage: "resource not found",
		},
		{
			name:        "empty message replaced with default",
			code:        500,
			message:     "",
			wantMessage: "operation failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewOperationError(tt.code, tt.message)
			assert.Error(t, err)

			var opErr *operationError
			assert.True(t, stderrors.As(err, &opErr))
			assert.Equal(t, tt.code, opErr.code)
			assert.Equal(t, tt.wantMessage, opErr.message)
			assert.Equal(t, tt.wantMessage, err.Error())
		})
	}
}

func TestGetCvpErrorCodeAndMessage(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantCode    int
		wantMessage string
		wantOk      bool
	}{
		{
			name:        "nil error",
			err:         nil,
			wantCode:    0,
			wantMessage: "",
			wantOk:      false,
		},
		{
			name:        "operationError",
			err:         NewOperationError(409, "conflict"),
			wantCode:    409,
			wantMessage: "conflict",
			wantOk:      true,
		},
		{
			name: "CvpApiError with code and message",
			err: &mockCvpApiError{
				payload: &cvpModels.Error{Code: 400, Message: "invalid input"},
			},
			wantCode:    400,
			wantMessage: "invalid input",
			wantOk:      true,
		},
		{
			name:        "CvpApiError with nil payload",
			err:         &mockCvpApiError{payload: nil},
			wantCode:    0,
			wantMessage: "",
			wantOk:      false,
		},
		{
			name: "CvpApiError with empty message",
			err: &mockCvpApiError{
				payload: &cvpModels.Error{Code: 500, Message: ""},
			},
			wantCode:    500,
			wantMessage: "operation failed",
			wantOk:      true,
		},
		{
			name:        "non-CVP error returns not ok",
			err:         fmt.Errorf("some random error"),
			wantCode:    0,
			wantMessage: "",
			wantOk:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, message, ok := GetCvpErrorCodeAndMessage(tt.err)
			assert.Equal(t, tt.wantCode, code)
			assert.Equal(t, tt.wantMessage, message)
			assert.Equal(t, tt.wantOk, ok)
		})
	}
}

func TestWrapCvpErrorByHTTPCodeAndMessage(t *testing.T) {
	tests := []struct {
		name             string
		code             int
		message          string
		wantTrackingID   int
		wantNonRetryable bool
		wantOriginal     string
	}{
		{
			name:             "400 Bad Request",
			code:             common.HTTPStatusBadRequest,
			message:          "bad request",
			wantTrackingID:   vsaerrors.ErrCVPBadRequest,
			wantNonRetryable: true,
			wantOriginal:     "bad request",
		},
		{
			name:             "401 Unauthorized",
			code:             common.HTTPStatusUnauthorized,
			message:          "unauthorized",
			wantTrackingID:   vsaerrors.ErrCVPUnauthorized,
			wantNonRetryable: true,
			wantOriginal:     "unauthorized",
		},
		{
			name:             "403 Forbidden",
			code:             common.HTTPStatusForbidden,
			message:          "forbidden",
			wantTrackingID:   vsaerrors.ErrCVPForbidden,
			wantNonRetryable: true,
			wantOriginal:     "forbidden",
		},
		{
			name:             "404 Not Found",
			code:             common.HTTPStatusNotFound,
			message:          "not found",
			wantTrackingID:   vsaerrors.ErrCVPNotFound,
			wantNonRetryable: true,
			wantOriginal:     "not found",
		},
		{
			name:             "409 Conflict",
			code:             common.HTTPStatusConflict,
			message:          "conflict",
			wantTrackingID:   vsaerrors.ErrCVPConflict,
			wantNonRetryable: true,
			wantOriginal:     "conflict",
		},
		{
			name:             "422 Unprocessable Entity",
			code:             common.HTTPStatusUnprocessableEntity,
			message:          "unprocessable",
			wantTrackingID:   vsaerrors.ErrCVPUnprocessableEntity,
			wantNonRetryable: true,
			wantOriginal:     "unprocessable",
		},
		{
			name:             "429 Too Many Requests is retryable",
			code:             common.HTTPStatusTooManyRequests,
			message:          "rate limited",
			wantTrackingID:   vsaerrors.ErrCVPTooManyRequests,
			wantNonRetryable: false,
			wantOriginal:     "rate limited",
		},
		{
			name:             "500 Internal Server Error is retryable",
			code:             common.HTTPStatusInternalServerError,
			message:          "internal error",
			wantTrackingID:   vsaerrors.ErrCVPInternalServerError,
			wantNonRetryable: false,
			wantOriginal:     "internal error",
		},
		{
			name:             "unknown code defaults to retryable internal error",
			code:             999,
			message:          "unknown failure",
			wantTrackingID:   vsaerrors.ErrCVPInternalServerError,
			wantNonRetryable: false,
			wantOriginal:     "unknown failure",
		},
		{
			name:             "empty message replaced with default",
			code:             common.HTTPStatusBadRequest,
			message:          "",
			wantTrackingID:   vsaerrors.ErrCVPBadRequest,
			wantNonRetryable: true,
			wantOriginal:     "operation failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := wrapCvpErrorByHTTPCodeAndMessage(tt.code, tt.message)
			assert.Error(t, err)

			customErr := vsaerrors.ExtractCustomError(err)
			assert.NotNil(t, customErr)
			assert.True(t, customErr.IsError(tt.wantTrackingID))

			var appErr *temporal.ApplicationError
			assert.True(t, stderrors.As(err, &appErr))
			assert.Equal(t, tt.wantNonRetryable, appErr.NonRetryable())
		})
	}
}

func TestWrapCvpError(t *testing.T) {
	t.Run("operationError wraps by code and message", func(t *testing.T) {
		err := WrapCvpError(NewOperationError(404, "not found"))
		assert.Error(t, err)

		customErr := vsaerrors.ExtractCustomError(err)
		assert.NotNil(t, customErr)
		assert.True(t, customErr.IsError(vsaerrors.ErrCVPNotFound))

		var appErr *temporal.ApplicationError
		assert.True(t, stderrors.As(err, &appErr))
		assert.True(t, appErr.NonRetryable())
	})

	t.Run("CvpApiError wraps by payload code and message", func(t *testing.T) {
		cvpErr := &mockCvpApiError{
			payload: &cvpModels.Error{Code: 409, Message: "resource conflict"},
		}
		err := WrapCvpError(cvpErr)
		assert.Error(t, err)

		customErr := vsaerrors.ExtractCustomError(err)
		assert.NotNil(t, customErr)
		assert.True(t, customErr.IsError(vsaerrors.ErrCVPConflict))

		var appErr *temporal.ApplicationError
		assert.True(t, stderrors.As(err, &appErr))
		assert.True(t, appErr.NonRetryable())
	})

	t.Run("non-structured error wraps as retryable internal error", func(t *testing.T) {
		err := WrapCvpError(fmt.Errorf("connection refused"))
		assert.Error(t, err)

		customErr := vsaerrors.ExtractCustomError(err)
		assert.NotNil(t, customErr)
		assert.True(t, customErr.IsError(vsaerrors.ErrCVPInternalServerError))

		var appErr *temporal.ApplicationError
		assert.True(t, stderrors.As(err, &appErr))
		assert.False(t, appErr.NonRetryable())
	})
}
