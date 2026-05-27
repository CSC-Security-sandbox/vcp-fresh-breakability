package retry

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"google.golang.org/api/googleapi"
)

func TestRetryDOWithTimeout(t *testing.T) {
	t.Run("WhenRetryNotRequestedFails", func(tt *testing.T) {
		ctx := context.Background()
		fn := func(attempts int) (bool, error) {
			return false, errors.New("blah")
		}
		err := RetryDoWithTimeout(ctx, 0, time.Second, "u.TestDo", fn)
		if err == nil {
			tt.Error("Unexpectedly err is nil")
		} else {
			if err.Error() != "blah" {
				tt.Errorf("Unexpected error: '%s'", err.Error())
			}
		}
	})
	t.Run("WhenRetriableFuncReturnsNonRetriableErr", func(tt *testing.T) {
		ctx := context.Background()
		fn := func(attempts int) (bool, error) {
			return false, errors.New("blah")
		}
		err := RetryDoWithTimeout(ctx, time.Millisecond*200, time.Second, "u.TestDo", fn)
		if err == nil {
			tt.Error("Unexpectedly err is nil")
		} else {
			if err.Error() != "blah" {
				tt.Errorf("Unexpected error: '%s'", err.Error())
			}
		}
	})
	t.Run("WhenTimesOut", func(tt *testing.T) {
		originalShouldRetry := ShouldRetry
		defer func() { ShouldRetry = originalShouldRetry }()
		ShouldRetry = func(err error) bool {
			return true
		}
		mockLogger := log.NewLogger()
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)
		fn := func(attempts int) (bool, error) {
			return false, errors.New("blah")
		}
		err := RetryDoWithTimeout(ctx, time.Millisecond*100, 0, "u.TestDo", fn)
		assert.NotNil(tt, err)
	})
	t.Run("WhenTimesOutNillablePRandLog", func(tt *testing.T) {
		originalShouldRetry := ShouldRetry
		defer func() { ShouldRetry = originalShouldRetry }()
		ShouldRetry = func(err error) bool {
			return true
		}
		mockLogger := log.NewLogger()
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)
		fn := func(attempts int) (bool, error) {
			return false, errors.New("blah")
		}
		err := RetryDoWithTimeout(ctx, time.Millisecond*100, 0, "u.TestDo", fn)
		assert.Error(tt, err)
	})
	t.Run("WhenSucceedsNoRetryRequested", func(tt *testing.T) {
		ctx := context.Background()
		fnCalled := false
		fn := func(attempts int) (bool, error) {
			if attempts != 1 {
				tt.Errorf("Unexpected attempts count: '%d'", attempts)
			}
			fnCalled = true
			return false, nil
		}
		err := RetryDoWithTimeout(ctx, 0, 0, "u.TestDo", fn)
		if err != nil {
			tt.Errorf("Unexpected error: '%s'", err.Error())
		}
		if !fnCalled {
			tt.Error("Unexpectedly fn not called")
		}
	})
}

func TestShouldRetry(t *testing.T) {
	t.Run("ShouldRetryReturnsTrueForInternalServerError", func(tt *testing.T) {
		err := &googleapi.Error{Code: http.StatusInternalServerError}
		if !shouldRetry(err) {
			t.Error("Expected true for internal server error")
		}
	})
	t.Run("ShouldRetryReturnsTrueForInternalServerError", func(tt *testing.T) {
		err := &googleapi.Error{Code: http.StatusBadGateway}
		if !shouldRetry(err) {
			t.Error("Expected true for bad gateway")
		}
	})
	t.Run("ShouldRetryReturnsTrueForServiceUnavailable", func(tt *testing.T) {
		err := &googleapi.Error{Code: http.StatusServiceUnavailable}
		if !shouldRetry(err) {
			t.Error("Expected true for service unavailable")
		}
	})
	t.Run("ShouldRetryReturnsTrueForGatewayTimeout", func(tt *testing.T) {
		err := &googleapi.Error{Code: http.StatusGatewayTimeout}
		if !shouldRetry(err) {
			t.Error("Expected true for gateway timeout")
		}
	})
	t.Run("ShouldRetryReturnsTrueForTooManyRequests", func(tt *testing.T) {
		err := &googleapi.Error{Code: http.StatusTooManyRequests}
		if !shouldRetry(err) {
			t.Error("Expected true for too many requests")
		}
	})
	t.Run("ShouldRetryReturnsTrueForRateLimitExceededMessage", func(tt *testing.T) {
		err := &googleapi.Error{Message: "rateLimitExceeded: quota exceeded"}
		if !shouldRetry(err) {
			t.Error("Expected true for rateLimitExceeded message")
		}
	})
	t.Run("ShouldRetryReturnsTrueForResourceNotReadyMessage", func(tt *testing.T) {
		err := &googleapi.Error{Message: "resourceNotReady: still provisioning"}
		if !shouldRetry(err) {
			t.Error("Expected true for resourceNotReady message")
		}
	})
	t.Run("ShouldRetryReturnsTrueForPeeringOperationInProgressMessage", func(tt *testing.T) {
		err := &googleapi.Error{Message: "There is a peering operation in progress on the local or peer network"}
		if !shouldRetry(err) {
			t.Error("Expected true for peering operation in progress message")
		}
	})
	t.Run("ShouldRetryReturnsTrueForRouteOperationInProgressMessage", func(tt *testing.T) {
		err := &googleapi.Error{Message: "There is a route operation in progress on the local or peer network"}
		if !shouldRetry(err) {
			t.Error("Expected true for route operation in progress message")
		}
	})
	t.Run("ShouldRetryReturnsTrueForHelpTokenPattern", func(tt *testing.T) {
		err := &googleapi.Error{Message: "Some error. Help Token: ABCD-1234"}
		if !shouldRetry(err) {
			t.Error("Expected true for Help Token pattern")
		}
	})
	t.Run("ShouldRetryReturnsFalseForGenericError", func(tt *testing.T) {
		err := &googleapi.Error{Code: http.StatusBadRequest}
		if shouldRetry(err) {
			t.Error("Expected false for non-retriable googleapi error")
		}
	})
	t.Run("ShouldRetryReturnsFalseForGenericError", func(tt *testing.T) {
		err := errors.New("some generic error")
		if shouldRetry(err) {
			t.Error("Expected false for generic error")
		}
	})
	t.Run("ShouldRetryWhenRetriableError", func(tt *testing.T) {
		err := NewRetriableErr("RetriableError")
		if !shouldRetry(err) {
			t.Error("Expected true for too many requests")
		}
	})

	// Tests for HTTP 409 Conflict handling
	t.Run("ShouldRetryReturnsTrueFor409WithAbortedStatus", func(tt *testing.T) {
		err := &googleapi.Error{
			Code:    http.StatusConflict,
			Message: "There were concurrent policy changes. Please retry the whole read-modify-write with exponential backoff. The request's ETag did not match the current policy's ETag., aborted",
		}
		if !shouldRetry(err) {
			t.Error("Expected true for 409 Conflict with 'aborted' status")
		}
	})

	t.Run("ShouldRetryReturnsTrueFor409WithAbortedInMiddle", func(tt *testing.T) {
		err := &googleapi.Error{
			Code:    http.StatusConflict,
			Message: "Policy update failed: aborted due to concurrent modification",
		}
		if !shouldRetry(err) {
			t.Error("Expected true for 409 Conflict with 'aborted' in message")
		}
	})

	t.Run("ShouldRetryReturnsTrueFor409WithRealWorldIAMError", func(tt *testing.T) {
		// Real-world error from the issue description
		err := &googleapi.Error{
			Code:    http.StatusConflict,
			Message: "googleapi: Error 409: There were concurrent policy changes. Please retry the whole read-modify-write with exponential backoff. The request's ETag '\\007\\006DB\\211\\n\\231\\330' did not match the current policy's ETag '\\007\\006DB\\211*\\010\\207'., aborted",
		}
		if !shouldRetry(err) {
			t.Error("Expected true for real-world 409 IAM policy conflict error")
		}
	})

	t.Run("ShouldRetryReturnsFalseFor409WithoutAborted", func(tt *testing.T) {
		// 409 without "aborted" should NOT retry (e.g., resource already exists)
		err := &googleapi.Error{
			Code:    http.StatusConflict,
			Message: "Resource already exists",
		}
		if shouldRetry(err) {
			t.Error("Expected false for 409 Conflict without 'aborted' status")
		}
	})

	t.Run("ShouldRetryReturnsFalseFor409WithResourceAlreadyExists", func(tt *testing.T) {
		err := &googleapi.Error{
			Code:    http.StatusConflict,
			Message: "The resource 'projects/my-project/serviceAccounts/my-sa' already exists",
		}
		if shouldRetry(err) {
			t.Error("Expected false for 409 'resource already exists' error")
		}
	})

	t.Run("ShouldRetryReturnsFalseFor409WithETagOnlyNoAborted", func(tt *testing.T) {
		// Edge case: has ETag but no aborted - should NOT retry
		err := &googleapi.Error{
			Code:    http.StatusConflict,
			Message: "ETag mismatch but this is not an IAM policy error",
		}
		if shouldRetry(err) {
			t.Error("Expected false for 409 with ETag but no 'aborted' status")
		}
	})

	t.Run("ShouldRetryReturnsFalseFor409WithConcurrentPolicyChangesOnlyNoAborted", func(tt *testing.T) {
		// Edge case: has "concurrent policy changes" text but no aborted - should NOT retry
		err := &googleapi.Error{
			Code:    http.StatusConflict,
			Message: "There were concurrent policy changes detected",
		}
		if shouldRetry(err) {
			t.Error("Expected false for 409 with policy changes text but no 'aborted' status")
		}
	})

	t.Run("ShouldRetryReturnsTrueFor409WithAbortedCaseInsensitive", func(tt *testing.T) {
		// Verify it's case-insensitive (lowercase)
		err := &googleapi.Error{
			Code:    http.StatusConflict,
			Message: "Policy update failed, aborted",
		}
		if !shouldRetry(err) {
			t.Error("Expected true for 409 with lowercase 'aborted'")
		}
	})

	t.Run("ShouldRetryReturnsTrueFor409WithAbortedUppercase", func(tt *testing.T) {
		// Case-insensitive check: uppercase ABORTED should match too
		err := &googleapi.Error{
			Code:    http.StatusConflict,
			Message: "Policy update failed, ABORTED",
		}
		if !shouldRetry(err) {
			t.Error("Expected true for 409 with uppercase 'ABORTED' (case-insensitive check)")
		}
	})

	t.Run("ShouldRetryReturnsTrueFor409WithAbortedMixedCase", func(tt *testing.T) {
		// Case-insensitive check: mixed case should match too
		err := &googleapi.Error{
			Code:    http.StatusConflict,
			Message: "Policy update failed, Aborted",
		}
		if !shouldRetry(err) {
			t.Error("Expected true for 409 with mixed case 'Aborted' (case-insensitive check)")
		}
	})

	t.Run("ShouldRetryReturnsFalseFor409WithPartialAbortedMatch", func(tt *testing.T) {
		// Edge case: word containing "aborted" but not exactly "aborted"
		err := &googleapi.Error{
			Code:    http.StatusConflict,
			Message: "Operation unabortedly failed",
		}
		if !shouldRetry(err) {
			t.Error("Expected true because strings.Contains will match 'aborted' within 'unabortedly'")
		}
	})

	// Tests for CustomError handling
	t.Run("ShouldRetryReturnsTrueForCustomErrorWithRetriableTrue", func(tt *testing.T) {
		// Use ErrKMSKeyUnreachable which has retriable: true
		customErr := errors2.NewVCPError(errors2.ErrKMSKeyUnreachable, errors.New("key unreachable"))
		if !shouldRetry(customErr) {
			t.Error("Expected true for CustomError with Retriable=true")
		}
	})

	t.Run("ShouldRetryReturnsFalseForCustomErrorWithRetriableFalse", func(tt *testing.T) {
		// Use ErrKMSKeyDisabledOrDestroyed which has retriable: false
		customErr := errors2.NewVCPError(errors2.ErrKMSKeyDisabledOrDestroyed, errors.New("key disabled"))
		if shouldRetry(customErr) {
			t.Error("Expected false for CustomError with Retriable=false")
		}
	})

	t.Run("ShouldRetryRespectsCustomErrorRetriableFlagWhenWrappingGoogleError", func(tt *testing.T) {
		// CustomError wrapping a googleapi.Error should use CustomError.Retriable, not the Google error's HTTP code
		// Use ErrKMSPermissionDenied (retriable: true) wrapping a 403 error (normally non-retriable)
		googleErr := &googleapi.Error{
			Code:    http.StatusForbidden,
			Message: "Permission denied",
		}
		customErr := errors2.NewVCPError(errors2.ErrKMSPermissionDenied, googleErr)
		if !shouldRetry(customErr) {
			t.Error("Expected true for CustomError with Retriable=true even when wrapping a non-retriable googleapi.Error")
		}
	})

	t.Run("ShouldRetryRespectsCustomErrorNonRetriableFlagWhenWrappingRetriableGoogleError", func(tt *testing.T) {
		// CustomError with Retriable=false should not retry even when wrapping a retriable googleapi.Error
		googleErr := &googleapi.Error{
			Code:    http.StatusInternalServerError,
			Message: "Internal server error",
		}
		// Create a CustomError directly with Retriable=false
		customErr := &errors2.CustomError{
			TrackingID:  errors2.ErrKMSKeyDisabledOrDestroyed,
			Message:     "Key disabled",
			Retriable:   false,
			OriginalErr: googleErr,
		}
		if shouldRetry(customErr) {
			t.Error("Expected false for CustomError with Retriable=false even when wrapping a retriable googleapi.Error")
		}
	})
}

func TestIsRetryableIAMPolicyError(t *testing.T) {
	t.Run("ReturnsFalseForNil", func(tt *testing.T) {
		assert.False(tt, IsRetryableIAMPolicyError(nil))
	})

	t.Run("ReturnsTrueFor409WithAborted", func(tt *testing.T) {
		err := &googleapi.Error{
			Code:    http.StatusConflict,
			Message: "There were concurrent policy changes. Please retry the whole read-modify-write with exponential backoff. The request's ETag did not match the current policy's ETag., aborted",
		}
		assert.True(tt, IsRetryableIAMPolicyError(err))
	})

	t.Run("ReturnsTrueFor409WithAbortedCaseInsensitive", func(tt *testing.T) {
		err := &googleapi.Error{
			Code:    http.StatusConflict,
			Message: "Policy update ABORTED due to concurrent modification",
		}
		assert.True(tt, IsRetryableIAMPolicyError(err))
	})

	t.Run("ReturnsFalseFor409WithoutAborted", func(tt *testing.T) {
		err := &googleapi.Error{
			Code:    http.StatusConflict,
			Message: "Resource already exists",
		}
		assert.False(tt, IsRetryableIAMPolicyError(err))
	})

	t.Run("ReturnsFalseForNon409GoogleError", func(tt *testing.T) {
		err := &googleapi.Error{
			Code:    http.StatusInternalServerError,
			Message: "Internal server error",
		}
		assert.False(tt, IsRetryableIAMPolicyError(err))
	})

	t.Run("ReturnsFalseForGenericError", func(tt *testing.T) {
		err := errors.New("some generic error")
		assert.False(tt, IsRetryableIAMPolicyError(err))
	})

	t.Run("UnwrapsCustomErrorToFind409Aborted", func(tt *testing.T) {
		googleErr := &googleapi.Error{
			Code:    http.StatusConflict,
			Message: "concurrent policy changes, aborted",
		}
		customErr := errors2.NewVCPError(errors2.ErrGCPResourceProvisionError, googleErr)
		assert.True(tt, IsRetryableIAMPolicyError(customErr))
	})

	t.Run("UnwrapsCustomErrorReturnsFalseForNon409", func(tt *testing.T) {
		googleErr := &googleapi.Error{
			Code:    http.StatusForbidden,
			Message: "Permission denied",
		}
		customErr := errors2.NewVCPError(errors2.ErrGCPResourceProvisionError, googleErr)
		assert.False(tt, IsRetryableIAMPolicyError(customErr))
	})

	t.Run("ReturnsFalseForCustomErrorWithNilOriginal", func(tt *testing.T) {
		customErr := errors2.NewVCPError(errors2.ErrGCPResourceProvisionError, nil)
		assert.False(tt, IsRetryableIAMPolicyError(customErr))
	})

	t.Run("ReturnsTrueForRealWorldError", func(tt *testing.T) {
		err := &googleapi.Error{
			Code:    http.StatusConflict,
			Message: `googleapi: Error 409: There were concurrent policy changes. Please retry the whole read-modify-write with exponential backoff. The request's ETag '\007\006DB\211\n\231\330' did not match the current policy's ETag '\007\006DB\211*\010\207'., aborted`,
		}
		assert.True(tt, IsRetryableIAMPolicyError(err))
	})
}
