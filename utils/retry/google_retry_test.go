package retry

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
}
