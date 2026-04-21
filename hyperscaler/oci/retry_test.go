package oci

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------
// mockNetError implements net.Error for testing the timeout branch.
// ---------------------------------------------------------------------------

type mockNetError struct {
	timeout   bool
	temporary bool
}

func (e *mockNetError) Error() string   { return "mock network error" }
func (e *mockNetError) Timeout() bool   { return e.timeout }
func (e *mockNetError) Temporary() bool { return e.temporary }

// ---------------------------------------------------------------------------
// TestShouldRetry
// ---------------------------------------------------------------------------

func TestShouldRetry(t *testing.T) {
	t.Run("429 TooManyRequests — retryable", func(t *testing.T) {
		err := &mockServiceError{statusCode: http.StatusTooManyRequests}
		assert.True(t, shouldRetry(err))
	})

	t.Run("500 InternalServerError — retryable", func(t *testing.T) {
		err := &mockServiceError{statusCode: http.StatusInternalServerError}
		assert.True(t, shouldRetry(err))
	})

	t.Run("502 BadGateway — retryable", func(t *testing.T) {
		err := &mockServiceError{statusCode: http.StatusBadGateway}
		assert.True(t, shouldRetry(err))
	})

	t.Run("503 ServiceUnavailable — retryable", func(t *testing.T) {
		err := &mockServiceError{statusCode: http.StatusServiceUnavailable}
		assert.True(t, shouldRetry(err))
	})

	t.Run("504 GatewayTimeout — retryable", func(t *testing.T) {
		err := &mockServiceError{statusCode: http.StatusGatewayTimeout}
		assert.True(t, shouldRetry(err))
	})

	t.Run("400 BadRequest — not retryable", func(t *testing.T) {
		err := &mockServiceError{statusCode: http.StatusBadRequest}
		assert.False(t, shouldRetry(err))
	})

	t.Run("401 Unauthorized — not retryable", func(t *testing.T) {
		err := &mockServiceError{statusCode: http.StatusUnauthorized}
		assert.False(t, shouldRetry(err))
	})

	t.Run("403 Forbidden — not retryable", func(t *testing.T) {
		err := &mockServiceError{statusCode: http.StatusForbidden}
		assert.False(t, shouldRetry(err))
	})

	t.Run("404 NotFound — not retryable", func(t *testing.T) {
		err := &mockServiceError{statusCode: http.StatusNotFound}
		assert.False(t, shouldRetry(err))
	})

	t.Run("409 Conflict — not retryable", func(t *testing.T) {
		err := &mockServiceError{statusCode: http.StatusConflict}
		assert.False(t, shouldRetry(err))
	})

	t.Run("net.Error with timeout — retryable", func(t *testing.T) {
		err := &mockNetError{timeout: true}
		assert.True(t, shouldRetry(err))
	})

	t.Run("net.Error without timeout — not retryable", func(t *testing.T) {
		err := &mockNetError{timeout: false}
		assert.False(t, shouldRetry(err))
	})

	t.Run("generic error — not retryable", func(t *testing.T) {
		err := errors.New("something broke")
		assert.False(t, shouldRetry(err))
	})
}

// ---------------------------------------------------------------------------
// TestSleep
// ---------------------------------------------------------------------------

func TestSleep(t *testing.T) {
	// Use a tiny base so the actual time.Sleep in the production code
	// finishes quickly during tests.
	tinyBase := time.Microsecond

	t.Run("retryable error within limit — sleeps and returns nil", func(t *testing.T) {
		origJitter := jitterBase
		jitterBase = 0
		defer func() { jitterBase = origJitter }()

		r := NewExponentialRetryStrategy(tinyBase, 3)
		err := &mockServiceError{statusCode: http.StatusTooManyRequests, message: "rate limited"}

		result := r.Sleep(err)
		assert.NoError(t, result)
		assert.Equal(t, uint(1), r.GetRetryCount())
	})

	t.Run("retryable error — can retry up to maxRetries", func(t *testing.T) {
		origJitter := jitterBase
		jitterBase = 0
		defer func() { jitterBase = origJitter }()

		r := NewExponentialRetryStrategy(tinyBase, 2)
		err := &mockServiceError{statusCode: http.StatusServiceUnavailable, message: "unavailable"}

		assert.NoError(t, r.Sleep(err))
		assert.Equal(t, uint(1), r.GetRetryCount())

		assert.NoError(t, r.Sleep(err))
		assert.Equal(t, uint(2), r.GetRetryCount())
	})

	t.Run("retryable error exceeds maxRetries — returns backoff error", func(t *testing.T) {
		origJitter := jitterBase
		jitterBase = 0
		defer func() { jitterBase = origJitter }()

		r := NewExponentialRetryStrategy(tinyBase, 1)
		err := &mockServiceError{statusCode: http.StatusBadGateway, message: "bad gateway"}

		assert.NoError(t, r.Sleep(err))
		assert.Equal(t, uint(1), r.GetRetryCount())

		result := r.Sleep(err)
		assert.Error(t, result)
		assert.Contains(t, result.Error(), "BackOff exceeded maximum retries")
	})

	t.Run("maxRetries=0 — first retry already exceeds limit", func(t *testing.T) {
		r := NewExponentialRetryStrategy(tinyBase, 0)
		err := &mockServiceError{statusCode: http.StatusInternalServerError, message: "server error"}

		result := r.Sleep(err)
		assert.Error(t, result)
		assert.Contains(t, result.Error(), "BackOff exceeded maximum retries")
		assert.Equal(t, uint(0), r.GetRetryCount())
	})

	t.Run("non-retryable error — returns original error immediately", func(t *testing.T) {
		r := NewExponentialRetryStrategy(tinyBase, 5)
		err := &mockServiceError{statusCode: http.StatusBadRequest, message: "invalid input"}

		result := r.Sleep(err)
		assert.Error(t, result)
		assert.Equal(t, err, result)
		assert.Equal(t, uint(0), r.GetRetryCount())
	})

	t.Run("generic error — returns original error immediately", func(t *testing.T) {
		r := NewExponentialRetryStrategy(tinyBase, 5)
		err := errors.New("dns lookup failed")

		result := r.Sleep(err)
		assert.Error(t, result)
		assert.Equal(t, err, result)
		assert.Equal(t, uint(0), r.GetRetryCount())
	})

	t.Run("net timeout error — retries like service errors", func(t *testing.T) {
		origJitter := jitterBase
		jitterBase = 0
		defer func() { jitterBase = origJitter }()

		r := NewExponentialRetryStrategy(tinyBase, 3)
		err := &mockNetError{timeout: true}

		assert.NoError(t, r.Sleep(err))
		assert.Equal(t, uint(1), r.GetRetryCount())
	})
}

// ---------------------------------------------------------------------------
// TestReset
// ---------------------------------------------------------------------------

func TestReset(t *testing.T) {
	t.Run("resets retry count to zero", func(t *testing.T) {
		origJitter := jitterBase
		jitterBase = 0
		defer func() { jitterBase = origJitter }()

		r := NewExponentialRetryStrategy(time.Microsecond, 5)
		err := &mockServiceError{statusCode: http.StatusTooManyRequests, message: "rate limited"}

		_ = r.Sleep(err)
		_ = r.Sleep(err)
		assert.Equal(t, uint(2), r.GetRetryCount())

		r.Reset()
		assert.Equal(t, uint(0), r.GetRetryCount())
	})

	t.Run("after reset — retries can be used again", func(t *testing.T) {
		origJitter := jitterBase
		jitterBase = 0
		defer func() { jitterBase = origJitter }()

		r := NewExponentialRetryStrategy(time.Microsecond, 1)
		err := &mockServiceError{statusCode: http.StatusServiceUnavailable, message: "down"}

		assert.NoError(t, r.Sleep(err))
		result := r.Sleep(err)
		assert.Error(t, result, "should exceed max after 1 retry")

		r.Reset()

		assert.NoError(t, r.Sleep(err), "should succeed again after reset")
		assert.Equal(t, uint(1), r.GetRetryCount())
	})
}

// ---------------------------------------------------------------------------
// TestGetRetryCount
// ---------------------------------------------------------------------------

func TestGetRetryCount(t *testing.T) {
	t.Run("starts at zero", func(t *testing.T) {
		r := NewExponentialRetryStrategy(time.Second, 5)
		assert.Equal(t, uint(0), r.GetRetryCount())
	})

	t.Run("increments with each successful retry", func(t *testing.T) {
		origJitter := jitterBase
		jitterBase = 0
		defer func() { jitterBase = origJitter }()

		r := NewExponentialRetryStrategy(time.Microsecond, 10)
		err := &mockServiceError{statusCode: http.StatusGatewayTimeout, message: "timeout"}

		for i := uint(1); i <= 3; i++ {
			_ = r.Sleep(err)
			assert.Equal(t, i, r.GetRetryCount())
		}
	})
}

// ---------------------------------------------------------------------------
// TestGenerateJitter
// ---------------------------------------------------------------------------

func TestGenerateJitter(t *testing.T) {
	r := &retry{}

	t.Run("returns duration in valid range", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			jitter := r.generateJitter()
			assert.GreaterOrEqual(t, jitter, time.Duration(0))
			assert.LessOrEqual(t, jitter, 30*time.Millisecond)
		}
	})

	t.Run("produces multiple distinct values", func(t *testing.T) {
		seen := make(map[time.Duration]bool)
		for i := 0; i < 100; i++ {
			seen[r.generateJitter()] = true
		}
		assert.GreaterOrEqual(t, len(seen), 5, "expected at least 5 unique jitter values from 100 calls")
	})

	t.Run("never returns negative", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			assert.GreaterOrEqual(t, r.generateJitter(), time.Duration(0))
		}
	})
}

// ---------------------------------------------------------------------------
// TestNewExponentialRetryStrategy
// ---------------------------------------------------------------------------

func TestNewExponentialRetryStrategy(t *testing.T) {
	t.Run("returns a non-nil retry with correct fields", func(t *testing.T) {
		r := NewExponentialRetryStrategy(500*time.Millisecond, 7)
		assert.NotNil(t, r)
		assert.Equal(t, 500*time.Millisecond, r.base)
		assert.Equal(t, uint(7), r.maxRetries)
		assert.Equal(t, uint(0), r.retries)
		assert.NotNil(t, r.function)
	})

	t.Run("backoff function is exponential (2<<retries)", func(t *testing.T) {
		r := NewExponentialRetryStrategy(100*time.Millisecond, 10)

		assert.Equal(t, 400*time.Millisecond, r.function(100*time.Millisecond, 1))  // 100 * (2<<1) = 100*4
		assert.Equal(t, 800*time.Millisecond, r.function(100*time.Millisecond, 2))  // 100 * (2<<2) = 100*8
		assert.Equal(t, 1600*time.Millisecond, r.function(100*time.Millisecond, 3)) // 100 * (2<<3) = 100*16
	})

	t.Run("implements RetryStrategy interface", func(t *testing.T) {
		var strategy RetryStrategy = NewExponentialRetryStrategy(time.Second, 3)
		assert.NotNil(t, strategy)
	})
}
