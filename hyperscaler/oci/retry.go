package oci

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	utilretry "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/retry"
)

var (
	jitterBase = time.Millisecond
)

// RetryStrategy defines methods for retrying OCI HTTP requests.
type RetryStrategy interface {
	Sleep(error) error
	Reset()
	GetRetryCount() uint
}

type retry struct {
	base       time.Duration
	retries    uint
	maxRetries uint
	function   func(time.Duration, uint) time.Duration
}

// shouldRetry returns true for OCI service errors that are transient and safe to retry.
func shouldRetry(err error) bool {
	if serviceErr, ok := common.IsServiceError(err); ok {
		switch serviceErr.GetHTTPStatusCode() {
		case http.StatusTooManyRequests,
			http.StatusInternalServerError,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout:
			return true
		default:
			return false
		}
	}
	if netErr, ok := err.(net.Error); ok {
		return netErr.Timeout()
	}
	return false
}

// Sleep checks if the error is retryable and sleeps if the strategy allows for it.
func (e *retry) Sleep(err error) error {
	if shouldRetry(err) {
		if e.retries >= e.maxRetries {
			return fmt.Errorf("'%s': BackOff exceeded maximum retries", err.Error())
		}
		e.retries++
		v := e.function(e.base, e.retries) + e.generateJitter()
		time.Sleep(v)
		return nil
	}
	return err
}

// Reset sets the retry count back to zero for a new request.
func (e *retry) Reset() {
	e.retries = 0
}

// GetRetryCount returns the current number of retries performed.
func (e *retry) GetRetryCount() uint {
	return e.retries
}

func (e *retry) generateJitter() time.Duration {
	jitter, err := utilretry.SecureIntn(30) // [0, 30] ms jitter
	if err != nil {
		return 0
	}
	return jitterBase * time.Duration(jitter)
}

// NewExponentialRetryStrategy returns a retry strategy with exponential back-off.
func NewExponentialRetryStrategy(base time.Duration, maxRetries uint) *retry {
	function := func(base time.Duration, retries uint) time.Duration {
		return base * time.Duration(2<<retries)
	}
	return &retry{base: base, function: function, maxRetries: maxRetries}
}
