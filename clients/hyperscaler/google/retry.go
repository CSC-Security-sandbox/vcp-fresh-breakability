package google

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	"google.golang.org/api/googleapi"
)

var jitterBase = time.Millisecond

// RetryStrategy defines methods for retrying http requests
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

func shouldRetry(err error) bool {
	if gerr, ok := err.(*googleapi.Error); ok {
		switch gerr.Code {
		case http.StatusInternalServerError:
			return true
		case http.StatusBadGateway:
			return true
		case http.StatusServiceUnavailable:
			return true
		case http.StatusGatewayTimeout:
			return true
		case http.StatusTooManyRequests:
			return true
		default:
			if strings.Contains(err.Error(), "rateLimitExceeded") {
				return true
			}
			if strings.Contains(err.Error(), "resourceNotReady") {
				return true
			}
			if strings.Contains(err.Error(), "unable to queue the operation") {
				return true
			}
			if strings.Contains(err.Error(), "notFound") {
				return false
			}
			return false
		}
	}
	if err, ok := err.(net.Error); ok {
		return err.Timeout()
	}

	return false
}

// Sleep check if the error is retryable and sleep if the strategy allows for it
func (e *retry) Sleep(err error) error {
	if shouldRetry(err) {
		if e.retries >= e.maxRetries {
			return fmt.Errorf("'%s': BackOff exceeded maximum retries", err.Error())
		}

		e.retries += 1
		v := e.function(e.base, e.retries) + e.generateJitter()
		time.Sleep(v)
		return nil
	}
	return err
}

// Reset sets the amount of retries to 0 to be used for a new http request
func (e *retry) Reset() {
	e.retries = 0
}

// GetRetryCount get the current amount of retries
func (e *retry) GetRetryCount() uint {
	return e.retries
}

func (e *retry) generateJitter() time.Duration {
	return jitterBase * time.Duration(rand.Intn(30)) // [0, 30] ms jitter
}

// NewExponentialRetryStrategy return a new retry strategy that implements exponential back-off
func NewExponentialRetryStrategy(base time.Duration, maxRetries uint) *retry {
	function := func(base time.Duration, retries uint) time.Duration {
		return base * time.Duration(2<<retries)
	}
	return &retry{base: base, function: function, maxRetries: maxRetries}
}
