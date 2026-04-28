// Package retry contains an engine to be used for all functions that can be retriable
// This is useful if the orchestration faces a transient error in which it should not halt a job without attempting to try again
package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"strings"
	"time"

	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	vsalogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"google.golang.org/api/googleapi"
)

var (
	ShouldRetry = shouldRetry
)

const (
	pattern                  = `Help Token: [A-Za-z0-9-]+`
	ExponentialBackoffFactor = 0.33333
)

// RetriableErr defines an error for when there is an error that should be retried
type RetriableErr struct {
	error
}

func NewRetriableErr(reason string) error {
	return &RetriableErr{error: errors.New(reason)}
}

// IsRetriableErr checks whether the specified error is a IsConflictErr
func IsRetriableErr(err error) bool {
	_, is := err.(*RetriableErr)
	return is
}

// RetryDoWithTimeout retries the provided function until it returns nil or the timeout is reached.
func RetryDoWithTimeout(ctx context.Context, timeout, wait time.Duration, caller string, fn Retriable) error {
	log := util.GetLogger(ctx)
	var err error
	if timeout <= 0 {
		_, err = fn(1)
		return err
	}
	maxExponentialBackOffDelay := time.Duration(math.Ceil((ExponentialBackoffFactor)*timeout.Seconds())) * time.Second
	t2 := time.Now().Add(timeout)
	i := 1
	for time.Now().Before(t2) {
		_, err = fn(i)
		if err == nil {
			return nil
		}

		if !ShouldRetry(err) {
			return err
		}

		log.WithFields("Retrying function", vsalogger.Fields{
			"caller":    caller,
			"err":       err,
			"attempt":   i,
			"countdown": time.Until(t2),
		}).WarnContext(ctx, "Retrying function")

		time.Sleep(min(maxExponentialBackOffDelay, wait*time.Duration(1<<(i-1))) + (time.Millisecond * time.Duration(utils.GenerateRandomInRange(100)+100)))
		i++
	}
	if err != nil {
		return fmt.Errorf("'%s' retry timeout: %w", caller, err)
	}
	return fmt.Errorf("'%s' retry timeout", caller)
}

// IsRetryableIAMPolicyError checks whether the error is a retryable IAM policy
// conflict (HTTP 409 + "aborted"). This is specifically for the read-modify-write
// retry loop around SetIamPolicy, distinct from the general shouldRetry which
// handles broader transient errors for Temporal/DB retries.
func IsRetryableIAMPolicyError(err error) bool {
	if err == nil {
		return false
	}
	var customErr *errors2.CustomError
	if errors.As(err, &customErr) && customErr.OriginalErr != nil {
		return IsRetryableIAMPolicyError(customErr.OriginalErr)
	}
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		return gerr.Code == http.StatusConflict &&
			strings.Contains(strings.ToLower(gerr.Error()), "aborted")
	}
	return false
}

func shouldRetry(err error) bool {
	var customErr *errors2.CustomError
	if errors.As(err, &customErr) {
		return customErr.IsRetriable()
	}
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
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
		case http.StatusConflict:
			// Retry on IAM policy conflicts (ABORTED status from concurrent modifications)
			// Google Cloud returns 409 with "aborted" status for ETag mismatches during
			// concurrent policy updates. Retrying with exponential backoff resolves these.
			// Reference: https://cloud.google.com/iam/docs/retry-strategy
			if strings.Contains(strings.ToLower(err.Error()), "aborted") {
				return true
			}
			return false
		default:
			if strings.Contains(err.Error(), "rateLimitExceeded") {
				return true
			}
			if strings.Contains(err.Error(), "resourceNotReady") {
				return true
			}
			if strings.Contains(err.Error(), "There is a peering operation in progress on the local or peer network") {
				return true
			}
			if strings.Contains(err.Error(), "There is a route operation in progress on the local or peer network") {
				return true
			}
			re := regexp.MustCompile(pattern)
			if re.MatchString(err.Error()) {
				return true
			}
			return false
		}
	} else if IsRetriableErr(err) {
		return true
	}
	return false
}
