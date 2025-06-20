package vsa

import (
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

var (
	// Retry for 5 minutes by default
	defaultMaxRetryCount      = 60
	defaultRetrySleepInterval = 5
	maxRetryCount             = env.GetInt("ONTAP_TRANSIENT_ERROR_RETRIES", defaultMaxRetryCount)
	retrySleepInterval        = time.Duration(env.GetInt("ONTAP_TRANSIENT_ERROR_SLEEP_SECONDS", defaultRetrySleepInterval)) * time.Second
	RetryOnErrors             = _retryOnErrors
)

type operation func() error

func _retryOnErrors(op operation, errs []string) (err error) {
	err = op()
	for i := 0; i < maxRetryCount && err != nil && shouldRetry(err, errs); i++ {
		time.Sleep(retrySleepInterval)
		err = op()
	}
	return
}

func shouldRetry(err error, errs []string) bool {
	for _, e := range errs {
		if strings.Contains(err.Error(), e) {
			return true
		}
	}
	return false
}
