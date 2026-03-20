package retry

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"runtime"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

var (
	logger     = log.NewLogger()
	maxRetries = env.GetInt("VCP_DB_RETRY_MAX", 10)
	waitMs     = env.GetInt("VCP_DB_RETRY_DELAY_MS", 250)
	retryStats = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "se_retries_histogram",
			Help:    "How many storage engine retries were executed, partitioned by function name and count.",
			Buckets: prometheus.LinearBuckets(1, 1, getMaxRetries()),
		},
		[]string{"method"},
	)

	runtimeCaller    = runtime.Caller
	runtimeFuncForPC = runtime.FuncForPC
)

func getMaxRetries() int {
	return maxRetries
}

func getRetryDelay() time.Duration {
	return time.Duration(waitMs) * time.Millisecond
}

func init() {
	prometheus.MustRegister(retryStats)
}

// SecureIntn generates a cryptographically secure random integer in [0, n)
func SecureIntn(n int) (int, error) {
	if n <= 0 {
		return 0, fmt.Errorf("n must be positive")
	}
	maxVal := big.NewInt(int64(n))
	result, err := rand.Int(rand.Reader, maxVal)
	if err != nil {
		return 0, err
	}
	if result == nil {
		return 0, fmt.Errorf("secure random: unexpected nil result")
	}
	return int(result.Int64()), nil
}

// Retriable represents functions that can be retried.
type Retriable func(attempt int) (retry bool, err error)

// Do keeps trying the function until the second argument
// returns false, or no error is returned.
func Do(fn Retriable) error {
	var err error
	var cont bool
	attempt := 1
	pAttempt := &attempt

	defer func() {
		retryStats.WithLabelValues(getCallerName(3)).Observe(float64(*pAttempt))
	}()

	for {
		cont, err = fn(attempt)
		if !cont || err == nil {
			break
		}

		if attempt >= getMaxRetries() {
			logger.Error("Exceeded function retry limit.", "attempt", *pAttempt, "error", err.Error(), "function", getCallerName(2))
			return err
		}

		attempt++
		logger.Warn("Retrying function", "attempt", *pAttempt, "function", getCallerName(2))
		delay := getRetryDelay()
		jitterMax := int(delay / 5)
		if jitterMax > 0 {
			jitter, err := SecureIntn(jitterMax)
			if err != nil {
				// Fallback should not happen, but handle gracefully by using no jitter
				logger.Warn("Failed to generate random jitter, using delay without jitter", "error", err)
				time.Sleep(delay)
			} else {
				randomJitter := time.Duration(jitter)
				time.Sleep(delay + randomJitter)
			}
		} else {
			time.Sleep(delay)
		}
	}
	return err
}

func getCallerName(skip int) string {
	pc, _, _, ok := runtimeCaller(skip)
	details := runtimeFuncForPC(pc)
	if ok && details != nil {
		fullName := details.Name()
		s := strings.Split(fullName, ".")
		return s[len(s)-1]
	}
	return ""
}
