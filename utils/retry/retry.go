package retry

import (
	"math/rand"
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
		randomJitter := time.Duration(rand.Intn(int(getRetryDelay()) / 5))
		time.Sleep(getRetryDelay() + randomJitter)
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
