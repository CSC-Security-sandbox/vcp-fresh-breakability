package utils

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"go.temporal.io/sdk/temporal"
)

var (
	HyperscalerHeartbeatTimeout        = env.GetString("HYPERSCALER_HEARTBEAT_TIMEOUT", "300s")
	HyperscalerRetryInitialInterval    = env.GetString("HYPERSCALER_RETRY_INITIAL_INTERVAL", "30s")
	HyperscalerLRORetryInitialInterval = env.GetString("HYPERSCALER_LRO_RETRY_INITIAL_INTERVAL", "30s")
	HyperscalerRetryMaximumAttempts    = env.GetInt("HYPERSCALER_RETRY_MAXIMUM_ATTEMPTS", 10)
	HyperscalerRetryBackoffCoefficient = env.GetFloat64("HYPERSCALER_RETRY_BACKOFF_COEFFICIENT", 1)
	HyperscalerRetryMaximumInterval    = env.GetString("HYPERSCALER_RETRY_MAXIMUM_INTERVAL", "30s")
	StartToCloseTimeoutForHyperscaler  = env.GetString("START_TO_CLOSE_TIMEOUT_FOR_HYPERSCALER", "5m")

	GetHeartbeatTimeoutForHyperscaler = getHeartbeatTimeoutForHyperscaler
	GetStartToCloseTimeoutHyperscaler = getStartToCloseTimeoutHyperscaler
	GetHyperscalerRetryPolicy         = getHyperscalerRetryPolicy
	GetHyperscalerLRORetryPolicy      = getHyperscalerLroRetryPolicy
)

func getHeartbeatTimeoutForHyperscaler() time.Duration {
	hyperscalerHeartBeatTimeout, err := time.ParseDuration(HyperscalerHeartbeatTimeout)
	if err != nil {
		hyperscalerHeartBeatTimeout = 300 * time.Second
	}
	return hyperscalerHeartBeatTimeout
}

func getStartToCloseTimeoutHyperscaler() time.Duration {
	startToCloseTimeoutForHyperscaler, err := time.ParseDuration(StartToCloseTimeoutForHyperscaler)
	if err != nil {
		startToCloseTimeoutForHyperscaler = 5 * time.Minute
	}
	return startToCloseTimeoutForHyperscaler
}

func getHyperscalerRetryPolicy() *temporal.RetryPolicy {
	hyperscalerRetryInitialInterval, err := time.ParseDuration(HyperscalerRetryInitialInterval)
	if err != nil {
		hyperscalerRetryInitialInterval = 30 * time.Second
	}
	hyperscalerRetryMaximumInterval, err := time.ParseDuration(HyperscalerRetryMaximumInterval)
	if err != nil {
		hyperscalerRetryMaximumInterval = 30 * time.Second
	}
	return &temporal.RetryPolicy{
		InitialInterval:        hyperscalerRetryInitialInterval,
		BackoffCoefficient:     HyperscalerRetryBackoffCoefficient,
		MaximumInterval:        hyperscalerRetryMaximumInterval,
		MaximumAttempts:        int32(HyperscalerRetryMaximumAttempts),
		NonRetryableErrorTypes: []string{"PanicError", "NonRetryableError", "NonRetryableErr"},
	}
}

func getHyperscalerLroRetryPolicy() *temporal.RetryPolicy {
	hyperscalerLRORetryInitialInterval, err := time.ParseDuration(HyperscalerLRORetryInitialInterval)
	if err != nil {
		hyperscalerLRORetryInitialInterval = 30 * time.Second
	}
	retryPolicy := getHyperscalerRetryPolicy()
	retryPolicy.InitialInterval = hyperscalerLRORetryInitialInterval
	return retryPolicy
}
