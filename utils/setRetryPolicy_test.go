package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
)

func TestGetHeartbeatTimeoutForHyperscaler(t *testing.T) {
	save := HyperscalerHeartbeatTimeout
	defer func() { HyperscalerHeartbeatTimeout = save }()

	t.Run("WhenValidDuration", func(t *testing.T) {
		HyperscalerHeartbeatTimeout = "1m"
		got := getHeartbeatTimeoutForHyperscaler()
		assert.Equal(t, time.Minute, got)
	})

	t.Run("WhenDefault300sIsInvalid", func(t *testing.T) {
		HyperscalerHeartbeatTimeout = "not-a-duration"
		got := getHeartbeatTimeoutForHyperscaler()
		assert.Equal(t, 300*time.Second, got)
	})

	t.Run("WhenParses300s", func(t *testing.T) {
		HyperscalerHeartbeatTimeout = "300s"
		got := getHeartbeatTimeoutForHyperscaler()
		assert.Equal(t, 300*time.Second, got)
	})
}

func TestGetStartToCloseTimeoutHyperscaler(t *testing.T) {
	save := StartToCloseTimeoutForHyperscaler
	defer func() { StartToCloseTimeoutForHyperscaler = save }()

	t.Run("WhenValidDuration", func(t *testing.T) {
		StartToCloseTimeoutForHyperscaler = "10m"
		got := getStartToCloseTimeoutHyperscaler()
		assert.Equal(t, 10*time.Minute, got)
	})

	t.Run("WhenInvalidUsesDefault5m", func(t *testing.T) {
		StartToCloseTimeoutForHyperscaler = "invalid"
		got := getStartToCloseTimeoutHyperscaler()
		assert.Equal(t, 5*time.Minute, got)
	})

	t.Run("WhenParses5m", func(t *testing.T) {
		StartToCloseTimeoutForHyperscaler = "5m"
		got := getStartToCloseTimeoutHyperscaler()
		assert.Equal(t, 5*time.Minute, got)
	})
}

func TestGetHyperscalerRetryPolicy(t *testing.T) {
	saveInitial := HyperscalerRetryInitialInterval
	saveMax := HyperscalerRetryMaximumInterval
	saveAttempts := HyperscalerRetryMaximumAttempts
	saveCoeff := HyperscalerRetryBackoffCoefficient
	defer func() {
		HyperscalerRetryInitialInterval = saveInitial
		HyperscalerRetryMaximumInterval = saveMax
		HyperscalerRetryMaximumAttempts = saveAttempts
		HyperscalerRetryBackoffCoefficient = saveCoeff
	}()

	t.Run("WhenValidEnvValues", func(t *testing.T) {
		HyperscalerRetryInitialInterval = "5s"
		HyperscalerRetryMaximumInterval = "60s"
		HyperscalerRetryMaximumAttempts = 5
		HyperscalerRetryBackoffCoefficient = 2.0

		policy := getHyperscalerRetryPolicy()
		require.NotNil(t, policy)
		assert.Equal(t, 5*time.Second, policy.InitialInterval)
		assert.Equal(t, 60*time.Second, policy.MaximumInterval)
		assert.Equal(t, int32(5), policy.MaximumAttempts)
		assert.Equal(t, 2.0, policy.BackoffCoefficient)
		assert.Equal(t, []string{"PanicError", "NonRetryableError", "NonRetryableErr"}, policy.NonRetryableErrorTypes)
	})

	t.Run("WhenInvalidInitialIntervalUses30sDefault", func(t *testing.T) {
		HyperscalerRetryInitialInterval = "bad"
		HyperscalerRetryMaximumInterval = "30s"
		HyperscalerRetryMaximumAttempts = 10
		HyperscalerRetryBackoffCoefficient = 1

		policy := getHyperscalerRetryPolicy()
		require.NotNil(t, policy)
		assert.Equal(t, 30*time.Second, policy.InitialInterval)
		assert.Equal(t, 30*time.Second, policy.MaximumInterval)
	})

	t.Run("WhenInvalidMaximumIntervalUses30sDefault", func(t *testing.T) {
		HyperscalerRetryInitialInterval = "30s"
		HyperscalerRetryMaximumInterval = "bad"
		HyperscalerRetryMaximumAttempts = 10
		HyperscalerRetryBackoffCoefficient = 1

		policy := getHyperscalerRetryPolicy()
		require.NotNil(t, policy)
		assert.Equal(t, 30*time.Second, policy.InitialInterval)
		assert.Equal(t, 30*time.Second, policy.MaximumInterval)
	})

	t.Run("WhenNonRetryableErrorTypesAreSet", func(t *testing.T) {
		policy := getHyperscalerRetryPolicy()
		require.NotNil(t, policy)
		expected := []string{"PanicError", "NonRetryableError", "NonRetryableErr"}
		assert.Equal(t, expected, policy.NonRetryableErrorTypes)
	})
}

func TestGetHyperscalerLroRetryPolicy(t *testing.T) {
	saveLRO := HyperscalerLRORetryInitialInterval
	saveInitial := HyperscalerRetryInitialInterval
	saveMax := HyperscalerRetryMaximumInterval
	saveAttempts := HyperscalerRetryMaximumAttempts
	saveCoeff := HyperscalerRetryBackoffCoefficient
	defer func() {
		HyperscalerLRORetryInitialInterval = saveLRO
		HyperscalerRetryInitialInterval = saveInitial
		HyperscalerRetryMaximumInterval = saveMax
		HyperscalerRetryMaximumAttempts = saveAttempts
		HyperscalerRetryBackoffCoefficient = saveCoeff
	}()

	t.Run("WhenLroInitialIntervalOverridesBase", func(t *testing.T) {
		HyperscalerRetryInitialInterval = "10s"
		HyperscalerRetryMaximumInterval = "30s"
		HyperscalerRetryMaximumAttempts = 10
		HyperscalerRetryBackoffCoefficient = 1
		HyperscalerLRORetryInitialInterval = "45s"

		policy := getHyperscalerLroRetryPolicy()
		require.NotNil(t, policy)
		assert.Equal(t, 45*time.Second, policy.InitialInterval, "LRO policy should use LRO initial interval")
		assert.Equal(t, 30*time.Second, policy.MaximumInterval)
		assert.Equal(t, int32(10), policy.MaximumAttempts)
		assert.Equal(t, []string{"PanicError", "NonRetryableError", "NonRetryableErr"}, policy.NonRetryableErrorTypes)
	})

	t.Run("WhenInvalidLroIntervalUses30sDefault", func(t *testing.T) {
		HyperscalerRetryInitialInterval = "20s"
		HyperscalerRetryMaximumInterval = "30s"
		HyperscalerRetryMaximumAttempts = 10
		HyperscalerRetryBackoffCoefficient = 1
		HyperscalerLRORetryInitialInterval = "not-valid"

		policy := getHyperscalerLroRetryPolicy()
		require.NotNil(t, policy)
		assert.Equal(t, 30*time.Second, policy.InitialInterval)
	})

	t.Run("WhenReturnsValidTemporalRetryPolicy", func(t *testing.T) {
		HyperscalerLRORetryInitialInterval = "1m"
		HyperscalerRetryInitialInterval = "10s"
		HyperscalerRetryMaximumInterval = "30s"
		HyperscalerRetryMaximumAttempts = 3
		HyperscalerRetryBackoffCoefficient = 1.5

		policy := getHyperscalerLroRetryPolicy()
		require.NotNil(t, policy)
		// LRO initial interval overrides
		assert.Equal(t, time.Minute, policy.InitialInterval)
		// Rest from base policy
		assert.Equal(t, 30*time.Second, policy.MaximumInterval)
		assert.Equal(t, int32(3), policy.MaximumAttempts)
		assert.Equal(t, 1.5, policy.BackoffCoefficient)
	})
}

// Test exported function variables call the same logic (smoke test)
func TestExportedRetryPolicyFunctions(t *testing.T) {
	// GetHeartbeatTimeoutForHyperscaler
	d := GetHeartbeatTimeoutForHyperscaler()
	assert.GreaterOrEqual(t, d, time.Duration(0))

	// GetStartToCloseTimeoutHyperscaler
	d = GetStartToCloseTimeoutHyperscaler()
	assert.GreaterOrEqual(t, d, time.Duration(0))

	// GetHyperscalerRetryPolicy
	policy := GetHyperscalerRetryPolicy()
	require.NotNil(t, policy)
	assert.NotEmpty(t, policy.NonRetryableErrorTypes)

	// GetHyperscalerLRORetryPolicy
	lroPolicy := GetHyperscalerLRORetryPolicy()
	require.NotNil(t, lroPolicy)
	assert.NotEmpty(t, lroPolicy.NonRetryableErrorTypes)
	// Ensure it's a valid Temporal retry policy (no nil deref)
	_ = policy.InitialInterval
	_ = lroPolicy.InitialInterval
}

// Ensure RetryPolicy struct is used as expected by Temporal (field presence)
func TestRetryPolicyStructure(t *testing.T) {
	policy := getHyperscalerRetryPolicy()
	require.NotNil(t, policy)
	// temporal.RetryPolicy expected fields
	var _ *temporal.RetryPolicy = policy
	assert.NotNil(t, policy.NonRetryableErrorTypes)
	assert.Len(t, policy.NonRetryableErrorTypes, 3)
}
