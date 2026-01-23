package common

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetCancellationTimeouts_NoResourceSpecificEnvVars_UsesDefaults(t *testing.T) {
	// Test for lines 19-21, 24-26, 29: When resource-specific env vars are not set, use defaults
	resourceType := "VOLUME"

	// Save original env vars
	originalAckTimeout := os.Getenv(resourceType + "_WORKFLOW_CANCELLATION_TIMEOUT")
	originalForceTimeout := os.Getenv(resourceType + "_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT")
	defer func() {
		if originalAckTimeout != "" {
			_ = os.Setenv(resourceType+"_WORKFLOW_CANCELLATION_TIMEOUT", originalAckTimeout)
		} else {
			_ = os.Unsetenv(resourceType + "_WORKFLOW_CANCELLATION_TIMEOUT")
		}
		if originalForceTimeout != "" {
			_ = os.Setenv(resourceType+"_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT", originalForceTimeout)
		} else {
			_ = os.Unsetenv(resourceType + "_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT")
		}
	}()

	// Unset resource-specific env vars to trigger default path
	_ = os.Unsetenv(resourceType + "_WORKFLOW_CANCELLATION_TIMEOUT")
	_ = os.Unsetenv(resourceType + "_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT")

	ackTimeout, forceTimeout := GetCancellationTimeouts(resourceType)

	// Should use default values (5 minutes and 30 seconds)
	assert.Equal(t, 5*time.Minute, ackTimeout)
	assert.Equal(t, 30*time.Second, forceTimeout)
}

func TestGetCancellationTimeouts_ACTIVE_DIRECTORY(t *testing.T) {
	// Test for lines 46-47: ACTIVE_DIRECTORY resource type
	resourceType := "ACTIVE_DIRECTORY"

	// Save original env vars
	originalAckTimeout := os.Getenv("ACTIVE_DIRECTORY_WORKFLOW_CANCELLATION_TIMEOUT")
	originalForceTimeout := os.Getenv("ACTIVE_DIRECTORY_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT")
	defer func() {
		if originalAckTimeout != "" {
			_ = os.Setenv("ACTIVE_DIRECTORY_WORKFLOW_CANCELLATION_TIMEOUT", originalAckTimeout)
		} else {
			_ = os.Unsetenv("ACTIVE_DIRECTORY_WORKFLOW_CANCELLATION_TIMEOUT")
		}
		if originalForceTimeout != "" {
			_ = os.Setenv("ACTIVE_DIRECTORY_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT", originalForceTimeout)
		} else {
			_ = os.Unsetenv("ACTIVE_DIRECTORY_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT")
		}
	}()

	// Note: Environment variables are read at package initialization time, so setting them here
	// won't affect the already-initialized package variables. The test verifies the default behavior.
	// To test custom values, the env vars would need to be set before the package is imported.
	ackTimeout, forceTimeout := GetCancellationTimeouts(resourceType)

	// The actual values are read at package init, so we expect the defaults
	assert.Equal(t, 5*time.Minute, ackTimeout)
	assert.Equal(t, 30*time.Second, forceTimeout)
}

func TestGetCancellationTimeouts_POOL(t *testing.T) {
	// Test for lines 49-50: POOL resource type
	resourceType := "POOL"

	// Save original env vars
	originalAckTimeout := os.Getenv("POOL_WORKFLOW_CANCELLATION_TIMEOUT")
	originalForceTimeout := os.Getenv("POOL_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT")
	defer func() {
		if originalAckTimeout != "" {
			_ = os.Setenv("POOL_WORKFLOW_CANCELLATION_TIMEOUT", originalAckTimeout)
		} else {
			_ = os.Unsetenv("POOL_WORKFLOW_CANCELLATION_TIMEOUT")
		}
		if originalForceTimeout != "" {
			_ = os.Setenv("POOL_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT", originalForceTimeout)
		} else {
			_ = os.Unsetenv("POOL_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT")
		}
	}()

	// Note: Environment variables are read at package initialization time, so setting them here
	// won't affect the already-initialized package variables. The test verifies the default behavior.
	ackTimeout, forceTimeout := GetCancellationTimeouts(resourceType)

	// The actual values are read at package init, so we expect the defaults
	assert.Equal(t, 5*time.Minute, ackTimeout)
	assert.Equal(t, 30*time.Second, forceTimeout)
}

func TestGetCancellationTimeouts_QUOTA_RULE(t *testing.T) {
	// Test for lines 52-53: QUOTA_RULE resource type
	resourceType := "QUOTA_RULE"

	// Save original env vars
	originalAckTimeout := os.Getenv("QUOTA_RULE_WORKFLOW_CANCELLATION_TIMEOUT")
	originalForceTimeout := os.Getenv("QUOTA_RULE_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT")
	defer func() {
		if originalAckTimeout != "" {
			_ = os.Setenv("QUOTA_RULE_WORKFLOW_CANCELLATION_TIMEOUT", originalAckTimeout)
		} else {
			_ = os.Unsetenv("QUOTA_RULE_WORKFLOW_CANCELLATION_TIMEOUT")
		}
		if originalForceTimeout != "" {
			_ = os.Setenv("QUOTA_RULE_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT", originalForceTimeout)
		} else {
			_ = os.Unsetenv("QUOTA_RULE_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT")
		}
	}()

	// Note: Environment variables are read at package initialization time, so setting them here
	// won't affect the already-initialized package variables. The test verifies the default behavior.
	ackTimeout, forceTimeout := GetCancellationTimeouts(resourceType)

	// The actual values are read at package init, so we expect the defaults
	assert.Equal(t, 5*time.Minute, ackTimeout)
	assert.Equal(t, 30*time.Second, forceTimeout)
}

func TestGetCancellationTimeouts_SNAPSHOT(t *testing.T) {
	// Test for lines 55-56: SNAPSHOT resource type
	resourceType := "SNAPSHOT"

	// Save original env vars
	originalAckTimeout := os.Getenv("SNAPSHOT_WORKFLOW_CANCELLATION_TIMEOUT")
	originalForceTimeout := os.Getenv("SNAPSHOT_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT")
	defer func() {
		if originalAckTimeout != "" {
			_ = os.Setenv("SNAPSHOT_WORKFLOW_CANCELLATION_TIMEOUT", originalAckTimeout)
		} else {
			_ = os.Unsetenv("SNAPSHOT_WORKFLOW_CANCELLATION_TIMEOUT")
		}
		if originalForceTimeout != "" {
			_ = os.Setenv("SNAPSHOT_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT", originalForceTimeout)
		} else {
			_ = os.Unsetenv("SNAPSHOT_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT")
		}
	}()

	// Note: Environment variables are read at package initialization time, so setting them here
	// won't affect the already-initialized package variables. The test verifies the default behavior.
	ackTimeout, forceTimeout := GetCancellationTimeouts(resourceType)

	// The actual values are read at package init, so we expect the defaults
	assert.Equal(t, 5*time.Minute, ackTimeout)
	assert.Equal(t, 30*time.Second, forceTimeout)
}

func TestGetCancellationTimeouts_KMS_CONFIG(t *testing.T) {
	// Test for lines 58-59: KMS_CONFIG resource type
	resourceType := "KMS_CONFIG"

	// Save original env vars
	originalAckTimeout := os.Getenv("KMS_CONFIG_WORKFLOW_CANCELLATION_TIMEOUT")
	originalForceTimeout := os.Getenv("KMS_CONFIG_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT")
	defer func() {
		if originalAckTimeout != "" {
			_ = os.Setenv("KMS_CONFIG_WORKFLOW_CANCELLATION_TIMEOUT", originalAckTimeout)
		} else {
			_ = os.Unsetenv("KMS_CONFIG_WORKFLOW_CANCELLATION_TIMEOUT")
		}
		if originalForceTimeout != "" {
			_ = os.Setenv("KMS_CONFIG_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT", originalForceTimeout)
		} else {
			_ = os.Unsetenv("KMS_CONFIG_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT")
		}
	}()

	// Note: Environment variables are read at package initialization time, so setting them here
	// won't affect the already-initialized package variables. The test verifies the default behavior.
	ackTimeout, forceTimeout := GetCancellationTimeouts(resourceType)

	// The actual values are read at package init, so we expect the defaults
	assert.Equal(t, 5*time.Minute, ackTimeout)
	assert.Equal(t, 30*time.Second, forceTimeout)
}

func TestGetCancellationTimeouts_FLEXCACHE(t *testing.T) {
	// Test for lines 61-62: FLEXCACHE resource type
	resourceType := "FLEXCACHE"

	// Save original env vars
	originalAckTimeout := os.Getenv("FLEXCACHE_WORKFLOW_CANCELLATION_TIMEOUT")
	originalForceTimeout := os.Getenv("FLEXCACHE_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT")
	defer func() {
		if originalAckTimeout != "" {
			_ = os.Setenv("FLEXCACHE_WORKFLOW_CANCELLATION_TIMEOUT", originalAckTimeout)
		} else {
			_ = os.Unsetenv("FLEXCACHE_WORKFLOW_CANCELLATION_TIMEOUT")
		}
		if originalForceTimeout != "" {
			_ = os.Setenv("FLEXCACHE_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT", originalForceTimeout)
		} else {
			_ = os.Unsetenv("FLEXCACHE_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT")
		}
	}()

	// Note: Environment variables are read at package initialization time, so setting them here
	// won't affect the already-initialized package variables. The test verifies the default behavior.
	ackTimeout, forceTimeout := GetCancellationTimeouts(resourceType)

	// The actual values are read at package init, so we expect the defaults
	assert.Equal(t, 5*time.Minute, ackTimeout)
	assert.Equal(t, 30*time.Second, forceTimeout)
}

func TestGetCancellationTimeouts_REPLICATION(t *testing.T) {
	// Test for lines 64-65: REPLICATION resource type
	resourceType := "REPLICATION"

	// Save original env vars
	originalAckTimeout := os.Getenv("REPLICATION_WORKFLOW_CANCELLATION_TIMEOUT")
	originalForceTimeout := os.Getenv("REPLICATION_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT")
	defer func() {
		if originalAckTimeout != "" {
			_ = os.Setenv("REPLICATION_WORKFLOW_CANCELLATION_TIMEOUT", originalAckTimeout)
		} else {
			_ = os.Unsetenv("REPLICATION_WORKFLOW_CANCELLATION_TIMEOUT")
		}
		if originalForceTimeout != "" {
			_ = os.Setenv("REPLICATION_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT", originalForceTimeout)
		} else {
			_ = os.Unsetenv("REPLICATION_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT")
		}
	}()

	// Note: Environment variables are read at package initialization time, so setting them here
	// won't affect the already-initialized package variables. The test verifies the default behavior.
	ackTimeout, forceTimeout := GetCancellationTimeouts(resourceType)

	// The actual values are read at package init, so we expect the defaults
	assert.Equal(t, 5*time.Minute, ackTimeout)
	assert.Equal(t, 30*time.Second, forceTimeout)
}

func TestGetCancellationTimeouts_UnknownResourceType_UsesDefaults(t *testing.T) {
	// Test for lines 68-69: Unknown resource type falls back to defaults
	resourceType := "UNKNOWN_RESOURCE_TYPE"

	ackTimeout, forceTimeout := GetCancellationTimeouts(resourceType)

	// Should use default values (5 minutes and 30 seconds)
	assert.Equal(t, 5*time.Minute, ackTimeout)
	assert.Equal(t, 30*time.Second, forceTimeout)
}
