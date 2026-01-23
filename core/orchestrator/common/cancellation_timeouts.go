package common

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

var (
	// DefaultCancellationAckTimeout is the default timeout in minutes for waiting for cancellation acknowledgment
	DefaultCancellationAckTimeout = env.GetUint64("WORKFLOW_CANCELLATION_TIMEOUT", 5) // minutes
	// DefaultForceTerminationAckTimeout is the default timeout in seconds for waiting for force termination acknowledgment
	DefaultForceTerminationAckTimeout = env.GetUint64("WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT", 30) // seconds

	// Resource-specific cancellation ack timeouts (in minutes)
	volumeWorkflowCancellationAckTimeout          = env.GetUint64("VOLUME_WORKFLOW_CANCELLATION_TIMEOUT", 5)
	activeDirectoryWorkflowCancellationAckTimeout = env.GetUint64("ACTIVE_DIRECTORY_WORKFLOW_CANCELLATION_TIMEOUT", 5)
	poolWorkflowCancellationAckTimeout            = env.GetUint64("POOL_WORKFLOW_CANCELLATION_TIMEOUT", 5)
	quotaRuleWorkflowCancellationAckTimeout       = env.GetUint64("QUOTA_RULE_WORKFLOW_CANCELLATION_TIMEOUT", 5)
	snapshotWorkflowCancellationAckTimeout        = env.GetUint64("SNAPSHOT_WORKFLOW_CANCELLATION_TIMEOUT", 5)
	kmsConfigWorkflowCancellationAckTimeout       = env.GetUint64("KMS_CONFIG_WORKFLOW_CANCELLATION_TIMEOUT", 5)
	flexcacheWorkflowCancellationAckTimeout       = env.GetUint64("FLEXCACHE_WORKFLOW_CANCELLATION_TIMEOUT", 5)
	replicationWorkflowCancellationAckTimeout     = env.GetUint64("REPLICATION_WORKFLOW_CANCELLATION_TIMEOUT", 5)

	// Resource-specific force termination ack timeouts (in seconds)
	volumeForceTerminationAckTimeout          = env.GetUint64("VOLUME_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT", 30)
	activeDirectoryForceTerminationAckTimeout = env.GetUint64("ACTIVE_DIRECTORY_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT", 30)
	poolForceTerminationAckTimeout            = env.GetUint64("POOL_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT", 30)
	quotaRuleForceTerminationAckTimeout       = env.GetUint64("QUOTA_RULE_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT", 30)
	snapshotForceTerminationAckTimeout        = env.GetUint64("SNAPSHOT_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT", 30)
	kmsConfigForceTerminationAckTimeout       = env.GetUint64("KMS_CONFIG_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT", 30)
	flexcacheForceTerminationAckTimeout       = env.GetUint64("FLEXCACHE_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT", 30)
	replicationForceTerminationAckTimeout     = env.GetUint64("REPLICATION_WORKFLOW_FORCE_CANCEL_WAIT_TIMEOUT", 30)
)

// GetCancellationTimeouts returns the cancellation timeouts for a given resource type.
func GetCancellationTimeouts(resourceType string) (ackTimeout time.Duration, forceTimeout time.Duration) {
	var ackTimeoutMinutes uint64
	var forceTimeoutSeconds uint64

	switch resourceType {
	case "VOLUME":
		ackTimeoutMinutes = volumeWorkflowCancellationAckTimeout
		forceTimeoutSeconds = volumeForceTerminationAckTimeout
	case "ACTIVE_DIRECTORY":
		ackTimeoutMinutes = activeDirectoryWorkflowCancellationAckTimeout
		forceTimeoutSeconds = activeDirectoryForceTerminationAckTimeout
	case "POOL":
		ackTimeoutMinutes = poolWorkflowCancellationAckTimeout
		forceTimeoutSeconds = poolForceTerminationAckTimeout
	case "QUOTA_RULE":
		ackTimeoutMinutes = quotaRuleWorkflowCancellationAckTimeout
		forceTimeoutSeconds = quotaRuleForceTerminationAckTimeout
	case "SNAPSHOT":
		ackTimeoutMinutes = snapshotWorkflowCancellationAckTimeout
		forceTimeoutSeconds = snapshotForceTerminationAckTimeout
	case "KMS_CONFIG":
		ackTimeoutMinutes = kmsConfigWorkflowCancellationAckTimeout
		forceTimeoutSeconds = kmsConfigForceTerminationAckTimeout
	case "FLEXCACHE":
		ackTimeoutMinutes = flexcacheWorkflowCancellationAckTimeout
		forceTimeoutSeconds = flexcacheForceTerminationAckTimeout
	case "REPLICATION":
		ackTimeoutMinutes = replicationWorkflowCancellationAckTimeout
		forceTimeoutSeconds = replicationForceTerminationAckTimeout
	default:
		// Fallback to VOLUME defaults for unknown resource types
		ackTimeoutMinutes = DefaultCancellationAckTimeout
		forceTimeoutSeconds = DefaultForceTerminationAckTimeout
	}

	return time.Duration(ackTimeoutMinutes) * time.Minute, time.Duration(forceTimeoutSeconds) * time.Second
}
