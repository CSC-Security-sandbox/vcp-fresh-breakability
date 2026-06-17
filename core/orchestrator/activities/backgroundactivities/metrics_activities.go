package backgroundactivities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/metrics"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// EmitCertificateRotationFailureMetric emits a Prometheus metric for certificate rotation failures
func EmitCertificateRotationFailureMetric(ctx context.Context, poolUUID, poolName, failureType, errorType string) error {
	logger := util.GetLogger(ctx)
	logger.Debugf("Emitting certificate rotation failure metric: poolUUID=%s, poolName=%s, failureType=%s, errorType=%s", poolUUID, poolName, failureType, errorType)
	metrics.EmitCertificateRotationFailure(poolUUID, poolName, failureType, errorType)
	return nil
}

// EmitPasswordRotationFailureMetric emits a Prometheus metric for password rotation failures
func EmitPasswordRotationFailureMetric(ctx context.Context, poolUUID, poolName, failureType, errorType string) error {
	logger := util.GetLogger(ctx)
	logger.Debugf("Emitting password rotation failure metric: poolUUID=%s, poolName=%s, failureType=%s, errorType=%s", poolUUID, poolName, failureType, errorType)
	metrics.EmitPasswordRotationFailure(poolUUID, poolName, failureType, errorType)
	return nil
}

// EmitZoneSwitchWorkflowMetric emits a Prometheus metric for zone switch workflow outcomes.
func EmitZoneSwitchWorkflowMetric(ctx context.Context, poolUUID, poolName, action, status, failureStep string) error {
	logger := util.GetLogger(ctx)
	logger.Debugf("Emitting zone switch workflow metric: poolUUID=%s, poolName=%s, action=%s, status=%s, failureStep=%s",
		poolUUID, poolName, action, status, failureStep)
	metrics.EmitZoneSwitchWorkflowMetric(poolUUID, poolName, action, status, failureStep)
	return nil
}

// EmitKmsKeyLimitReachedMetric emits a Prometheus metric when KMS key rotation is blocked due to key limit
func EmitKmsKeyLimitReachedMetric(ctx context.Context, kmsConfigUUID, limitType string) error {
	logger := util.GetLogger(ctx)
	logger.Debugf("Emitting KMS key limit reached metric: kmsConfigUUID=%s, limitType=%s", kmsConfigUUID, limitType)
	metrics.EmitKmsKeyLimitReached(kmsConfigUUID, limitType)
	return nil
}

// EmitKmsRotationFailureMetric emits a Prometheus metric when KMS key rotation fails for a KMS config
func EmitKmsRotationFailureMetric(ctx context.Context, kmsConfigUUID, serviceAccountEmail, failureType string) error {
	logger := util.GetLogger(ctx)
	logger.Debugf("Emitting KMS rotation failure metric: kmsConfigUUID=%s, serviceAccountEmail=%s, failureType=%s", kmsConfigUUID, serviceAccountEmail, failureType)
	metrics.EmitKmsRotationFailure(kmsConfigUUID, serviceAccountEmail, failureType)
	return nil
}
