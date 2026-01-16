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

