package metrics

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/metricsinterface"
)

// Ensure PrometheusKmsMetricsEmitter implements the interface at compile time
var _ metricsinterface.KmsMetricsEmitter = (*PrometheusKmsMetricsEmitter)(nil)

// PrometheusKmsMetricsEmitter is the concrete implementation of KmsMetricsEmitter
// that emits metrics to Prometheus. This implementation lives in worker/metrics
// to maintain the architectural boundary between core and worker modules.
type PrometheusKmsMetricsEmitter struct{}

// NewPrometheusKmsMetricsEmitter creates a new instance of PrometheusKmsMetricsEmitter
func NewPrometheusKmsMetricsEmitter() *PrometheusKmsMetricsEmitter {
	return &PrometheusKmsMetricsEmitter{}
}

// EmitKmsKeyLimitReached emits a metric when KMS key rotation is blocked due to key limit
func (p *PrometheusKmsMetricsEmitter) EmitKmsKeyLimitReached(kmsConfigUUID, limitType string) {
	EmitKmsKeyLimitReached(kmsConfigUUID, limitType)
}

// EmitKmsRotationFailure emits a metric when KMS key rotation fails
func (p *PrometheusKmsMetricsEmitter) EmitKmsRotationFailure(kmsConfigUUID, serviceAccountEmail, failureType string) {
	EmitKmsRotationFailure(kmsConfigUUID, serviceAccountEmail, failureType)
}
