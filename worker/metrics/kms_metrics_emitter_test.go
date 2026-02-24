package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/metricsinterface"
)

func TestNewPrometheusKmsMetricsEmitter(t *testing.T) {
	emitter := NewPrometheusKmsMetricsEmitter()
	assert.NotNil(t, emitter)

	// Verify it implements the KmsMetricsEmitter interface
	var _ metricsinterface.KmsMetricsEmitter = emitter
}

func TestPrometheusKmsMetricsEmitter_EmitKmsKeyLimitReached(t *testing.T) {
	// Ensure counter is registered
	prometheus.Unregister(KmsKeyLimitReachedCounter)
	RegisterKmsKeyLimitReachedCounter()

	emitter := NewPrometheusKmsMetricsEmitter()
	// Should not panic
	emitter.EmitKmsKeyLimitReached("test-kms-uuid", "pending_deletion")
}

func TestPrometheusKmsMetricsEmitter_EmitKmsRotationFailure(t *testing.T) {
	// Ensure counter is registered
	prometheus.Unregister(KmsRotationFailureCounter)
	RegisterKmsRotationFailureCounter()

	emitter := NewPrometheusKmsMetricsEmitter()
	// Should not panic
	emitter.EmitKmsRotationFailure("test-kms-uuid", "test@project.iam.gserviceaccount.com", "pool_migration")
}
