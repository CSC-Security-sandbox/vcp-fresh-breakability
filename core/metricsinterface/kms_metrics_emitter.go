// Package metricsinterface defines interfaces for metrics emission.
// This package provides abstraction for metrics to maintain architectural boundaries
// between core and worker modules. The core module defines interfaces here,
// and worker provides concrete implementations.
package metricsinterface

// KmsMetricsEmitter defines the interface for emitting KMS-related metrics.
// This interface allows the core module to remain hyperscaler-agnostic by
// abstracting away the concrete metrics implementation which lives in worker/metrics.
type KmsMetricsEmitter interface {
	// EmitKmsKeyLimitReached emits a metric when KMS key rotation is blocked
	// due to reaching the key limit (either pending_deletion or total_keys)
	EmitKmsKeyLimitReached(kmsConfigUUID, limitType string)

	// EmitKmsRotationFailure emits a metric when KMS key rotation fails
	EmitKmsRotationFailure(kmsConfigUUID, serviceAccountEmail, failureType string)
}

// NoOpKmsMetricsEmitter is a no-op implementation of KmsMetricsEmitter
// used in tests and scenarios where metrics are not required.
type NoOpKmsMetricsEmitter struct{}

func (n *NoOpKmsMetricsEmitter) EmitKmsKeyLimitReached(kmsConfigUUID, limitType string) {}

func (n *NoOpKmsMetricsEmitter) EmitKmsRotationFailure(kmsConfigUUID, serviceAccountEmail, failureType string) {
}
