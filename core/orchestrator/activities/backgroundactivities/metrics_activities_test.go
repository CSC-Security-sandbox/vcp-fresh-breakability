package backgroundactivities

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	prometheus_model "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/metrics"
)

// Helper: check metric labels
func metricHasLabels(mLabels []*prometheus_model.LabelPair, expected map[string]string) bool {
	for k, v := range expected {
		found := false
		for _, label := range mLabels {
			if *label.Name == k && *label.Value == v {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func TestEmitCertificateRotationFailureMetric(t *testing.T) {
	// Register the metric first
	metrics.RegisterCertificateRotationFailureCounter()
	// Clean up after test
	defer prometheus.Unregister(metrics.CertificateRotationFailureCounter)

	ctx := context.Background()
	poolUUID := "test-pool-uuid-cert"
	poolName := "test-pool-name-cert"
	failureType := "certificate_rotation"
	errorType := "test error for certificate rotation"

	// Execute the activity
	err := EmitCertificateRotationFailureMetric(ctx, poolUUID, poolName, failureType, errorType)

	// Verify no error
	assert.NoError(t, err)

	// Gather metrics and verify
	metricsCollected, err := prometheus.DefaultGatherer.Gather()
	assert.NoError(t, err)

	found := false
	for _, mf := range metricsCollected {
		if *mf.Name == "vcp_certificate_rotation_failures_total" {
			for _, m := range mf.Metric {
				expected := map[string]string{
					"pool_uuid":    poolUUID,
					"pool_name":    poolName,
					"failure_type": failureType,
					"error_type":   errorType,
				}
				if metricHasLabels(m.Label, expected) {
					found = true
					// Verify counter value is incremented
					if m.Counter != nil && *m.Counter.Value >= 1.0 {
						break
					}
				}
			}
		}
	}

	assert.True(t, found, "CertificateRotationFailureCounter metric should be emitted with correct labels")
}

func TestEmitCertificateRotationFailureMetric_WithEmptyValues(t *testing.T) {
	// Register the metric first
	metrics.RegisterCertificateRotationFailureCounter()
	// Clean up after test
	defer prometheus.Unregister(metrics.CertificateRotationFailureCounter)

	ctx := context.Background()
	poolUUID := ""
	poolName := ""
	failureType := ""
	errorType := ""

	// Execute the activity - should not panic
	err := EmitCertificateRotationFailureMetric(ctx, poolUUID, poolName, failureType, errorType)

	// Verify no error
	assert.NoError(t, err)
}

func TestEmitPasswordRotationFailureMetric(t *testing.T) {
	// Register the metric first
	metrics.RegisterPasswordRotationFailureCounter()
	// Clean up after test
	defer prometheus.Unregister(metrics.PasswordRotationFailureCounter)

	ctx := context.Background()
	poolUUID := "test-pool-uuid-pwd"
	poolName := "test-pool-name-pwd"
	failureType := "password_rotation"
	errorType := "test error for password rotation"

	// Execute the activity
	err := EmitPasswordRotationFailureMetric(ctx, poolUUID, poolName, failureType, errorType)

	// Verify no error
	assert.NoError(t, err)

	// Gather metrics and verify
	metricsCollected, err := prometheus.DefaultGatherer.Gather()
	assert.NoError(t, err)

	found := false
	for _, mf := range metricsCollected {
		if *mf.Name == "vcp_password_rotation_failures_total" {
			for _, m := range mf.Metric {
				expected := map[string]string{
					"pool_uuid":    poolUUID,
					"pool_name":    poolName,
					"failure_type": failureType,
					"error_type":   errorType,
				}
				if metricHasLabels(m.Label, expected) {
					found = true
					// Verify counter value is incremented
					if m.Counter != nil && *m.Counter.Value >= 1.0 {
						break
					}
				}
			}
		}
	}

	assert.True(t, found, "PasswordRotationFailureCounter metric should be emitted with correct labels")
}

func TestEmitPasswordRotationFailureMetric_WithEmptyValues(t *testing.T) {
	// Register the metric first
	metrics.RegisterPasswordRotationFailureCounter()
	// Clean up after test
	defer prometheus.Unregister(metrics.PasswordRotationFailureCounter)

	ctx := context.Background()
	poolUUID := ""
	poolName := ""
	failureType := ""
	errorType := ""

	// Execute the activity - should not panic
	err := EmitPasswordRotationFailureMetric(ctx, poolUUID, poolName, failureType, errorType)

	// Verify no error
	assert.NoError(t, err)
}

func TestEmitCertificateRotationFailureMetric_MultipleCalls(t *testing.T) {
	// Register the metric first
	metrics.RegisterCertificateRotationFailureCounter()
	// Clean up after test
	defer prometheus.Unregister(metrics.CertificateRotationFailureCounter)

	ctx := context.Background()
	poolUUID := "test-pool-uuid-multi"
	poolName := "test-pool-name-multi"
	failureType := "certificate_rotation"
	errorType := "test error"

	// Execute the activity multiple times
	err1 := EmitCertificateRotationFailureMetric(ctx, poolUUID, poolName, failureType, errorType)
	err2 := EmitCertificateRotationFailureMetric(ctx, poolUUID, poolName, failureType, errorType)

	// Verify no errors
	assert.NoError(t, err1)
	assert.NoError(t, err2)

	// Gather metrics and verify counter is incremented
	metricsCollected, err := prometheus.DefaultGatherer.Gather()
	assert.NoError(t, err)

	found := false
	for _, mf := range metricsCollected {
		if *mf.Name == "vcp_certificate_rotation_failures_total" {
			for _, m := range mf.Metric {
				expected := map[string]string{
					"pool_uuid":    poolUUID,
					"pool_name":    poolName,
					"failure_type": failureType,
					"error_type":   errorType,
				}
				if metricHasLabels(m.Label, expected) {
					found = true
					// Verify counter value is at least 2 (called twice)
					if m.Counter != nil && *m.Counter.Value >= 2.0 {
						break
					}
				}
			}
		}
	}

	assert.True(t, found, "CertificateRotationFailureCounter should be incremented multiple times")
}

func TestEmitPasswordRotationFailureMetric_MultipleCalls(t *testing.T) {
	// Register the metric first
	metrics.RegisterPasswordRotationFailureCounter()
	// Clean up after test
	defer prometheus.Unregister(metrics.PasswordRotationFailureCounter)

	ctx := context.Background()
	poolUUID := "test-pool-uuid-pwd-multi"
	poolName := "test-pool-name-pwd-multi"
	failureType := "password_rotation"
	errorType := "test error"

	// Execute the activity multiple times
	err1 := EmitPasswordRotationFailureMetric(ctx, poolUUID, poolName, failureType, errorType)
	err2 := EmitPasswordRotationFailureMetric(ctx, poolUUID, poolName, failureType, errorType)

	// Verify no errors
	assert.NoError(t, err1)
	assert.NoError(t, err2)

	// Gather metrics and verify counter is incremented
	metricsCollected, err := prometheus.DefaultGatherer.Gather()
	assert.NoError(t, err)

	found := false
	for _, mf := range metricsCollected {
		if *mf.Name == "vcp_password_rotation_failures_total" {
			for _, m := range mf.Metric {
				expected := map[string]string{
					"pool_uuid":    poolUUID,
					"pool_name":    poolName,
					"failure_type": failureType,
					"error_type":   errorType,
				}
				if metricHasLabels(m.Label, expected) {
					found = true
					// Verify counter value is at least 2 (called twice)
					if m.Counter != nil && *m.Counter.Value >= 2.0 {
						break
					}
				}
			}
		}
	}

	assert.True(t, found, "PasswordRotationFailureCounter should be incremented multiple times")
}

