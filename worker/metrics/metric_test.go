package metrics

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	prometheus_model "github.com/prometheus/client_model/go"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
)

// Helper: common test volumes
func getTestVolumes() []*datamodel.Volume {
	return []*datamodel.Volume{
		{Name: "volume1", State: "active", AccountID: 123, Account: &datamodel.Account{Name: "account1"}, AutoTieringEnabled: true, LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{LargeCapacity: true}, SnapshotPolicy: &datamodel.SnapshotPolicy{IsEnabled: true}},
		{Name: "volume2", State: "inactive", AccountID: 456, Account: &datamodel.Account{Name: "account2"}, AutoTieringEnabled: true, LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{LargeCapacity: true}},
		{Name: "volume3", State: "active", AccountID: 789, Account: nil, LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{LargeCapacity: true}, SnapshotPolicy: &datamodel.SnapshotPolicy{IsEnabled: true}},
		{Name: "volE", State: "READY"},
		{Name: "volF", State: "READY"},
	}
}

// Helper: expected keys for each metric type
func getExpectedKeys(metric string) []string {
	switch metric {
	case "autotier":
		return []string{"volume1_active_account1", "volume2_inactive_account2"}
	case "crr":
		return []string{"volume1_active_account1", "volume3_active_"}
	case "largevolume":
		return []string{"volume1_active_account1", "volume2_inactive_account2", "volume3_active_"}
	case "cbs":
		return []string{}
	case "eligibility":
		return []string{"volE_READY", "volF_READY"}
	default:
		return nil
	}
}

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

func TestIncJobStatusCounter(t *testing.T) {
	ctx := context.Background()
	IncJobStatusCounter(ctx, "errorDetails", "state")
	metric, err := JobStatusCounter.GetMetricWithLabelValues("test_project_id", "errorDetails", "state")
	if err != nil {
		t.Errorf("Failed to get metric: %v", err)
	}
	if metric == nil {
		t.Error("Metric not found")
	}
}

func TestEmitAutoTierEnabledMetric(t *testing.T) {
	RegisterAutoTierEnabledGauge()
	volumes := getTestVolumes()
	EmitAutoTierEnabledMetric(volumes)
	metrics, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Errorf("Failed to gather metrics: %v", err)
	}
	expectedKeys := getExpectedKeys("autotier")
	for _, key := range expectedKeys {
		found := false
		for _, mf := range metrics {
			if *mf.Name == "gcnv_vsa_autotier_volume" {
				for _, m := range mf.Metric {
					expectedLabelValues := strings.Split(key, "_")
					expected := map[string]string{
						"name":         expectedLabelValues[0],
						"state":        expectedLabelValues[1],
						"account_name": expectedLabelValues[2],
					}
					if metricHasLabels(m.Label, expected) {
						found = true
						break
					}
				}
			}
		}
		if !found {
			t.Errorf("AutoTierEnabledMetric gauge not found for key %s", key)
		}
	}
}

func TestEmitCRREnabledMetric(t *testing.T) {
	RegisterCRREnabledGauge()
	volumes := getTestVolumes()
	EmitCRREnabledMetric(volumes)
	metrics, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Errorf("Failed to gather metrics: %v", err)
	}
	expectedKeys := getExpectedKeys("crr")
	for _, key := range expectedKeys {
		found := false
		for _, mf := range metrics {
			if *mf.Name == "gcnv_vsa_crr_volume" {
				for _, m := range mf.Metric {
					expectedLabelValues := strings.Split(key, "_")
					expected := map[string]string{
						"name":         expectedLabelValues[0],
						"state":        expectedLabelValues[1],
						"account_name": expectedLabelValues[2],
					}
					if metricHasLabels(m.Label, expected) {
						found = true
						break
					}
				}
			}
		}
		if !found {
			t.Errorf("CRREnabledMetric gauge not found for key %s", key)
		}
	}
}

func TestEmitLargeVolumeEnabledMetric(t *testing.T) {
	RegisterLargeVolumeEnabledGauge()
	volumes := getTestVolumes()
	EmitLargeVolumeEnabledMetric(volumes)
	metrics, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Errorf("Failed to gather metrics: %v", err)
	}
	expectedKeys := getExpectedKeys("largevolume")
	for _, key := range expectedKeys {
		found := false
		for _, mf := range metrics {
			if *mf.Name == "gcnv_vsa_large_volume" {
				for _, m := range mf.Metric {
					expectedLabelValues := strings.Split(key, "_")
					expected := map[string]string{
						"name":         expectedLabelValues[0],
						"state":        expectedLabelValues[1],
						"account_name": expectedLabelValues[2],
					}
					if metricHasLabels(m.Label, expected) {
						found = true
						break
					}
				}
			}
		}
		if !found {
			t.Errorf("LargeVolumeEnabledMetric gauge not found for key %s", key)
		}
	}
}

func TestEmitEligibilityStringMetric(t *testing.T) {
	RegisterEligibilityStringGauge()
	volumes := getTestVolumes()
	EmitEligibilityStringMetric(volumes)
	metrics, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Errorf("Failed to gather metrics: %v", err)
	}
	expectedKeys := getExpectedKeys("eligibility")
	for _, key := range expectedKeys {
		found := false
		for _, mf := range metrics {
			if *mf.Name == "gcnv_volumes_eligibility" {
				for _, m := range mf.Metric {
					expectedLabelValues := strings.Split(key, "_")
					expected := map[string]string{
						"name":  expectedLabelValues[0],
						"state": expectedLabelValues[1],
					}
					if metricHasLabels(m.Label, expected) {
						found = true
						break
					}
				}
			}
		}
		if !found {
			t.Errorf("EligibilityStringMetric gauge not found for key %s", key)
		}
	}
}

func TestEmitCBSEnabledMetric(t *testing.T) {
	RegisterCBSEnabledGauge()
	volumes := getTestVolumes()
	EmitCBSEnabledMetric(volumes)
	metrics, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Errorf("Failed to gather metrics: %v", err)
	}
	// You need to define expectedKeys for CBS, similar to getExpectedKeys("crr")
	expectedKeys := getExpectedKeys("cbs")
	for _, key := range expectedKeys {
		found := false
		for _, mf := range metrics {
			if *mf.Name == "gcnv_vsa_cbs_volume" {
				for _, m := range mf.Metric {
					expectedLabelValues := strings.Split(key, "_")
					expected := map[string]string{
						"name":         expectedLabelValues[0],
						"state":        expectedLabelValues[1],
						"account_name": expectedLabelValues[2],
					}
					if metricHasLabels(m.Label, expected) {
						found = true
						break
					}
				}
			}
		}
		if !found {
			t.Errorf("CBSEnabledMetric gauge not found for key %s", key)
		}
	}
}

func TestRegisterAutoTierEnabledGauge(t *testing.T) {
	// Unregister first to ensure clean state
	prometheus.Unregister(autoTierEnabledGauge)

	// Should register without error
	RegisterAutoTierEnabledGauge()
	// Register again to trigger AlreadyRegisteredError
	RegisterAutoTierEnabledGauge()

	// Check that the collector is still a GaugeVec
	if autoTierEnabledGauge == nil {
		t.Error("autoTierEnabledGauge is nil after registration")
	}
}

func TestRegisterLargeVolumeEnabledGauge(t *testing.T) {
	// Unregister first to ensure clean state
	prometheus.Unregister(largeVolumeEnabledGauge)

	// Should register without error
	RegisterLargeVolumeEnabledGauge()
	// Register again to trigger AlreadyRegisteredError
	RegisterLargeVolumeEnabledGauge()

	// Check that the collector is still a GaugeVec
	if largeVolumeEnabledGauge == nil {
		t.Error("largeVolumeEnabledGauge is nil after registration")
	}
}

func TestRegisterCRREnabledGauge(t *testing.T) {
	// Unregister first to ensure clean state
	prometheus.Unregister(crrEnabledGauge)

	// Should register without error
	RegisterCRREnabledGauge()
	// Register again to trigger AlreadyRegisteredError
	RegisterCRREnabledGauge()

	// Check that the collector is still a GaugeVec
	if crrEnabledGauge == nil {
		t.Error("crrEnabledGauge is nil after registration")
	}
}

func TestRegisterCBSEnabledGauge(t *testing.T) {
	// Unregister first to ensure clean state
	prometheus.Unregister(cbsEnabledGauge)

	// Should register without error
	RegisterCBSEnabledGauge()
	// Register again to trigger AlreadyRegisteredError
	RegisterCBSEnabledGauge()

	// Check that the collector is still a GaugeVec
	if cbsEnabledGauge == nil {
		t.Error("cbsEnabledGauge is nil after registration")
	}
}

func TestRegisterEligibilityStringGauge(t *testing.T) {
	// Unregister first to ensure clean state
	prometheus.Unregister(eligibilityStringGauge)

	// Should register without error
	RegisterEligibilityStringGauge()
	// Register again to trigger AlreadyRegisteredError
	RegisterEligibilityStringGauge()

	// Check that the collector is still a GaugeVec
	if eligibilityStringGauge == nil {
		t.Error("EligibilityString is nil after registration")
	}
}

func TestRegisterBackupSizeGauge(t *testing.T) {
	// Unregister first to ensure clean state
	prometheus.Unregister(backupSizeGauge)

	// Should register without error
	RegisterBackupSizeGauge()
	// Register again to trigger AlreadyRegisteredError
	RegisterBackupSizeGauge()

	// Check that the collector is still a GaugeVec
	if backupSizeGauge == nil {
		t.Error("backupSizeGauge is nil after registration")
	}
}

func TestRegisterCertificateRotationFailureCounter(t *testing.T) {
	// Unregister first to ensure clean state
	prometheus.Unregister(CertificateRotationFailureCounter)

	// Should register without error
	RegisterCertificateRotationFailureCounter()
	// Register again to trigger AlreadyRegisteredError
	RegisterCertificateRotationFailureCounter()

	// Check that the collector is still a CounterVec
	if CertificateRotationFailureCounter == nil {
		t.Error("CertificateRotationFailureCounter is nil after registration")
	}
}

func TestRegisterPasswordRotationFailureCounter(t *testing.T) {
	// Unregister first to ensure clean state
	prometheus.Unregister(PasswordRotationFailureCounter)

	// Should register without error
	RegisterPasswordRotationFailureCounter()
	// Register again to trigger AlreadyRegisteredError
	RegisterPasswordRotationFailureCounter()

	// Check that the collector is still a CounterVec
	if PasswordRotationFailureCounter == nil {
		t.Error("PasswordRotationFailureCounter is nil after registration")
	}
}

func TestEmitCertificateRotationFailure(t *testing.T) {
	RegisterCertificateRotationFailureCounter()
	
	poolUUID := "test-pool-uuid-123"
	poolName := "test-pool-name"
	failureType := "certificate_rotation"
	errorType := "connection timeout"

	// Emit the metric
	EmitCertificateRotationFailure(poolUUID, poolName, failureType, errorType)

	// Gather metrics and verify
	metrics, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Errorf("Failed to gather metrics: %v", err)
		return
	}

	found := false
	for _, mf := range metrics {
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

	if !found {
		t.Error("CertificateRotationFailureCounter metric not found with expected labels")
	}
}

func TestEmitCertificateRotationFailure_TruncatesLongErrorType(t *testing.T) {
	// Unregister first to ensure clean state
	prometheus.Unregister(CertificateRotationFailureCounter)
	RegisterCertificateRotationFailureCounter()
	
	poolUUID := "test-pool-uuid-456"
	poolName := "test-pool-name-2"
	failureType := "certificate_rotation"
	// Create a very long error type (> 200 chars)
	longErrorType := strings.Repeat("a", 250)

	// Emit the metric
	EmitCertificateRotationFailure(poolUUID, poolName, failureType, longErrorType)

	// Gather metrics and verify truncation
	metrics, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Errorf("Failed to gather metrics: %v", err)
		return
	}

	found := false
	expectedErrorType := strings.Repeat("a", 200) + "..."
	for _, mf := range metrics {
		if *mf.Name == "vcp_certificate_rotation_failures_total" {
			for _, m := range mf.Metric {
				// Check that this metric has the expected labels
				expectedLabels := map[string]string{
					"pool_uuid":    poolUUID,
					"pool_name":    poolName,
					"failure_type": failureType,
					"error_type":   expectedErrorType,
				}
				if metricHasLabels(m.Label, expectedLabels) {
					found = true
					// Verify the error_type is truncated correctly
					for _, label := range m.Label {
						if *label.Name == "error_type" {
							errorTypeValue := *label.Value
							if errorTypeValue != expectedErrorType {
								t.Errorf("Expected error_type to be %q (length %d), got %q (length %d)", 
									expectedErrorType, len(expectedErrorType), errorTypeValue, len(errorTypeValue))
							}
							if !strings.HasSuffix(errorTypeValue, "...") {
								t.Error("Long error type should be truncated with '...' suffix")
							}
							break
						}
					}
					break
				}
			}
		}
	}

	if !found {
		t.Error("CertificateRotationFailureCounter metric not found with expected labels")
	}
}

func TestEmitPasswordRotationFailure(t *testing.T) {
	RegisterPasswordRotationFailureCounter()
	
	poolUUID := "test-pool-uuid-789"
	poolName := "test-pool-name-3"
	failureType := "password_rotation"
	errorType := "authentication failed"

	// Emit the metric
	EmitPasswordRotationFailure(poolUUID, poolName, failureType, errorType)

	// Gather metrics and verify
	metrics, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Errorf("Failed to gather metrics: %v", err)
		return
	}

	found := false
	for _, mf := range metrics {
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

	if !found {
		t.Error("PasswordRotationFailureCounter metric not found with expected labels")
	}
}

func TestEmitPasswordRotationFailure_TruncatesLongErrorType(t *testing.T) {
	// Unregister first to ensure clean state
	prometheus.Unregister(PasswordRotationFailureCounter)
	RegisterPasswordRotationFailureCounter()
	
	poolUUID := "test-pool-uuid-101"
	poolName := "test-pool-name-4"
	failureType := "password_rotation"
	// Create a very long error type (> 200 chars)
	longErrorType := strings.Repeat("b", 300)

	// Emit the metric
	EmitPasswordRotationFailure(poolUUID, poolName, failureType, longErrorType)

	// Gather metrics and verify truncation
	metrics, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Errorf("Failed to gather metrics: %v", err)
		return
	}

	found := false
	expectedErrorType := strings.Repeat("b", 200) + "..."
	for _, mf := range metrics {
		if *mf.Name == "vcp_password_rotation_failures_total" {
			for _, m := range mf.Metric {
				// Check that this metric has the expected labels
				expectedLabels := map[string]string{
					"pool_uuid":    poolUUID,
					"pool_name":    poolName,
					"failure_type": failureType,
					"error_type":   expectedErrorType,
				}
				if metricHasLabels(m.Label, expectedLabels) {
					found = true
					// Verify the error_type is truncated correctly
					for _, label := range m.Label {
						if *label.Name == "error_type" {
							errorTypeValue := *label.Value
							if errorTypeValue != expectedErrorType {
								t.Errorf("Expected error_type to be %q (length %d), got %q (length %d)", 
									expectedErrorType, len(expectedErrorType), errorTypeValue, len(errorTypeValue))
							}
							if !strings.HasSuffix(errorTypeValue, "...") {
								t.Error("Long error type should be truncated with '...' suffix")
							}
							break
						}
					}
					break
				}
			}
		}
	}

	if !found {
		t.Error("PasswordRotationFailureCounter metric not found with expected labels")
	}
}
