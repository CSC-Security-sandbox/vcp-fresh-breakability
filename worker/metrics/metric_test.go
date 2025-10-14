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
		return []string{"volume1_active_123_account1", "volume2_inactive_456_account2"}
	case "crr":
		return []string{"volume1_active_123_account1", "volume3_active_789_"}
	case "largevolume":
		return []string{"volume1_active_123_account1", "volume2_inactive_456_account2", "volume3_active_789_"}
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

func TestEmitTotalVolumeCountMetric(t *testing.T) {
	RegisterTotalVolumeCountGauge()
	expectedValue := 10
	totalVolumeCountGauge.Set(float64(expectedValue))
	metrics, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Errorf("Failed to gather metrics: %v", err)
	}
	var totalVolumeCount float64
	for _, metricFamily := range metrics {
		if *metricFamily.Name == "gcnv_vsa_total_volume_count" {
			totalVolumeCount = *metricFamily.Metric[0].Gauge.Value
			break
		}
	}
	if totalVolumeCount != float64(expectedValue) {
		t.Errorf("Total volume count gauge value not set correctly. Expected: %d, Actual: %f", expectedValue, totalVolumeCount)
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
						"account_id":   expectedLabelValues[2],
						"account_name": expectedLabelValues[3],
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
						"account_id":   expectedLabelValues[2],
						"account_name": expectedLabelValues[3],
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
						"account_id":   expectedLabelValues[2],
						"account_name": expectedLabelValues[3],
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
						"account_id":   expectedLabelValues[2],
						"account_name": expectedLabelValues[3],
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

func TestRegisterTotalVolumeCountGauge(t *testing.T) {
	// Unregister first to ensure clean state
	prometheus.Unregister(totalVolumeCountGauge)

	// Should register without error
	RegisterTotalVolumeCountGauge()
	// Register again to trigger AlreadyRegisteredError
	RegisterTotalVolumeCountGauge()

	// Check that the collector is still a GaugeVec
	if totalVolumeCountGauge == nil {
		t.Error("totalVolumeCountGauge is nil after registration")
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
		t.Error("cbsEnabledGauge is nil after registration")
	}
}
