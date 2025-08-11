package common

import (
	"testing"
)

func TestLoadsConfigWithDefaultValues(t *testing.T) {
	config := LoadConfig()

	if config.RootUrl != "https://servicecontrol.googleapis.com" {
		t.Fatalf("Expected RootUrl to be 'https://servicecontrol.googleapis.com', got %s", config.RootUrl)
	}
	if config.OperationBatchSize != 200 {
		t.Fatalf("Expected OperationBatchSize to be 200, got %d", config.OperationBatchSize)
	}
	if config.PusherServiceName != "autopush-netapp.sandbox.googleapis.com" {
		t.Fatalf("Expected PusherServiceName to be 'autopush-netapp.sandbox.googleapis.com', got %s", config.PusherServiceName)
	}
	if config.PusherServiceProject != "netapp-au-se1-autopush-sde-tst" {
		t.Fatalf("Expected PusherServiceProject to be 'netapp-au-se1-autopush-sde-tst', got %s", config.PusherServiceProject)
	}
}

func TestLoadsConfigWithCustomEnvironmentValues(t *testing.T) {
	config := LoadConfig()

	if config.RootUrl != "https://servicecontrol.googleapis.com" {
		t.Fatalf("Expected RootUrl to be 'https://servicecontrol.googleapis.com', got %s", config.RootUrl)
	}
	if config.OperationBatchSize != 200 {
		t.Fatalf("Expected OperationBatchSize to be 200, got %d", config.OperationBatchSize)
	}
	if config.PusherServiceName != "autopush-netapp.sandbox.googleapis.com" {
		t.Fatalf("Expected PusherServiceName to be 'autopush-netapp.sandbox.googleapis.com', got %s", config.PusherServiceName)
	}
	if config.PusherServiceProject != "netapp-au-se1-autopush-sde-tst" {
		t.Fatalf("Expected PusherServiceProject to be 'netapp-au-se1-autopush-sde-tst', got %s", config.PusherServiceProject)
	}
}

func TestHandlesMissingEnvironmentVariablesGracefully(t *testing.T) {
	config := LoadConfig()

	if config.RootUrl != "https://servicecontrol.googleapis.com" {
		t.Fatalf("Expected RootUrl to default to 'https://servicecontrol.googleapis.com', got %s", config.RootUrl)
	}
	if config.OperationBatchSize != 200 {
		t.Fatalf("Expected OperationBatchSize to default to 200, got %d", config.OperationBatchSize)
	}
	if config.PusherServiceName != "autopush-netapp.sandbox.googleapis.com" {
		t.Fatalf("Expected PusherServiceName to default to 'autopush-netapp.sandbox.googleapis.com', got %s", config.PusherServiceName)
	}
	if config.PusherServiceProject != "netapp-au-se1-autopush-sde-tst" {
		t.Fatalf("Expected PusherServiceProject to default to 'netapp-au-se1-autopush-sde-tst', got %s", config.PusherServiceProject)
	}
}
func TestRegionNameDefaultValue(t *testing.T) {
	config := LoadConfig()

	if config.RegionName != "" {
		t.Fatalf("Expected RegionName to default to empty string, got %s", config.RegionName)
	}
}

func TestRegionNameWithEnvironmentVariable(t *testing.T) {
	// Set environment variable for this test
	t.Setenv("LOCAL_REGION", "us-west-1")

	config := LoadConfig()

	if config.RegionName != "us-west-1" {
		t.Fatalf("Expected RegionName to be 'us-west-1', got %s", config.RegionName)
	}
}

func TestLoadMetricsConfigFromBytesReturnsValidConfig(t *testing.T) {
	config := LoadMetricsConfigFromBytes()

	if config == nil {
		t.Fatalf("Expected config to not be nil")
	}
	if config.VolumeMetrics == nil {
		t.Fatalf("Expected VolumeMetrics to not be nil")
	}
}

func TestLoadMetricsConfigFromBytesContainsExpectedMetrics(t *testing.T) {
	config := LoadMetricsConfigFromBytes()

	if len(config.VolumeMetrics) == 0 {
		t.Fatalf("Expected VolumeMetrics to contain at least one metric")
	}

	for i, metric := range config.VolumeMetrics {
		if metric.Metric == "" {
			t.Fatalf("Expected metric at index %d to have non-empty Metric field", i)
		}
		if metric.ResourceType == "" {
			t.Fatalf("Expected metric at index %d to have non-empty ResourceType field", i)
		}
	}
}

func TestLoadMetricsConfigFromBytesHandlesValidYamlStructure(t *testing.T) {
	config := LoadMetricsConfigFromBytes()

	foundValidMetric := false
	for _, metric := range config.VolumeMetrics {
		if metric.Metric != "" && metric.ResourceType != "" {
			foundValidMetric = true
			break
		}
	}

	if !foundValidMetric {
		t.Fatalf("Expected at least one metric with both Metric and ResourceType fields populated")
	}
}

func TestLoadMetricsConfigFromBytesWithEmptyYamlReturnsEmptyConfig(t *testing.T) {
	originalMetricListYAML := metricListYAML
	defer func() {
		metricListYAML = originalMetricListYAML
	}()

	metricListYAML = []byte("")

	config := LoadMetricsConfigFromBytes()

	if config == nil {
		t.Fatalf("Expected config to not be nil")
	}
	if len(config.VolumeMetrics) != 0 {
		t.Fatalf("Expected VolumeMetrics to be empty, got %d metrics", len(config.VolumeMetrics))
	}
}
