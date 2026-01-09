package common

import (
	"testing"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
)

func TestReturnsCorrectMappingForValidKey(t *testing.T) {
	metricsMappingMap := CreateMetricsMappingMap()
	key := metadata.CombinedKeyResourceTypeMeasuredType{
		ResourceType: metadata.VolumePool,
		MeasuredType: metadata.PoolAllocatedSize,
	}
	expected := Triple{Left: "capacity", Middle: "", Right: ""}
	result, exists := metricsMappingMap[key]
	if !exists {
		t.Fatalf("Expected key to exist in the map")
	}
	if result != expected {
		t.Fatalf("Expected %v, got %v", expected, result)
	}
}

func TestReturnsEmptyMapWhenNoMappingsExist(t *testing.T) {
	metricsMappingMap := CreateMetricsMappingMap()
	if len(metricsMappingMap) == 0 {
		t.Fatalf("Expected non-empty map, got empty map")
	}
}

func TestHandlesNonExistentKeyGracefully(t *testing.T) {
	metricsMappingMap := CreateMetricsMappingMap()
	key := metadata.CombinedKeyResourceTypeMeasuredType{
		ResourceType: metadata.VolumePool,
		MeasuredType: metadata.UnknownMeasuredType,
	}
	_, exists := metricsMappingMap[key]
	if exists {
		t.Fatalf("Expected key to not exist in the map")
	}
}

// TestRegionalHAPoolMappings tests the new regional HA pool metric mappings
func TestRegionalHAPoolMappings(t *testing.T) {
	metricsMappingMap := CreateMetricsMappingMap()

	testCases := []struct {
		name         string
		resourceType metadata.ResourceType
		measuredType metadata.MeasuredType
		expectedLeft string
	}{
		{
			name:         "Regional HA Pool Allocated Size",
			resourceType: metadata.VolumePoolRegionalHA,
			measuredType: metadata.PoolAllocatedSize,
			expectedLeft: "capacity",
		},
		{
			name:         "Regional HA Pool Allocated Used",
			resourceType: metadata.VolumePoolRegionalHA,
			measuredType: metadata.AllocatedUsed,
			expectedLeft: "allocated",
		},
		{
			name:         "Regional HA Volume Throughput",
			resourceType: metadata.VolumeRegionalHA,
			measuredType: metadata.VolumeAllocatedThroughput,
			expectedLeft: "throughput_limit",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			key := metadata.CombinedKeyResourceTypeMeasuredType{
				ResourceType: tc.resourceType,
				MeasuredType: tc.measuredType,
			}

			result, exists := metricsMappingMap[key]
			if !exists {
				t.Fatalf("Expected key %+v to exist in the map", key)
			}

			if result.Left != tc.expectedLeft {
				t.Fatalf("Expected Left to be '%s', got '%s'", tc.expectedLeft, result.Left)
			}

			// Verify Middle and Right are empty for these mappings
			if result.Middle != "" {
				t.Fatalf("Expected Middle to be empty, got '%s'", result.Middle)
			}
			if result.Right != "" {
				t.Fatalf("Expected Right to be empty, got '%s'", result.Right)
			}
		})
	}
}

// TestAllVolumePoolTypesSupported tests that both regular and regional HA pools have mappings
func TestAllVolumePoolTypesSupported(t *testing.T) {
	metricsMappingMap := CreateMetricsMappingMap()

	poolTypes := []metadata.ResourceType{
		metadata.VolumePool,
		metadata.VolumePoolRegionalHA,
	}

	metricTypes := []metadata.MeasuredType{
		metadata.PoolAllocatedSize,
		metadata.AllocatedUsed,
	}

	for _, poolType := range poolTypes {
		for _, metricType := range metricTypes {
			key := metadata.CombinedKeyResourceTypeMeasuredType{
				ResourceType: poolType,
				MeasuredType: metricType,
			}

			result, exists := metricsMappingMap[key]
			if !exists {
				t.Fatalf("Expected key %+v to exist in the map", key)
			}

			// Both pool types should have the same mapping values
			expectedLeft := ""
			if metricType == metadata.PoolAllocatedSize {
				expectedLeft = "capacity"
			} else if metricType == metadata.AllocatedUsed {
				expectedLeft = "allocated"
			}

			if result.Left != expectedLeft {
				t.Fatalf("Expected Left to be '%s' for %s/%s, got '%s'",
					expectedLeft, poolType, metricType, result.Left)
			}
		}
	}
}

// TestVolumeRegionalHAThroughputMapping tests the volume regional HA throughput mapping
func TestVolumeRegionalHAThroughputMapping(t *testing.T) {
	metricsMappingMap := CreateMetricsMappingMap()

	// Test regular volume throughput
	regularVolumeKey := metadata.CombinedKeyResourceTypeMeasuredType{
		ResourceType: metadata.Volume,
		MeasuredType: metadata.VolumeAllocatedThroughput,
	}

	regularResult, exists := metricsMappingMap[regularVolumeKey]
	if !exists {
		t.Fatalf("Expected regular volume throughput key to exist in the map")
	}

	// Test regional HA volume throughput
	regionalHAVolumeKey := metadata.CombinedKeyResourceTypeMeasuredType{
		ResourceType: metadata.VolumeRegionalHA,
		MeasuredType: metadata.VolumeAllocatedThroughput,
	}

	regionalHAResult, exists := metricsMappingMap[regionalHAVolumeKey]
	if !exists {
		t.Fatalf("Expected regional HA volume throughput key to exist in the map")
	}

	// Both should map to the same value
	if regularResult.Left != regionalHAResult.Left {
		t.Fatalf("Expected both volume types to have same throughput mapping, got regular: '%s', regional HA: '%s'",
			regularResult.Left, regionalHAResult.Left)
	}

	if regionalHAResult.Left != "throughput_limit" {
		t.Fatalf("Expected regional HA volume throughput to map to 'throughput_limit', got '%s'", regionalHAResult.Left)
	}
}

// TestMetricsMappingMapCompleteness tests that all expected mappings are present
func TestMetricsMappingMapCompleteness(t *testing.T) {
	metricsMappingMap := CreateMetricsMappingMap()

	expectedMappings := []metadata.CombinedKeyResourceTypeMeasuredType{
		// Regular volume pool mappings
		{ResourceType: metadata.VolumePool, MeasuredType: metadata.PoolAllocatedSize},
		{ResourceType: metadata.VolumePool, MeasuredType: metadata.AllocatedUsed},
		{ResourceType: metadata.VolumePool, MeasuredType: metadata.PoolTotalThroughputMibps},
		{ResourceType: metadata.VolumePool, MeasuredType: metadata.PoolTotalIops},
		{ResourceType: metadata.VolumePool, MeasuredType: metadata.CoolTierDataReadSize},
		{ResourceType: metadata.VolumePool, MeasuredType: metadata.CoolTierDataWriteSize},

		// Regional HA volume pool mappings
		{ResourceType: metadata.VolumePoolRegionalHA, MeasuredType: metadata.PoolAllocatedSize},
		{ResourceType: metadata.VolumePoolRegionalHA, MeasuredType: metadata.AllocatedUsed},
		{ResourceType: metadata.VolumePoolRegionalHA, MeasuredType: metadata.PoolTotalThroughputMibps},
		{ResourceType: metadata.VolumePoolRegionalHA, MeasuredType: metadata.PoolTotalIops},

		// Volume mappings
		{ResourceType: metadata.Volume, MeasuredType: metadata.VolumeAllocatedThroughput},
		{ResourceType: metadata.Volume, MeasuredType: metadata.AverageReadLatency},
		{ResourceType: metadata.Volume, MeasuredType: metadata.AverageWriteLatency},
		{ResourceType: metadata.Volume, MeasuredType: metadata.AverageOtherLatency},
		{ResourceType: metadata.Volume, MeasuredType: metadata.ReadIo},
		{ResourceType: metadata.Volume, MeasuredType: metadata.WriteIo},
		{ResourceType: metadata.Volume, MeasuredType: metadata.OtherIo},
		{ResourceType: metadata.Volume, MeasuredType: metadata.CoolTierDataReadSize},
		{ResourceType: metadata.Volume, MeasuredType: metadata.CoolTierDataWriteSize},

		// Regional HA volume mappings
		{ResourceType: metadata.VolumeRegionalHA, MeasuredType: metadata.VolumeAllocatedThroughput},

		// SFR metrics mappings
		{ResourceType: metadata.Volume, MeasuredType: metadata.SFRTotalSizeRestoredBytes},
		{ResourceType: metadata.VolumeRegionalHA, MeasuredType: metadata.SFRTotalSizeRestoredBytes},
		{ResourceType: metadata.Volume, MeasuredType: metadata.SFRTotalFilesRestoredCount},
		{ResourceType: metadata.VolumeRegionalHA, MeasuredType: metadata.SFRTotalFilesRestoredCount},

		// Backup mappings
		{ResourceType: metadata.Backup, MeasuredType: metadata.BackupLogicalSize},
	}

	for _, expectedKey := range expectedMappings {
		_, exists := metricsMappingMap[expectedKey]
		if !exists {
			t.Fatalf("Expected mapping for key %+v to exist", expectedKey)
		}
	}

	// Verify we have the expected number of mappings
	expectedCount := len(expectedMappings)
	actualCount := len(metricsMappingMap)

	if actualCount != expectedCount {
		t.Fatalf("Expected %d mappings, got %d", expectedCount, actualCount)
	}
}
