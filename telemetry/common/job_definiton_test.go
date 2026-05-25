package common

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
)

func TestDefaultAggregationJobDefinitions(t *testing.T) {
	// Test that the default job definitions are not empty
	assert.NotEmpty(t, DefaultAggregationJobDefinitions, "Default aggregation job definitions should not be empty")

	// Test that at least one resource type has job definitions
	foundDefinitions := false
	for key := range DefaultAggregationJobDefinitions {
		if key.ResourceType == metadata.Volume {
			foundDefinitions = true
			break
		}
	}
	assert.True(t, foundDefinitions, "Should have job definitions for resource type %s", metadata.Volume)
}

func TestIsBillableMetric(t *testing.T) {
	ctx := context.Background()
	// Test the exported isBillableMetric function
	tests := []struct {
		name         string
		resourceType metadata.ResourceType
		measuredType metadata.MeasuredType
		expected     bool
	}{
		{
			name:         "volume allocated size",
			resourceType: metadata.Volume,
			measuredType: metadata.AllocatedSize,
			expected:     false, // based on DefaultAggregationJobDefinitions
		},
		{
			name:         "unknown combination",
			resourceType: metadata.VolumePool,
			measuredType: metadata.UnknownMeasuredType,
			expected:     false, // should default to false
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsBillableMetric(ctx, tt.resourceType, tt.measuredType)
			assert.Equal(t, tt.expected, got, "IsBillableMetric returned unexpected value")
		})
	}
}

func TestGetJobDefinition(t *testing.T) {
	// Test looking up a job definition that exists
	key := metadata.CombinedKeyResourceTypeMeasuredType{
		ResourceType: metadata.Volume,
		MeasuredType: metadata.AllocatedSize,
	}

	job, exists := DefaultAggregationJobDefinitions[key]
	assert.True(t, exists, "Should find job definition for volume allocated size")
	assert.Equal(t, IntegralAggregation, job.AggregationType, "Job should have correct aggregation type")
	assert.Equal(t, false, job.IsBillable, "Job should have correct billable flag")

	// Test looking up a job definition that doesn't exist
	nonExistingKey := metadata.CombinedKeyResourceTypeMeasuredType{
		ResourceType: metadata.VolumePool,
		MeasuredType: metadata.UnknownMeasuredType,
	}

	_, exists = DefaultAggregationJobDefinitions[nonExistingKey]
	assert.False(t, exists, "Should not find job definition for non-existing combination")
}

func TestInitBillableMetricsMap(t *testing.T) {
	// Test that the default job definitions are properly accessible
	// This indirectly tests that the map is initialized correctly

	// Check for known entries based on DefaultAggregationJobDefinitions
	key := metadata.CombinedKeyResourceTypeMeasuredType{
		ResourceType: metadata.Volume,
		MeasuredType: metadata.AllocatedSize,
	}

	jobDef, exists := DefaultAggregationJobDefinitions[key]
	assert.True(t, exists, "Default job definitions should contain entry for Volume-AllocatedSize")
	assert.Equal(t, false, jobDef.IsBillable, "Volume-AllocatedSize should match the billable flag in DefaultAggregationJobDefinitions")
	assert.Equal(t, IntegralAggregation, jobDef.AggregationType, "Volume-AllocatedSize should have correct aggregation type")
}

// TestAutoTieringRegionalHAJobDefinitions verifies VolumePoolRegionalHA auto-tiering job definitions
// exist and match their VolumePool counterparts (same SKU and aggregation type)
func TestAutoTieringRegionalHAJobDefinitions(t *testing.T) {
	autoTieringMetrics := []metadata.MeasuredType{
		metadata.CoolTierDataReadSizeRaw,
		metadata.CoolTierDataWriteSizeRaw,
		metadata.PoolHotTierProvisionedSize,
		metadata.PoolCapacityTierLogicalFootprint,
	}

	for _, measuredType := range autoTieringMetrics {
		t.Run(string(measuredType), func(t *testing.T) {
			// Get VolumePool job definition (baseline)
			poolKey := metadata.CombinedKeyResourceTypeMeasuredType{
				ResourceType: metadata.VolumePool,
				MeasuredType: measuredType,
			}
			poolJobDef, poolExists := DefaultAggregationJobDefinitions[poolKey]
			assert.True(t, poolExists, "VolumePool job definition should exist for %s", measuredType)

			// Get VolumePoolRegionalHA job definition (what we added)
			haKey := metadata.CombinedKeyResourceTypeMeasuredType{
				ResourceType: metadata.VolumePoolRegionalHA,
				MeasuredType: measuredType,
			}
			haJobDef, haExists := DefaultAggregationJobDefinitions[haKey]
			assert.True(t, haExists, "VolumePoolRegionalHA job definition should exist for %s", measuredType)

			// Verify RegionalHA matches VolumePool
			assert.Equal(t, poolJobDef.SKU, haJobDef.SKU, "RegionalHA SKU should match VolumePool")
			assert.Equal(t, poolJobDef.AggregationType, haJobDef.AggregationType, "RegionalHA aggregation should match VolumePool")
			assert.Equal(t, poolJobDef.IsBillable, haJobDef.IsBillable, "RegionalHA billable should match VolumePool")
		})
	}
}

func TestBackupEnabledVolumeAllocatedSizeRegionalHAJobDefinition(t *testing.T) {
	volumeKey := metadata.CombinedKeyResourceTypeMeasuredType{
		ResourceType: metadata.Volume,
		MeasuredType: metadata.BackupEnabledVolumeAllocatedSize,
	}
	volumeJobDef, volumeExists := DefaultAggregationJobDefinitions[volumeKey]
	assert.True(t, volumeExists, "Volume job definition should exist for BackupEnabledVolumeAllocatedSize")

	haKey := metadata.CombinedKeyResourceTypeMeasuredType{
		ResourceType: metadata.VolumeRegionalHA,
		MeasuredType: metadata.BackupEnabledVolumeAllocatedSize,
	}
	haJobDef, haExists := DefaultAggregationJobDefinitions[haKey]
	assert.True(t, haExists, "VolumeRegionalHA job definition should exist for BackupEnabledVolumeAllocatedSize")

	assert.Equal(t, volumeJobDef.SKU, haJobDef.SKU, "RegionalHA SKU should match Volume")
	assert.Equal(t, volumeJobDef.AggregationType, haJobDef.AggregationType, "RegionalHA aggregation should match Volume")
	assert.Equal(t, volumeJobDef.IsBillable, haJobDef.IsBillable, "RegionalHA billable should match Volume")

	volumeFormatter, volumeFormatterOk := volumeJobDef.TimeSeriesFormatter.(*SampledMetricsFormatter)
	haFormatter, haFormatterOk := haJobDef.TimeSeriesFormatter.(*SampledMetricsFormatter)
	assert.True(t, volumeFormatterOk, "Volume formatter should be SampledMetricsFormatter")
	assert.True(t, haFormatterOk, "RegionalHA formatter should be SampledMetricsFormatter")
	if volumeFormatterOk && haFormatterOk {
		assert.Equal(t, volumeFormatter.Mode, haFormatter.Mode, "RegionalHA formatter mode should match Volume")
		assert.Equal(t, volumeFormatter.BackfillLimit, haFormatter.BackfillLimit, "RegionalHA formatter backfill limit should match Volume")
	}

	poolHAKey := metadata.CombinedKeyResourceTypeMeasuredType{
		ResourceType: metadata.VolumePoolRegionalHA,
		MeasuredType: metadata.BackupEnabledVolumeAllocatedSize,
	}
	_, poolHAExists := DefaultAggregationJobDefinitions[poolHAKey]
	assert.False(t, poolHAExists, "VolumePoolRegionalHA should NOT have BackupEnabledVolumeAllocatedSize (it is a volume metric, not a pool metric)")
}

func TestVolumeATRawJobDefinitions(t *testing.T) {
	volumeATRawMetrics := []metadata.MeasuredType{
		metadata.CoolTierDataReadSizeRaw,
		metadata.CoolTierDataWriteSizeRaw,
	}

	resourceTypes := []metadata.ResourceType{
		metadata.Volume,
		metadata.VolumeRegionalHA,
	}

	for _, measuredType := range volumeATRawMetrics {
		for _, resourceType := range resourceTypes {
			t.Run(string(resourceType)+"/"+string(measuredType), func(t *testing.T) {
				key := metadata.CombinedKeyResourceTypeMeasuredType{
					ResourceType: resourceType,
					MeasuredType: measuredType,
				}
				jobDef, exists := DefaultAggregationJobDefinitions[key]
				assert.True(t, exists, "Job definition should exist for %s/%s", resourceType, measuredType)
				assert.Equal(t, CounterAggregation, jobDef.AggregationType, "Should be CounterAggregation")
				assert.False(t, jobDef.IsBillable, "Should not be billable")
				assert.Empty(t, jobDef.SKU, "Should have no SKU")
			})
		}
	}
}
