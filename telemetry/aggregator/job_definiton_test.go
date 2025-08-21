package aggregator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
)

func TestDefaultAggregationJobDefinitions(t *testing.T) {
	// Test that the default job definitions are not empty
	assert.NotEmpty(t, defaultAggregationJobDefinitions, "Default aggregation job definitions should not be empty")

	// Test that at least one resource type has job definitions
	foundDefinitions := false
	for key := range defaultAggregationJobDefinitions {
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
			expected:     false, // based on defaultAggregationJobDefinitions
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
			got := isBillableMetric(ctx, tt.resourceType, tt.measuredType)
			assert.Equal(t, tt.expected, got, "isBillableMetric returned unexpected value")
		})
	}
}

func TestGetJobDefinition(t *testing.T) {
	// Test looking up a job definition that exists
	key := metadata.CombinedKeyResourceTypeMeasuredType{
		ResourceType: metadata.Volume,
		MeasuredType: metadata.AllocatedSize,
	}

	job, exists := defaultAggregationJobDefinitions[key]
	assert.True(t, exists, "Should find job definition for volume allocated size")
	assert.Equal(t, IntegralAggregation, job.AggregationType, "Job should have correct aggregation type")
	assert.Equal(t, false, job.IsBillable, "Job should have correct billable flag")

	// Test looking up a job definition that doesn't exist
	nonExistingKey := metadata.CombinedKeyResourceTypeMeasuredType{
		ResourceType: metadata.VolumePool,
		MeasuredType: metadata.UnknownMeasuredType,
	}

	_, exists = defaultAggregationJobDefinitions[nonExistingKey]
	assert.False(t, exists, "Should not find job definition for non-existing combination")
}

func TestInitBillableMetricsMap(t *testing.T) {
	// Test that the default job definitions are properly accessible
	// This indirectly tests that the map is initialized correctly

	// Check for known entries based on defaultAggregationJobDefinitions
	key := metadata.CombinedKeyResourceTypeMeasuredType{
		ResourceType: metadata.Volume,
		MeasuredType: metadata.AllocatedSize,
	}

	jobDef, exists := defaultAggregationJobDefinitions[key]
	assert.True(t, exists, "Default job definitions should contain entry for Volume-AllocatedSize")
	assert.Equal(t, false, jobDef.IsBillable, "Volume-AllocatedSize should match the billable flag in defaultAggregationJobDefinitions")
	assert.Equal(t, IntegralAggregation, jobDef.AggregationType, "Volume-AllocatedSize should have correct aggregation type")
}
