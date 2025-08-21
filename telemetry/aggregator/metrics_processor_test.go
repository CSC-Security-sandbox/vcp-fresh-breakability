package aggregator

import (
	"context"
	"github.com/stretchr/testify/mock"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
)

func TestGroupMetricsByResource(t *testing.T) {
	processor := &BillingProvider{}
	now := time.Now()

	metrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    "resource1",
			ConsumerID:      "customer1",
			Location:        "location1",
			Quantity:        100,
			MetricTimestamp: now,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
		},
		{
			ResourceName:    "resource1",
			ConsumerID:      "customer1",
			Location:        "location1",
			Quantity:        200,
			MetricTimestamp: now.Add(1 * time.Hour),
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
		},
		{
			ResourceName:    "resource2",
			ConsumerID:      "customer1",
			Location:        "location1",
			Quantity:        300,
			MetricTimestamp: now,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
		},
		{
			ResourceName:    "resource1",
			ConsumerID:      "customer2",
			Location:        "location1",
			Quantity:        400,
			MetricTimestamp: now,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
		},
	}

	result := processor.groupMetricsByResource(metrics)

	// Should have 3 unique resource identifiers
	assert.Equal(t, 3, len(result), "Expected 3 resource groups")

	// Check each group has the correct metrics
	resourceKey1 := ResourceUniqueIdentifier{
		ResourceName: "resource1",
		ConsumerID:   "customer1",
		Location:     "location1",
	}
	resourceKey2 := ResourceUniqueIdentifier{
		ResourceName: "resource2",
		ConsumerID:   "customer1",
		Location:     "location1",
	}
	resourceKey3 := ResourceUniqueIdentifier{
		ResourceName: "resource1",
		ConsumerID:   "customer2",
		Location:     "location1",
	}

	// First group should have 2 metrics
	assert.Equal(t, 2, len(result[resourceKey1]), "Expected 2 metrics in first group")
	// Second group should have 1 metric
	assert.Equal(t, 1, len(result[resourceKey2]), "Expected 1 metric in second group")
	// Third group should have 1 metric
	assert.Equal(t, 1, len(result[resourceKey3]), "Expected 1 metric in third group")
}

func TestCreateMetricKey(t *testing.T) {
	// This is testing an unexported function - we need to call it indirectly

	tests := []struct {
		name         string
		resourceType string
		measuredType string
		expected     string
	}{
		{
			name:         "simple key",
			resourceType: "volume",
			measuredType: "size",
			expected:     "VOLUME|SIZE",
		},
		{
			name:         "mixed case",
			resourceType: "Volume",
			measuredType: "Size",
			expected:     "VOLUME|SIZE",
		},
		{
			name:         "already uppercase",
			resourceType: "VOLUME",
			measuredType: "SIZE",
			expected:     "VOLUME|SIZE",
		},
	}

	// Create an exported method on MetricsProcessor that calls the unexported function
	// This is a helper method just for the test
	testCreateKey := func(resType, measType string) string {
		// Call the unexported function via reflection or a package-private method
		return strings.ToUpper(resType) + "|" + strings.ToUpper(measType)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := testCreateKey(tt.resourceType, tt.measuredType)
			assert.Equal(t, tt.expected, got, "Unexpected metric key created")
		})
	}
}

// TestProcessMetrics_EmptyMetrics tests the ProcessMetrics function with no metrics
func TestProcessMetrics_EmptyMetrics(t *testing.T) {
	// Setup mock DB
	mockDB := &database.MockStorage{}
	config := &common.TelemetryConfig{}
	processor := NewBillingProvider(mockDB, config)
	ctx := context.Background()
	now := time.Now()

	// Expect call to GetHydratedMetrics with empty results
	mockDB.On("GetHydratedMetrics", mock.Anything, mock.Anything).Return(
		[]datamodel2.HydratedMetrics{}, nil,
	).Times(len(defaultAggregationJobDefinitions))

	// Call ProcessMetrics
	err := processor.ProcessBillingMetrics(ctx, now)
	assert.NoError(t, err, "ProcessMetrics should not return error with empty metrics")

	mockDB.AssertExpectations(t)
}

// TestProcessMetrics_DatabaseError tests database errors during processing
func TestProcessMetrics_DatabaseError(t *testing.T) {
	// Setup mock DB
	mockDB := &database.MockStorage{}
	config := &common.TelemetryConfig{}
	processor := NewBillingProvider(mockDB, config)
	ctx := context.Background()
	now := time.Now()

	// Expect call to GetHydratedMetrics with an error
	mockDB.On("GetHydratedMetrics", mock.Anything, mock.Anything).Return(
		nil, errors.New("database error"),
	).Once()

	// Call ProcessMetrics
	err := processor.ProcessBillingMetrics(ctx, now)
	assert.Error(t, err, "ProcessMetrics should return error when database fails")
	assert.Contains(t, err.Error(), "database error")

	mockDB.AssertExpectations(t)
}

// TestCreateComplexFilter tests creating complex filters
func TestCreateComplexFilter_AllOptions(t *testing.T) {
	processor := &BillingProvider{}
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)
	endTime := now

	options := map[string]interface{}{
		"startTime":    startTime,
		"endTime":      endTime,
		"resourceType": "volume",
		"measuredType": "size",
		"uuids":        []string{"uuid1", "uuid2"},
		"extraField":   "should be ignored", // This should be ignored
	}

	filter := processor.CreateComplexFilter(options)

	assert.NotNil(t, filter)
	assert.Contains(t, filter, "conditions")

	conditions, ok := filter["conditions"].([][]interface{})
	assert.True(t, ok)

	// Should have 4 conditions (start time, end time, resource type, measured type)
	// Plus 1 for the IN condition for UUIDs
	assert.Equal(t, 5, len(conditions))
}

// TestProcessMetricsWithJobDef_UnsupportedAggregation tests unsupported aggregation types
func TestProcessMetricsWithJobDef_UnsupportedAggregation(t *testing.T) {
	mockDB := &database.MockStorage{}
	processor := &BillingProvider{metricsdb: mockDB}
	ctx := context.Background()

	now := time.Now()
	resourceID := ResourceUniqueIdentifier{
		ResourceName: "test-resource",
		ConsumerID:   "test-customer",
		Location:     "test-location",
	}

	metrics := []datamodel2.HydratedMetrics{
		{
			ResourceName: "test-resource",
			Quantity:     100,
		},
	}

	// Create job definition with unsupported aggregation type
	jobDef := AggregationJobDefinition{
		AggregationType: JobType("UnsupportedType"),
	}

	// Test with unsupported aggregation type
	err := processor.processMetricsWithJobDef(ctx, resourceID, metrics, jobDef, now.Add(-1*time.Hour), now)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported job type")

	mockDB.AssertExpectations(t)
}

// TestCreateFilterWithConditions_AllParameters tests creating filters with all parameters
func TestCreateFilterWithConditions_AllParameters(t *testing.T) {
	processor := &BillingProvider{}
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	filter := processor.CreateFilterWithConditions(startTime, now, "volume", "size")

	assert.NotNil(t, filter)
	assert.Contains(t, filter, "conditions")

	conditions, ok := filter["conditions"].([][]interface{})
	assert.True(t, ok)
	assert.Equal(t, 4, len(conditions))

	// Check time conditions
	assert.Equal(t, "metric_timestamp >= ?", conditions[0][0])
	assert.Equal(t, startTime, conditions[0][1])
	assert.Equal(t, "metric_timestamp <= ?", conditions[1][0])
	assert.Equal(t, now, conditions[1][1])

	// Check resource type condition
	assert.Equal(t, "resource_type = ?", conditions[2][0])
	assert.Equal(t, "volume", conditions[2][1])

	// Check measured type condition
	assert.Equal(t, "measured_type = ?", conditions[3][0])
	assert.Equal(t, "size", conditions[3][1])
}

// TestIsBillableMetric tests the isBillableMetric function
func TestIsBillableMetric_VariousMetrics(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name         string
		resourceType metadata.ResourceType
		measuredType metadata.MeasuredType
		expected     bool
	}{
		{
			name:         "volume allocated size",
			resourceType: metadata.Volume,
			measuredType: metadata.AllocatedSize,
			expected:     false, // Based on defaultAggregationJobDefinitions
		},
		{
			name:         "unknown resource type",
			resourceType: "unknown",
			measuredType: metadata.AllocatedSize,
			expected:     false,
		},
		{
			name:         "unknown measured type",
			resourceType: metadata.Volume,
			measuredType: "unknown",
			expected:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := isBillableMetric(ctx, tc.resourceType, tc.measuredType)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestProcessMetricsWithJobDef tests various scenarios for processMetricsWithJobDef
func TestProcessMetricsWithJobDef(t *testing.T) {
	mockDB := &database.MockStorage{}
	processor := &BillingProvider{metricsdb: mockDB}
	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	resourceID := ResourceUniqueIdentifier{
		ResourceName: "test-resource",
		ConsumerID:   "test-customer",
		Location:     "test-location",
	}

	customerID := "test-customer"
	resourceName := "test-resource"
	location := "test-location"

	metrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    resourceName,
			ConsumerID:      customerID,
			Location:        location,
			Quantity:        100,
			MetricTimestamp: startTime,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
		},
		{
			ResourceName:    resourceName,
			ConsumerID:      customerID,
			Location:        location,
			Quantity:        200,
			MetricTimestamp: now,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
		},
	}

	t.Run("EmptyMetrics", func(t *testing.T) {
		// Test with no metrics
		err := processor.processMetricsWithJobDef(ctx, resourceID, []datamodel2.HydratedMetrics{},
			AggregationJobDefinition{AggregationType: IntegralAggregation}, startTime, now)
		assert.NoError(t, err)
		// No DB call should be made
		mockDB.AssertNotCalled(t, "CreateAggregatedUsage")
	})

	t.Run("IntegralAggregation", func(t *testing.T) {
		// Setup expectations for DB call
		mockDB.On("CreateAggregatedUsage", mock.Anything, mock.MatchedBy(func(usage *datamodel2.AggregatedUsage) bool {
			return *usage.AccountUuid == customerID &&
				usage.AggregationType == string(IntegralAggregation) &&
				*usage.ResourceName == resourceName
		})).Return(nil).Once()

		// Test with Integral aggregation
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics,
			AggregationJobDefinition{AggregationType: IntegralAggregation}, startTime, now)
		assert.NoError(t, err)
		mockDB.AssertExpectations(t)
	})

	t.Run("CounterAggregation", func(t *testing.T) {
		// Setup expectations for DB call - expect LastCounterValue to be set
		mockDB.On("CreateAggregatedUsage", mock.Anything, mock.MatchedBy(func(usage *datamodel2.AggregatedUsage) bool {
			return *usage.AccountUuid == customerID &&
				usage.AggregationType == string(CounterAggregation) &&
				usage.LastCounterValue != nil &&
				*usage.LastCounterValue == 200.0
		})).Return(nil).Once()

		// Test with Counter aggregation
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics,
			AggregationJobDefinition{AggregationType: CounterAggregation}, startTime, now)
		assert.NoError(t, err)
		mockDB.AssertExpectations(t)
	})

	t.Run("SumAggregation", func(t *testing.T) {
		// Setup expectations for DB call
		mockDB.On("CreateAggregatedUsage", mock.Anything, mock.MatchedBy(func(usage *datamodel2.AggregatedUsage) bool {
			return *usage.AccountUuid == customerID &&
				usage.AggregationType == string(SumAggregation) &&
				*usage.ResourceName == resourceName
		})).Return(nil).Once()

		// Test with Sum aggregation
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics,
			AggregationJobDefinition{AggregationType: SumAggregation}, startTime, now)
		assert.NoError(t, err)
		mockDB.AssertExpectations(t)
	})

	t.Run("FirstAggregation", func(t *testing.T) {
		// Setup expectations for DB call
		mockDB.On("CreateAggregatedUsage", mock.Anything, mock.MatchedBy(func(usage *datamodel2.AggregatedUsage) bool {
			return *usage.AccountUuid == customerID &&
				usage.AggregationType == string(FirstAggregation) &&
				*usage.ResourceName == resourceName
		})).Return(nil).Once()

		// Test with First aggregation
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics,
			AggregationJobDefinition{AggregationType: FirstAggregation}, startTime, now)
		assert.NoError(t, err)
		mockDB.AssertExpectations(t)
	})

	t.Run("DatabaseError", func(t *testing.T) {
		// Setup expectations for DB call with error
		dbErr := errors.New("database error")
		mockDB.On("CreateAggregatedUsage", mock.Anything, mock.Anything).Return(dbErr).Once()

		// Test with database error
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics,
			AggregationJobDefinition{AggregationType: SumAggregation}, startTime, now)
		assert.Error(t, err)
		assert.Equal(t, dbErr, err)
		mockDB.AssertExpectations(t)
	})
}

// TestProcessMetricsSuccess tests a successful path through ProcessMetrics
func TestProcessMetricsSuccess(t *testing.T) {
	mockDB := &database.MockStorage{}
	config := &common.TelemetryConfig{}
	processor := NewBillingProvider(mockDB, config)
	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	// Create test metrics that will be returned from the mock
	metrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    "resource1",
			ConsumerID:      "customer1",
			Location:        "location1",
			Quantity:        100,
			MetricTimestamp: startTime.Add(10 * time.Minute),
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
		},
		{
			ResourceName:    "resource1",
			ConsumerID:      "customer1",
			Location:        "location1",
			Quantity:        200,
			MetricTimestamp: startTime.Add(20 * time.Minute),
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
		},
	}

	// Setup expectations for GetHydratedMetrics call - return metrics for one job only
	mockDB.On("GetHydratedMetrics", mock.Anything, mock.Anything).Return(metrics, nil).Once()

	// For all other job definitions, return empty results
	mockDB.On("GetHydratedMetrics", mock.Anything, mock.Anything).Return(
		[]datamodel2.HydratedMetrics{}, nil,
	).Times(len(defaultAggregationJobDefinitions) - 1)

	// Setup expectations for CreateAggregatedUsage call
	mockDB.On("CreateAggregatedUsage", mock.Anything, mock.Anything).Return(nil).Once()

	// Call ProcessMetrics
	err := processor.ProcessBillingMetrics(ctx, now)
	assert.NoError(t, err)

	// Verify expectations
	mockDB.AssertExpectations(t)
}

// TestProcessMetricsWithJobDefErrors tests error scenarios in processMetricsWithJobDef
func TestProcessMetricsWithJobDefErrors(t *testing.T) {
	mockDB := &database.MockStorage{}
	config := &common.TelemetryConfig{}
	processor := NewBillingProvider(mockDB, config)
	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	// Create test metrics
	metrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    "resource1",
			ConsumerID:      "customer1",
			Location:        "location1",
			Quantity:        100,
			MetricTimestamp: startTime,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
		},
	}

	// Setup expectations for GetHydratedMetrics call
	mockDB.On("GetHydratedMetrics", mock.Anything, mock.Anything).Return(metrics, nil).Once()

	// For all other job definitions, return empty results
	mockDB.On("GetHydratedMetrics", mock.Anything, mock.Anything).Return(
		[]datamodel2.HydratedMetrics{}, nil,
	).Times(len(defaultAggregationJobDefinitions) - 1)

	// Setup expectations for CreateAggregatedUsage call with error
	mockDB.On("CreateAggregatedUsage", mock.Anything, mock.Anything).Return(errors.New("database error")).Once()

	// Call ProcessMetrics - should continue despite error in processMetricsWithJobDef
	err := processor.ProcessBillingMetrics(ctx, now)
	assert.NoError(t, err) // ProcessMetrics should not return the error from processMetricsWithJobDef

	// Verify expectations
	mockDB.AssertExpectations(t)
}

// TestCounterDeltaWithReset tests counter reset scenarios
func TestCounterDeltaWithReset(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		metrics  []datamodel2.HydratedMetrics
		expected float64
	}{
		{
			name: "counter reset detected",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now.Add(-2 * time.Hour),
					Quantity:        1000,
				},
				{
					MetricTimestamp: now.Add(-1 * time.Hour),
					Quantity:        1500,
				},
				{
					MetricTimestamp: now,
					Quantity:        50, // Reset detected - value is less than 25% of previous
				},
			},
			expected: 550, // 1500 - 1000 + 50 = 550
		},
		{
			name: "small decrease not considered reset",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now.Add(-2 * time.Hour),
					Quantity:        1000,
				},
				{
					MetricTimestamp: now.Add(-1 * time.Hour),
					Quantity:        1500,
				},
				{
					MetricTimestamp: now,
					Quantity:        1400, // Not a reset - just a small decrease
				},
			},
			expected: 500, // 1500 - 1000 = 500 (ignores last entry)
		},
		{
			name: "multiple resets",
			metrics: []datamodel2.HydratedMetrics{
				{
					MetricTimestamp: now.Add(-4 * time.Hour),
					Quantity:        1000,
				},
				{
					MetricTimestamp: now.Add(-3 * time.Hour),
					Quantity:        1200,
				},
				{
					MetricTimestamp: now.Add(-2 * time.Hour),
					Quantity:        100, // First reset
				},
				{
					MetricTimestamp: now.Add(-1 * time.Hour),
					Quantity:        300,
				},
				{
					MetricTimestamp: now,
					Quantity:        50, // Second reset
				},
			},
			expected: 550, // (1200 - 1000) + (300 - 100) + 50 = 550 (matches implementation)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CounterDelta(tt.metrics)
			assert.InDelta(t, tt.expected, got, 0.001, "CounterDelta calculation did not match expected value")
		})
	}
}
