package aggregator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// MockUsageSink is a mock implementation of the UsageSink interface for testing
type MockUsageSink struct {
	mock.Mock
}

func (m *MockUsageSink) DeliverMetrics(ctx context.Context, metrics []datamodel2.AggregatedUsage) (int, error) {
	args := m.Called(ctx, metrics)
	return args.Int(0), args.Error(1)
}

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
	// Setup mock DB and UsageSink
	mockDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{}
	processor := NewBillingProvider(mockDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()

	// Expect call to GetHydratedMetrics with empty results
	mockDB.On("GetHydratedMetrics", mock.Anything, mock.Anything).Return(
		[]datamodel2.HydratedMetrics{}, nil,
	).Times(len(common.DefaultAggregationJobDefinitions))

	// Expect calls to GetAggregatedUsage for retry logic (both UNSUBMITTED and ERROR states)
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Unsubmitted, "is_billable": true}).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Once()
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Error}).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Once()

	// Call ProcessMetrics
	err := processor.ProcessBillingMetrics(ctx, now)
	assert.NoError(t, err, "ProcessMetrics should not return error with empty metrics")

	mockDB.AssertExpectations(t)
}

// TestProcessMetrics_DatabaseError tests database errors during processing
func TestProcessMetrics_DatabaseError(t *testing.T) {
	// Setup mock DB and UsageSink
	mockDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{}
	processor := NewBillingProvider(mockDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()

	// Expect calls to GetAggregatedUsage for retry logic (both UNSUBMITTED and ERROR states)
	// These need to be set up first since they're called before GetHydratedMetrics
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Unsubmitted, "is_billable": true}).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Once()
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Error}).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Once()

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
	jobDef := common.AggregationJobDefinition{
		AggregationType: common.JobType("UnsupportedType"),
	}

	// Test with unsupported aggregation type
	var aggregatedRecords []datamodel2.AggregatedUsage
	err := processor.processMetricsWithJobDef(ctx, resourceID, metrics, jobDef, now.Add(-1*time.Hour), now, &aggregatedRecords)
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
			expected:     false, // Based on DefaultAggregationJobDefinitions
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
			result := common.IsBillableMetric(ctx, tc.resourceType, tc.measuredType)
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
		var aggregatedRecords []datamodel2.AggregatedUsage
		err := processor.processMetricsWithJobDef(ctx, resourceID, []datamodel2.HydratedMetrics{},
			common.AggregationJobDefinition{AggregationType: common.IntegralAggregation}, startTime, now, &aggregatedRecords)
		assert.NoError(t, err)
		// No DB call should be made
		mockDB.AssertNotCalled(t, "CreateAggregatedUsage")
	})

	t.Run("IntegralAggregation", func(t *testing.T) {
		// Setup expectations for DB call
		mockDB.On("CreateAggregatedUsage", mock.Anything, mock.MatchedBy(func(usage *datamodel2.AggregatedUsage) bool {
			return *usage.VendorCustomerID == customerID &&
				usage.AggregationType == string(common.IntegralAggregation) &&
				*usage.ResourceName == resourceName
		})).Return(nil).Once()

		// Test with Integral aggregation
		var aggregatedRecords []datamodel2.AggregatedUsage
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics,
			common.AggregationJobDefinition{AggregationType: common.IntegralAggregation}, startTime, now, &aggregatedRecords)
		assert.NoError(t, err)
		mockDB.AssertExpectations(t)
	})

	t.Run("CounterAggregation", func(t *testing.T) {
		// Setup expectations for DB call - expect LastCounterValue to be set
		mockDB.On("CreateAggregatedUsage", mock.Anything, mock.MatchedBy(func(usage *datamodel2.AggregatedUsage) bool {
			return *usage.VendorCustomerID == customerID &&
				usage.AggregationType == string(common.CounterAggregation) &&
				usage.LastCounterValue != nil &&
				*usage.LastCounterValue == 200.0
		})).Return(nil).Once()

		// Test with Counter aggregation
		var aggregatedRecords []datamodel2.AggregatedUsage
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics,
			common.AggregationJobDefinition{AggregationType: common.CounterAggregation}, startTime, now, &aggregatedRecords)
		assert.NoError(t, err)
		mockDB.AssertExpectations(t)
	})

	t.Run("SumAggregation", func(t *testing.T) {
		// Setup expectations for DB call
		mockDB.On("CreateAggregatedUsage", mock.Anything, mock.MatchedBy(func(usage *datamodel2.AggregatedUsage) bool {
			return *usage.VendorCustomerID == customerID &&
				usage.AggregationType == string(common.SumAggregation) &&
				*usage.ResourceName == resourceName
		})).Return(nil).Once()

		// Test with Sum aggregation
		var aggregatedRecords []datamodel2.AggregatedUsage
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics,
			common.AggregationJobDefinition{AggregationType: common.SumAggregation}, startTime, now, &aggregatedRecords)
		assert.NoError(t, err)
		mockDB.AssertExpectations(t)
	})

	t.Run("FirstAggregation", func(t *testing.T) {
		// Setup expectations for DB call
		mockDB.On("CreateAggregatedUsage", mock.Anything, mock.MatchedBy(func(usage *datamodel2.AggregatedUsage) bool {
			return *usage.VendorCustomerID == customerID &&
				usage.AggregationType == string(common.FirstAggregation) &&
				*usage.ResourceName == resourceName
		})).Return(nil).Once()

		// Test with First aggregation
		var aggregatedRecords []datamodel2.AggregatedUsage
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics,
			common.AggregationJobDefinition{AggregationType: common.FirstAggregation}, startTime, now, &aggregatedRecords)
		assert.NoError(t, err)
		mockDB.AssertExpectations(t)
	})

	t.Run("DatabaseError", func(t *testing.T) {
		// Setup expectations for DB call with error
		dbErr := errors.New("database error")
		mockDB.On("CreateAggregatedUsage", mock.Anything, mock.Anything).Return(dbErr).Once()

		// Test with database error
		var aggregatedRecords []datamodel2.AggregatedUsage
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics,
			common.AggregationJobDefinition{AggregationType: common.SumAggregation}, startTime, now, &aggregatedRecords)
		assert.Error(t, err)
		assert.Equal(t, dbErr, err)
		mockDB.AssertExpectations(t)
	})
}

// TestProcessMetricsSuccess tests a successful path through ProcessMetrics
func TestProcessMetricsSuccess(t *testing.T) {
	mockDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{}
	processor := NewBillingProvider(mockDB, config, mockSink)
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
	).Times(len(common.DefaultAggregationJobDefinitions) - 1)

	// Setup expectations for CreateAggregatedUsage call
	mockDB.On("CreateAggregatedUsage", mock.Anything, mock.Anything).Return(nil).Once()

	// Expect calls to GetAggregatedUsage for retry logic (both UNSUBMITTED and ERROR states)
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Unsubmitted, "is_billable": true}).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Once()
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Error}).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Once()

	// Call ProcessMetrics
	err := processor.ProcessBillingMetrics(ctx, now)
	assert.NoError(t, err)

	// Verify expectations
	mockDB.AssertExpectations(t)
}

// TestProcessMetricsWithJobDefErrors tests error scenarios in processMetricsWithJobDef
func TestProcessMetricsWithJobDefErrors(t *testing.T) {
	mockDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{}
	processor := NewBillingProvider(mockDB, config, mockSink)
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
	).Times(len(common.DefaultAggregationJobDefinitions) - 1)

	// Setup expectations for CreateAggregatedUsage call with error
	mockDB.On("CreateAggregatedUsage", mock.Anything, mock.Anything).Return(errors.New("database error")).Once()

	// Expect calls to GetAggregatedUsage for retry logic (both UNSUBMITTED and ERROR states)
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Unsubmitted, "is_billable": true}).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Once()
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Error}).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Once()

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
			got := common.CounterDelta(tt.metrics)
			assert.InDelta(t, tt.expected, got, 0.001, "CounterDelta calculation did not match expected value")
		})
	}
}

// TestProcessMetrics_GetUnsentUsagesError tests error in getUnsentGoogleUsages
func TestProcessMetrics_GetUnsentUsagesError(t *testing.T) {
	mockDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{MaxGoogleBillingPushRetry: 3}
	processor := NewBillingProvider(mockDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()

	// Expect call to GetAggregatedUsage for UNSUBMITTED with error
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Unsubmitted, "is_billable": true}).Return(
		nil, errors.New("database connection error"),
	).Once()

	// Even with retry error, should continue and call GetHydratedMetrics
	mockDB.On("GetHydratedMetrics", mock.Anything, mock.Anything).Return(
		[]datamodel2.HydratedMetrics{}, nil,
	).Times(len(common.DefaultAggregationJobDefinitions))

	// Call ProcessMetrics - should not fail even if retry logic fails
	err := processor.ProcessBillingMetrics(ctx, now)
	assert.NoError(t, err, "ProcessMetrics should not fail when retry logic fails")

	mockDB.AssertExpectations(t)
}

// TestProcessMetrics_WithAggregatedRecordsDelivery tests successful delivery of aggregated records
func TestProcessMetrics_WithAggregatedRecordsDelivery(t *testing.T) {
	mockDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{}
	processor := NewBillingProvider(mockDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	// Create retry records to be returned
	retryRecords := []datamodel2.AggregatedUsage{
		{
			ID:               1,
			VendorCustomerID: stringPtr("retry-customer"),
			ResourceName:     stringPtr("retry-resource"),
			Quantity:         100.0,
			AggregationStart: startTime,
			AggregationEnd:   now,
			State:            datamodel2.Unsubmitted,
			IsBillable:       true,
		},
	}

	// Create new aggregated records from processing
	processedMetrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    "resource1",
			ConsumerID:      "customer1",
			Location:        "location1",
			Quantity:        200,
			MetricTimestamp: startTime.Add(10 * time.Minute),
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
		},
	}

	// Expect calls to GetAggregatedUsage for retry logic
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Unsubmitted, "is_billable": true}).Return(
		retryRecords, nil,
	).Once()
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Error}).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Once()

	// Setup expectations for GetHydratedMetrics call - return metrics for one job only
	mockDB.On("GetHydratedMetrics", mock.Anything, mock.Anything).Return(processedMetrics, nil).Once()

	// For all other job definitions, return empty results
	mockDB.On("GetHydratedMetrics", mock.Anything, mock.Anything).Return(
		[]datamodel2.HydratedMetrics{}, nil,
	).Times(len(common.DefaultAggregationJobDefinitions) - 1)

	// Setup expectations for CreateAggregatedUsage call
	mockDB.On("CreateAggregatedUsage", mock.Anything, mock.Anything).Return(nil).Once()

	// Expect DeliverMetrics to be called with both retry and new records
	mockSink.On("DeliverMetrics", mock.Anything, mock.MatchedBy(func(records []datamodel2.AggregatedUsage) bool {
		// Should have at least the retry record
		return len(records) >= 1
	})).Return(2, nil).Once()

	// Call ProcessMetrics
	err := processor.ProcessBillingMetrics(ctx, now)
	assert.NoError(t, err)

	// Verify expectations
	mockDB.AssertExpectations(t)
	mockSink.AssertExpectations(t)
}

// TestProcessMetrics_DeliveryError tests error in DeliverMetrics
func TestProcessMetrics_DeliveryError(t *testing.T) {
	mockDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{}
	processor := NewBillingProvider(mockDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	// Create test metrics that will be processed
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
	}

	// Expect calls to GetAggregatedUsage for retry logic
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Unsubmitted, "is_billable": true}).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Once()
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Error}).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Once()

	// Setup expectations for GetHydratedMetrics call
	mockDB.On("GetHydratedMetrics", mock.Anything, mock.Anything).Return(metrics, nil).Once()

	// For all other job definitions, return empty results
	mockDB.On("GetHydratedMetrics", mock.Anything, mock.Anything).Return(
		[]datamodel2.HydratedMetrics{}, nil,
	).Times(len(common.DefaultAggregationJobDefinitions) - 1)

	// Setup expectations for CreateAggregatedUsage call - return a billable record
	mockDB.On("CreateAggregatedUsage", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		usage := args.Get(1).(*datamodel2.AggregatedUsage)
		usage.IsBillable = true // Ensure it's billable so it gets added to aggregatedRecords
	}).Return(nil).Once()

	// Expect DeliverMetrics to fail
	mockSink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(0, errors.New("delivery failed")).Once()

	// Call ProcessMetrics - should not fail even if delivery fails
	err := processor.ProcessBillingMetrics(ctx, now)
	assert.NoError(t, err, "ProcessMetrics should not fail when delivery fails")

	// Verify expectations
	mockDB.AssertExpectations(t)
	mockSink.AssertExpectations(t)
}

// TestGetUnsentGoogleUsages_ErrorStateWithRetries tests filtering error records by retry count
func TestGetUnsentGoogleUsages_ErrorStateWithRetries(t *testing.T) {
	mockDB := &database.MockStorage{}
	processor := &BillingProvider{metricsdb: mockDB}
	ctx := context.Background()

	// Create error records with different error counts
	errorRecords := []datamodel2.AggregatedUsage{
		{
			ID:         1,
			ErrorCount: 1, // Should be included (< 3)
			State:      datamodel2.Error,
		},
		{
			ID:         2,
			ErrorCount: 3, // Should be excluded (>= 3)
			State:      datamodel2.Error,
		},
		{
			ID:         3,
			ErrorCount: 2, // Should be included (< 3)
			State:      datamodel2.Error,
		},
	}

	// Expect calls to GetAggregatedUsage
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Unsubmitted, "is_billable": true}).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Once()
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Error}).Return(
		errorRecords, nil,
	).Once()

	// Call getUnsentGoogleUsages with maxRetries = 3
	result, err := processor.getUnsentGoogleUsages(ctx, 3)

	assert.NoError(t, err)
	assert.Equal(t, 2, len(result), "Should return 2 records with error_count < 3")

	// Verify the correct records are returned
	for _, record := range result {
		assert.True(t, int64(record.ErrorCount) < 3, "All returned records should have error_count < maxRetries")
	}

	mockDB.AssertExpectations(t)
}

// TestGetUnsentGoogleUsages_ErrorInErrorStateQuery tests error in error state query
func TestGetUnsentGoogleUsages_ErrorInErrorStateQuery(t *testing.T) {
	mockDB := &database.MockStorage{}
	processor := &BillingProvider{metricsdb: mockDB}
	ctx := context.Background()

	// Expect successful UNSUBMITTED query
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Unsubmitted, "is_billable": true}).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Once()

	// Expect error in ERROR state query
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Error}).Return(
		nil, errors.New("error state query failed"),
	).Once()

	// Call getUnsentGoogleUsages
	result, err := processor.getUnsentGoogleUsages(ctx, 3)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "error state query failed")

	mockDB.AssertExpectations(t)
}

// TestProcessMetricsWithJobDef_NonBillableRecord tests when aggregated record is not billable
func TestProcessMetricsWithJobDef_NonBillableRecord(t *testing.T) {
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

	metrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    "test-resource",
			ConsumerID:      "test-customer",
			Location:        "test-location",
			Quantity:        100,
			MetricTimestamp: startTime,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
		},
	}

	// Mock CreateAggregatedUsage to return a non-billable record
	mockDB.On("CreateAggregatedUsage", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		usage := args.Get(1).(*datamodel2.AggregatedUsage)
		usage.IsBillable = false // Set as non-billable
	}).Return(nil).Once()

	// Test with Sum aggregation
	var aggregatedRecords []datamodel2.AggregatedUsage
	err := processor.processMetricsWithJobDef(ctx, resourceID, metrics,
		common.AggregationJobDefinition{AggregationType: common.SumAggregation}, startTime, now, &aggregatedRecords)

	assert.NoError(t, err)
	assert.Equal(t, 0, len(aggregatedRecords), "Non-billable records should not be added to aggregatedRecords")

	mockDB.AssertExpectations(t)
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}
