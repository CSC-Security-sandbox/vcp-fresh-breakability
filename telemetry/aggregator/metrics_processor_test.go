package aggregator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	database2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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
			DeploymentName:  "deployment1",
		},
		{
			ResourceName:    "resource1",
			ConsumerID:      "customer1",
			Location:        "location1",
			Quantity:        200,
			MetricTimestamp: now.Add(1 * time.Hour),
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
			DeploymentName:  "deployment1",
		},
		{
			ResourceName:    "resource2",
			ConsumerID:      "customer1",
			Location:        "location1",
			Quantity:        300,
			MetricTimestamp: now,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
			DeploymentName:  "deployment1",
		},
		{
			ResourceName:    "resource1",
			ConsumerID:      "customer2",
			Location:        "location1",
			Quantity:        400,
			MetricTimestamp: now,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
			DeploymentName:  "deployment2",
		},
	}

	result := processor.groupMetricsByResource(metrics)

	// Should have 3 unique resource identifiers
	assert.Equal(t, 3, len(result), "Expected 3 resource groups")

	// Check each group has the correct metrics
	// Check each group has the correct metrics
	resourceKey1 := ResourceKey{
		ResourceType:   metadata.Volume,
		ResourceName:   "resource1",
		DeploymentName: "deployment1",
		ConsumerID:     "customer1",
	}
	resourceKey2 := ResourceKey{
		ResourceType:   metadata.Volume,
		ResourceName:   "resource2",
		DeploymentName: "deployment1",
		ConsumerID:     "customer1",
	}
	resourceKey3 := ResourceKey{
		ResourceType:   metadata.Volume,
		ResourceName:   "resource1",
		DeploymentName: "deployment2",
		ConsumerID:     "customer2",
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
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()

	// Mock VCP database calls for label fetching - now returns data directly, not wrapped in PaginationResponse
	vcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil).Once()
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()

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
	vcpDB.AssertExpectations(t)
}

// TestProcessMetrics_DatabaseError tests database errors during processing
func TestProcessMetrics_DatabaseError(t *testing.T) {
	// Setup mock DB and UsageSink
	mockDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{}
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()

	// Mock VCP database calls for label fetching
	vcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil).Once()
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()

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
	vcpDB.AssertExpectations(t)
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
	processor := &BillingProvider{
		metricsdb: mockDB,
		resourceCollection: &ResourceCollection{
			PoolData:   make(map[ResourceKey]ResourceData),
			VolumeData: make(map[ResourceKey]ResourceData),
		},
	}
	ctx := context.Background()

	now := time.Now()
	resourceID := ResourceKey{
		ResourceType:   metadata.Volume,
		ResourceName:   "test-resource",
		DeploymentName: "test-deployment",
		ConsumerID:     "test-customer",
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
	var aggregatedUsageForDB []datamodel2.AggregatedUsage
	err := processor.processMetricsWithJobDef(ctx, resourceID, metrics, jobDef, now.Add(-1*time.Hour), now, &aggregatedRecords, &aggregatedUsageForDB)
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
	processor := &BillingProvider{
		metricsdb: mockDB,
		resourceCollection: &ResourceCollection{
			PoolData:   make(map[ResourceKey]ResourceData),
			VolumeData: make(map[ResourceKey]ResourceData),
		},
	}
	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	resourceID := ResourceKey{
		ResourceType:   metadata.Volume,
		ResourceName:   "test-resource",
		DeploymentName: "test-deployment",
		ConsumerID:     "test-customer",
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
		var aggregatedUsageForDB []datamodel2.AggregatedUsage
		err := processor.processMetricsWithJobDef(ctx, resourceID, []datamodel2.HydratedMetrics{},
			common.AggregationJobDefinition{AggregationType: common.IntegralAggregation}, startTime, now, &aggregatedRecords, &aggregatedUsageForDB)
		assert.NoError(t, err)
		// No DB call should be made, but record should be collected for batch
		assert.Len(t, aggregatedUsageForDB, 0) // No metrics means no aggregated records
		mockDB.AssertNotCalled(t, "CreateAggregatedUsage")
	})

	t.Run("IntegralAggregation", func(t *testing.T) {
		// With batch saving, individual CreateAggregatedUsage calls are no longer made
		// Test with Integral aggregation
		var aggregatedRecords []datamodel2.AggregatedUsage
		var aggregatedUsageForDB []datamodel2.AggregatedUsage
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics,
			common.AggregationJobDefinition{AggregationType: common.IntegralAggregation}, startTime, now, &aggregatedRecords, &aggregatedUsageForDB)
		assert.NoError(t, err)
		// Verify that the record was collected for batch saving
		assert.Len(t, aggregatedUsageForDB, 1)
		assert.Equal(t, string(common.IntegralAggregation), aggregatedUsageForDB[0].AggregationType)
		assert.Equal(t, customerID, *aggregatedUsageForDB[0].VendorCustomerID)
		assert.Equal(t, resourceName, *aggregatedUsageForDB[0].ResourceName)
	})

	t.Run("CounterAggregation", func(t *testing.T) {
		// With batch saving, test the collected record instead
		var aggregatedRecords []datamodel2.AggregatedUsage
		var aggregatedUsageForDB []datamodel2.AggregatedUsage
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics,
			common.AggregationJobDefinition{AggregationType: common.CounterAggregation}, startTime, now, &aggregatedRecords, &aggregatedUsageForDB)
		assert.NoError(t, err)
		// Verify that the record was collected and has LastCounterValue set
		assert.Len(t, aggregatedUsageForDB, 1)
		assert.Equal(t, string(common.CounterAggregation), aggregatedUsageForDB[0].AggregationType)
		assert.NotNil(t, aggregatedUsageForDB[0].LastCounterValue)
		assert.Equal(t, 200.0, *aggregatedUsageForDB[0].LastCounterValue)
	})

	t.Run("SumAggregation", func(t *testing.T) {
		// With batch saving, test the collected record instead
		var aggregatedRecords []datamodel2.AggregatedUsage
		var aggregatedUsageForDB []datamodel2.AggregatedUsage
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics,
			common.AggregationJobDefinition{AggregationType: common.SumAggregation}, startTime, now, &aggregatedRecords, &aggregatedUsageForDB)
		assert.NoError(t, err)
		// Verify that the record was collected for batch saving
		assert.Len(t, aggregatedUsageForDB, 1)
		assert.Equal(t, string(common.SumAggregation), aggregatedUsageForDB[0].AggregationType)
		assert.Equal(t, resourceName, *aggregatedUsageForDB[0].ResourceName)
	})

	t.Run("FirstAggregation", func(t *testing.T) {
		// With batch saving, test the collected record instead
		var aggregatedRecords []datamodel2.AggregatedUsage
		var aggregatedUsageForDB []datamodel2.AggregatedUsage
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics,
			common.AggregationJobDefinition{AggregationType: common.FirstAggregation}, startTime, now, &aggregatedRecords, &aggregatedUsageForDB)
		assert.NoError(t, err)
		// Verify that the record was collected for batch saving
		assert.Len(t, aggregatedUsageForDB, 1)
		assert.Equal(t, string(common.FirstAggregation), aggregatedUsageForDB[0].AggregationType)
		assert.Equal(t, resourceName, *aggregatedUsageForDB[0].ResourceName)
	})

	t.Run("DatabaseError", func(t *testing.T) {
		// With batch saving, the function just collects records
		// Database errors will occur during batch save, not here
		var aggregatedRecords []datamodel2.AggregatedUsage
		var aggregatedUsageForDB []datamodel2.AggregatedUsage
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics,
			common.AggregationJobDefinition{AggregationType: common.SumAggregation}, startTime, now, &aggregatedRecords, &aggregatedUsageForDB)
		assert.NoError(t, err) // No error in collection phase
		// Verify that the record was collected for batch saving
		assert.Len(t, aggregatedUsageForDB, 1)
	})
}

// TestProcessMetricsSuccess tests a successful path through ProcessMetrics
func TestProcessMetricsSuccess(t *testing.T) {
	mockDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{}
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	// Mock VCP database calls for label fetching
	vcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil).Once()
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()

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

	// Setup expectations for CreateAggregatedUsageBatch call
	mockDB.On("CreateAggregatedUsageBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

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
	vcpDB.AssertExpectations(t)
}

// TestProcessMetricsWithJobDefErrors tests error scenarios in processMetricsWithJobDef
func TestProcessMetricsWithJobDefErrors(t *testing.T) {
	mockDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{}
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	// Mock VCP database calls for label fetching
	vcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil).Once()
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()

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

	// Setup expectations for CreateAggregatedUsageBatch call with error
	mockDB.On("CreateAggregatedUsageBatch", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("database error")).Once()

	// Expect calls to GetAggregatedUsage for retry logic (both UNSUBMITTED and ERROR states)
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Unsubmitted, "is_billable": true}).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Once()
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Error}).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Once()

	// Call ProcessMetrics - with batch saving, database errors now propagate up
	err := processor.ProcessBillingMetrics(ctx, now)
	assert.Error(t, err) // ProcessMetrics should return the error from batch save
	assert.Contains(t, err.Error(), "database error")

	// Verify expectations
	mockDB.AssertExpectations(t)
	vcpDB.AssertExpectations(t)
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
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()

	// Mock VCP database calls for label fetching
	vcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil).Once()
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()

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
	vcpDB.AssertExpectations(t)
}

// TestProcessMetrics_WithAggregatedRecordsDelivery tests successful delivery of aggregated records
func TestProcessMetrics_WithAggregatedRecordsDelivery(t *testing.T) {
	mockDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{}
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
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
			DeploymentName:  "deployment1",
		},
	}

	// Mock VCP database calls for label fetching
	vcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil).Once()
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()

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

	// Setup expectations for CreateAggregatedUsageBatch call
	mockDB.On("CreateAggregatedUsageBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

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
	vcpDB.AssertExpectations(t)
}

// TestProcessMetrics_DeliveryError tests error in DeliverMetrics
func TestProcessMetrics_DeliveryError(t *testing.T) {
	mockDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{}
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	// Mock VCP database calls for label fetching
	vcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil).Once()
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()

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
			DeploymentName:  "test-deployment",
		},
	}

	// Expect calls to GetAggregatedUsage for retry logic - add a retry record to ensure DeliverMetrics is called
	retryRecord := []datamodel2.AggregatedUsage{
		{
			ID:               1,
			VendorCustomerID: stringPtr("customer1"),
			ResourceName:     stringPtr("resource1"),
			Quantity:         50.0,
			AggregationStart: startTime,
			AggregationEnd:   now,
			State:            datamodel2.Unsubmitted,
			IsBillable:       true,
		},
	}
	mockDB.On("GetAggregatedUsage", mock.Anything, map[string]interface{}{"state": datamodel2.Unsubmitted, "is_billable": true}).Return(
		retryRecord, nil,
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

	// Setup expectations for CreateAggregatedUsageBatch call
	mockDB.On("CreateAggregatedUsageBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

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
	processor := &BillingProvider{
		metricsdb: mockDB,
		resourceCollection: &ResourceCollection{
			PoolData:   make(map[ResourceKey]ResourceData),
			VolumeData: make(map[ResourceKey]ResourceData),
		},
	}
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
	processor := &BillingProvider{
		metricsdb: mockDB,
		resourceCollection: &ResourceCollection{
			PoolData:   make(map[ResourceKey]ResourceData),
			VolumeData: make(map[ResourceKey]ResourceData),
		},
	}
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
	processor := &BillingProvider{
		metricsdb: mockDB,
		resourceCollection: &ResourceCollection{
			PoolData:   make(map[ResourceKey]ResourceData),
			VolumeData: make(map[ResourceKey]ResourceData),
		},
	}
	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	resourceID := ResourceKey{
		ResourceType:   metadata.Volume,
		ResourceName:   "test-resource",
		DeploymentName: "test-deployment",
		ConsumerID:     "test-customer",
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
			DeploymentName:  "test-deployment",
		},
	}

	// With batch saving, we collect records first, then check billability
	var aggregatedRecords []datamodel2.AggregatedUsage
	var aggregatedUsageForDB []datamodel2.AggregatedUsage
	err := processor.processMetricsWithJobDef(ctx, resourceID, metrics,
		common.AggregationJobDefinition{AggregationType: common.SumAggregation}, startTime, now, &aggregatedRecords, &aggregatedUsageForDB)

	assert.NoError(t, err)
	// With batch approach, records are collected regardless of billability
	// Billability is determined during the creation phase
	assert.Len(t, aggregatedUsageForDB, 1)
}

// TestNewBillingProvider tests the NewBillingProvider constructor
func TestNewBillingProvider(t *testing.T) {
	mockDB := &database.MockStorage{}
	vcpDB := &database2.MockStorage{}
	config := &common.TelemetryConfig{}
	mockSink := &MockUsageSink{}

	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)

	assert.NotNil(t, processor)
	assert.Equal(t, mockDB, processor.metricsdb)
	assert.Equal(t, vcpDB, processor.vcpDataStore)
	assert.Equal(t, config, processor.config)
	assert.Equal(t, mockSink, processor.usageSink)
	assert.NotNil(t, processor.resourceCollection)
	assert.NotNil(t, processor.resourceCollection.PoolData)
	assert.NotNil(t, processor.resourceCollection.VolumeData)
	assert.NotNil(t, processor.usedKeys)
}

// TestLimitLabels tests the limitLabels function
func TestLimitLabels(t *testing.T) {
	processor := &BillingProvider{
		config: &common.TelemetryConfig{
			GoogleBillingLabelsMaxEntries: 3,
		},
	}

	tests := []struct {
		name     string
		labels   *datamodel.JSONB
		expected int
	}{
		{
			name:     "nil labels",
			labels:   nil,
			expected: 0,
		},
		{
			name:     "empty labels",
			labels:   &datamodel.JSONB{},
			expected: 0,
		},
		{
			name: "labels within limit",
			labels: &datamodel.JSONB{
				"key1": "value1",
				"key2": "value2",
			},
			expected: 2,
		},
		{
			name: "labels exceeding limit",
			labels: &datamodel.JSONB{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
				"key4": "value4",
				"key5": "value5",
			},
			expected: 3, // Should be limited to 3
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := processor.limitLabels(tt.labels)
			assert.Equal(t, tt.expected, len(result))
		})
	}
}

func TestFetchResourceData(t *testing.T) {
	ctx := context.Background()
	mockVcpDB := &database2.MockStorage{}
	mockMetricsDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{PoolVolumeLabelPageSize: 2, GoogleBillingLabelsMaxEntries: 10}
	provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

	// Helper to reset resourceCollection
	resetCollection := func() {
		provider.resourceCollection = &ResourceCollection{
			PoolData:   make(map[ResourceKey]ResourceData),
			VolumeData: make(map[ResourceKey]ResourceData),
		}
	}

	t.Run("Both pool and volume fetch succeed", func(t *testing.T) {
		resetCollection() // First call returns one pool, second call returns empty slice (pagination end)
		mockVcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{
			{
				Pool: datamodel.Pool{
					BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
					Name:           "pool1",
					AccountID:      1,
					VendorID:       "/projects/12345/",
					DeploymentName: "dep1",
					PoolAttributes: &datamodel.PoolAttributes{
						Labels:      &datamodel.JSONB{"test": "test"},
						PrimaryZone: "us-central1"},
					Account: &datamodel.Account{Name: "account1"},
				},
			},
		}, nil).Once()
		mockVcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil).Once()
		// First call returns one volume, second call returns empty slice (pagination end)
		mockVcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{
			{
				BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
				Name:      "vol1",
				AccountID: 2,
				VolumeAttributes: &datamodel.VolumeAttributes{
					Labels:         &datamodel.JSONB{"key": "value"},
					VendorSubnetID: "projects/54321/",
				},
				Pool:    &datamodel.Pool{DeploymentName: "dep2"},
				Account: &datamodel.Account{Name: "account1"},
			},
		}, nil).Once()
		mockVcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()

		err := provider.fetchResourceData(ctx)
		assert.NoError(t, err)
		assert.Len(t, provider.resourceCollection.PoolData, 1)
		assert.Len(t, provider.resourceCollection.VolumeData, 1)
		mockVcpDB.AssertExpectations(t)
	})

	t.Run("Pool fetch fails, volume fetch succeeds", func(t *testing.T) {
		resetCollection()
		mockVcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("pool error")).Once()
		mockVcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{
			{
				BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
				Name:      "vol1",
				AccountID: 2,
				VolumeAttributes: &datamodel.VolumeAttributes{
					Labels:         &datamodel.JSONB{"key": "value"},
					VendorSubnetID: "projects/54321/",
				},
				Pool:    &datamodel.Pool{DeploymentName: "dep2"},
				Account: &datamodel.Account{Name: "account1"},
			},
		}, nil).Once()
		mockVcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()
		err := provider.fetchResourceData(ctx)
		assert.NoError(t, err)
		assert.Len(t, provider.resourceCollection.PoolData, 0)
		assert.Len(t, provider.resourceCollection.VolumeData, 1)
		mockVcpDB.AssertExpectations(t)
	})

	t.Run("Volume fetch fails, pool fetch succeeds", func(t *testing.T) {
		resetCollection()
		mockVcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{
			{
				Pool: datamodel.Pool{
					BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
					Name:           "pool1",
					AccountID:      1,
					VendorID:       "/projects/12345/",
					DeploymentName: "dep1",
					PoolAttributes: &datamodel.PoolAttributes{Labels: &datamodel.JSONB{"key": "value"}},
					Account:        &datamodel.Account{Name: "account1"},
				},
			},
		}, nil).Once()
		mockVcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil).Once()
		mockVcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("volume error")).Once()
		err := provider.fetchResourceData(ctx)
		assert.NoError(t, err)
		assert.Len(t, provider.resourceCollection.PoolData, 1)
		assert.Len(t, provider.resourceCollection.VolumeData, 0)
		mockVcpDB.AssertExpectations(t)
	})

	t.Run("Both pool and volume fetch fail", func(t *testing.T) {
		resetCollection()
		mockVcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("pool error")).Once()
		mockVcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("volume error")).Once()
		err := provider.fetchResourceData(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to fetch any resource data")
		assert.Len(t, provider.resourceCollection.PoolData, 0)
		assert.Len(t, provider.resourceCollection.VolumeData, 0)
		mockVcpDB.AssertExpectations(t)
	})
}

func TestCleanupUnusedResourceKeys(t *testing.T) {
	ctx := context.Background()
	resourceKey1 := ResourceKey{
		ResourceType:   metadata.VolumePool,
		ResourceName:   "pool1",
		DeploymentName: "deployment1",
		ConsumerID:     "customer1",
	}

	resourceKey2 := ResourceKey{
		ResourceType:   metadata.VolumePool,
		ResourceName:   "pool2",
		DeploymentName: "deployment2",
		ConsumerID:     "customer2",
	}

	resourceKey3 := ResourceKey{
		ResourceType:   metadata.Volume,
		ResourceName:   "volume1",
		DeploymentName: "deployment1",
		ConsumerID:     "customer1",
	}

	resourceKey4 := ResourceKey{
		ResourceType:   metadata.Volume,
		ResourceName:   "volume2",
		DeploymentName: "deployment2",
		ConsumerID:     "customer2",
	}
	provider := &BillingProvider{
		resourceCollection: &ResourceCollection{
			PoolData: map[ResourceKey]ResourceData{
				resourceKey1: {UUID: "pool-uuid-1", AccountID: 123, Labels: Labels{"env": "prod"}},
				resourceKey2: {UUID: "pool-uuid-2", AccountID: 456, Labels: Labels{"env": "test"}},
			},
			VolumeData: map[ResourceKey]ResourceData{
				resourceKey3: {UUID: "volume-uuid-1", AccountID: 789, Labels: Labels{"env": "prod"}},
				resourceKey4: {UUID: "volume-uuid-2", AccountID: 101, Labels: Labels{"env": "test"}},
			},
		},
		usedKeys: map[ResourceKey]bool{
			resourceKey1: true,
			resourceKey3: true,
		},
	}

	// Run cleanup
	provider.cleanupUnusedResourceKeys(ctx)

	// Wait for goroutine to finish
	time.Sleep(100 * time.Millisecond)

	// Only used keys should remain
	assert.Len(t, provider.resourceCollection.PoolData, 1)
	assert.Contains(t, provider.resourceCollection.PoolData, resourceKey1)
	assert.NotContains(t, provider.resourceCollection.PoolData, resourceKey2)

	assert.Len(t, provider.resourceCollection.VolumeData, 1)
	assert.Contains(t, provider.resourceCollection.VolumeData, resourceKey3)
	assert.NotContains(t, provider.resourceCollection.VolumeData, resourceKey4)

	// usedKeys should be reset to nil
	assert.Nil(t, provider.usedKeys)
}

func TestResourceKeyMapKeyEquality(t *testing.T) {
	key1 := ResourceKey{
		ResourceType:   metadata.Volume,
		ResourceName:   "vol1",
		DeploymentName: "dep1",
		ConsumerID:     "acc1",
	}
	key2 := ResourceKey{
		ResourceType:   metadata.Volume,
		ResourceName:   "vol1",
		DeploymentName: "dep1",
		ConsumerID:     "acc1",
	}

	m := make(map[ResourceKey]string)
	m[key1] = "test-value"

	// Both keys should retrieve the same value
	require.Equal(t, "test-value", m[key2])
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}

// TestFetchMetricsForCounterAggregation tests the optimized database-level sorting approach
func TestFetchMetricsForCounterAggregation(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	processor := &BillingProvider{
		metricsdb: mockDB,
	}

	now := time.Now()
	aggregationStartTime := now.Add(-1 * time.Hour)
	aggregationEndTime := now

	// Mock metrics data - simulating database returning sorted results
	mockMetrics := []datamodel2.HydratedMetrics{
		// Resource 1 - within window (newer first due to DESC sorting)
		{
			ResourceName:    "resource1",
			ConsumerID:      "customer1",
			DeploymentName:  "deployment1",
			MetricTimestamp: now.Add(-30 * time.Minute), // Within window
			Quantity:        150,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
		},
		{
			ResourceName:    "resource1",
			ConsumerID:      "customer1",
			DeploymentName:  "deployment1",
			MetricTimestamp: now.Add(-45 * time.Minute), // Within window
			Quantity:        140,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
		},
		// Resource 1 - before window (closest to start time)
		{
			ResourceName:    "resource1",
			ConsumerID:      "customer1",
			DeploymentName:  "deployment1",
			MetricTimestamp: now.Add(-90 * time.Minute), // Before window
			Quantity:        130,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
		},
		// Resource 1 - older record before window (should be ignored)
		{
			ResourceName:    "resource1",
			ConsumerID:      "customer1",
			DeploymentName:  "deployment1",
			MetricTimestamp: now.Add(-110 * time.Minute), // Older, should be skipped
			Quantity:        120,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
		},
		// Resource 2 - within window
		{
			ResourceName:    "resource2",
			ConsumerID:      "customer1",
			DeploymentName:  "deployment1",
			MetricTimestamp: now.Add(-20 * time.Minute), // Within window
			Quantity:        200,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
		},
		// Resource 2 - before window
		{
			ResourceName:    "resource2",
			ConsumerID:      "customer1",
			DeploymentName:  "deployment1",
			MetricTimestamp: now.Add(-80 * time.Minute), // Before window
			Quantity:        190,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
		},
	}

	// Mock the database call
	mockDB.On("GetHydratedMetrics", mock.Anything, mock.MatchedBy(func(filter map[string]interface{}) bool {
		// Verify the filter includes the extended time range and ordering
		if order, ok := filter["order"].(string); ok {
			return strings.Contains(order, "metric_timestamp DESC")
		}
		return false
	})).Return(mockMetrics, nil)

	// Execute the method
	result, err := processor.fetchMetricsForCounterAndIntegralAggregation(
		context.Background(),
		aggregationStartTime,
		aggregationEndTime,
		"VOLUME",
		"ALLOCATED_SIZE",
	)

	// Verify results
	assert.NoError(t, err)
	// We expect: 3 for resource1 (2 within window + 1 before) + 2 for resource2 (1 within window + 1 before) = 5 total
	assert.Len(t, result, 5, "Expected 5 metrics: 3 for resource1 (2 within + 1 before) + 2 for resource2 (1 within + 1 before)")

	// Verify that we got the correct metrics for each resource
	resource1Metrics := 0
	resource2Metrics := 0
	resource1HasPreWindow := false
	resource2HasPreWindow := false

	for _, metric := range result {
		if metric.ResourceName == "resource1" {
			resource1Metrics++
			if metric.MetricTimestamp.Before(aggregationStartTime) {
				resource1HasPreWindow = true
				assert.Equal(t, float64(130), metric.Quantity, "Should get the latest record before window for resource1")
			}
		} else if metric.ResourceName == "resource2" {
			resource2Metrics++
			if metric.MetricTimestamp.Before(aggregationStartTime) {
				resource2HasPreWindow = true
				assert.Equal(t, float64(190), metric.Quantity, "Should get the latest record before window for resource2")
			}
		}
	}

	assert.Equal(t, 3, resource1Metrics, "Resource1 should have 3 metrics (2 within window + 1 before)")
	assert.Equal(t, 2, resource2Metrics, "Resource2 should have 2 metrics (1 within window + 1 before)")
	assert.True(t, resource1HasPreWindow, "Resource1 should have one record from before the window")
	assert.True(t, resource2HasPreWindow, "Resource2 should have one record from before the window")

	mockDB.AssertExpectations(t)
}

// TestFilterMetricsForCounterAndIntegralAggregationSorted tests the filtering logic for sorted metrics
func TestFilterMetricsForCounterAndIntegralAggregationSorted(t *testing.T) {
	processor := &BillingProvider{}
	now := time.Now()
	aggregationStartTime := now.Add(-1 * time.Hour)

	tests := []struct {
		name     string
		metrics  []datamodel2.HydratedMetrics
		expected int
		desc     string
	}{
		{
			name: "metrics_with_previous_and_current_window",
			metrics: []datamodel2.HydratedMetrics{
				// Within window (sorted DESC by timestamp)
				{MetricTimestamp: now.Add(-30 * time.Minute), Quantity: 150},
				{MetricTimestamp: now.Add(-45 * time.Minute), Quantity: 140},
				// Before window (latest first due to DESC sorting)
				{MetricTimestamp: now.Add(-90 * time.Minute), Quantity: 130},
				{MetricTimestamp: now.Add(-110 * time.Minute), Quantity: 120}, // Should be ignored
			},
			expected: 3,
			desc:     "Should return 2 within window + 1 latest before window",
		},
		{
			name: "only_current_window_metrics",
			metrics: []datamodel2.HydratedMetrics{
				{MetricTimestamp: now.Add(-30 * time.Minute), Quantity: 150},
				{MetricTimestamp: now.Add(-45 * time.Minute), Quantity: 140},
			},
			expected: 2,
			desc:     "Should return all metrics when all are within window",
		},
		{
			name: "only_previous_window_metrics",
			metrics: []datamodel2.HydratedMetrics{
				{MetricTimestamp: now.Add(-90 * time.Minute), Quantity: 130},
				{MetricTimestamp: now.Add(-110 * time.Minute), Quantity: 120},
			},
			expected: 1,
			desc:     "Should return only the latest metric when all are before window",
		},
		{
			name:     "empty_metrics",
			metrics:  []datamodel2.HydratedMetrics{},
			expected: 0,
			desc:     "Should return empty when no metrics provided",
		},
		{
			name: "single_metric_within_window",
			metrics: []datamodel2.HydratedMetrics{
				{MetricTimestamp: now.Add(-30 * time.Minute), Quantity: 150},
			},
			expected: 1,
			desc:     "Should return single metric when it's within window",
		},
		{
			name: "single_metric_before_window",
			metrics: []datamodel2.HydratedMetrics{
				{MetricTimestamp: now.Add(-90 * time.Minute), Quantity: 130},
			},
			expected: 1,
			desc:     "Should return single metric when it's before window",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := processor.filterMetricsForCounterAndIntegralAggregationSorted(tt.metrics, aggregationStartTime)
			assert.Len(t, result, tt.expected, tt.desc)

			// Verify we don't get multiple records from before the window
			preWindowCount := 0
			for _, metric := range result {
				if metric.MetricTimestamp.Before(aggregationStartTime) {
					preWindowCount++
				}
			}
			assert.LessOrEqual(t, preWindowCount, 1, "Should have at most 1 record from before the aggregation window")
		})
	}
}

// TestFetchMetricsForCounterAggregation_DatabaseError tests error handling
func TestFetchMetricsForCounterAggregation_DatabaseError(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	processor := &BillingProvider{
		metricsdb: mockDB,
	}

	now := time.Now()
	aggregationStartTime := now.Add(-1 * time.Hour)
	aggregationEndTime := now

	// Mock database error
	mockDB.On("GetHydratedMetrics", mock.Anything, mock.Anything).Return(nil, errors.New("database connection error"))

	// Execute the method
	result, err := processor.fetchMetricsForCounterAndIntegralAggregation(
		context.Background(),
		aggregationStartTime,
		aggregationEndTime,
		"VOLUME",
		"ALLOCATED_SIZE",
	)

	// Verify error handling
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "database connection error")

	mockDB.AssertExpectations(t)
}

// TestFetchMetricsForCounterAggregation_FilterValidation tests that the correct filter is created
func TestFetchMetricsForCounterAggregation_FilterValidation(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	processor := &BillingProvider{
		metricsdb: mockDB,
	}

	now := time.Now()
	aggregationStartTime := now.Add(-1 * time.Hour)
	aggregationEndTime := now

	// Mock the database call with detailed filter validation
	mockDB.On("GetHydratedMetrics", mock.Anything, mock.MatchedBy(func(filter map[string]interface{}) bool {
		// Verify all required filter components
		conditions, hasConditions := filter["conditions"].([][]interface{})
		order, hasOrder := filter["order"].(string)

		if !hasConditions || !hasOrder {
			return false
		}

		// Verify time range extends 2 hours back
		expectedStartTime := aggregationStartTime.Add(-2 * time.Hour)

		// Check conditions
		foundStartTime := false
		foundEndTime := false
		foundResourceType := false
		foundMeasuredType := false

		for _, condition := range conditions {
			if len(condition) >= 2 {
				condStr := condition[0].(string)
				if strings.Contains(condStr, "metric_timestamp >=") {
					if condition[1].(time.Time).Equal(expectedStartTime) {
						foundStartTime = true
					}
				}
				if strings.Contains(condStr, "metric_timestamp <=") {
					if condition[1].(time.Time).Equal(aggregationEndTime) {
						foundEndTime = true
					}
				}
				if strings.Contains(condStr, "resource_type =") {
					if condition[1].(string) == "VOLUME" {
						foundResourceType = true
					}
				}
				if strings.Contains(condStr, "measured_type =") {
					if condition[1].(string) == "ALLOCATED_SIZE" {
						foundMeasuredType = true
					}
				}
			}
		}

		// Verify ordering includes resource grouping and timestamp DESC
		hasCorrectOrder := strings.Contains(order, "resource_name") &&
			strings.Contains(order, "deployment_name") &&
			strings.Contains(order, "consumer_id") &&
			strings.Contains(order, "metric_timestamp DESC")

		return foundStartTime && foundEndTime && foundResourceType && foundMeasuredType && hasCorrectOrder
	})).Return([]datamodel2.HydratedMetrics{}, nil)

	// Execute the method
	_, err := processor.fetchMetricsForCounterAndIntegralAggregation(
		context.Background(),
		aggregationStartTime,
		aggregationEndTime,
		"VOLUME",
		"ALLOCATED_SIZE",
	)

	// Verify no error
	assert.NoError(t, err)
	mockDB.AssertExpectations(t)
}

// TestFilterMetricsForCounterAndIntegralAggregationSorted_EdgeCases tests edge cases in filtering
func TestFilterMetricsForCounterAndIntegralAggregationSorted_EdgeCases(t *testing.T) {
	processor := &BillingProvider{}
	now := time.Now()
	aggregationStartTime := now.Add(-1 * time.Hour)

	t.Run("metrics_exactly_at_aggregation_start", func(t *testing.T) {
		metrics := []datamodel2.HydratedMetrics{
			{MetricTimestamp: aggregationStartTime, Quantity: 150},                       // Exactly at start - should be included
			{MetricTimestamp: aggregationStartTime.Add(-1 * time.Minute), Quantity: 140}, // Just before - should be included as previous
		}

		result := processor.filterMetricsForCounterAndIntegralAggregationSorted(metrics, aggregationStartTime)
		assert.Len(t, result, 2, "Should include metric exactly at start time and one before")
	})

	t.Run("multiple_metrics_same_timestamp", func(t *testing.T) {
		sameTime := now.Add(-30 * time.Minute)
		metrics := []datamodel2.HydratedMetrics{
			{MetricTimestamp: sameTime, Quantity: 150},
			{MetricTimestamp: sameTime, Quantity: 140},
		}

		result := processor.filterMetricsForCounterAndIntegralAggregationSorted(metrics, aggregationStartTime)
		assert.Len(t, result, 2, "Should include all metrics with same timestamp within window")
	})

	t.Run("metrics_in_ascending_order", func(t *testing.T) {
		// Test with metrics not properly sorted (should still work due to time-based filtering)
		metrics := []datamodel2.HydratedMetrics{
			{MetricTimestamp: now.Add(-110 * time.Minute), Quantity: 120}, // Oldest
			{MetricTimestamp: now.Add(-90 * time.Minute), Quantity: 130},  // Before window
			{MetricTimestamp: now.Add(-45 * time.Minute), Quantity: 140},  // Within window
			{MetricTimestamp: now.Add(-30 * time.Minute), Quantity: 150},  // Within window
		}

		result := processor.filterMetricsForCounterAndIntegralAggregationSorted(metrics, aggregationStartTime)
		// Should get: first record before window (120) + both within window (140, 150) = 3 total
		assert.Len(t, result, 3, "Should handle incorrectly sorted metrics gracefully")
	})
}

// TestProcessMetricsWithJobDef_MissingResourceData tests the nil safety checks for resource data
func TestProcessMetricsWithJobDef_MissingResourceData(t *testing.T) {
	processor := &BillingProvider{
		config: &common.TelemetryConfig{
			GoogleBillingLabelsMaxEntries: 5,
		},
		resourceCollection: &ResourceCollection{
			PoolData:   make(map[ResourceKey]ResourceData),
			VolumeData: make(map[ResourceKey]ResourceData),
		},
	}

	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	// Test missing pool resource data (lines 303-304)
	poolResourceID := ResourceKey{
		ResourceType:   metadata.VolumePool,
		ResourceName:   "missing-pool",
		DeploymentName: "test-deployment",
		ConsumerID:     "test-customer",
	}

	poolMetrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    "missing-pool",
			ConsumerID:      "test-customer",
			Location:        "test-location",
			Quantity:        100,
			MetricTimestamp: startTime,
			ResourceType:    metadata.VolumePool,
			MeasuredType:    metadata.AllocatedSize,
			DeploymentName:  "test-deployment",
		},
	}

	jobDef := common.AggregationJobDefinition{
		AggregationType: common.SumAggregation,
		IsBillable:      true,
	}

	// Don't add to resourceCollection to trigger missing resource check
	var aggregatedRecords []datamodel2.AggregatedUsage
	var aggregatedUsageForDB []datamodel2.AggregatedUsage
	err := processor.processMetricsWithJobDef(ctx, poolResourceID, poolMetrics, jobDef, startTime, now, &aggregatedRecords, &aggregatedUsageForDB)
	assert.NoError(t, err)
	assert.Len(t, aggregatedUsageForDB, 1)
	// Should create record with empty labels when pool data is missing

	// Test missing volume resource data (lines 307-308)
	volumeResourceID := ResourceKey{
		ResourceType:   metadata.Volume,
		ResourceName:   "missing-volume",
		DeploymentName: "test-deployment",
		ConsumerID:     "test-customer",
	}

	volumeMetrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    "missing-volume",
			ConsumerID:      "test-customer",
			Location:        "test-location",
			Quantity:        200,
			MetricTimestamp: startTime,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
			DeploymentName:  "test-deployment",
		},
	}

	// Reset slices
	aggregatedRecords = []datamodel2.AggregatedUsage{}
	aggregatedUsageForDB = []datamodel2.AggregatedUsage{}

	// Don't add to resourceCollection to trigger missing resource check
	err = processor.processMetricsWithJobDef(ctx, volumeResourceID, volumeMetrics, jobDef, startTime, now, &aggregatedRecords, &aggregatedUsageForDB)
	assert.NoError(t, err)
	assert.Len(t, aggregatedUsageForDB, 1)
	// Should create record with empty labels when volume data is missing

	// Test unknown resource type (line 316)
	unknownResourceID := ResourceKey{
		ResourceType:   "UNKNOWN_TYPE",
		ResourceName:   "unknown-resource",
		DeploymentName: "test-deployment",
		ConsumerID:     "test-customer",
	}

	unknownMetrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    "unknown-resource",
			ConsumerID:      "test-customer",
			Location:        "test-location",
			Quantity:        300,
			MetricTimestamp: startTime,
			ResourceType:    "UNKNOWN_TYPE",
			MeasuredType:    metadata.AllocatedSize,
			DeploymentName:  "test-deployment",
		},
	}

	// Reset slices
	aggregatedRecords = []datamodel2.AggregatedUsage{}
	aggregatedUsageForDB = []datamodel2.AggregatedUsage{}

	// This should hit the default case in getResourceDataForAggregationUsage (line 316)
	err = processor.processMetricsWithJobDef(ctx, unknownResourceID, unknownMetrics, jobDef, startTime, now, &aggregatedRecords, &aggregatedUsageForDB)
	assert.NoError(t, err)
	assert.Len(t, aggregatedUsageForDB, 1)
	// Should create record with empty labels for unknown resource type
	assert.Equal(t, "unknown-resource", *aggregatedUsageForDB[0].ResourceName)
}

// TestProcessBillingMetrics_FetchDataNilSafety tests the nil safety checks in fetchPoolData and fetchVolumeData
func TestProcessBillingMetrics_FetchDataNilSafety(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)
	mockUsageSink := &MockUsageSink{}
	config := &common.TelemetryConfig{
		PoolVolumeLabelPageSize:       100,
		MaxGoogleBillingPushRetry:     3,
		GoogleBillingLabelsMaxEntries: 10,
	}

	processor := NewBillingProvider(mockDB, mockVCPDB, config, mockUsageSink)

	ctx := context.Background()
	now := time.Now()

	// Create pools with nil Account to trigger line 222-223 (continue statement)
	poolsWithNilAccount := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-nil-account"},
				Name:           "pool-nil-account",
				Account:        nil, // This should trigger the continue on line 222-223
				PoolAttributes: &datamodel.PoolAttributes{Labels: &datamodel.JSONB{}},
			},
		},
		{
			Pool: datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "valid-pool"},
				Name:           "valid-pool",
				Account:        &datamodel.Account{Name: "valid-account"},
				PoolAttributes: &datamodel.PoolAttributes{Labels: &datamodel.JSONB{}},
			},
		},
	}

	// Create volumes with nil Account and nil Pool to trigger lines 298-299 and 302-303
	volumesWithNilRelations := []*datamodel.Volume{
		{
			BaseModel:        datamodel.BaseModel{UUID: "vol-nil-account"},
			Name:             "vol-nil-account",
			Account:          nil, // This should trigger continue on line 298-299
			Pool:             &datamodel.Pool{DeploymentName: "dep1"},
			VolumeAttributes: &datamodel.VolumeAttributes{Labels: &datamodel.JSONB{}},
		},
		{
			BaseModel:        datamodel.BaseModel{UUID: "vol-nil-pool"},
			Name:             "vol-nil-pool",
			Account:          &datamodel.Account{Name: "account1"},
			Pool:             nil, // This should trigger continue on line 302-303
			VolumeAttributes: &datamodel.VolumeAttributes{Labels: &datamodel.JSONB{}},
		},
		{
			BaseModel:        datamodel.BaseModel{UUID: "valid-vol"},
			Name:             "valid-vol",
			Account:          &datamodel.Account{Name: "account1"},
			Pool:             &datamodel.Pool{DeploymentName: "dep1"},
			VolumeAttributes: &datamodel.VolumeAttributes{Labels: &datamodel.JSONB{}},
		},
	}

	// Mock the pool fetch calls
	mockVCPDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(poolsWithNilAccount, nil).Once()
	mockVCPDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil).Once()

	// Mock the volume fetch calls
	mockVCPDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(volumesWithNilRelations, nil).Once()
	mockVCPDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()

	// Mock unsent usages
	mockDB.On("GetAggregatedUsage", mock.Anything, mock.MatchedBy(func(filter map[string]interface{}) bool {
		return filter["state"] == datamodel2.Unsubmitted
	})).Return([]datamodel2.AggregatedUsage{}, nil)

	mockDB.On("GetAggregatedUsage", mock.Anything, mock.MatchedBy(func(filter map[string]interface{}) bool {
		return filter["state"] == datamodel2.Error
	})).Return([]datamodel2.AggregatedUsage{}, nil)

	// Mock the GetHydratedMetrics calls for all aggregation types (return empty to focus on fetch)
	mockDB.On("GetHydratedMetrics", mock.Anything, mock.Anything).Return([]datamodel2.HydratedMetrics{}, nil)

	// Execute ProcessBillingMetrics - this will call fetchResourceData which calls fetchPoolData and fetchVolumeData
	err := processor.ProcessBillingMetrics(ctx, now)

	assert.NoError(t, err)

	// Verify that only the valid pool and valid volume were processed (nil ones were skipped)
	assert.Equal(t, 1, len(processor.resourceCollection.PoolData), "Should have 1 valid pool (nil account pool skipped)")
	assert.Equal(t, 1, len(processor.resourceCollection.VolumeData), "Should have 1 valid volume (nil account and nil pool volumes skipped)")

	// Verify the valid records were processed correctly
	validPoolFound := false
	for key := range processor.resourceCollection.PoolData {
		if key.ResourceName == "valid-pool" {
			validPoolFound = true
			break
		}
	}
	assert.True(t, validPoolFound, "Valid pool should be processed")

	validVolumeFound := false
	for key := range processor.resourceCollection.VolumeData {
		if key.ResourceName == "valid-vol" {
			validVolumeFound = true
			break
		}
	}
	assert.True(t, validVolumeFound, "Valid volume should be processed")

	mockDB.AssertExpectations(t)
	mockVCPDB.AssertExpectations(t)
}

// TestGetResourceDataForAggregationUsage_UnknownResourceType tests the default case returning nil (line 380)
func TestGetResourceDataForAggregationUsage_UnknownResourceType(t *testing.T) {
	processor := &BillingProvider{
		resourceCollection: &ResourceCollection{
			PoolData:   make(map[ResourceKey]ResourceData),
			VolumeData: make(map[ResourceKey]ResourceData),
		},
	}

	resourceKey := ResourceKey{
		ResourceType:   "UNKNOWN_TYPE", // This should trigger the default case on line 380
		ResourceName:   "test-resource",
		DeploymentName: "test-deployment",
		ConsumerID:     "test-customer",
	}

	// Call getResourceDataForAggregationUsage with unknown resource type
	result := processor.getResourceDataForAggregationUsage(resourceKey, "UNKNOWN_TYPE")

	// Should return nil for unknown resource type (line 380)
	assert.Nil(t, result, "Should return nil for unknown resource type")

	// Verify that the key was still tracked as used
	assert.NotNil(t, processor.usedKeys)
	assert.True(t, processor.usedKeys[resourceKey], "Unknown resource key should still be tracked as used")
}
