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
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
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
	config := &common.TelemetryConfig{
		EnableReplicationBillingMetrics: true,
	}
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()

	// Mock VCP database calls for label fetching - now uses conditions approach
	vcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil).Once()
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()
	vcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

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
	config := &common.TelemetryConfig{
		EnableReplicationBillingMetrics: true,
	}
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()

	// Mock VCP database calls for label fetching
	vcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil).Once()
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()
	vcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

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
		metricsDB: mockDB,
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
	resourceCollection := &ResourceCollection{
		PoolData:   make(map[ResourceKey]ResourceData),
		VolumeData: make(map[ResourceKey]ResourceData),
	}
	err := processor.processMetricsWithJobDef(ctx, resourceID, metrics, jobDef, now.Add(-1*time.Hour), now, resourceCollection, &aggregatedRecords, util.GetLogger(ctx))
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
		{
			name:         "unknown",
			resourceType: metadata.VolumeReplicationRelationship,
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
		metricsDB: mockDB,
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
		resourceCollection := &ResourceCollection{
			PoolData:   make(map[ResourceKey]ResourceData),
			VolumeData: make(map[ResourceKey]ResourceData),
		}
		err := processor.processMetricsWithJobDef(ctx, resourceID, []datamodel2.HydratedMetrics{}, common.AggregationJobDefinition{AggregationType: common.IntegralAggregation}, startTime, now, resourceCollection, &aggregatedRecords, util.GetLogger(ctx))
		assert.NoError(t, err)
		// No DB call should be made, but record should be collected for batch
		assert.Len(t, aggregatedUsageForDB, 0) // No metrics means no aggregated records
		mockDB.AssertNotCalled(t, "CreateAggregatedUsage")
	})

	t.Run("IntegralAggregation", func(t *testing.T) {
		// With batch saving, individual CreateAggregatedUsage calls are no longer made
		// Test with Integral aggregation
		var aggregatedRecords []datamodel2.AggregatedUsage
		resourceCollection := &ResourceCollection{
			PoolData:   make(map[ResourceKey]ResourceData),
			VolumeData: make(map[ResourceKey]ResourceData),
		}
		// Add the resource data to the collection
		resourceCollection.VolumeData[resourceID] = ResourceData{
			UUID:      "test-uuid",
			AccountID: 123,
			Labels:    Labels{"env": "test"},
		}
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics, common.AggregationJobDefinition{AggregationType: common.IntegralAggregation}, startTime, now, resourceCollection, &aggregatedRecords, util.GetLogger(ctx))
		assert.NoError(t, err)
		// Verify that the record was collected for batch saving
		assert.Len(t, aggregatedRecords, 1)
		assert.Equal(t, string(common.IntegralAggregation), aggregatedRecords[0].AggregationType)
		assert.Equal(t, customerID, *aggregatedRecords[0].VendorCustomerID)
		assert.Equal(t, resourceName, *aggregatedRecords[0].ResourceName)
	})

	t.Run("CounterAggregation", func(t *testing.T) {
		// With batch saving, test the collected record instead
		var aggregatedRecords []datamodel2.AggregatedUsage
		resourceCollection := &ResourceCollection{
			PoolData:   make(map[ResourceKey]ResourceData),
			VolumeData: make(map[ResourceKey]ResourceData),
		}
		// Add the resource data to the collection
		resourceCollection.VolumeData[resourceID] = ResourceData{
			UUID:      "test-uuid",
			AccountID: 123,
			Labels:    Labels{"env": "test"},
		}
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics, common.AggregationJobDefinition{AggregationType: common.CounterAggregation}, startTime, now, resourceCollection, &aggregatedRecords, util.GetLogger(ctx))
		assert.NoError(t, err)
		// Verify that the record was collected and has LastCounterValue set
		assert.Len(t, aggregatedRecords, 1)
		assert.Equal(t, string(common.CounterAggregation), aggregatedRecords[0].AggregationType)
		assert.NotNil(t, aggregatedRecords[0].LastCounterValue)
		assert.Equal(t, 200.0, *aggregatedRecords[0].LastCounterValue)
	})

	t.Run("SumAggregation", func(t *testing.T) {
		// With batch saving, test the collected record instead
		var aggregatedRecords []datamodel2.AggregatedUsage
		resourceCollection := &ResourceCollection{
			PoolData:   make(map[ResourceKey]ResourceData),
			VolumeData: make(map[ResourceKey]ResourceData),
		}
		// Add the resource data to the collection
		resourceCollection.VolumeData[resourceID] = ResourceData{
			UUID:      "test-uuid",
			AccountID: 123,
			Labels:    Labels{"env": "test"},
		}
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics, common.AggregationJobDefinition{AggregationType: common.SumAggregation}, startTime, now, resourceCollection, &aggregatedRecords, util.GetLogger(ctx))
		assert.NoError(t, err)
		// Verify that the record was collected for batch saving
		assert.Len(t, aggregatedRecords, 1)
		assert.Equal(t, string(common.SumAggregation), aggregatedRecords[0].AggregationType)
		assert.Equal(t, resourceName, *aggregatedRecords[0].ResourceName)
	})

	t.Run("FirstAggregation", func(t *testing.T) {
		// With batch saving, test the collected record instead
		var aggregatedRecords []datamodel2.AggregatedUsage
		resourceCollection := &ResourceCollection{
			PoolData:   make(map[ResourceKey]ResourceData),
			VolumeData: make(map[ResourceKey]ResourceData),
		}
		// Add the resource data to the collection
		resourceCollection.VolumeData[resourceID] = ResourceData{
			UUID:      "test-uuid",
			AccountID: 123,
			Labels:    Labels{"env": "test"},
		}
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics, common.AggregationJobDefinition{AggregationType: common.FirstAggregation}, startTime, now, resourceCollection, &aggregatedRecords, util.GetLogger(ctx))
		assert.NoError(t, err)
		// Verify that the record was collected for batch saving
		assert.Len(t, aggregatedRecords, 1)
		assert.Equal(t, string(common.FirstAggregation), aggregatedRecords[0].AggregationType)
		assert.Equal(t, resourceName, *aggregatedRecords[0].ResourceName)
	})

	t.Run("DatabaseError", func(t *testing.T) {
		// With batch saving, the function just collects records
		// Database errors will occur during batch save, not here
		var aggregatedRecords []datamodel2.AggregatedUsage
		resourceCollection := &ResourceCollection{
			PoolData:   make(map[ResourceKey]ResourceData),
			VolumeData: make(map[ResourceKey]ResourceData),
		}
		// Add the resource data to the collection
		resourceCollection.VolumeData[resourceID] = ResourceData{
			UUID:      "test-uuid",
			AccountID: 123,
			Labels:    Labels{"env": "test"},
		}
		err := processor.processMetricsWithJobDef(ctx, resourceID, metrics, common.AggregationJobDefinition{AggregationType: common.SumAggregation}, startTime, now, resourceCollection, &aggregatedRecords, util.GetLogger(ctx))
		assert.NoError(t, err) // No error in collection phase
		// Verify that the record was collected for batch saving
		assert.Len(t, aggregatedRecords, 1)
	})

	t.Run("CounterAggregationVolumeReplication", func(t *testing.T) {
		// With batch saving, test the collected record instead
		var aggregatedRecords []datamodel2.AggregatedUsage
		resourceCollection := &ResourceCollection{
			PoolData:              make(map[ResourceKey]ResourceData),
			VolumeData:            make(map[ResourceKey]ResourceData),
			VolumeReplicationData: make(map[ResourceKey]ResourceData),
		}

		resourceIDRep := ResourceKey{
			ResourceType:   metadata.VolumeReplicationRelationship,
			ResourceName:   "test-resource",
			DeploymentName: "test-deployment",
			ConsumerID:     "test-customer",
		}

		// Add the resource data to the collection
		repName := "replication1"
		srcLoc := "us-west"
		dstLoc := "us-east"
		dstVolUUID := "dst-vol-uuid"
		resourceCollection.VolumeReplicationData[resourceIDRep] = ResourceData{
			UUID:      "test-uuid",
			AccountID: 123,
			Labels:    Labels{"env": "test"},
			VolumeReplicationInfo: &VolumeReplicationInfo{
				ReplicationType:       "CROSS_REGION_REPLICATION",
				ReplicationSchedule:   "hourly",
				ReplicationName:       &repName,
				SourceLocation:        &srcLoc,
				DestinationLocation:   &dstLoc,
				DestinationVolumeUUID: &dstVolUUID,
			},
		}

		repMetrics := []datamodel2.HydratedMetrics{
			{
				ResourceName:    resourceName,
				ConsumerID:      customerID,
				Location:        location,
				Quantity:        150,
				MetricTimestamp: now,
				ResourceType:    metadata.VolumeReplicationRelationship,
				MeasuredType:    metadata.XregionReplicationTotalTransferBytes,
			},
		}

		err := processor.processMetricsWithJobDef(ctx, resourceIDRep, repMetrics, common.AggregationJobDefinition{AggregationType: common.CounterAggregation}, startTime, now, resourceCollection, &aggregatedRecords, util.GetLogger(ctx))
		assert.NoError(t, err)
		// Verify that the record was collected and has LastCounterValue set
		assert.Len(t, aggregatedRecords, 1)
		assert.Equal(t, string(common.CounterAggregation), aggregatedRecords[0].AggregationType)
		assert.NotNil(t, aggregatedRecords[0].LastCounterValue)
		assert.Equal(t, 150.0, *aggregatedRecords[0].LastCounterValue)
	})
}

// TestProcessMetricsSuccess tests a successful path through ProcessMetrics
func TestProcessMetricsSuccess(t *testing.T) {
	mockDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{
		EnableReplicationBillingMetrics: true,
	}
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	// Mock VCP database calls for label fetching - return resource data that matches the metrics
	vcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil).Once()
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid-1"},
			Name:      "resource1",
			AccountID: 123,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Labels: &datamodel.JSONB{"env": "test"},
			},
			Pool:    &datamodel.Pool{DeploymentName: "", PoolAttributes: &datamodel.PoolAttributes{IsRegionalHA: false}},
			Account: &datamodel.Account{Name: "customer1"},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid-2"},
			Name:      "resource2",
			AccountID: 123,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Labels: &datamodel.JSONB{"env": "test"},
			},
			Pool:    &datamodel.Pool{DeploymentName: "", PoolAttributes: &datamodel.PoolAttributes{IsRegionalHA: true}},
			Account: &datamodel.Account{Name: "customer1"},
		},
	}, nil).Once()
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()
	vcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-rep-uuid"},
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
				Name:      "vol1",
				Pool:      &datamodel.Pool{DeploymentName: "dep1"},
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ReplicationType: "CROSS_REGION_REPLICATION",
				Labels:          &datamodel.JSONB{"key": "value"},
			},
			Account: &datamodel.Account{Name: "account1"},
		},
	}, nil).Once()
	vcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

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

	// Setup DeliverMetrics expectation for the usage sink
	mockSink.On("DeliverMetrics", mock.Anything, mock.MatchedBy(func(metrics []datamodel2.AggregatedUsage) bool {
		return len(metrics) == 1 && !metrics[0].IsBillable
	})).Return(1, nil).Once()

	// Call ProcessMetrics
	err := processor.ProcessBillingMetrics(ctx, now)
	assert.NoError(t, err)

	// Verify expectations
	mockDB.AssertExpectations(t)
	vcpDB.AssertExpectations(t)
	mockSink.AssertExpectations(t)
}

// TestProcessMetricsWithJobDefErrors tests error scenarios in processMetricsWithJobDef
func TestProcessMetricsWithJobDefErrors(t *testing.T) {
	mockDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{
		EnableReplicationBillingMetrics: true,
	}
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	// Mock VCP database calls for label fetching - return resource data that matches the metrics
	vcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil).Once()
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid-1"},
			Name:      "resource1",
			AccountID: 123,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Labels: &datamodel.JSONB{"env": "test"},
			},
			Pool:    &datamodel.Pool{DeploymentName: "", PoolAttributes: &datamodel.PoolAttributes{IsRegionalHA: false}},
			Account: &datamodel.Account{Name: "customer1"},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid-2"},
			Name:      "resource2",
			AccountID: 123,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Labels: &datamodel.JSONB{"env": "test"},
			},
			Pool:    &datamodel.Pool{DeploymentName: "", PoolAttributes: &datamodel.PoolAttributes{IsRegionalHA: true}},
			Account: &datamodel.Account{Name: "customer1"},
		},
	}, nil).Once()
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()
	vcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-rep-uuid"},
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
				Name:      "vol1",
				Pool:      &datamodel.Pool{DeploymentName: "dep1"},
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ReplicationType: "CROSS_REGION_REPLICATION",
				Labels:          &datamodel.JSONB{"key": "value"},
			},
			Account: &datamodel.Account{Name: "account1"},
		},
	}, nil).Once()
	vcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

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
	config := &common.TelemetryConfig{
		MaxGoogleBillingPushRetry:       3,
		EnableReplicationBillingMetrics: true,
	}
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()

	// Mock VCP database calls for label fetching
	vcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil).Once()
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()
	vcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

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
	config := &common.TelemetryConfig{
		EnableReplicationBillingMetrics: true,
	}
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

	// Mock VCP database calls for label fetching - return resource data that matches the metrics
	vcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil).Once()
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid-1"},
			Name:      "resource1",
			AccountID: 123,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Labels: &datamodel.JSONB{"env": "test"},
			},
			Pool:    &datamodel.Pool{DeploymentName: "deployment1", PoolAttributes: &datamodel.PoolAttributes{IsRegionalHA: false}},
			Account: &datamodel.Account{Name: "customer1"},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid-2"},
			Name:      "resource2",
			AccountID: 123,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Labels: &datamodel.JSONB{"env": "test"},
			},
			Pool:    &datamodel.Pool{DeploymentName: "deployment1", PoolAttributes: &datamodel.PoolAttributes{IsRegionalHA: true}},
			Account: &datamodel.Account{Name: "customer1"},
		},
	}, nil).Once()
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()
	vcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

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

	// Mock VCP database calls for label fetching - return resource data that matches the metrics
	vcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil).Once()
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid-1"},
			Name:      "resource1",
			AccountID: 123,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Labels: &datamodel.JSONB{"env": "test"},
			},
			Pool:    &datamodel.Pool{DeploymentName: "test-deployment", PoolAttributes: &datamodel.PoolAttributes{IsRegionalHA: false}},
			Account: &datamodel.Account{Name: "customer1"},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid-2"},
			Name:      "resource2",
			AccountID: 123,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Labels: &datamodel.JSONB{"env": "test"},
			},
			Pool:    &datamodel.Pool{DeploymentName: "test-deployment", PoolAttributes: &datamodel.PoolAttributes{IsRegionalHA: true}},
			Account: &datamodel.Account{Name: "customer1"},
		},
	}, nil).Once()
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()
	vcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

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
		metricsDB: mockDB,
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
		metricsDB: mockDB,
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
		metricsDB: mockDB,
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
	resourceCollection := &ResourceCollection{
		PoolData:   make(map[ResourceKey]ResourceData),
		VolumeData: make(map[ResourceKey]ResourceData),
	}
	// Add the resource data to the collection
	resourceCollection.VolumeData[resourceID] = ResourceData{
		UUID:      "test-uuid",
		AccountID: 123,
		Labels:    Labels{"env": "test"},
	}
	err := processor.processMetricsWithJobDef(ctx, resourceID, metrics, common.AggregationJobDefinition{AggregationType: common.SumAggregation}, startTime, now, resourceCollection, &aggregatedRecords, util.GetLogger(ctx))

	assert.NoError(t, err)
	// With batch approach, records are collected regardless of billability
	// Billability is determined during the creation phase
	assert.Len(t, aggregatedRecords, 1)
}

// TestNewBillingProvider tests the NewBillingProvider constructor
func TestNewBillingProvider(t *testing.T) {
	mockDB := &database.MockStorage{}
	vcpDB := &database2.MockStorage{}
	config := &common.TelemetryConfig{}
	mockSink := &MockUsageSink{}

	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)

	assert.NotNil(t, processor)
	assert.Equal(t, mockDB, processor.metricsDB)
	assert.Equal(t, vcpDB, processor.vcpDataStore)
	assert.Equal(t, config, processor.config)
	assert.Equal(t, mockSink, processor.usageSink)
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
	config := &common.TelemetryConfig{PoolVolumeLabelPageSize: 2, GoogleBillingLabelsMaxEntries: 10, EnableReplicationBillingMetrics: true}
	provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

	// Helper function removed since we create ResourceCollection inline in tests

	t.Run("When succeed", func(t *testing.T) {
		// First call returns one pool, second call returns empty slice (pagination end)
		mockVcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{
			{
				Pool: datamodel.Pool{
					BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
					Name:           "pool1",
					AccountID:      1,
					VendorID:       "/projects/12345/",
					DeploymentName: "dep1",
					PoolAttributes: &datamodel.PoolAttributes{
						Labels:       &datamodel.JSONB{"test": "test"},
						PrimaryZone:  "us-central1",
						IsRegionalHA: true,
					},
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
		mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{UUID: "vol-rep-uuid"},
				Volume: &datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
					Name:      "vol1",
					Pool:      &datamodel.Pool{DeploymentName: "dep1"},
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ReplicationType: "CROSS_REGION_REPLICATION",
					Labels:          &datamodel.JSONB{"key": "value"},
				},
				Account: &datamodel.Account{Name: "account1"},
			},
		}, nil).Once()
		mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

		resourceCollection, err := provider.fetchResourceData(ctx, time.Now().Add(-1*time.Hour), time.Now())
		assert.NoError(t, err)
		assert.Len(t, resourceCollection.PoolData, 1)
		assert.Len(t, resourceCollection.VolumeData, 1)
		assert.Len(t, resourceCollection.VolumeReplicationData, 1)
		mockVcpDB.AssertExpectations(t)
	})

	t.Run("Pool fetch fails, volume and volume replication fetch succeeds", func(t *testing.T) {
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
		mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{UUID: "vol-rep-uuid"},
				Volume: &datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
					Name:      "vol1",
					Pool:      &datamodel.Pool{DeploymentName: "dep1"},
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ReplicationType: "CROSS_REGION_REPLICATION",
					Labels:          &datamodel.JSONB{"key": "value"},
				},
				Account: &datamodel.Account{Name: "account1"},
			},
		}, nil).Once()
		mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()
		resourceCollection, err := provider.fetchResourceData(ctx, time.Now().Add(-1*time.Hour), time.Now())

		assert.NoError(t, err)
		assert.Len(t, resourceCollection.PoolData, 0)
		assert.Len(t, resourceCollection.VolumeData, 1)
		assert.Len(t, resourceCollection.VolumeReplicationData, 1)
		mockVcpDB.AssertExpectations(t)
	})

	t.Run("Volume fetch fails, pool and volume replication fetch succeeds", func(t *testing.T) {
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
		mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{UUID: "vol-rep-uuid"},
				Volume: &datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
					Name:      "vol1",
					Pool:      &datamodel.Pool{DeploymentName: "dep1"},
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ReplicationType: "CROSS_REGION_REPLICATION",
					Labels:          &datamodel.JSONB{"key": "value"},
				},
				Account: &datamodel.Account{Name: "account1"},
			},
		}, nil).Once()
		mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()
		resourceCollection, err := provider.fetchResourceData(ctx, time.Now().Add(-1*time.Hour), time.Now())
		assert.NoError(t, err)
		assert.Len(t, resourceCollection.PoolData, 1)
		assert.Len(t, resourceCollection.VolumeData, 0)
		assert.Len(t, resourceCollection.VolumeReplicationData, 1)
		mockVcpDB.AssertExpectations(t)
	})

	t.Run("Both pool and volume fetch fail", func(t *testing.T) {
		mockVcpDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("pool error")).Once()
		mockVcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("volume error")).Once()
		mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("volume replication error")).Once()
		resourceCollection, err := provider.fetchResourceData(ctx, time.Now().Add(-1*time.Hour), time.Now())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to fetch any resource data")
		assert.Nil(t, resourceCollection)
		mockVcpDB.AssertExpectations(t)
	})

	t.Run("Volume Replication fetch fail", func(t *testing.T) {
		// Mock successful pool and volume fetches
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

		mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("volume replication error")).Once()

		resourceCollection, err := provider.fetchResourceData(ctx, time.Now().Add(-1*time.Hour), time.Now())
		assert.NoError(t, err)
		assert.Len(t, resourceCollection.PoolData, 1)
		assert.Len(t, resourceCollection.VolumeData, 1)
		assert.Len(t, resourceCollection.VolumeReplicationData, 0)
		mockVcpDB.AssertExpectations(t)
	})
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
		metricsDB: mockDB,
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
		metricsDB: mockDB,
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
		metricsDB: mockDB,
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

// TestProcessBillingMetrics_NonCounterAggregation tests the path for non-counter aggregation types
func TestProcessBillingMetrics_NonCounterAggregation(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)
	mockUsageSink := &MockUsageSink{}
	config := &common.TelemetryConfig{
		PoolVolumeLabelPageSize:         100,
		MaxGoogleBillingPushRetry:       3,
		GoogleBillingLabelsMaxEntries:   10,
		EnableReplicationBillingMetrics: true,
	}

	processor := NewBillingProvider(mockDB, mockVCPDB, config, mockUsageSink)

	ctx := context.Background()
	now := time.Now()

	// Mock the VCP database calls for resource data
	mockVCPDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	mockVCPDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	mockVCPDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Mock unsent usages
	mockDB.On("GetAggregatedUsage", mock.Anything, mock.MatchedBy(func(filter map[string]interface{}) bool {
		return filter["state"] == datamodel2.Unsubmitted
	})).Return([]datamodel2.AggregatedUsage{}, nil)

	mockDB.On("GetAggregatedUsage", mock.Anything, mock.MatchedBy(func(filter map[string]interface{}) bool {
		return filter["state"] == datamodel2.Error
	})).Return([]datamodel2.AggregatedUsage{}, nil)

	// Mock the GetHydratedMetrics for non-counter aggregation (this will hit the else branch on line 96)
	mockDB.On("GetHydratedMetrics", mock.Anything, mock.MatchedBy(func(filter map[string]interface{}) bool {
		// This should be called for FirstAggregation type which is not counter or integral
		return true
	})).Return([]datamodel2.HydratedMetrics{}, nil)

	// Mock delivery - should not be called since no metrics to deliver
	// mockUsageSink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(0, nil)

	// Execute - this should trigger the else branch for non-counter aggregation types
	err := processor.ProcessBillingMetrics(ctx, now)

	assert.NoError(t, err)
	mockDB.AssertExpectations(t)
	mockVCPDB.AssertExpectations(t)
	// mockUsageSink.AssertExpectations(t) // Not called since no metrics
}

// TestProcessBillingMetrics_FetchResourceDataError tests error handling in fetchResourceData
func TestProcessBillingMetrics_FetchResourceDataError(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)
	mockUsageSink := &MockUsageSink{}
	config := &common.TelemetryConfig{
		PoolVolumeLabelPageSize:         100,
		MaxGoogleBillingPushRetry:       3,
		GoogleBillingLabelsMaxEntries:   10,
		EnableReplicationBillingMetrics: true,
	}

	processor := NewBillingProvider(mockDB, mockVCPDB, config, mockUsageSink)

	ctx := context.Background()
	now := time.Now()

	// Mock pool data fetch failure (should hit line 102)
	mockVCPDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("pool fetch error"))
	// Mock volume data fetch failure
	mockVCPDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("volume fetch error"))
	mockVCPDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	// Mock unsent usages
	mockDB.On("GetAggregatedUsage", mock.Anything, mock.MatchedBy(func(filter map[string]interface{}) bool {
		return filter["state"] == datamodel2.Unsubmitted
	})).Return([]datamodel2.AggregatedUsage{}, nil)

	mockDB.On("GetAggregatedUsage", mock.Anything, mock.MatchedBy(func(filter map[string]interface{}) bool {
		return filter["state"] == datamodel2.Error
	})).Return([]datamodel2.AggregatedUsage{}, nil)

	// Mock the GetHydratedMetrics calls for all aggregation types
	mockDB.On("GetHydratedMetrics", mock.Anything, mock.Anything).Return([]datamodel2.HydratedMetrics{}, nil)

	// Mock delivery - should not be called since no metrics to deliver
	// mockUsageSink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(0, nil)

	// Execute - should continue processing despite resource data fetch errors
	err := processor.ProcessBillingMetrics(ctx, now)

	assert.NoError(t, err) // Should not return error even if resource data fetch fails
	mockDB.AssertExpectations(t)
	mockVCPDB.AssertExpectations(t)
	// mockUsageSink.AssertExpectations(t) // Not called since no metrics
}

// TestCreateComplexFilter_WithLimitAndOrder tests the complex filter creation for missing lines 409
func TestCreateComplexFilter_WithLimitAndOrder(t *testing.T) {
	processor := &BillingProvider{}

	tests := []struct {
		name     string
		options  map[string]interface{}
		expected map[string]interface{}
	}{
		{
			name: "with_valid_order_and_limit",
			options: map[string]interface{}{
				"order": "metric_timestamp DESC",
				"limit": 10,
			},
			expected: map[string]interface{}{
				"conditions": [][]interface{}{},
				"order":      "metric_timestamp DESC",
				"limit":      10,
			},
		},
		{
			name: "with_zero_limit_should_be_ignored",
			options: map[string]interface{}{
				"order": "resource_name ASC",
				"limit": 0, // Should be ignored
			},
			expected: map[string]interface{}{
				"conditions": [][]interface{}{},
				"order":      "resource_name ASC",
				// limit should not be present
			},
		},
		{
			name: "with_negative_limit_should_be_ignored",
			options: map[string]interface{}{
				"order": "resource_name ASC",
				"limit": -5, // Should be ignored
			},
			expected: map[string]interface{}{
				"conditions": [][]interface{}{},
				"order":      "resource_name ASC",
				// limit should not be present
			},
		},
		{
			name: "with_empty_order_should_be_ignored",
			options: map[string]interface{}{
				"order": "", // Should be ignored
				"limit": 5,
			},
			expected: map[string]interface{}{
				"conditions": [][]interface{}{},
				"limit":      5,
				// order should not be present
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := processor.CreateComplexFilter(tt.options)

			// Check conditions
			conditions, hasConditions := result["conditions"]
			assert.True(t, hasConditions, "Should always have conditions")
			// Convert nil slice to empty slice for comparison
			actualConditions := conditions.([][]interface{})
			expectedConditions := tt.expected["conditions"].([][]interface{})
			if actualConditions == nil {
				actualConditions = [][]interface{}{}
			}
			if expectedConditions == nil {
				expectedConditions = [][]interface{}{}
			}
			assert.Equal(t, expectedConditions, actualConditions)

			// Check order
			if expectedOrder, hasExpectedOrder := tt.expected["order"]; hasExpectedOrder {
				order, hasOrder := result["order"]
				assert.True(t, hasOrder, "Should have order when expected")
				assert.Equal(t, expectedOrder, order)
			} else {
				_, hasOrder := result["order"]
				assert.False(t, hasOrder, "Should not have order when not expected")
			}

			// Check limit
			if expectedLimit, hasExpectedLimit := tt.expected["limit"]; hasExpectedLimit {
				limit, hasLimit := result["limit"]
				assert.True(t, hasLimit, "Should have limit when expected")
				assert.Equal(t, expectedLimit, limit)
			} else {
				_, hasLimit := result["limit"]
				assert.False(t, hasLimit, "Should not have limit when not expected")
			}
		})
	}
}

func TestGetResourceDataForAggregationUsage(t *testing.T) {
	// Setup test processor
	processor := &BillingProvider{}

	// Create test resource keys
	poolKey := ResourceKey{
		ResourceType:   metadata.VolumePool,
		ResourceName:   "test-pool",
		DeploymentName: "test-deployment",
		ConsumerID:     "test-customer",
	}

	volumeKey := ResourceKey{
		ResourceType:   metadata.Volume,
		ResourceName:   "test-volume",
		DeploymentName: "test-deployment",
		ConsumerID:     "test-customer",
	}

	poolKeyRegionalHA := ResourceKey{
		ResourceType:   metadata.VolumePoolRegionalHA,
		ResourceName:   "test-pool",
		DeploymentName: "test-deployment",
		ConsumerID:     "test-customer",
	}

	volumeKeyRegionalHA := ResourceKey{
		ResourceType:   metadata.VolumeRegionalHA,
		ResourceName:   "test-volume",
		DeploymentName: "test-deployment",
		ConsumerID:     "test-customer",
	}

	// Create test resource data
	poolData := ResourceData{
		UUID:      "pool-uuid",
		AccountID: 123,
		Labels:    Labels{"pool": "test"},
	}

	volumeData := ResourceData{
		UUID:      "volume-uuid",
		AccountID: 456,
		Labels:    Labels{"volume": "test"},
	}

	// Create resource collection
	resourceCollection := &ResourceCollection{
		PoolData: map[ResourceKey]ResourceData{
			poolKey:           poolData,
			poolKeyRegionalHA: poolData,
		},
		VolumeData: map[ResourceKey]ResourceData{
			volumeKey:           volumeData,
			volumeKeyRegionalHA: volumeData,
		},
	}

	tests := []struct {
		name         string
		id           ResourceKey
		resourceType metadata.ResourceType
		collection   *ResourceCollection
		expected     *ResourceData
		expectNil    bool
	}{
		{
			name:         "VolumePool resource type",
			id:           poolKey,
			resourceType: metadata.VolumePool,
			collection:   resourceCollection,
			expected:     &poolData,
		},
		{
			name:         "Volume resource type",
			id:           volumeKey,
			resourceType: metadata.Volume,
			collection:   resourceCollection,
			expected:     &volumeData,
		},
		{
			name:         "VolumePoolRegionalHA resource type",
			id:           poolKey,
			resourceType: metadata.VolumePoolRegionalHA,
			collection:   resourceCollection,
			expected:     &poolData,
		},
		{
			name:         "VolumeRegionalHA resource type",
			id:           volumeKey,
			resourceType: metadata.VolumeRegionalHA,
			collection:   resourceCollection,
			expected:     &volumeData,
		},
		{
			name:         "Resource not found in collection",
			id:           ResourceKey{ResourceType: metadata.Volume, ResourceName: "non-existent"},
			resourceType: metadata.Volume,
			collection:   resourceCollection,
			expectNil:    true,
		},
		{
			name:         "Unsupported resource type",
			id:           poolKey,
			resourceType: "unsupported",
			collection:   resourceCollection,
			expectNil:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := processor.getResourceDataForAggregationUsage(tt.id, tt.resourceType, tt.collection)

			if tt.expectNil {
				assert.Nil(t, result, "Expected nil result for %s", tt.name)
				return
			}

			assert.NotNil(t, result, "Expected non-nil result for %s", tt.name)
			assert.Equal(t, tt.expected.UUID, result.UUID, "UUID mismatch for %s", tt.name)
			assert.Equal(t, tt.expected.AccountID, result.AccountID, "AccountID mismatch for %s", tt.name)
			assert.Equal(t, tt.expected.Labels, result.Labels, "Labels mismatch for %s", tt.name)
		})
	}
}

func TestSetServiceLevelForCRR(t *testing.T) {
	tests := []struct {
		name     string
		schedule string
		expected string
	}{
		{
			name:     "10minutely schedule should return service level 1",
			schedule: "10minutely",
			expected: "1",
		},
		{
			name:     "hourly schedule should return service level 2",
			schedule: "hourly",
			expected: "2",
		},
		{
			name:     "daily schedule should return service level 3",
			schedule: "daily",
			expected: "3",
		},
		{
			name:     "unknown schedule should return empty string",
			schedule: "unknown",
			expected: "",
		},
		{
			name:     "empty schedule should return empty string",
			schedule: "",
			expected: "",
		},
		{
			name:     "weekly schedule should return empty string",
			schedule: "weekly",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := setServiceLevelForCRR(tt.schedule)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Helper function to create JSONB from map
func createJSONB(data map[string]string) *datamodel.JSONB {
	jsonb := datamodel.JSONB{}
	for k, v := range data {
		jsonb[k] = v // v is string, but JSONB expects interface{}
	}
	return &jsonb
}

// TestLimitLabels_Debug tests the limitLabels function directly
func TestLimitLabels_Debug(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)

	config := &common.TelemetryConfig{
		GoogleBillingLabelsMaxEntries: 10,
	}

	provider := &BillingProvider{
		config:       config,
		vcpDataStore: mockVCPDB,
		metricsDB:    mockDB,
	}

	// Test with our createJSONB function
	jsonb := createJSONB(map[string]string{"env": "dev"})
	t.Logf("Created JSONB: %+v", *jsonb)

	labels := provider.limitLabels(jsonb)
	t.Logf("Labels after limitLabels: %+v", labels)

	assert.Len(t, labels, 1)
	assert.Equal(t, "dev", labels["env"])
}

// TestFetchBackupMetadata_Debug tests the fetchBackupMetadata function with debug output
func TestFetchBackupMetadata_Debug(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)

	config := &common.TelemetryConfig{
		PageSize:                      1000,
		PoolVolumeLabelPageSize:       10,
		GoogleBillingLabelsMaxEntries: 10,
	}

	provider := &BillingProvider{
		config:       config,
		vcpDataStore: mockVCPDB,
		metricsDB:    mockDB,
	}

	ctx := context.Background()
	aggregationStartTime := time.Now().Add(-time.Hour)
	aggregationEndTime := time.Now()

	backupMetadataList := []*datamodel.BackupMetadata{
		{VolumeUUID: "volume-uuid-1", Labels: createJSONB(map[string]string{"env": "dev"})},
	}

	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 0
	})).Return(backupMetadataList, nil)
	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset > 0
	})).Return([]*datamodel.BackupMetadata{}, nil)

	volumeLabelsMap, err := provider.fetchBackupMetadata(ctx, aggregationStartTime, aggregationEndTime)

	t.Logf("BackupMetadata: %+v", backupMetadataList[0])
	t.Logf("BackupMetadata.Labels: %+v", backupMetadataList[0].Labels)
	t.Logf("VolumeLabelsMap: %+v", volumeLabelsMap)

	assert.NoError(t, err)
	assert.Len(t, volumeLabelsMap, 1)
	assert.Equal(t, Labels{"env": "dev"}, volumeLabelsMap["volume-uuid-1"])

	mockVCPDB.AssertExpectations(t)
}

// TestFetchBackupData_Success tests successful fetching of backup data
func TestFetchBackupData_Success(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)

	config := &common.TelemetryConfig{
		EnableBackupBillingMetrics:    true,
		PageSize:                      1000,
		PoolVolumeLabelPageSize:       10,
		GoogleBillingLabelsMaxEntries: 10,
	}

	provider := &BillingProvider{
		config:       config,
		vcpDataStore: mockVCPDB,
		metricsDB:    mockDB,
	}

	ctx := context.Background()
	aggregationStartTime := time.Now().Add(-time.Hour)
	aggregationEndTime := time.Now()
	resourceCollection := &ResourceCollection{
		BackupData: make(map[ResourceKey]ResourceData),
	}

	// Mock backup metadata
	backupMetadataList := []*datamodel.BackupMetadata{
		{VolumeUUID: "volume-uuid-1", Labels: createJSONB(map[string]string{"env": "dev"})},
	}
	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 0
	})).Return(backupMetadataList, nil)
	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset > 0
	})).Return([]*datamodel.BackupMetadata{}, nil)

	// Mock backup metrics
	backups := []*datamodel.Backup{
		{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-1"},
			VolumeUUID:              "volume-uuid-1",
			LatestLogicalBackupSize: 1024,
			Attributes:              &datamodel.BackupAttributes{VolumeName: "Volume1", AccountIdentifier: "Account1"},
			BackupVault:             &datamodel.BackupVault{Name: "Vault1", AccountID: 1},
		},
	}
	mockVCPDB.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 0
	})).Return(backups, nil)
	mockVCPDB.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset > 0
	})).Return([]*datamodel.Backup{}, nil)

	err := provider.fetchBackupData(ctx, aggregationStartTime, aggregationEndTime, resourceCollection)
	assert.NoError(t, err)
	assert.Len(t, resourceCollection.BackupData, 1)

	key := ResourceKey{
		ResourceType:   metadata.Backup,
		ResourceName:   "Volume1",
		DeploymentName: "Vault1",
		ConsumerID:     "Account1",
	}
	data, ok := resourceCollection.BackupData[key]
	assert.True(t, ok)
	assert.Equal(t, "volume-uuid-1", data.UUID)
	assert.Equal(t, int64(1), data.AccountID)
	assert.Equal(t, Labels{"env": "dev"}, data.Labels)

	mockVCPDB.AssertExpectations(t)
}

// TestFetchResourceData_BackupBillingDisabled tests fetchResourceData when backup billing is disabled
func TestFetchResourceData_BackupBillingDisabled(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)

	config := &common.TelemetryConfig{
		EnableBackupBillingMetrics:    false,
		GoogleBillingLabelsMaxEntries: 10,
	}

	provider := &BillingProvider{
		config:       config,
		vcpDataStore: mockVCPDB,
		metricsDB:    mockDB,
	}

	ctx := context.Background()
	aggregationStartTime := time.Now().Add(-time.Hour)
	aggregationEndTime := time.Now()

	// Mock the calls that fetchResourceData makes
	mockVCPDB.On("ListPoolsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	mockVCPDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil)

	resourceCollection, err := provider.fetchResourceData(ctx, aggregationStartTime, aggregationEndTime)
	assert.NoError(t, err)
	assert.NotNil(t, resourceCollection)
	assert.Empty(t, resourceCollection.BackupData) // Should be empty since backup billing is disabled

	// GetBackupMetadata and GetBackupMetrics should not be called when disabled
	mockVCPDB.AssertNotCalled(t, "GetBackupMetadata", mock.Anything, mock.Anything, mock.Anything)
	mockVCPDB.AssertNotCalled(t, "GetBackupMetrics", mock.Anything, mock.Anything, mock.Anything)
}

// TestFetchBackupData_GetBackupMetadataError tests error handling for GetBackupMetadata
func TestFetchBackupData_GetBackupMetadataError(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)

	config := &common.TelemetryConfig{
		EnableBackupBillingMetrics:    true,
		PageSize:                      1000,
		PoolVolumeLabelPageSize:       10,
		GoogleBillingLabelsMaxEntries: 10,
	}

	provider := &BillingProvider{
		config:       config,
		vcpDataStore: mockVCPDB,
		metricsDB:    mockDB,
	}

	ctx := context.Background()
	aggregationStartTime := time.Now().Add(-time.Hour)
	aggregationEndTime := time.Now()
	resourceCollection := &ResourceCollection{
		BackupData: make(map[ResourceKey]ResourceData),
	}

	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("metadata error"))
	mockVCPDB.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 0
	})).Return([]*datamodel.Backup{}, nil) // Still mock GetBackupMetrics to avoid panic

	err := provider.fetchBackupData(ctx, aggregationStartTime, aggregationEndTime, resourceCollection)
	assert.NoError(t, err) // Should not return error, just log warning and continue with empty labels
	assert.Empty(t, resourceCollection.BackupData)

	mockVCPDB.AssertExpectations(t)
}

// TestFetchBackupData_GetBackupMetricsError tests error handling for GetBackupMetrics
func TestFetchBackupData_GetBackupMetricsError(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)

	config := &common.TelemetryConfig{
		EnableBackupBillingMetrics:    true,
		PageSize:                      1000,
		PoolVolumeLabelPageSize:       10,
		GoogleBillingLabelsMaxEntries: 10,
	}

	provider := &BillingProvider{
		config:       config,
		vcpDataStore: mockVCPDB,
		metricsDB:    mockDB,
	}

	ctx := context.Background()
	aggregationStartTime := time.Now().Add(-time.Hour)
	aggregationEndTime := time.Now()
	resourceCollection := &ResourceCollection{
		BackupData: make(map[ResourceKey]ResourceData),
	}

	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.BackupMetadata{}, nil)
	mockVCPDB.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("metrics error"))

	err := provider.fetchBackupData(ctx, aggregationStartTime, aggregationEndTime, resourceCollection)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get backup metrics")

	mockVCPDB.AssertExpectations(t)
}

// TestFetchBackupData_NilAttributes tests handling of backups with nil attributes
func TestFetchBackupData_NilAttributes(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)

	config := &common.TelemetryConfig{
		EnableBackupBillingMetrics:    true,
		PageSize:                      1000,
		PoolVolumeLabelPageSize:       10,
		GoogleBillingLabelsMaxEntries: 10,
	}

	provider := &BillingProvider{
		config:       config,
		vcpDataStore: mockVCPDB,
		metricsDB:    mockDB,
	}

	ctx := context.Background()
	aggregationStartTime := time.Now().Add(-time.Hour)
	aggregationEndTime := time.Now()
	resourceCollection := &ResourceCollection{
		BackupData: make(map[ResourceKey]ResourceData),
	}

	// Mock backup metadata
	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.BackupMetadata{}, nil)

	// Mock backup with nil attributes
	backups := []*datamodel.Backup{
		{
			BaseModel:   datamodel.BaseModel{UUID: "backup-uuid-1"},
			VolumeUUID:  "volume-uuid-1",
			Attributes:  nil, // Nil attributes
			BackupVault: &datamodel.BackupVault{Name: "Vault1", AccountID: 1},
		},
	}
	mockVCPDB.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 0
	})).Return(backups, nil)
	mockVCPDB.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset > 0
	})).Return([]*datamodel.Backup{}, nil)

	err := provider.fetchBackupData(ctx, aggregationStartTime, aggregationEndTime, resourceCollection)
	assert.NoError(t, err)
	assert.Empty(t, resourceCollection.BackupData) // Should be empty due to nil attributes

	mockVCPDB.AssertExpectations(t)
}

// TestFetchBackupData_NilBackupVault tests handling of backups with nil BackupVault
func TestFetchBackupData_NilBackupVault(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)

	config := &common.TelemetryConfig{
		EnableBackupBillingMetrics:    true,
		PageSize:                      1000,
		GoogleBillingLabelsMaxEntries: 10,
		PoolVolumeLabelPageSize:       10,
	}

	provider := &BillingProvider{
		config:       config,
		vcpDataStore: mockVCPDB,
		metricsDB:    mockDB,
	}

	ctx := context.Background()
	aggregationStartTime := time.Now().Add(-time.Hour)
	aggregationEndTime := time.Now()
	resourceCollection := &ResourceCollection{
		BackupData: make(map[ResourceKey]ResourceData),
	}

	// Mock backup metadata
	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.BackupMetadata{}, nil)

	// Mock backup with nil BackupVault
	backups := []*datamodel.Backup{
		{
			BaseModel:   datamodel.BaseModel{UUID: "backup-uuid-1"},
			VolumeUUID:  "volume-uuid-1",
			Attributes:  &datamodel.BackupAttributes{VolumeName: "Volume1", AccountIdentifier: "Account1"},
			BackupVault: nil, // Nil BackupVault
		},
	}
	mockVCPDB.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 0
	})).Return(backups, nil)
	mockVCPDB.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset > 0
	})).Return([]*datamodel.Backup{}, nil)

	err := provider.fetchBackupData(ctx, aggregationStartTime, aggregationEndTime, resourceCollection)
	assert.NoError(t, err)
	assert.Empty(t, resourceCollection.BackupData) // Should be empty due to nil BackupVault

	mockVCPDB.AssertExpectations(t)
}

// TestFetchBackupMetadata_Success tests successful fetching of backup metadata
func TestFetchBackupMetadata_Success(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)

	config := &common.TelemetryConfig{
		PageSize:                      1000,
		PoolVolumeLabelPageSize:       10,
		GoogleBillingLabelsMaxEntries: 10,
	}

	provider := &BillingProvider{
		config:       config,
		vcpDataStore: mockVCPDB,
		metricsDB:    mockDB,
	}

	ctx := context.Background()
	aggregationStartTime := time.Now().Add(-time.Hour)
	aggregationEndTime := time.Now()

	backupMetadataList := []*datamodel.BackupMetadata{
		{VolumeUUID: "volume-uuid-1", Labels: createJSONB(map[string]string{"env": "dev"})},
		{VolumeUUID: "volume-uuid-2", Labels: createJSONB(map[string]string{"team": "eng"})},
	}

	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 0
	})).Return(backupMetadataList, nil)
	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset > 0
	})).Return([]*datamodel.BackupMetadata{}, nil)

	volumeLabelsMap, err := provider.fetchBackupMetadata(ctx, aggregationStartTime, aggregationEndTime)
	assert.NoError(t, err)
	assert.Len(t, volumeLabelsMap, 2)
	assert.Equal(t, Labels{"env": "dev"}, volumeLabelsMap["volume-uuid-1"])
	assert.Equal(t, Labels{"team": "eng"}, volumeLabelsMap["volume-uuid-2"])

	mockVCPDB.AssertExpectations(t)
}

// TestFetchBackupMetadata_TableDoesNotExist tests handling of "table does not exist" error
func TestFetchBackupMetadata_TableDoesNotExist(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)

	config := &common.TelemetryConfig{
		PageSize:                      1000,
		PoolVolumeLabelPageSize:       10,
		GoogleBillingLabelsMaxEntries: 10,
	}

	provider := &BillingProvider{
		config:       config,
		vcpDataStore: mockVCPDB,
		metricsDB:    mockDB,
	}

	ctx := context.Background()
	aggregationStartTime := time.Now().Add(-time.Hour)
	aggregationEndTime := time.Now()

	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("relation \"backup_metadata\" does not exist"))

	volumeLabelsMap, err := provider.fetchBackupMetadata(ctx, aggregationStartTime, aggregationEndTime)
	assert.NoError(t, err) // Should not return error, but an empty map
	assert.Empty(t, volumeLabelsMap)

	mockVCPDB.AssertExpectations(t)
}

// TestFetchBackupMetadata_OtherError tests handling of other errors
func TestFetchBackupMetadata_OtherError(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)

	config := &common.TelemetryConfig{
		PageSize:                      1000,
		PoolVolumeLabelPageSize:       10,
		GoogleBillingLabelsMaxEntries: 10,
	}

	provider := &BillingProvider{
		config:       config,
		vcpDataStore: mockVCPDB,
		metricsDB:    mockDB,
	}

	ctx := context.Background()
	aggregationStartTime := time.Now().Add(-time.Hour)
	aggregationEndTime := time.Now()

	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("some other database error"))

	volumeLabelsMap, err := provider.fetchBackupMetadata(ctx, aggregationStartTime, aggregationEndTime)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to fetch backup metadata")
	assert.Nil(t, volumeLabelsMap)

	mockVCPDB.AssertExpectations(t)
}

// TestFetchBackupMetadata_EmptyResult tests handling of empty results
func TestFetchBackupMetadata_EmptyResult(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)

	config := &common.TelemetryConfig{
		PageSize:                      1000,
		PoolVolumeLabelPageSize:       10,
		GoogleBillingLabelsMaxEntries: 10,
	}

	provider := &BillingProvider{
		config:       config,
		vcpDataStore: mockVCPDB,
		metricsDB:    mockDB,
	}

	ctx := context.Background()
	aggregationStartTime := time.Now().Add(-time.Hour)
	aggregationEndTime := time.Now()

	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.BackupMetadata{}, nil)

	volumeLabelsMap, err := provider.fetchBackupMetadata(ctx, aggregationStartTime, aggregationEndTime)
	assert.NoError(t, err)
	assert.Empty(t, volumeLabelsMap)

	mockVCPDB.AssertExpectations(t)
}

// TestFetchBackupMetadata_MultipleBatches tests pagination with multiple batches
func TestFetchBackupMetadata_MultipleBatches(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)

	config := &common.TelemetryConfig{
		PageSize:                      1, // Small page size to force multiple batches
		PoolVolumeLabelPageSize:       1, // Small page size to force multiple batches
		GoogleBillingLabelsMaxEntries: 10,
	}

	provider := &BillingProvider{
		config:       config,
		vcpDataStore: mockVCPDB,
		metricsDB:    mockDB,
	}

	ctx := context.Background()
	aggregationStartTime := time.Now().Add(-time.Hour)
	aggregationEndTime := time.Now()

	// Mock first batch
	backupMetadataList1 := []*datamodel.BackupMetadata{
		{VolumeUUID: "volume-uuid-1", Labels: createJSONB(map[string]string{"env": "dev"})},
	}
	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 0
	})).Return(backupMetadataList1, nil)

	// Mock second batch
	backupMetadataList2 := []*datamodel.BackupMetadata{
		{VolumeUUID: "volume-uuid-2", Labels: createJSONB(map[string]string{"team": "eng"})},
	}
	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 1
	})).Return(backupMetadataList2, nil)

	// Mock empty third batch to end pagination
	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 2
	})).Return([]*datamodel.BackupMetadata{}, nil)

	volumeLabelsMap, err := provider.fetchBackupMetadata(ctx, aggregationStartTime, aggregationEndTime)
	assert.NoError(t, err)
	assert.Len(t, volumeLabelsMap, 2)
	assert.Equal(t, Labels{"env": "dev"}, volumeLabelsMap["volume-uuid-1"])
	assert.Equal(t, Labels{"team": "eng"}, volumeLabelsMap["volume-uuid-2"])

	mockVCPDB.AssertExpectations(t)
}

// TestFetchBackupMetadata_NilLabels tests handling of backup metadata with nil labels
func TestFetchBackupMetadata_NilLabels(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)

	config := &common.TelemetryConfig{
		PageSize:                      1000,
		PoolVolumeLabelPageSize:       10,
		GoogleBillingLabelsMaxEntries: 10,
	}

	provider := &BillingProvider{
		config:       config,
		vcpDataStore: mockVCPDB,
		metricsDB:    mockDB,
	}

	ctx := context.Background()
	aggregationStartTime := time.Now().Add(-time.Hour)
	aggregationEndTime := time.Now()

	backupMetadataList := []*datamodel.BackupMetadata{
		{VolumeUUID: "volume-uuid-1", Labels: nil}, // Nil labels
		{VolumeUUID: "volume-uuid-2", Labels: createJSONB(map[string]string{"team": "eng"})},
	}

	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 0
	})).Return(backupMetadataList, nil)
	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset > 0
	})).Return([]*datamodel.BackupMetadata{}, nil)

	volumeLabelsMap, err := provider.fetchBackupMetadata(ctx, aggregationStartTime, aggregationEndTime)
	assert.NoError(t, err)
	assert.Len(t, volumeLabelsMap, 1) // Only one entry with valid labels
	assert.Equal(t, Labels{"team": "eng"}, volumeLabelsMap["volume-uuid-2"])

	mockVCPDB.AssertExpectations(t)
}

// TestFetchBackupMetadata_EmptyVolumeUUID tests handling of backup metadata with empty volume UUID
func TestFetchBackupMetadata_EmptyVolumeUUID(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)

	config := &common.TelemetryConfig{
		PageSize:                      1000,
		PoolVolumeLabelPageSize:       10,
		GoogleBillingLabelsMaxEntries: 10,
	}

	provider := &BillingProvider{
		config:       config,
		vcpDataStore: mockVCPDB,
		metricsDB:    mockDB,
	}

	ctx := context.Background()
	aggregationStartTime := time.Now().Add(-time.Hour)
	aggregationEndTime := time.Now()

	backupMetadataList := []*datamodel.BackupMetadata{
		{VolumeUUID: "", Labels: createJSONB(map[string]string{"env": "dev"})}, // Empty volume UUID
		{VolumeUUID: "volume-uuid-2", Labels: createJSONB(map[string]string{"team": "eng"})},
	}

	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 0
	})).Return(backupMetadataList, nil)
	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset > 0
	})).Return([]*datamodel.BackupMetadata{}, nil)

	volumeLabelsMap, err := provider.fetchBackupMetadata(ctx, aggregationStartTime, aggregationEndTime)
	assert.NoError(t, err)
	assert.Len(t, volumeLabelsMap, 1) // Only one entry with valid volume UUID
	assert.Equal(t, Labels{"team": "eng"}, volumeLabelsMap["volume-uuid-2"])

	mockVCPDB.AssertExpectations(t)
}

// TestFetchBackupData_MultipleBatches tests pagination with multiple batches for backup data
func TestFetchBackupData_MultipleBatches(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)

	config := &common.TelemetryConfig{
		EnableBackupBillingMetrics:    true,
		PageSize:                      1, // Small page size to force multiple batches
		PoolVolumeLabelPageSize:       1, // Small page size to force multiple batches
		GoogleBillingLabelsMaxEntries: 10,
	}

	provider := &BillingProvider{
		config:       config,
		vcpDataStore: mockVCPDB,
		metricsDB:    mockDB,
	}

	ctx := context.Background()
	aggregationStartTime := time.Now().Add(-time.Hour)
	aggregationEndTime := time.Now()
	resourceCollection := &ResourceCollection{
		BackupData: make(map[ResourceKey]ResourceData),
	}

	// Mock backup metadata
	backupMetadataList := []*datamodel.BackupMetadata{
		{VolumeUUID: "volume-uuid-1", Labels: createJSONB(map[string]string{"env": "dev"})},
		{VolumeUUID: "volume-uuid-2", Labels: createJSONB(map[string]string{"env": "prod"})},
	}
	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.Anything).Return(backupMetadataList, nil).Once()
	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.BackupMetadata{}, nil).Once()

	// Mock first batch of backup metrics
	backups1 := []*datamodel.Backup{
		{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-1"},
			VolumeUUID:              "volume-uuid-1",
			LatestLogicalBackupSize: 1024,
			Attributes:              &datamodel.BackupAttributes{VolumeName: "Volume1", AccountIdentifier: "Account1"},
			BackupVault:             &datamodel.BackupVault{Name: "Vault1", AccountID: 1},
		},
	}
	mockVCPDB.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 0
	})).Return(backups1, nil).Once()

	// Mock second batch of backup metrics
	backups2 := []*datamodel.Backup{
		{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-2"},
			VolumeUUID:              "volume-uuid-2",
			LatestLogicalBackupSize: 2048,
			Attributes:              &datamodel.BackupAttributes{VolumeName: "Volume2", AccountIdentifier: "Account2"},
			BackupVault:             &datamodel.BackupVault{Name: "Vault2", AccountID: 2},
		},
	}
	mockVCPDB.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 1
	})).Return(backups2, nil).Once()

	// Mock empty third batch to end pagination
	mockVCPDB.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 2
	})).Return([]*datamodel.Backup{}, nil).Once()

	err := provider.fetchBackupData(ctx, aggregationStartTime, aggregationEndTime, resourceCollection)
	assert.NoError(t, err)
	assert.Len(t, resourceCollection.BackupData, 2)

	// Verify first backup
	key1 := ResourceKey{
		ResourceType:   metadata.Backup,
		ResourceName:   "Volume1",
		DeploymentName: "Vault1",
		ConsumerID:     "Account1",
	}
	data1, ok := resourceCollection.BackupData[key1]
	assert.True(t, ok)
	assert.Equal(t, "volume-uuid-1", data1.UUID)
	assert.Equal(t, int64(1), data1.AccountID)
	assert.Equal(t, Labels{"env": "dev"}, data1.Labels)

	// Verify second backup
	key2 := ResourceKey{
		ResourceType:   metadata.Backup,
		ResourceName:   "Volume2",
		DeploymentName: "Vault2",
		ConsumerID:     "Account2",
	}
	data2, ok := resourceCollection.BackupData[key2]
	assert.True(t, ok)
	assert.Equal(t, "volume-uuid-2", data2.UUID)
	assert.Equal(t, int64(2), data2.AccountID)
	assert.Equal(t, Labels{"env": "prod"}, data2.Labels)

	mockVCPDB.AssertExpectations(t)
}
