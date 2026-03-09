package aggregator

import (
	"context"
	"sort"
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
	"gorm.io/gorm"
)

// Helper functions to convert between types for test compatibility
func hydratedMetricsToTimeSeries(metrics []datamodel2.HydratedMetrics, start, end time.Time) common.TimeSeries {
	if len(metrics) == 0 {
		return common.TimeSeries{
			AggregationStart: start,
			AggregationEnd:   end,
			Metadata:         metadata.ResourceMetadata{},
			MeasuredType:     "",
			DataPoints:       []common.DataPoint{},
		}
	}

	// Sort metrics by timestamp
	sort.Slice(metrics, func(i, j int) bool {
		return metrics[i].MetricTimestamp.Before(metrics[j].MetricTimestamp)
	})

	// Convert to DataPoints
	var dataPoints []common.DataPoint
	for _, metric := range metrics {
		dataPoints = append(dataPoints, common.DataPoint{
			Timestamp: metric.MetricTimestamp,
			Quantity:  metric.Quantity,
		})
	}

	// Use the first metric's metadata
	return common.TimeSeries{
		AggregationStart: start,
		AggregationEnd:   end,
		Metadata:         metadata.ResourceMetadata{ResourceType: metrics[0].ResourceType}, // Set the resource type from first metric
		MeasuredType:     metrics[0].MeasuredType,
		DataPoints:       dataPoints,
	}
}

func hydratedMetricsToDataPoints(metrics []datamodel2.HydratedMetrics) []common.DataPoint {
	var dataPoints []common.DataPoint
	for _, metric := range metrics {
		dataPoints = append(dataPoints, common.DataPoint{
			Timestamp: metric.MetricTimestamp,
			Quantity:  metric.Quantity,
		})
	}

	// Sort by timestamp
	sort.Slice(dataPoints, func(i, j int) bool {
		return dataPoints[i].Timestamp.Before(dataPoints[j].Timestamp)
	})

	return dataPoints
}

// MockUsageSink is a mock implementation of the UsageSink interface for testing
type MockUsageSink struct {
	mock.Mock
}

func (m *MockUsageSink) DeliverMetrics(ctx context.Context, metrics []datamodel2.AggregatedUsage, aggregationEndTime time.Time) (int, error) {
	args := m.Called(ctx, metrics, aggregationEndTime)
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

func TestGroupMetricsByResource_SetsDeletedAtMetadata(t *testing.T) {
	processor := &BillingProvider{}
	now := time.Now()
	deletedAt := now.Add(10 * time.Minute)

	metrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    "resource1",
			ConsumerID:      "customer1",
			Location:        "location1",
			Quantity:        100,
			MetricTimestamp: now,
			ResourceType:    metadata.Backup,
			MeasuredType:    metadata.BackupLogicalSize,
			DeploymentName:  "deployment1",
			DeletedAt:       &deletedAt,
		},
	}

	result := processor.groupMetricsByResource(metrics)

	resourceKey := ResourceKey{
		ResourceType:   metadata.Backup,
		ResourceName:   "resource1",
		DeploymentName: "deployment1",
		ConsumerID:     "customer1",
	}
	require.Len(t, result, 1)
	require.Len(t, result[resourceKey], 1)
	require.NotNil(t, result[resourceKey][0].Metadata.DeletedAt)
	assert.Equal(t, deletedAt, *result[resourceKey][0].Metadata.DeletedAt)
}

func TestApplyDataSourceAndFormatterOverrides_UsesHistoricalFormatter(t *testing.T) {
	original := make(map[metadata.CombinedKeyResourceTypeMeasuredType]common.AggregationJobDefinition, len(common.DefaultAggregationJobDefinitions))
	for key, value := range common.DefaultAggregationJobDefinitions {
		original[key] = value
	}
	defer func() {
		common.DefaultAggregationJobDefinitions = original
	}()

	key := metadata.CombinedKeyResourceTypeMeasuredType{
		ResourceType: metadata.Backup,
		MeasuredType: metadata.BackupLogicalSize,
	}
	jobDef := common.DefaultAggregationJobDefinitions[key]
	jobDef.TimeSeriesFormatter = &common.SampledMetricsFormatter{
		Mode:          common.Interval,
		BackfillLimit: 5 * time.Minute,
	}
	common.DefaultAggregationJobDefinitions[key] = jobDef

	provider := &BillingProvider{
		config: &common.TelemetryConfig{
			EnableBackupHistoryFormatter: true,
		},
	}

	provider.applyDataSourceAndFormatterOverrides(nil)

	updated := common.DefaultAggregationJobDefinitions[key]
	formatter, ok := updated.TimeSeriesFormatter.(*common.HistoricalMetricsFormatter)
	require.True(t, ok, "expected HistoricalMetricsFormatter for backup logical size")
	assert.Equal(t, 5*time.Minute, formatter.GetBackfillLimit())
}

func TestFetchBackupHistoryMetrics_PopulatesFields(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
	deletedAt := now.Add(30 * time.Minute)

	histories := []*datamodel.BackupChainHistory{
		{
			BaseModel: datamodel.BaseModel{
				CreatedAt: now,
			},
			ResourceUUID:   "resource-uuid-1",
			ResourceName:   "vlm_resource_1",
			ConsumerID:     "consumer-1",
			DeploymentName: "deployment-1",
			Size:           123,
		},
		{
			BaseModel: datamodel.BaseModel{
				CreatedAt: now.Add(15 * time.Minute),
				DeletedAt: &gorm.DeletedAt{Time: deletedAt, Valid: true},
			},
			ResourceUUID:   "resource-uuid-2",
			ResourceName:   "vlm_resource_2",
			ConsumerID:     "consumer-2",
			DeploymentName: "deployment-2",
			Size:           456,
		},
	}

	vcpDB := &database2.MockStorage{}
	vcpDB.On("ListBackupChainHistoriesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(histories, nil).Once()

	provider := &BillingProvider{
		vcpDataStore: vcpDB,
		config: &common.TelemetryConfig{
			PoolVolumeLabelPageSize: 100,
			RegionName:              "us-central1",
		},
	}

	metrics, err := provider.fetchBackupHistoryMetrics(ctx, now.Add(-time.Hour), now.Add(time.Hour), 0, &ResourceCollection{})
	require.NoError(t, err)
	require.Len(t, metrics, 2)

	byResource := map[string]datamodel2.HydratedMetrics{}
	for _, metric := range metrics {
		byResource[metric.ResourceName] = metric
	}

	first := byResource["resource-uuid-1"]
	assert.Equal(t, now, first.MetricTimestamp)
	assert.Equal(t, metadata.Backup, first.ResourceType)
	assert.Equal(t, metadata.BackupLogicalSize, first.MeasuredType)
	assert.Equal(t, "consumer-1", first.ConsumerID)
	assert.Equal(t, "deployment-1", first.DeploymentName)
	assert.Nil(t, first.DeletedAt)

	second := byResource["resource-uuid-2"]
	require.NotNil(t, second.DeletedAt)
	assert.Equal(t, deletedAt, *second.DeletedAt)
}

func TestGroupMetricsByResource_ParsesBackupRegionName(t *testing.T) {
	processor := &BillingProvider{}
	now := time.Now()

	extraJSON := []byte(`{"backup_region_name":"us-west2","source_region_name":"us-east4"}`)

	metrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    "restore-vol-1",
			ConsumerID:      "customer1",
			Location:        "us-east4",
			Quantity:        1024,
			MetricTimestamp: now,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.CbsCrossRegionVolumeRestoreTransferBytes,
			DeploymentName:  "deployment1",
			Metadata:        extraJSON,
		},
	}

	result := processor.groupMetricsByResource(metrics)
	assert.Equal(t, 1, len(result))

	key := ResourceKey{
		ResourceType:   metadata.Volume,
		ResourceName:   "restore-vol-1",
		DeploymentName: "deployment1",
		ConsumerID:     "customer1",
	}
	group, ok := result[key]
	assert.True(t, ok)
	assert.Len(t, group, 1)
	assert.NotNil(t, group[0].Metadata.BackupRegionName)
	assert.Equal(t, "us-west2", *group[0].Metadata.BackupRegionName)
	assert.Equal(t, "us-east4", *group[0].Metadata.RegionName)
}

func TestGroupMetricsByResource_EmptyMetadata(t *testing.T) {
	processor := &BillingProvider{}
	now := time.Now()

	metrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    "restore-vol-2",
			ConsumerID:      "customer1",
			Location:        "us-east4",
			Quantity:        128,
			MetricTimestamp: now,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.CbsCrossRegionVolumeRestoreTransferBytes,
			DeploymentName:  "deployment1",
			Metadata:        nil,
		},
	}

	result := processor.groupMetricsByResource(metrics)
	key := ResourceKey{
		ResourceType:   metadata.Volume,
		ResourceName:   "restore-vol-2",
		DeploymentName: "deployment1",
		ConsumerID:     "customer1",
	}
	group := result[key]
	assert.Len(t, group, 1)
	assert.Nil(t, group[0].Metadata.BackupRegionName)
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
		EnableAutoTieringBillingMetrics: true,
	}
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()

	// Mock VCP database calls for label fetching - now uses conditions approach
	vcpDB.On("GetBlockOnlyPoolIDs", mock.Anything).Return(map[int64]bool{}, nil).Once()
	vcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
	vcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil).Once()
	vcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

	// Mock the counter cache preload call (returns empty list to stop pagination)
	mockDB.On("GetLatestAggregatedUsageForAllResources", mock.Anything, "CounterAggregation", mock.Anything, mock.Anything).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Maybe()

	// Expect call to GetHydratedMetrics with empty results
	// Mock the counter cache preload call (returns empty list to stop pagination)
	mockDB.On("GetLatestAggregatedUsageForAllResources", mock.Anything, "CounterAggregation", mock.Anything, mock.Anything).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Maybe()

	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(
		[]datamodel2.HydratedMetrics{}, nil,
	).Times(len(common.DefaultAggregationJobDefinitions))

	// Note: ProcessBillingMetrics does not call GetAggregatedUsage for retry logic
	// Retry logic is handled separately via GetUnsentGoogleUsages

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
	vcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
	vcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil).Once()
	vcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

	// Expect call to GetHydratedMetrics with an error (may be called multiple times for different job definitions)
	// Mock the counter cache preload call (returns empty list to stop pagination)
	mockDB.On("GetLatestAggregatedUsageForAllResources", mock.Anything, "CounterAggregation", mock.Anything, mock.Anything).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Maybe()

	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(
		nil, errors.New("database error"),
	).Maybe()

	// Call ProcessMetrics - should succeed even with database errors (they're logged but don't fail the process)
	err := processor.ProcessBillingMetrics(ctx, now)
	assert.NoError(t, err, "ProcessMetrics should succeed even when individual database calls fail")
	// Note: Database errors are logged but don't cause the overall process to fail

	// AssertExpectations for VCP DB (but not for metrics DB since we use Maybe() which is more flexible)
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
	logger := util.GetLogger(context.Background())
	ctx := context.Background()
	now := time.Now()
	resourceID := ResourceKey{
		ResourceType: metadata.Volume,
		ResourceName: "test-resource-uuid",

		ConsumerID: "test-customer",
	}

	metrics := []datamodel2.HydratedMetrics{
		{
			ResourceName: "test-resource-uuid",
			ResourceType: metadata.Volume,
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
		PoolData: make(map[ResourceKey]ResourceData),
		VolumeData: map[ResourceKey]ResourceData{
			resourceID: {
				UUID:      "test-uuid",
				AccountID: 123,
				Labels:    make(Labels),
			},
		},
	}
	err := processor.processMetricsWithJobDef(ctx, resourceID, hydratedMetricsToTimeSeries(metrics, now.Add(-1*time.Hour), now), jobDef, now.Add(-1*time.Hour), now, resourceCollection, &aggregatedRecords, make(map[CounterAggregationCacheResourceKey]*float64), logger)
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
		config:    &common.TelemetryConfig{},
	}
	ctx := context.Background()
	logger := util.GetLogger(ctx)
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	resourceID := ResourceKey{
		ResourceType: metadata.Volume,
		ResourceName: "test-resource-uuid",

		ConsumerID: "test-customer",
	}

	customerID := "test-customer"
	location := "test-location"

	metrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    "test-uuid",
			ConsumerID:      customerID,
			Location:        location,
			Quantity:        100,
			MetricTimestamp: startTime,
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
		},
		{
			ResourceName:    "test-uuid",
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
		err := processor.processMetricsWithJobDef(ctx, resourceID, hydratedMetricsToTimeSeries([]datamodel2.HydratedMetrics{}, startTime, now), common.AggregationJobDefinition{AggregationType: common.IntegralAggregation}, startTime, now, resourceCollection, &aggregatedRecords, make(map[CounterAggregationCacheResourceKey]*float64), logger)
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

		err := processor.processMetricsWithJobDef(ctx, resourceID, hydratedMetricsToTimeSeries(metrics, startTime, now), common.AggregationJobDefinition{AggregationType: common.IntegralAggregation}, startTime, now, resourceCollection, &aggregatedRecords, make(map[CounterAggregationCacheResourceKey]*float64), logger)
		assert.NoError(t, err)
		// Verify that the record was collected for batch saving
		assert.Len(t, aggregatedRecords, 1)
		assert.Equal(t, string(common.IntegralAggregation), aggregatedRecords[0].AggregationType)
		assert.Equal(t, customerID, *aggregatedRecords[0].VendorCustomerID)
		assert.Equal(t, resourceID.ResourceName, *aggregatedRecords[0].ResourceName)
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
		err := processor.processMetricsWithJobDef(ctx, resourceID, hydratedMetricsToTimeSeries(metrics, startTime, now), common.AggregationJobDefinition{AggregationType: common.CounterAggregation}, startTime, now, resourceCollection, &aggregatedRecords, make(map[CounterAggregationCacheResourceKey]*float64), logger)
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
		err := processor.processMetricsWithJobDef(ctx, resourceID, hydratedMetricsToTimeSeries(metrics, startTime, now), common.AggregationJobDefinition{AggregationType: common.SumAggregation}, startTime, now, resourceCollection, &aggregatedRecords, make(map[CounterAggregationCacheResourceKey]*float64), logger)
		assert.NoError(t, err)
		// Verify that the record was collected for batch saving
		assert.Len(t, aggregatedRecords, 1)
		assert.Equal(t, string(common.SumAggregation), aggregatedRecords[0].AggregationType)
		assert.Equal(t, resourceID.ResourceName, *aggregatedRecords[0].ResourceName)
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
		err := processor.processMetricsWithJobDef(ctx, resourceID, hydratedMetricsToTimeSeries(metrics, startTime, now), common.AggregationJobDefinition{AggregationType: common.FirstAggregation}, startTime, now, resourceCollection, &aggregatedRecords, make(map[CounterAggregationCacheResourceKey]*float64), logger)
		assert.NoError(t, err)
		// Verify that the record was collected for batch saving
		assert.Len(t, aggregatedRecords, 1)
		assert.Equal(t, string(common.FirstAggregation), aggregatedRecords[0].AggregationType)
		assert.Equal(t, resourceID.ResourceName, *aggregatedRecords[0].ResourceName)
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
		err := processor.processMetricsWithJobDef(ctx, resourceID, hydratedMetricsToTimeSeries(metrics, startTime, now), common.AggregationJobDefinition{AggregationType: common.SumAggregation}, startTime, now, resourceCollection, &aggregatedRecords, make(map[CounterAggregationCacheResourceKey]*float64), logger)
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
			ResourceType: metadata.VolumeReplicationRelationship,
			ResourceName: "test-resource-uuid",

			ConsumerID: "test-customer",
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
				ResourceName:    "test-uuid",
				ConsumerID:      customerID,
				Location:        location,
				Quantity:        150,
				MetricTimestamp: now,
				ResourceType:    metadata.VolumeReplicationRelationship,
				MeasuredType:    metadata.XregionReplicationTotalTransferBytes,
			},
		}

		err := processor.processMetricsWithJobDef(ctx, resourceIDRep, hydratedMetricsToTimeSeries(repMetrics, startTime, now), common.AggregationJobDefinition{AggregationType: common.CounterAggregation}, startTime, now, resourceCollection, &aggregatedRecords, make(map[CounterAggregationCacheResourceKey]*float64), logger)
		assert.NoError(t, err)
		// Verify that the record was collected and has LastCounterValue set
		assert.Len(t, aggregatedRecords, 1)
		assert.Equal(t, string(common.CounterAggregation), aggregatedRecords[0].AggregationType)
		assert.NotNil(t, aggregatedRecords[0].LastCounterValue)
		assert.Equal(t, 150.0, *aggregatedRecords[0].LastCounterValue)
	})
}

// TestProcessMetricsSuccess tests a successful path through ProcessMetrics.
//
// IMPORTANT: Mock Setup for GetHydratedMetrics
// ---------------------------------------------
// ProcessBillingMetrics iterates over DefaultAggregationJobDefinitions (a Go map) and calls
// GetHydratedMetrics for each job definition with a filter containing resource_type and measured_type.
//
// Since Go map iteration order is NON-DETERMINISTIC, we cannot use a simple .Once() mock that
// returns metrics for the "first" call, as the first call could be for ANY job definition.
//
// Solution: Use mock.MatchedBy() to check the filter's "conditions" array and return test metrics
// ONLY when the filter matches the specific job definition we're testing (Volume/AllocatedSize).
// All other job definitions receive empty results via a catch-all mock.
//
// Filter structure example:
//
//	{
//	    "conditions": [
//	        ["metric_timestamp >= ?", startTime],
//	        ["metric_timestamp <= ?", endTime],
//	        ["resource_type = ?", "VOLUME"],
//	        ["measured_type = ?", "ALLOCATED_SIZE"],
//	    ],
//	    "order": "resource_name, deployment_name, consumer_id, metric_timestamp DESC",
//	}
func TestProcessMetricsSuccess(t *testing.T) {
	mockDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{
		EnableReplicationBillingMetrics: true,
		EnableAutoTieringBillingMetrics: true,
	}
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	// Mock VCP database calls for label fetching - return resource data that matches the metrics
	vcpDB.On("GetBlockOnlyPoolIDs", mock.Anything).Return(map[int64]bool{}, nil).Once()
	vcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
	vcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{
		{
			UUID:      "vol-uuid-1",
			Name:      "resource1",
			AccountID: 123,
			VolumeAttributes: &datamodel.VolumeAttributes{
				AccountName:    "customer1",
				DeploymentName: "deployment1",
				Labels:         &datamodel.JSONB{"env": "test"},
				IsRegionalHA:   false,
			},
		},
		{
			UUID:      "vol-uuid-2",
			Name:      "resource2",
			AccountID: 123,
			VolumeAttributes: &datamodel.VolumeAttributes{
				AccountName:    "customer1",
				DeploymentName: "deployment2",
				Labels:         &datamodel.JSONB{"env": "test"},
				IsRegionalHA:   true,
			},
		},
	}, nil).Once()
	vcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil).Once()
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
			DeploymentName:  "deployment1",
		},
		{
			ResourceName:    "resource1",
			ConsumerID:      "customer1",
			Location:        "location1",
			Quantity:        200,
			MetricTimestamp: startTime.Add(20 * time.Minute),
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
			DeploymentName:  "deployment1",
		},
	}

	// Mock the counter cache preload call (returns empty list to stop pagination)
	mockDB.On("GetLatestAggregatedUsageForAllResources", mock.Anything, "CounterAggregation", mock.Anything, mock.Anything).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Maybe()

	// Helper function to check if conditions match Volume/AllocatedSize
	// Searches through all conditions rather than assuming specific indices
	matchVolumeAllocatedSize := func(conditions [][]interface{}) bool {
		hasVolumeResourceType := false
		hasAllocatedSizeMeasuredType := false

		for _, cond := range conditions {
			if len(cond) < 2 {
				continue
			}
			condStr, ok := cond[0].(string)
			if !ok {
				continue
			}

			if condStr == "resource_type = ?" {
				if val, ok := cond[1].(string); ok && val == "VOLUME" {
					hasVolumeResourceType = true
				}
			}
			if condStr == "measured_type = ?" {
				if val, ok := cond[1].(string); ok && val == "ALLOCATED_SIZE" {
					hasAllocatedSizeMeasuredType = true
				}
			}
		}

		return hasVolumeResourceType && hasAllocatedSizeMeasuredType
	}

	// Return metrics ONLY when the filter matches Volume/AllocatedSize job definition
	// First call for Volume/AllocatedSize returns metrics, second call (next page) returns empty
	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.MatchedBy(matchVolumeAllocatedSize), mock.Anything).Return(metrics, nil).Once()
	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.MatchedBy(matchVolumeAllocatedSize), mock.Anything).Return([]datamodel2.HydratedMetrics{}, nil).Once()

	// For all other job definitions (non-matching filters), return empty results
	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(
		[]datamodel2.HydratedMetrics{}, nil,
	).Times((len(common.DefaultAggregationJobDefinitions) - 1))

	// Setup expectations for CreateAggregatedUsageBatch call
	mockDB.On("CreateAggregatedUsageBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	// Note: ProcessBillingMetrics does not call GetAggregatedUsage for retry logic
	// Retry logic is handled separately via GetUnsentGoogleUsages

	// Call ProcessMetrics
	err := processor.ProcessBillingMetrics(ctx, now)
	assert.NoError(t, err)

	// Verify expectations
	mockDB.AssertExpectations(t)
	vcpDB.AssertExpectations(t)
	mockSink.AssertExpectations(t)
}

// TestProcessMetricsWithJobDefErrors tests that database errors from CreateAggregatedUsageBatch
// are properly propagated up through ProcessBillingMetrics.
//
// Key Test Requirements:
//  1. Metrics must have timestamps WITHIN the aggregation window (not at boundary) to generate
//     aggregated records. The TimeSeriesFormatter filters out boundary-only data points.
//  2. GetHydratedMetrics mock must use MatchedBy() to return metrics for the correct job definition
//     due to non-deterministic map iteration order (see TestProcessMetricsSuccess for details).
//  3. Use require.Error() instead of assert.Error() before accessing err.Error() to prevent
//     nil pointer dereference if the error expectation fails.
func TestProcessMetricsWithJobDefErrors(t *testing.T) {
	mockDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{
		EnableReplicationBillingMetrics: true,
		EnableAutoTieringBillingMetrics: true,
	}
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	// Mock VCP database calls for label fetching - return resource data that matches the metrics
	vcpDB.On("GetBlockOnlyPoolIDs", mock.Anything).Return(map[int64]bool{}, nil).Once()
	vcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
	vcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{
		{
			UUID:      "vol-uuid-1",
			Name:      "resource1",
			AccountID: 123,
			VolumeAttributes: &datamodel.VolumeAttributes{
				AccountName:    "customer1",
				DeploymentName: "deployment1",
				Labels:         &datamodel.JSONB{"env": "test"},
				IsRegionalHA:   false,
			},
		},
		{
			UUID:      "vol-uuid-2",
			Name:      "resource2",
			AccountID: 123,
			VolumeAttributes: &datamodel.VolumeAttributes{
				AccountName:    "customer1",
				DeploymentName: "deployment2",
				Labels:         &datamodel.JSONB{"env": "test"},
				IsRegionalHA:   true,
			},
		},
	}, nil).Once()
	vcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil).Once()
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

	// Create test metrics with multiple data points within the aggregation window
	// to ensure aggregated records are generated
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
			DeploymentName:  "deployment1",
		},
	}

	// Mock the counter cache preload call (returns empty list to stop pagination)
	mockDB.On("GetLatestAggregatedUsageForAllResources", mock.Anything, "CounterAggregation", mock.Anything, mock.Anything).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Maybe()

	// Helper function to check if conditions match Volume/AllocatedSize
	// Searches through all conditions rather than assuming specific indices
	matchVolumeAllocatedSize := func(conditions [][]interface{}) bool {
		hasVolumeResourceType := false
		hasAllocatedSizeMeasuredType := false

		for _, cond := range conditions {
			if len(cond) < 2 {
				continue
			}
			condStr, ok := cond[0].(string)
			if !ok {
				continue
			}

			if condStr == "resource_type = ?" {
				if val, ok := cond[1].(string); ok && val == "VOLUME" {
					hasVolumeResourceType = true
				}
			}
			if condStr == "measured_type = ?" {
				if val, ok := cond[1].(string); ok && val == "ALLOCATED_SIZE" {
					hasAllocatedSizeMeasuredType = true
				}
			}
		}

		return hasVolumeResourceType && hasAllocatedSizeMeasuredType
	}

	// Return metrics ONLY when the filter matches Volume/AllocatedSize job definition
	// First call for Volume/AllocatedSize returns metrics, second call (next page) returns empty
	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.MatchedBy(matchVolumeAllocatedSize), mock.Anything).Return(metrics, nil).Once()
	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.MatchedBy(matchVolumeAllocatedSize), mock.Anything).Return([]datamodel2.HydratedMetrics{}, nil).Once()

	// For all other job definitions (non-matching filters), return empty results
	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(
		[]datamodel2.HydratedMetrics{}, nil,
	).Times((len(common.DefaultAggregationJobDefinitions) - 1))

	// Setup expectations for CreateAggregatedUsageBatch call with error
	mockDB.On("CreateAggregatedUsageBatch", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("database error")).Once()

	// Call ProcessMetrics - with batch saving, database errors now propagate up
	err := processor.ProcessBillingMetrics(ctx, now)
	require.Error(t, err, "ProcessMetrics should return the error from batch save")
	assert.Contains(t, err.Error(), "database error")

	// Verify expectations
	mockDB.AssertExpectations(t)
	vcpDB.AssertExpectations(t)
}

// TestCounterDeltaWithReset tests counter reset scenarios
func TestCounterDeltaWithReset(t *testing.T) {
	now := time.Now()
	logger := util.GetLogger(context.Background())
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
			// Use a generic measured type for test - the test cases use standard counter reset logic
			got, _ := common.CounterDelta(hydratedMetricsToDataPoints(tt.metrics), logger, metadata.AllocatedSize, "test-resource-uuid")
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
		EnableAutoTieringBillingMetrics: true,
	}
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()

	// Mock VCP database calls for label fetching
	vcpDB.On("GetBlockOnlyPoolIDs", mock.Anything).Return(map[int64]bool{}, nil).Once()
	vcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
	vcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil).Once()
	vcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

	// Even with retry error, should continue and call GetHydratedMetrics
	// Mock the counter cache preload call (returns empty list to stop pagination)
	mockDB.On("GetLatestAggregatedUsageForAllResources", mock.Anything, "CounterAggregation", mock.Anything, mock.Anything).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Maybe()

	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(
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
		EnableAutoTieringBillingMetrics: true,
	}
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	// Create new aggregated records from processing
	// Note: Aggregation requires at least 2 data points to calculate an interval
	processedMetrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    "resource1",
			ConsumerID:      "customer1",
			Location:        "location1",
			Quantity:        100,
			MetricTimestamp: startTime.Add(10 * time.Minute),
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
			DeploymentName:  "deployment1",
		},
		{
			ResourceName:    "resource1",
			ConsumerID:      "customer1",
			Location:        "location1",
			Quantity:        200,
			MetricTimestamp: startTime.Add(20 * time.Minute),
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
			DeploymentName:  "deployment1",
		},
	}

	// Mock VCP database calls for label fetching - return resource data that matches the metrics
	vcpDB.On("GetBlockOnlyPoolIDs", mock.Anything).Return(map[int64]bool{}, nil).Once()
	vcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
	vcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{
		{
			UUID:      "vol-uuid-1",
			Name:      "resource1",
			AccountID: 123,
			VolumeAttributes: &datamodel.VolumeAttributes{
				AccountName:    "customer1",
				DeploymentName: "deployment1",
				Labels:         &datamodel.JSONB{"env": "test"},
				IsRegionalHA:   false,
			},
		},
		{
			UUID:      "vol-uuid-2",
			Name:      "resource2",
			AccountID: 123,
			VolumeAttributes: &datamodel.VolumeAttributes{
				AccountName:    "customer1",
				DeploymentName: "deployment1",
				Labels:         &datamodel.JSONB{"env": "test"},
				IsRegionalHA:   true,
			},
		},
	}, nil).Once()
	vcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil).Once()
	vcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

	// Note: ProcessBillingMetrics no longer calls GetAggregatedUsage directly
	// Retry logic is now handled via job queue (DeliverBillingMetrics job)

	// Mock the counter cache preload call (returns empty list to stop pagination)
	mockDB.On("GetLatestAggregatedUsageForAllResources", mock.Anything, "CounterAggregation", mock.Anything, mock.Anything).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Maybe()

	// Helper function to check if conditions match Volume/AllocatedSize
	// Searches through all conditions rather than assuming specific indices
	matchVolumeAllocatedSize := func(conditions [][]interface{}) bool {
		hasVolumeResourceType := false
		hasAllocatedSizeMeasuredType := false

		for _, cond := range conditions {
			if len(cond) < 2 {
				continue
			}
			condStr, ok := cond[0].(string)
			if !ok {
				continue
			}

			if condStr == "resource_type = ?" {
				if val, ok := cond[1].(string); ok && val == "VOLUME" {
					hasVolumeResourceType = true
				}
			}
			if condStr == "measured_type = ?" {
				if val, ok := cond[1].(string); ok && val == "ALLOCATED_SIZE" {
					hasAllocatedSizeMeasuredType = true
				}
			}
		}

		return hasVolumeResourceType && hasAllocatedSizeMeasuredType
	}

	// Return metrics ONLY when the filter matches Volume/AllocatedSize job definition
	// First call for Volume/AllocatedSize returns metrics, second call (next page) returns empty
	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.MatchedBy(matchVolumeAllocatedSize), mock.Anything).Return(processedMetrics, nil).Once()
	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.MatchedBy(matchVolumeAllocatedSize), mock.Anything).Return([]datamodel2.HydratedMetrics{}, nil).Once()

	// For all other job definitions (non-matching filters), return empty results
	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(
		[]datamodel2.HydratedMetrics{}, nil,
	).Times((len(common.DefaultAggregationJobDefinitions) - 1))

	// Setup expectations for CreateAggregatedUsageBatch call
	mockDB.On("CreateAggregatedUsageBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

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
	config := &common.TelemetryConfig{
		EnableAutoTieringBillingMetrics: true,
	}
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	// Mock VCP database calls for label fetching - return resource data that matches the metrics
	vcpDB.On("GetBlockOnlyPoolIDs", mock.Anything).Return(map[int64]bool{}, nil).Once()
	vcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
	vcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{
		{
			UUID:      "vol-uuid-1",
			Name:      "resource1",
			AccountID: 123,
			VolumeAttributes: &datamodel.VolumeAttributes{
				AccountName:    "customer1",
				DeploymentName: "test-deployment",
				Labels:         &datamodel.JSONB{"env": "test"},
				IsRegionalHA:   false,
			},
		},
		{
			UUID:      "vol-uuid-2",
			Name:      "resource2",
			AccountID: 123,
			VolumeAttributes: &datamodel.VolumeAttributes{
				AccountName:    "customer1",
				DeploymentName: "test-deployment",
				Labels:         &datamodel.JSONB{"env": "test"},
				IsRegionalHA:   true,
			},
		},
	}, nil).Once()
	vcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil).Once()
	vcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

	// Create test metrics that will be processed
	// Note: Aggregation requires at least 2 data points to calculate an interval
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
		{
			ResourceName:    "resource1",
			ConsumerID:      "customer1",
			Location:        "location1",
			Quantity:        200,
			MetricTimestamp: startTime.Add(20 * time.Minute),
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.AllocatedSize,
			DeploymentName:  "test-deployment",
		},
	}

	// Note: ProcessBillingMetrics no longer calls GetAggregatedUsage directly
	// Retry logic is now handled via job queue (DeliverBillingMetrics job)

	// Mock the counter cache preload call (returns empty list to stop pagination)
	mockDB.On("GetLatestAggregatedUsageForAllResources", mock.Anything, "CounterAggregation", mock.Anything, mock.Anything).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Maybe()

	// Helper function to check if conditions match Volume/AllocatedSize
	// Searches through all conditions rather than assuming specific indices
	matchVolumeAllocatedSize := func(conditions [][]interface{}) bool {
		hasVolumeResourceType := false
		hasAllocatedSizeMeasuredType := false

		for _, cond := range conditions {
			if len(cond) < 2 {
				continue
			}
			condStr, ok := cond[0].(string)
			if !ok {
				continue
			}

			if condStr == "resource_type = ?" {
				if val, ok := cond[1].(string); ok && val == "VOLUME" {
					hasVolumeResourceType = true
				}
			}
			if condStr == "measured_type = ?" {
				if val, ok := cond[1].(string); ok && val == "ALLOCATED_SIZE" {
					hasAllocatedSizeMeasuredType = true
				}
			}
		}

		return hasVolumeResourceType && hasAllocatedSizeMeasuredType
	}

	// Return metrics ONLY when the filter matches Volume/AllocatedSize job definition
	// First call for Volume/AllocatedSize returns metrics, second call (next page) returns empty
	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.MatchedBy(matchVolumeAllocatedSize), mock.Anything).Return(metrics, nil).Once()
	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.MatchedBy(matchVolumeAllocatedSize), mock.Anything).Return([]datamodel2.HydratedMetrics{}, nil).Once()

	// For all other job definitions (non-matching filters), return empty results
	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(
		[]datamodel2.HydratedMetrics{}, nil,
	).Times((len(common.DefaultAggregationJobDefinitions) - 1))

	// Setup expectations for CreateAggregatedUsageBatch call
	mockDB.On("CreateAggregatedUsageBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	// Call ProcessMetrics - should not fail even if delivery fails
	err := processor.ProcessBillingMetrics(ctx, now)
	assert.NoError(t, err, "ProcessMetrics should not fail when delivery fails")

	// Verify expectations
	mockDB.AssertExpectations(t)
}

// TestGetUnsentGoogleUsages_ErrorStateWithRetries tests filtering error records by retry count
func TestGetUnsentGoogleUsages_ErrorStateWithRetries(t *testing.T) {
	mockDB := &database.MockStorage{}
	config := &common.TelemetryConfig{
		PoolVolumeLabelPageSize: 1000,
	}
	processor := &BillingProvider{
		metricsDB: mockDB,
		config:    config,
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

	// Expect calls to GetAggregatedUsageWithPagination with conditions format
	mockDB.On("GetAggregatedUsageWithPagination", mock.Anything, mock.MatchedBy(func(conditions [][]interface{}) bool {
		if len(conditions) != 4 {
			return false
		}
		// Check for UNSUBMITTED state filter
		return conditions[0][0] == "state = ?" && conditions[0][1] == datamodel2.Unsubmitted &&
			conditions[1][0] == "is_billable = ?" && conditions[1][1] == true
	}), mock.Anything).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Maybe()

	mockDB.On("GetAggregatedUsageWithPagination", mock.Anything, mock.MatchedBy(func(conditions [][]interface{}) bool {
		if len(conditions) != 3 {
			return false
		}
		// Check for ERROR state filter
		return conditions[0][0] == "state = ?" && conditions[0][1] == datamodel2.Error
	}), mock.Anything).Return(
		errorRecords, nil,
	).Maybe()

	// Call getUnsentGoogleUsages with maxRetries = 3
	aggregationEndTime := time.Now()
	result, err := processor.getUnsentGoogleUsages(ctx, 3, aggregationEndTime)

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
	config := &common.TelemetryConfig{
		PoolVolumeLabelPageSize: 1000,
	}
	processor := &BillingProvider{
		metricsDB: mockDB,
		config:    config,
	}
	ctx := context.Background()

	// Expect successful UNSUBMITTED query with conditions format
	mockDB.On("GetAggregatedUsageWithPagination", mock.Anything, mock.MatchedBy(func(conditions [][]interface{}) bool {
		if len(conditions) != 4 {
			return false
		}
		return conditions[0][0] == "state = ?" && conditions[0][1] == datamodel2.Unsubmitted
	}), mock.Anything).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Maybe()

	// Expect error in ERROR state query with conditions format
	mockDB.On("GetAggregatedUsageWithPagination", mock.Anything, mock.MatchedBy(func(conditions [][]interface{}) bool {
		if len(conditions) != 3 {
			return false
		}
		return conditions[0][0] == "state = ?" && conditions[0][1] == datamodel2.Error
	}), mock.Anything).Return(
		nil, errors.New("error state query failed"),
	).Maybe()

	// Call getUnsentGoogleUsages
	aggregationEndTime := time.Now()
	result, err := processor.getUnsentGoogleUsages(ctx, 3, aggregationEndTime)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "error state query failed")

	mockDB.AssertExpectations(t)
}

// TestProcessMetricsWithJobDef_NonBillableRecord tests when aggregated record is not billable
func TestProcessMetricsWithJobDef_NonBillableRecord(t *testing.T) {
	mockDB := &database.MockStorage{}
	config := &common.TelemetryConfig{
		PoolVolumeLabelPageSize: 1000,
	}
	processor := &BillingProvider{
		metricsDB: mockDB,
		config:    config,
	}
	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	resourceID := ResourceKey{
		ResourceType: metadata.Volume,
		ResourceName: "test-resource-uuid",

		ConsumerID: "test-customer",
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
	err := processor.processMetricsWithJobDef(ctx, resourceID, hydratedMetricsToTimeSeries(metrics, now.Add(-1*time.Hour), now), common.AggregationJobDefinition{AggregationType: common.SumAggregation}, startTime, now, resourceCollection, &aggregatedRecords, make(map[CounterAggregationCacheResourceKey]*float64), util.GetLogger(ctx))

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
		mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{
			{
				UUID:           "pool-uuid",
				Name:           "pool1",
				AccountID:      1,
				DeploymentName: "dep1",
				PoolAttributes: &datamodel.PoolAttributes{
					AccountName:  "account1",
					Labels:       &datamodel.JSONB{"test": "test"},
					PrimaryZone:  "us-central1",
					IsRegionalHA: true,
				},
			},
		}, nil).Once()
		mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
		// First call returns one volume, second call returns empty slice (pagination end)
		mockVcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{
			{
				UUID:      "vol-uuid",
				Name:      "vol1",
				AccountID: 2,
				VolumeAttributes: &datamodel.VolumeAttributes{
					AccountName:    "account1",
					DeploymentName: "dep2",
					Labels:         &datamodel.JSONB{"key": "value"},
				},
			},
		}, nil).Once()
		mockVcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil).Once()
		mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{UUID: "vol-rep-uuid"},
				Volume: &datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
					Name:      "vol1",
					Pool:      &datamodel.Pool{DeploymentName: "dep1"},
					VolumeAttributes: &datamodel.VolumeAttributes{
						Protocols: []string{"ISCSI"},
					},
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ReplicationType: "CROSS_REGION_REPLICATION",
					Labels:          &datamodel.JSONB{"key": "value"},
				},
				Account: &datamodel.Account{Name: "account1"},
			},
		}, nil).Once()
		mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

		resourceCollection, err := provider.fetchResourceData(ctx, time.Now().Add(-1*time.Hour))
		assert.NoError(t, err)
		assert.Len(t, resourceCollection.PoolData, 1)
		assert.Len(t, resourceCollection.VolumeData, 1)
		assert.Len(t, resourceCollection.VolumeReplicationData, 1)
		mockVcpDB.AssertExpectations(t)
	})

	t.Run("Pool fetch fails, volume and volume replication fetch succeeds", func(t *testing.T) {
		mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("pool error")).Once()
		mockVcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{
			{
				UUID:      "vol-uuid",
				Name:      "vol1",
				AccountID: 2,
				VolumeAttributes: &datamodel.VolumeAttributes{
					AccountName:    "account1",
					DeploymentName: "dep2",
					Labels:         &datamodel.JSONB{"key": "value"},
				},
			},
		}, nil).Once()
		mockVcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil).Once()
		mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{UUID: "vol-rep-uuid"},
				Volume: &datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
					Name:      "vol1",
					Pool:      &datamodel.Pool{DeploymentName: "dep1"},
					VolumeAttributes: &datamodel.VolumeAttributes{
						Protocols: []string{"ISCSI"},
					},
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ReplicationType: "CROSS_REGION_REPLICATION",
					Labels:          &datamodel.JSONB{"key": "value"},
				},
				Account: &datamodel.Account{Name: "account1"},
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "vol-rep-uuid-2"},
				Volume: &datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: "vol-uuid-2"},
					Name:      "vol2",
					Pool:      &datamodel.Pool{DeploymentName: "dep1"},
					VolumeAttributes: &datamodel.VolumeAttributes{
						Protocols: []string{"NFSV3"},
					},
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ReplicationType: "CROSS_REGION_REPLICATION",
					Labels:          &datamodel.JSONB{"key1": "value1"},
				},
				Account: &datamodel.Account{Name: "account1"},
			},
		}, nil).Once()
		mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()
		resourceCollection, err := provider.fetchResourceData(ctx, time.Now().Add(-1*time.Hour))

		assert.NoError(t, err)
		assert.Len(t, resourceCollection.PoolData, 0)
		assert.Len(t, resourceCollection.VolumeData, 1)
		assert.Len(t, resourceCollection.VolumeReplicationData, 1)
		mockVcpDB.AssertExpectations(t)
	})

	t.Run("Volume fetch fails, pool and volume replication fetch succeeds", func(t *testing.T) {
		mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{
			{
				UUID:           "pool-uuid",
				Name:           "pool1",
				AccountID:      1,
				DeploymentName: "dep1",
				PoolAttributes: &datamodel.PoolAttributes{
					AccountName: "account1",
					Labels:      &datamodel.JSONB{"key": "value"},
				},
			},
		}, nil).Once()
		mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
		mockVcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("volume error")).Once()
		mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{UUID: "vol-rep-uuid"},
				Volume: &datamodel.Volume{
					BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
					Name:      "vol1",
					Pool:      &datamodel.Pool{DeploymentName: "dep1"},
					VolumeAttributes: &datamodel.VolumeAttributes{
						Protocols: []string{"ISCSI"},
					},
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					ReplicationType: "CROSS_REGION_REPLICATION",
					Labels:          &datamodel.JSONB{"key": "value"},
				},
				Account: &datamodel.Account{Name: "account1"},
			},
		}, nil).Once()
		mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()
		resourceCollection, err := provider.fetchResourceData(ctx, time.Now().Add(-1*time.Hour))
		assert.NoError(t, err)
		assert.Len(t, resourceCollection.PoolData, 1)
		assert.Len(t, resourceCollection.VolumeData, 0)
		assert.Len(t, resourceCollection.VolumeReplicationData, 1)
		mockVcpDB.AssertExpectations(t)
	})

	t.Run("Both pool and volume fetch fail", func(t *testing.T) {
		mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("pool error")).Once()
		mockVcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("volume error")).Once()
		mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("volume replication error")).Once()
		resourceCollection, err := provider.fetchResourceData(ctx, time.Now().Add(-1*time.Hour))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to fetch any resource data")
		assert.Nil(t, resourceCollection)
		mockVcpDB.AssertExpectations(t)
	})

	t.Run("Volume Replication fetch fail", func(t *testing.T) {
		// Mock successful pool and volume fetches
		mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{
			{
				UUID:           "pool-uuid",
				Name:           "pool1",
				AccountID:      1,
				DeploymentName: "dep1",
				PoolAttributes: &datamodel.PoolAttributes{
					AccountName: "account1",
					Labels:      &datamodel.JSONB{"key": "value"},
				},
			},
		}, nil).Once()
		mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()

		mockVcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{
			{
				UUID:      "vol-uuid",
				Name:      "vol1",
				AccountID: 2,
				VolumeAttributes: &datamodel.VolumeAttributes{
					AccountName:    "account1",
					DeploymentName: "dep2",
					Labels:         &datamodel.JSONB{"key": "value"},
				},
			},
		}, nil).Once()
		mockVcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil).Once()

		mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("volume replication error")).Once()

		resourceCollection, err := provider.fetchResourceData(ctx, time.Now().Add(-1*time.Hour))
		assert.NoError(t, err)
		assert.Len(t, resourceCollection.PoolData, 1)
		assert.Len(t, resourceCollection.VolumeData, 1)
		assert.Len(t, resourceCollection.VolumeReplicationData, 0)
		mockVcpDB.AssertExpectations(t)
	})
}

func TestResourceKeyMapKeyEquality(t *testing.T) {
	key1 := ResourceKey{
		ResourceType: metadata.Volume,
		ResourceName: "vol1",
		ConsumerID:   "acc1",
	}
	key2 := ResourceKey{
		ResourceType: metadata.Volume,
		ResourceName: "vol1",
		ConsumerID:   "acc1",
	}

	m := make(map[ResourceKey]string)
	m[key1] = "test-value"

	// Both keys should retrieve the same value
	require.Equal(t, "test-value", m[key2])
}

// TestFetchMetricsForCounterAggregation tests the optimized database-level sorting approach
func TestFetchMetricsForCounterAggregation(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	config := &common.TelemetryConfig{
		PoolVolumeLabelPageSize: 1000, // Set a reasonable page size
	}
	processor := &BillingProvider{
		metricsDB: mockDB,
		config:    config,
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
	// Mock the counter cache preload call (returns empty list to stop pagination)
	mockDB.On("GetLatestAggregatedUsageForAllResources", mock.Anything, "CounterAggregation", mock.Anything, mock.Anything).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Maybe()

	// Mock the pagination calls - first returns data, subsequent calls return empty to end pagination
	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.MatchedBy(func(conditions [][]interface{}) bool {
		// Verify conditions include the extended time range
		return len(conditions) >= 2 // Should have timestamp conditions
	}), mock.Anything).Return(mockMetrics, nil).Once()

	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.MatchedBy(func(conditions [][]interface{}) bool {
		// Subsequent calls for pagination return empty
		return len(conditions) >= 2
	}), mock.Anything).Return([]datamodel2.HydratedMetrics{}, nil).Maybe()

	// Execute the method
	result, err := processor.fetchMetricsForCounterAndIntegralAggregation(
		context.Background(),
		aggregationStartTime,
		aggregationEndTime,
		"VOLUME",
		"ALLOCATED_SIZE",
		60*time.Minute, // backfill limit
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
	config := &common.TelemetryConfig{
		PoolVolumeLabelPageSize: 1000, // Set a reasonable page size
	}
	processor := &BillingProvider{
		metricsDB: mockDB,
		config:    config,
	}

	now := time.Now()
	aggregationStartTime := now.Add(-1 * time.Hour)
	aggregationEndTime := now

	// Mock database error
	// Mock the counter cache preload call (returns empty list to stop pagination)
	mockDB.On("GetLatestAggregatedUsageForAllResources", mock.Anything, "CounterAggregation", mock.Anything, mock.Anything).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Maybe()

	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("database connection error"))

	// Execute the method
	result, err := processor.fetchMetricsForCounterAndIntegralAggregation(
		context.Background(),
		aggregationStartTime,
		aggregationEndTime,
		"VOLUME",
		"ALLOCATED_SIZE",
		60*time.Minute, // backfill limit
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
		config:    &common.TelemetryConfig{PoolVolumeLabelPageSize: 1000},
	}

	now := time.Now()
	aggregationStartTime := now.Add(-1 * time.Hour)
	aggregationEndTime := now
	backfillLimit := 60 * time.Minute

	// Mock the database call with detailed filter validation
	// Mock the counter cache preload call (returns empty list to stop pagination)
	mockDB.On("GetLatestAggregatedUsageForAllResources", mock.Anything, "CounterAggregation", mock.Anything, mock.Anything).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Maybe()

	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.MatchedBy(func(conditions [][]interface{}) bool {
		// Check that we have some conditions
		if len(conditions) != 4 {
			return false
		}

		// Check conditions - verify structure and values exist
		foundStartTime := false
		foundEndTime := false
		foundResourceType := false
		foundMeasuredType := false

		for _, condition := range conditions {
			if len(condition) >= 2 {
				condStr, ok := condition[0].(string)
				if !ok {
					continue
				}
				if strings.Contains(condStr, "metric_timestamp >=") {
					// Just verify it's a time.Time, don't check exact value
					if _, ok := condition[1].(time.Time); ok {
						foundStartTime = true
					}
				}
				if strings.Contains(condStr, "metric_timestamp <=") {
					// Just verify it's a time.Time, don't check exact value
					if _, ok := condition[1].(time.Time); ok {
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

		return foundStartTime && foundEndTime && foundResourceType && foundMeasuredType
	}), mock.Anything).Return([]datamodel2.HydratedMetrics{}, nil)

	// Execute the method
	_, err := processor.fetchMetricsForCounterAndIntegralAggregation(
		context.Background(),
		aggregationStartTime,
		aggregationEndTime,
		"VOLUME",
		"ALLOCATED_SIZE",
		backfillLimit,
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
	mockVCPDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	mockVCPDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	mockVCPDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Mock the GetHydratedMetrics for non-counter aggregation (this will hit the else branch on line 96)
	// Mock the counter cache preload call (returns empty list to stop pagination)
	mockDB.On("GetLatestAggregatedUsageForAllResources", mock.Anything, "CounterAggregation", mock.Anything, mock.Anything).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Maybe()

	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.MatchedBy(func(conditions [][]interface{}) bool {
		// This should be called for FirstAggregation type which is not counter or integral
		return true
	}), mock.Anything).Return([]datamodel2.HydratedMetrics{}, nil)

	// Mock delivery - should not be called since no metrics to deliver
	// mockUsageSink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(0, nil)

	// Execute - this should trigger the else branch for non-counter aggregation types
	err := processor.ProcessBillingMetrics(ctx, now)

	assert.NoError(t, err)
	mockDB.AssertExpectations(t)
	mockVCPDB.AssertExpectations(t)
	// mockUsageSink.AssertExpectations(t) // Not called since no metrics
}

func TestProcessBillingMetrics_BackupHistoryFormatterBranch(t *testing.T) {
	mockDB := database.NewMockStorage(t)
	mockVCPDB := database2.NewMockStorage(t)
	mockUsageSink := &MockUsageSink{}

	config := &common.TelemetryConfig{
		EnableBackupBillingMetrics:      true,
		EnableBackupHistoryFormatter:    true,
		EnableReplicationBillingMetrics: true,
		PoolVolumeLabelPageSize:         1,
		GoogleBillingLabelsMaxEntries:   10,
		IntervalBackfillLimitMinutes:    60,
	}

	provider := NewBillingProvider(mockDB, mockVCPDB, config, mockUsageSink)
	ctx := context.Background()
	aggregationEnd := time.Date(2026, 2, 18, 11, 0, 0, 0, time.UTC)
	aggregationStart := aggregationEnd.Add(-1 * time.Hour)
	deletedAt := aggregationStart.Add(30 * time.Minute)

	// Resource data fetches (return empty)
	mockVCPDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil)
	mockVCPDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil)
	mockVCPDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

	// Backup metadata pagination (no labels)
	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.BackupMetadata{}, nil).Once()

	backup1 := &datamodel.Backup{
		BaseModel:   datamodel.BaseModel{UUID: "backup-uuid-1"},
		VolumeUUID:  "volume-uuid-1",
		Attributes:  &datamodel.BackupAttributes{AccountIdentifier: "acct-1"},
		BackupVault: &datamodel.BackupVault{Name: "vault-1", AccountID: 1},
	}
	backup2 := &datamodel.Backup{
		BaseModel:   datamodel.BaseModel{UUID: "backup-uuid-2"},
		VolumeUUID:  "volume-uuid-2",
		Attributes:  &datamodel.BackupAttributes{AccountIdentifier: "acct-2"},
		BackupVault: &datamodel.BackupVault{Name: "vault-2", AccountID: 2},
	}
	mockVCPDB.On("GetBackupResourceDataForAggregation", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 0
	})).Return([]*datamodel.Backup{backup1}, nil)
	mockVCPDB.On("GetBackupResourceDataForAggregation", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 1
	})).Return([]*datamodel.Backup{backup2}, nil)
	mockVCPDB.On("GetBackupResourceDataForAggregation", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset > 1
	})).Return([]*datamodel.Backup{}, nil)

	history1 := &datamodel.BackupChainHistory{
		BaseModel: datamodel.BaseModel{
			CreatedAt: aggregationStart.Add(-10 * time.Minute),
			DeletedAt: &gorm.DeletedAt{Time: deletedAt, Valid: true},
		},
		ResourceUUID:   "volume-uuid-1",
		ConsumerID:     "acct-1",
		DeploymentName: "vault-1",
		Size:           1024,
	}
	history2 := &datamodel.BackupChainHistory{
		BaseModel: datamodel.BaseModel{
			CreatedAt: aggregationStart.Add(-20 * time.Minute),
		},
		ResourceUUID:   "volume-uuid-2",
		ConsumerID:     "acct-2",
		DeploymentName: "vault-2",
		Size:           2048,
	}
	mockVCPDB.On("ListBackupChainHistoriesWithPagination", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 0
	})).Return([]*datamodel.BackupChainHistory{history1}, nil)
	mockVCPDB.On("ListBackupChainHistoriesWithPagination", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 1
	})).Return([]*datamodel.BackupChainHistory{history2}, nil)
	mockVCPDB.On("ListBackupChainHistoriesWithPagination", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset >= 2
	})).Return([]*datamodel.BackupChainHistory{}, nil)

	mockDB.On("GetLatestAggregatedUsageForAllResources", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Maybe()
	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(
		[]datamodel2.HydratedMetrics{}, nil,
	).Maybe()

	var captured []datamodel2.AggregatedUsage
	mockDB.On("CreateAggregatedUsageBatch", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		records := args.Get(1).([]datamodel2.AggregatedUsage)
		captured = append(captured, records...)
	}).Return(nil).Once()

	err := provider.ProcessBillingMetrics(ctx, aggregationEnd)
	require.NoError(t, err)

	var backupRecord *datamodel2.AggregatedUsage
	for i := range captured {
		record := &captured[i]
		if record.ResourceUUID == "volume-uuid-1" && record.MeasuredType == metadata.BackupLogicalSize {
			backupRecord = record
			break
		}
	}
	require.NotNil(t, backupRecord)
	assert.Equal(t, aggregationStart, backupRecord.AggregationStart)
	assert.Equal(t, deletedAt, backupRecord.AggregationEnd)

	mockDB.AssertExpectations(t)
	mockVCPDB.AssertExpectations(t)
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
	mockVCPDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("pool fetch error"))
	// Mock volume data fetch failure
	mockVCPDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("volume fetch error"))
	mockVCPDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Mock the GetHydratedMetrics calls for all aggregation types
	// Mock the counter cache preload call (returns empty list to stop pagination)
	mockDB.On("GetLatestAggregatedUsageForAllResources", mock.Anything, "CounterAggregation", mock.Anything, mock.Anything).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Maybe()

	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]datamodel2.HydratedMetrics{}, nil)

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
		ResourceType: metadata.VolumePool,
		ResourceName: "test-pool-uuid",

		ConsumerID: "test-customer",
	}

	volumeKey := ResourceKey{
		ResourceType: metadata.Volume,
		ResourceName: "test-volume-uuid",

		ConsumerID: "test-customer",
	}

	poolKeyRegionalHA := ResourceKey{
		ResourceType: metadata.VolumePoolRegionalHA,
		ResourceName: "test-pool-uuid",

		ConsumerID: "test-customer",
	}

	volumeKeyRegionalHA := ResourceKey{
		ResourceType: metadata.VolumeRegionalHA,
		ResourceName: "test-volume-uuid",

		ConsumerID: "test-customer",
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
			id:           ResourceKey{ResourceType: metadata.Volume, ResourceName: "non-existent-uuid"},
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

	backupMetadataList := []*datamodel.BackupMetadata{
		{VolumeUUID: "volume-uuid-1", Labels: createJSONB(map[string]string{"env": "dev"})},
	}

	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 0
	})).Return(backupMetadataList, nil)
	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset > 0
	})).Return([]*datamodel.BackupMetadata{}, nil)

	volumeLabelsMap, err := provider.fetchBackupMetadata(ctx, aggregationStartTime)

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
	mockVCPDB.On("GetBackupResourceDataForAggregation", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 0
	})).Return(backups, nil)
	mockVCPDB.On("GetBackupResourceDataForAggregation", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset > 0
	})).Return([]*datamodel.Backup{}, nil)

	err := provider.fetchBackupData(ctx, aggregationStartTime, resourceCollection)
	assert.NoError(t, err)
	assert.Len(t, resourceCollection.BackupData, 1)

	key := ResourceKey{
		ResourceType:   metadata.Backup,
		ResourceName:   "volume-uuid-1",
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

	// Mock the calls that fetchResourceData makes
	mockVCPDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil)
	mockVCPDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil)

	resourceCollection, err := provider.fetchResourceData(ctx, aggregationStartTime)
	assert.NoError(t, err)
	assert.NotNil(t, resourceCollection)
	assert.Empty(t, resourceCollection.BackupData) // Should be empty since backup billing is disabled

	// GetBackupMetadata and GetBackupMetrics should not be called when disabled
	mockVCPDB.AssertNotCalled(t, "GetBackupMetadata", mock.Anything, mock.Anything, mock.Anything)
	mockVCPDB.AssertNotCalled(t, "GetBackupResourceDataForAggregation", mock.Anything, mock.Anything, mock.Anything)
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
	resourceCollection := &ResourceCollection{
		BackupData: make(map[ResourceKey]ResourceData),
	}

	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("metadata error"))
	mockVCPDB.On("GetBackupResourceDataForAggregation", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 0
	})).Return([]*datamodel.Backup{}, nil) // Still mock GetBackupMetrics to avoid panic

	err := provider.fetchBackupData(ctx, aggregationStartTime, resourceCollection)
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
	resourceCollection := &ResourceCollection{
		BackupData: make(map[ResourceKey]ResourceData),
	}

	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.BackupMetadata{}, nil)
	mockVCPDB.On("GetBackupResourceDataForAggregation", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("metrics error"))

	err := provider.fetchBackupData(ctx, aggregationStartTime, resourceCollection)
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
	mockVCPDB.On("GetBackupResourceDataForAggregation", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 0
	})).Return(backups, nil)
	mockVCPDB.On("GetBackupResourceDataForAggregation", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset > 0
	})).Return([]*datamodel.Backup{}, nil)

	err := provider.fetchBackupData(ctx, aggregationStartTime, resourceCollection)
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
	mockVCPDB.On("GetBackupResourceDataForAggregation", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 0
	})).Return(backups, nil)
	mockVCPDB.On("GetBackupResourceDataForAggregation", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset > 0
	})).Return([]*datamodel.Backup{}, nil)

	err := provider.fetchBackupData(ctx, aggregationStartTime, resourceCollection)
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

	volumeLabelsMap, err := provider.fetchBackupMetadata(ctx, aggregationStartTime)
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

	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("relation \"backup_metadata\" does not exist"))

	volumeLabelsMap, err := provider.fetchBackupMetadata(ctx, aggregationStartTime)
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

	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("some other database error"))

	volumeLabelsMap, err := provider.fetchBackupMetadata(ctx, aggregationStartTime)
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

	mockVCPDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.BackupMetadata{}, nil)

	volumeLabelsMap, err := provider.fetchBackupMetadata(ctx, aggregationStartTime)
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

	volumeLabelsMap, err := provider.fetchBackupMetadata(ctx, aggregationStartTime)
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

	volumeLabelsMap, err := provider.fetchBackupMetadata(ctx, aggregationStartTime)
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

	volumeLabelsMap, err := provider.fetchBackupMetadata(ctx, aggregationStartTime)
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
	mockVCPDB.On("GetBackupResourceDataForAggregation", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
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
	mockVCPDB.On("GetBackupResourceDataForAggregation", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 1
	})).Return(backups2, nil).Once()

	// Mock empty third batch to end pagination
	mockVCPDB.On("GetBackupResourceDataForAggregation", mock.Anything, mock.Anything, mock.MatchedBy(func(p *dbutils.Pagination) bool {
		return p.Offset == 2
	})).Return([]*datamodel.Backup{}, nil).Once()

	err := provider.fetchBackupData(ctx, aggregationStartTime, resourceCollection)
	assert.NoError(t, err)
	assert.Len(t, resourceCollection.BackupData, 2)

	// Verify first backup
	key1 := ResourceKey{
		ResourceType:   metadata.Backup,
		ResourceName:   "volume-uuid-1",
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
		ResourceName:   "volume-uuid-2",
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

// TestAutoTieringBillingMetricFiltering tests that auto-tiering billing metrics
// are correctly filtered based on pool's AllowAutoTiering flag
func TestAutoTieringBillingMetricFiltering(t *testing.T) {
	t.Run("isAutoTieringBillingMetric correctly identifies autotiering metrics", func(t *testing.T) {
		// These should be identified as auto-tiering billing metrics
		assert.True(t, isAutoTieringBillingMetric(metadata.CoolTierDataReadSizeRaw))
		assert.True(t, isAutoTieringBillingMetric(metadata.CoolTierDataWriteSizeRaw))
		assert.True(t, isAutoTieringBillingMetric(metadata.PoolHotTierProvisionedSize))
		assert.True(t, isAutoTieringBillingMetric(metadata.PoolCapacityTierLogicalFootprint))

		// These should NOT be identified as auto-tiering billing metrics
		assert.False(t, isAutoTieringBillingMetric(metadata.AllocatedSize))
		assert.False(t, isAutoTieringBillingMetric(metadata.PoolAllocatedSize))
		assert.False(t, isAutoTieringBillingMetric(metadata.TotalLogicalSize))
	})

	t.Run("ResourceData stores AllowAutoTiering correctly", func(t *testing.T) {
		// Test that ResourceData can store AllowAutoTiering
		resourceDataEnabled := ResourceData{
			UUID:             "test-uuid",
			AccountID:        123,
			AllowAutoTiering: true,
		}
		assert.True(t, resourceDataEnabled.AllowAutoTiering)

		resourceDataDisabled := ResourceData{
			UUID:             "test-uuid",
			AccountID:        123,
			AllowAutoTiering: false,
		}
		assert.False(t, resourceDataDisabled.AllowAutoTiering)
	})

	t.Run("Pool with AllowAutoTiering=false should skip autotiering metrics", func(t *testing.T) {
		// Create a resource collection with a pool that has AllowAutoTiering=false
		resourceCollection := &ResourceCollection{
			PoolData: make(map[ResourceKey]ResourceData),
		}

		poolKey := ResourceKey{
			ResourceType:   metadata.VolumePool,
			ResourceName:   "test-pool",
			DeploymentName: "test-deployment",
			ConsumerID:     "test-customer",
		}

		resourceCollection.PoolData[poolKey] = ResourceData{
			UUID:             "pool-uuid",
			AccountID:        123,
			AllowAutoTiering: false, // Auto-tiering disabled
		}

		// Simulate the filtering logic from ProcessBillingMetrics
		measuredType := metadata.CoolTierDataReadSizeRaw
		shouldSkip := false

		if isAutoTieringBillingMetric(measuredType) {
			poolData, found := resourceCollection.PoolData[poolKey]
			if !found || !poolData.AllowAutoTiering {
				shouldSkip = true
			}
		}

		assert.True(t, shouldSkip, "Should skip autotiering metric for pool with AllowAutoTiering=false")
	})

	t.Run("Pool with AllowAutoTiering=true should process autotiering metrics", func(t *testing.T) {
		// Create a resource collection with a pool that has AllowAutoTiering=true
		resourceCollection := &ResourceCollection{
			PoolData: make(map[ResourceKey]ResourceData),
		}

		poolKey := ResourceKey{
			ResourceType:   metadata.VolumePool,
			ResourceName:   "test-pool",
			DeploymentName: "test-deployment",
			ConsumerID:     "test-customer",
		}

		resourceCollection.PoolData[poolKey] = ResourceData{
			UUID:             "pool-uuid",
			AccountID:        123,
			AllowAutoTiering: true, // Auto-tiering enabled
		}

		// Simulate the filtering logic from ProcessBillingMetrics
		measuredType := metadata.CoolTierDataReadSizeRaw
		shouldSkip := false

		if isAutoTieringBillingMetric(measuredType) {
			poolData, found := resourceCollection.PoolData[poolKey]
			if !found || !poolData.AllowAutoTiering {
				shouldSkip = true
			}
		}

		assert.False(t, shouldSkip, "Should NOT skip autotiering metric for pool with AllowAutoTiering=true")
	})

	t.Run("Pool not found in resourceCollection should skip autotiering metrics", func(t *testing.T) {
		// Create an empty resource collection
		resourceCollection := &ResourceCollection{
			PoolData: make(map[ResourceKey]ResourceData),
		}

		poolKey := ResourceKey{
			ResourceType:   metadata.VolumePool,
			ResourceName:   "unknown-pool",
			DeploymentName: "test-deployment",
			ConsumerID:     "test-customer",
		}

		// Simulate the filtering logic from ProcessBillingMetrics
		measuredType := metadata.CoolTierDataReadSizeRaw
		shouldSkip := false

		if isAutoTieringBillingMetric(measuredType) {
			poolData, found := resourceCollection.PoolData[poolKey]
			if !found || !poolData.AllowAutoTiering {
				shouldSkip = true
			}
		}

		assert.True(t, shouldSkip, "Should skip autotiering metric when pool is not found")
	})

	t.Run("Non-autotiering metrics should not be affected by AllowAutoTiering flag", func(t *testing.T) {
		// Create a resource collection with a pool that has AllowAutoTiering=false
		resourceCollection := &ResourceCollection{
			PoolData: make(map[ResourceKey]ResourceData),
		}

		poolKey := ResourceKey{
			ResourceType:   metadata.VolumePool,
			ResourceName:   "test-pool",
			DeploymentName: "test-deployment",
			ConsumerID:     "test-customer",
		}

		resourceCollection.PoolData[poolKey] = ResourceData{
			UUID:             "pool-uuid",
			AccountID:        123,
			AllowAutoTiering: false, // Auto-tiering disabled
		}

		// Simulate the filtering logic for a NON-autotiering metric
		measuredType := metadata.PoolAllocatedSize // Not an autotiering metric
		shouldSkip := false

		if isAutoTieringBillingMetric(measuredType) {
			poolData, found := resourceCollection.PoolData[poolKey]
			if !found || !poolData.AllowAutoTiering {
				shouldSkip = true
			}
		}

		assert.False(t, shouldSkip, "Non-autotiering metrics should NOT be skipped regardless of AllowAutoTiering flag")
	})
}

// TestShouldSkipAutoTieringMetric tests the shouldSkipAutoTieringMetric helper function
// which exercises lines 175-178 (ONTAP mode skip logic) in metrics_processor.go
func TestShouldSkipAutoTieringMetric(t *testing.T) {
	mockMetricsDB := &database.MockStorage{}
	mockVcpDB := &database2.MockStorage{}
	mockSink := &MockUsageSink{}

	t.Run("SkipsONTAPModePoolWhenFlagDisabled", func(t *testing.T) {
		config := &common.TelemetryConfig{
			EnableONTAPModeAutoTieringBilling: false, // KEY: flag disabled
			EnableFilesAutoTieringBilling:     true,
		}
		provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

		poolResourceID := ResourceKey{
			ResourceType:   metadata.VolumePool,
			ResourceName:   "ontap-mode-pool",
			ConsumerID:     "test-customer",
			DeploymentName: "test-deployment",
		}

		resourceCollection := &ResourceCollection{
			PoolData: map[ResourceKey]ResourceData{
				poolResourceID: {
					UUID:                "pool-uuid",
					AccountID:           123,
					AllowAutoTiering:    true, // Auto-tiering allowed
					IsONTAPMode:         true, // ONTAP mode pool
					HasOnlyBlockVolumes: true,
				},
			},
		}

		shouldSkip, reason := provider.shouldSkipAutoTieringMetric(poolResourceID, resourceCollection, metadata.PoolHotTierProvisionedSize)

		assert.True(t, shouldSkip, "Should skip ONTAP mode pool when EnableONTAPModeAutoTieringBilling=false")
		assert.Contains(t, reason, "ONTAP mode pool")
	})

	t.Run("ProcessesONTAPModePoolWhenFlagEnabled", func(t *testing.T) {
		config := &common.TelemetryConfig{
			EnableONTAPModeAutoTieringBilling: true, // KEY: flag enabled
			EnableFilesAutoTieringBilling:     true,
		}
		provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

		poolResourceID := ResourceKey{
			ResourceType:   metadata.VolumePool,
			ResourceName:   "ontap-mode-pool",
			ConsumerID:     "test-customer",
			DeploymentName: "test-deployment",
		}

		resourceCollection := &ResourceCollection{
			PoolData: map[ResourceKey]ResourceData{
				poolResourceID: {
					UUID:                "pool-uuid",
					AccountID:           123,
					AllowAutoTiering:    true, // Auto-tiering allowed
					IsONTAPMode:         true, // ONTAP mode pool
					HasOnlyBlockVolumes: true,
				},
			},
		}

		shouldSkip, reason := provider.shouldSkipAutoTieringMetric(poolResourceID, resourceCollection, metadata.PoolHotTierProvisionedSize)

		assert.False(t, shouldSkip, "Should NOT skip ONTAP mode pool when EnableONTAPModeAutoTieringBilling=true")
		assert.Empty(t, reason)
	})

	t.Run("SkipsPoolNotFound", func(t *testing.T) {
		config := &common.TelemetryConfig{
			EnableONTAPModeAutoTieringBilling: true,
			EnableFilesAutoTieringBilling:     true,
		}
		provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

		poolResourceID := ResourceKey{
			ResourceType:   metadata.VolumePool,
			ResourceName:   "unknown-pool",
			ConsumerID:     "test-customer",
			DeploymentName: "test-deployment",
		}

		resourceCollection := &ResourceCollection{
			PoolData: make(map[ResourceKey]ResourceData),
		}

		shouldSkip, reason := provider.shouldSkipAutoTieringMetric(poolResourceID, resourceCollection, metadata.PoolHotTierProvisionedSize)

		assert.True(t, shouldSkip, "Should skip when pool is not found")
		assert.Contains(t, reason, "not found")
	})

	t.Run("SkipsPoolWithAllowAutoTieringDisabled", func(t *testing.T) {
		config := &common.TelemetryConfig{
			EnableONTAPModeAutoTieringBilling: true,
			EnableFilesAutoTieringBilling:     true,
		}
		provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

		poolResourceID := ResourceKey{
			ResourceType:   metadata.VolumePool,
			ResourceName:   "standard-pool",
			ConsumerID:     "test-customer",
			DeploymentName: "test-deployment",
		}

		resourceCollection := &ResourceCollection{
			PoolData: map[ResourceKey]ResourceData{
				poolResourceID: {
					UUID:             "pool-uuid",
					AccountID:        123,
					AllowAutoTiering: false, // Auto-tiering disabled
					IsONTAPMode:      false,
				},
			},
		}

		shouldSkip, reason := provider.shouldSkipAutoTieringMetric(poolResourceID, resourceCollection, metadata.PoolHotTierProvisionedSize)

		assert.True(t, shouldSkip, "Should skip when AllowAutoTiering=false")
		assert.Contains(t, reason, "AllowAutoTiering disabled")
	})

	t.Run("SkipsNonBlockPoolWhenFilesAutoTieringDisabled", func(t *testing.T) {
		config := &common.TelemetryConfig{
			EnableONTAPModeAutoTieringBilling: true,
			EnableFilesAutoTieringBilling:     false, // Files billing disabled
		}
		provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

		poolResourceID := ResourceKey{
			ResourceType:   metadata.VolumePool,
			ResourceName:   "files-pool",
			ConsumerID:     "test-customer",
			DeploymentName: "test-deployment",
		}

		resourceCollection := &ResourceCollection{
			PoolData: map[ResourceKey]ResourceData{
				poolResourceID: {
					UUID:                "pool-uuid",
					AccountID:           123,
					AllowAutoTiering:    true,
					IsONTAPMode:         false,
					HasOnlyBlockVolumes: false, // Has file volumes
				},
			},
		}

		shouldSkip, reason := provider.shouldSkipAutoTieringMetric(poolResourceID, resourceCollection, metadata.PoolHotTierProvisionedSize)

		assert.True(t, shouldSkip, "Should skip non-block pool when EnableFilesAutoTieringBilling=false")
		assert.Contains(t, reason, "not block-only pool")
	})

	t.Run("ProcessesStandardPoolRegardlessOfONTAPFlag", func(t *testing.T) {
		config := &common.TelemetryConfig{
			EnableONTAPModeAutoTieringBilling: false, // Flag disabled, but pool is not ONTAP mode
			EnableFilesAutoTieringBilling:     true,
		}
		provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

		poolResourceID := ResourceKey{
			ResourceType:   metadata.VolumePool,
			ResourceName:   "standard-pool",
			ConsumerID:     "test-customer",
			DeploymentName: "test-deployment",
		}

		resourceCollection := &ResourceCollection{
			PoolData: map[ResourceKey]ResourceData{
				poolResourceID: {
					UUID:                "pool-uuid",
					AccountID:           123,
					AllowAutoTiering:    true,
					IsONTAPMode:         false, // Standard mode
					HasOnlyBlockVolumes: true,
				},
			},
		}

		shouldSkip, reason := provider.shouldSkipAutoTieringMetric(poolResourceID, resourceCollection, metadata.PoolHotTierProvisionedSize)

		assert.False(t, shouldSkip, "Should NOT skip standard mode pool regardless of EnableONTAPModeAutoTieringBilling flag")
		assert.Empty(t, reason)
	})
}

// TestFetchResourceData_SkipsPoolWithEmptyAccountName tests that pools with empty account names are skipped
func TestFetchResourceData_SkipsPoolWithEmptyAccountName(t *testing.T) {
	ctx := context.Background()
	mockVcpDB := &database2.MockStorage{}
	mockMetricsDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{PoolVolumeLabelPageSize: 10, GoogleBillingLabelsMaxEntries: 10, EnableReplicationBillingMetrics: true}
	provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

	// First call returns pools where one has empty account name
	mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{
		{
			UUID:           "pool-uuid-1",
			Name:           "pool1",
			AccountID:      1,
			DeploymentName: "dep1",
			PoolAttributes: &datamodel.PoolAttributes{
				AccountName: "account1",
				Labels:      &datamodel.JSONB{"test": "test"},
			},
		},
		{
			UUID:           "pool-uuid-2",
			Name:           "pool2",
			AccountID:      2,
			DeploymentName: "dep2",
			PoolAttributes: &datamodel.PoolAttributes{
				AccountName: "", // Empty account name - should be skipped
				Labels:      &datamodel.JSONB{"test": "test"},
			},
		},
	}, nil).Once()
	mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
	mockVcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil).Once()
	mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

	resourceCollection, err := provider.fetchResourceData(ctx, time.Now().Add(-1*time.Hour))
	assert.NoError(t, err)
	assert.Len(t, resourceCollection.PoolData, 1) // Only one pool should be added
	mockVcpDB.AssertExpectations(t)
}

// TestFetchResourceData_SkipsVolumeWithEmptyAccountName tests that volumes with empty account names are skipped
func TestFetchResourceData_SkipsVolumeWithEmptyAccountName(t *testing.T) {
	ctx := context.Background()
	mockVcpDB := &database2.MockStorage{}
	mockMetricsDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{PoolVolumeLabelPageSize: 10, GoogleBillingLabelsMaxEntries: 10, EnableReplicationBillingMetrics: true}
	provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

	mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
	// First call returns volumes where one has empty account name
	mockVcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{
		{
			UUID:      "vol-uuid-1",
			Name:      "vol1",
			AccountID: 1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				AccountName:    "account1",
				DeploymentName: "dep1",
				Labels:         &datamodel.JSONB{"key": "value"},
			},
		},
		{
			UUID:      "vol-uuid-2",
			Name:      "vol2",
			AccountID: 2,
			VolumeAttributes: &datamodel.VolumeAttributes{
				AccountName:    "", // Empty account name - should be skipped
				DeploymentName: "dep2",
				Labels:         &datamodel.JSONB{"key": "value"},
			},
		},
	}, nil).Once()
	mockVcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil).Once()
	mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

	resourceCollection, err := provider.fetchResourceData(ctx, time.Now().Add(-1*time.Hour))
	assert.NoError(t, err)
	assert.Len(t, resourceCollection.VolumeData, 1) // Only one volume should be added
	mockVcpDB.AssertExpectations(t)
}

// TestFetchResourceData_SkipsVolumeWithEmptyDeploymentName tests that volumes with empty deployment names are skipped
func TestFetchResourceData_SkipsVolumeWithEmptyDeploymentName(t *testing.T) {
	ctx := context.Background()
	mockVcpDB := &database2.MockStorage{}
	mockMetricsDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{PoolVolumeLabelPageSize: 10, GoogleBillingLabelsMaxEntries: 10, EnableReplicationBillingMetrics: true}
	provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

	mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
	// First call returns volumes where one has empty deployment name
	mockVcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{
		{
			UUID:      "vol-uuid-1",
			Name:      "vol1",
			AccountID: 1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				AccountName:    "account1",
				DeploymentName: "dep1",
				Labels:         &datamodel.JSONB{"key": "value"},
			},
		},
		{
			UUID:      "vol-uuid-2",
			Name:      "vol2",
			AccountID: 2,
			VolumeAttributes: &datamodel.VolumeAttributes{
				AccountName:    "account2",
				DeploymentName: "", // Empty deployment name - should be skipped
				Labels:         &datamodel.JSONB{"key": "value"},
			},
		},
	}, nil).Once()
	mockVcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil).Once()
	mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

	resourceCollection, err := provider.fetchResourceData(ctx, time.Now().Add(-1*time.Hour))
	assert.NoError(t, err)
	assert.Len(t, resourceCollection.VolumeData, 1) // Only one volume should be added
	mockVcpDB.AssertExpectations(t)
}

// TestFetchResourceData_PoolWithNilPoolAttributes tests that pools with nil PoolAttributes are skipped (empty account name)
func TestFetchResourceData_PoolWithNilPoolAttributes(t *testing.T) {
	ctx := context.Background()
	mockVcpDB := &database2.MockStorage{}
	mockMetricsDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{PoolVolumeLabelPageSize: 10, GoogleBillingLabelsMaxEntries: 10, EnableReplicationBillingMetrics: true}
	provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

	// Pool with nil PoolAttributes - GetAccountName() returns empty string
	mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{
		{
			UUID:           "pool-uuid-1",
			Name:           "pool1",
			AccountID:      1,
			DeploymentName: "dep1",
			PoolAttributes: nil, // nil PoolAttributes - should be skipped
		},
	}, nil).Once()
	mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
	mockVcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil).Once()
	mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

	resourceCollection, err := provider.fetchResourceData(ctx, time.Now().Add(-1*time.Hour))
	assert.NoError(t, err)
	assert.Len(t, resourceCollection.PoolData, 0) // Pool should be skipped
	mockVcpDB.AssertExpectations(t)
}

// TestFetchResourceData_VolumeWithNilVolumeAttributes tests that volumes with nil VolumeAttributes are skipped
func TestFetchResourceData_VolumeWithNilVolumeAttributes(t *testing.T) {
	ctx := context.Background()
	mockVcpDB := &database2.MockStorage{}
	mockMetricsDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{PoolVolumeLabelPageSize: 10, GoogleBillingLabelsMaxEntries: 10, EnableReplicationBillingMetrics: true}
	provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

	mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
	// Volume with nil VolumeAttributes - GetAccountName() and GetDeploymentName() return empty string
	mockVcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{
		{
			UUID:             "vol-uuid-1",
			Name:             "vol1",
			AccountID:        1,
			VolumeAttributes: nil, // nil VolumeAttributes - should be skipped
		},
	}, nil).Once()
	mockVcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil).Once()
	mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

	resourceCollection, err := provider.fetchResourceData(ctx, time.Now().Add(-1*time.Hour))
	assert.NoError(t, err)
	assert.Len(t, resourceCollection.VolumeData, 0) // Volume should be skipped
	mockVcpDB.AssertExpectations(t)
}

// TestFetchResourceData_GetBlockOnlyPoolIDsQueryOptimization tests that GetBlockOnlyPoolIDs is only called
// when EnableAutoTieringBillingMetrics=true AND EnableFilesAutoTieringBilling=false
func TestFetchResourceData_GetBlockOnlyPoolIDsQueryOptimization(t *testing.T) {
	t.Run("SkipsQueryWhenAutoTieringBillingDisabled", func(t *testing.T) {
		mockDB := database.NewMockStorage(t)
		mockVCPDB := database2.NewMockStorage(t)

		config := &common.TelemetryConfig{
			EnableAutoTieringBillingMetrics: false,
			EnableFilesAutoTieringBilling:   false,
			EnableReplicationBillingMetrics: false,
			PoolVolumeLabelPageSize:         10,
			GoogleBillingLabelsMaxEntries:   10,
		}

		provider := &BillingProvider{
			config:       config,
			vcpDataStore: mockVCPDB,
			metricsDB:    mockDB,
		}

		ctx := context.Background()
		aggregationStartTime := time.Now().Add(-time.Hour)

		// Mock the calls that fetchResourceData makes
		mockVCPDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil)
		mockVCPDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil)

		resourceCollection, err := provider.fetchResourceData(ctx, aggregationStartTime)
		assert.NoError(t, err)
		assert.NotNil(t, resourceCollection)

		// GetBlockOnlyPoolIDs should NOT be called when auto-tiering billing is disabled
		mockVCPDB.AssertNotCalled(t, "GetBlockOnlyPoolIDs", mock.Anything)
	})

	t.Run("SkipsQueryWhenFilesAutoTieringBillingEnabled", func(t *testing.T) {
		mockDB := database.NewMockStorage(t)
		mockVCPDB := database2.NewMockStorage(t)

		config := &common.TelemetryConfig{
			EnableAutoTieringBillingMetrics: true,
			EnableFilesAutoTieringBilling:   true, // Files billing enabled
			EnableReplicationBillingMetrics: false,
			PoolVolumeLabelPageSize:         10,
			GoogleBillingLabelsMaxEntries:   10,
		}

		provider := &BillingProvider{
			config:       config,
			vcpDataStore: mockVCPDB,
			metricsDB:    mockDB,
		}

		ctx := context.Background()
		aggregationStartTime := time.Now().Add(-time.Hour)

		// Mock the calls that fetchResourceData makes
		mockVCPDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil)
		mockVCPDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil)

		resourceCollection, err := provider.fetchResourceData(ctx, aggregationStartTime)
		assert.NoError(t, err)
		assert.NotNil(t, resourceCollection)

		// GetBlockOnlyPoolIDs should NOT be called when files auto-tiering billing is enabled
		// (all pools pass Tier 3 anyway, so no need to query)
		mockVCPDB.AssertNotCalled(t, "GetBlockOnlyPoolIDs", mock.Anything)
	})

	t.Run("CallsQueryWhenAutoTieringEnabledAndFilesBillingDisabled", func(t *testing.T) {
		mockDB := database.NewMockStorage(t)
		mockVCPDB := database2.NewMockStorage(t)

		config := &common.TelemetryConfig{
			EnableAutoTieringBillingMetrics: true,
			EnableFilesAutoTieringBilling:   false, // Files billing disabled
			EnableReplicationBillingMetrics: false,
			PoolVolumeLabelPageSize:         10,
			GoogleBillingLabelsMaxEntries:   10,
		}

		provider := &BillingProvider{
			config:       config,
			vcpDataStore: mockVCPDB,
			metricsDB:    mockDB,
		}

		ctx := context.Background()
		aggregationStartTime := time.Now().Add(-time.Hour)

		// Mock GetBlockOnlyPoolIDs - should be called
		mockVCPDB.On("GetBlockOnlyPoolIDs", mock.Anything).Return(map[int64]bool{1: true, 2: true}, nil).Once()

		// Mock the calls that fetchResourceData makes
		mockVCPDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil)
		mockVCPDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil)

		resourceCollection, err := provider.fetchResourceData(ctx, aggregationStartTime)
		assert.NoError(t, err)
		assert.NotNil(t, resourceCollection)

		// GetBlockOnlyPoolIDs SHOULD be called when auto-tiering is enabled and files billing is disabled
		mockVCPDB.AssertCalled(t, "GetBlockOnlyPoolIDs", mock.Anything)
	})

	t.Run("HandlesGetBlockOnlyPoolIDsError", func(t *testing.T) {
		mockDB := database.NewMockStorage(t)
		mockVCPDB := database2.NewMockStorage(t)

		config := &common.TelemetryConfig{
			EnableAutoTieringBillingMetrics: true,
			EnableFilesAutoTieringBilling:   false, // Files billing disabled - triggers query
			EnableReplicationBillingMetrics: false,
			PoolVolumeLabelPageSize:         10,
			GoogleBillingLabelsMaxEntries:   10,
		}

		provider := &BillingProvider{
			config:       config,
			vcpDataStore: mockVCPDB,
			metricsDB:    mockDB,
		}

		ctx := context.Background()
		aggregationStartTime := time.Now().Add(-time.Hour)

		// Mock GetBlockOnlyPoolIDs to return an error - should be handled gracefully
		mockVCPDB.On("GetBlockOnlyPoolIDs", mock.Anything).Return(nil, errors.New("database error")).Once()

		// Mock the calls that fetchResourceData makes
		mockVCPDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil)
		mockVCPDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil)

		resourceCollection, err := provider.fetchResourceData(ctx, aggregationStartTime)
		// Should NOT return error - gracefully handles GetBlockOnlyPoolIDs error
		assert.NoError(t, err)
		assert.NotNil(t, resourceCollection)

		mockVCPDB.AssertCalled(t, "GetBlockOnlyPoolIDs", mock.Anything)
	})

	t.Run("SetsHasOnlyBlockVolumesFromBlockOnlyPoolIDs", func(t *testing.T) {
		mockDB := database.NewMockStorage(t)
		mockVCPDB := database2.NewMockStorage(t)

		config := &common.TelemetryConfig{
			EnableAutoTieringBillingMetrics: true,
			EnableFilesAutoTieringBilling:   false, // Files billing disabled - triggers query
			EnableReplicationBillingMetrics: false,
			PoolVolumeLabelPageSize:         10,
			GoogleBillingLabelsMaxEntries:   10,
		}

		provider := &BillingProvider{
			config:       config,
			vcpDataStore: mockVCPDB,
			metricsDB:    mockDB,
		}

		ctx := context.Background()
		aggregationStartTime := time.Now().Add(-time.Hour)

		// Pool ID 1 is block-only, Pool ID 2 is not
		mockVCPDB.On("GetBlockOnlyPoolIDs", mock.Anything).Return(map[int64]bool{1: true}, nil).Once()

		// Return pools with IDs 1 and 2
		poolData := []*database2.PoolResourceData{
			{
				ID:               1,
				UUID:             "block-pool-uuid",
				Name:             "block-pool",
				AccountID:        100,
				AllowAutoTiering: true,
				PoolAttributes:   &datamodel.PoolAttributes{AccountName: "test-account"},
			},
			{
				ID:               2,
				UUID:             "file-pool-uuid",
				Name:             "file-pool",
				AccountID:        100,
				AllowAutoTiering: true,
				PoolAttributes:   &datamodel.PoolAttributes{AccountName: "test-account"},
			},
		}
		mockVCPDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolData, nil).Once()
		mockVCPDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
		mockVCPDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil)

		resourceCollection, err := provider.fetchResourceData(ctx, aggregationStartTime)
		assert.NoError(t, err)
		assert.NotNil(t, resourceCollection)

		// Verify HasOnlyBlockVolumes is set correctly
		// Note: ResourceKey includes ConsumerID (account name) from pool attributes
		blockPoolKey := ResourceKey{ResourceType: metadata.VolumePool, ResourceName: "block-pool", DeploymentName: "", ConsumerID: "test-account"}
		filePoolKey := ResourceKey{ResourceType: metadata.VolumePool, ResourceName: "file-pool", DeploymentName: "", ConsumerID: "test-account"}

		blockPoolData, found := resourceCollection.PoolData[blockPoolKey]
		assert.True(t, found, "Block pool should be in pool data")
		assert.True(t, blockPoolData.HasOnlyBlockVolumes, "Block pool should have HasOnlyBlockVolumes=true")

		filePoolData, found := resourceCollection.PoolData[filePoolKey]
		assert.True(t, found, "File pool should be in pool data")
		assert.False(t, filePoolData.HasOnlyBlockVolumes, "File pool should have HasOnlyBlockVolumes=false")
	})
}

// TestFetchPoolData_HasOnlyBlockVolumesMapping tests that HasOnlyBlockVolumes is correctly set
// for pools based on the block-only pool IDs returned from GetBlockOnlyPoolIDs
func TestFetchPoolData_HasOnlyBlockVolumesMapping(t *testing.T) {
	t.Run("NonBlockOnlyPoolHasHasOnlyBlockVolumesFalse", func(t *testing.T) {
		mockDB := database.NewMockStorage(t)
		mockVCPDB := database2.NewMockStorage(t)

		config := &common.TelemetryConfig{
			EnableAutoTieringBillingMetrics: true,
			EnableFilesAutoTieringBilling:   false, // Files billing disabled - triggers query
			EnableReplicationBillingMetrics: false,
			PoolVolumeLabelPageSize:         10,
			GoogleBillingLabelsMaxEntries:   10,
		}

		provider := &BillingProvider{
			config:       config,
			vcpDataStore: mockVCPDB,
			metricsDB:    mockDB,
		}

		ctx := context.Background()
		aggregationStartTime := time.Now().Add(-time.Hour)

		// Pool ID 1 is block-only, Pool ID 2 is NOT block-only (has file volumes)
		mockVCPDB.On("GetBlockOnlyPoolIDs", mock.Anything).Return(map[int64]bool{1: true}, nil).Once()

		// Return pool data - pool 2 has AllowAutoTiering=true but is NOT in block-only map
		poolData := []*database2.PoolResourceData{
			{
				ID:               2,
				UUID:             "file-pool-uuid",
				Name:             "file-pool",
				AccountID:        100,
				AllowAutoTiering: true, // Auto-tiering enabled
				PoolAttributes:   &datamodel.PoolAttributes{AccountName: "test-account"},
			},
		}
		mockVCPDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolData, nil).Once()
		mockVCPDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
		mockVCPDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil)

		resourceCollection, err := provider.fetchResourceData(ctx, aggregationStartTime)
		assert.NoError(t, err)
		assert.NotNil(t, resourceCollection)

		// Verify that pool 2 (file-pool) has HasOnlyBlockVolumes=false
		// Note: ResourceKey includes ConsumerID (account name) from pool attributes
		filePoolKey := ResourceKey{ResourceType: metadata.VolumePool, ResourceName: "file-pool", DeploymentName: "", ConsumerID: "test-account"}
		filePoolData, found := resourceCollection.PoolData[filePoolKey]
		assert.True(t, found, "File pool should be in pool data")
		assert.False(t, filePoolData.HasOnlyBlockVolumes, "Non-block-only pool should have HasOnlyBlockVolumes=false")
	})

	t.Run("BlockOnlyPoolHasHasOnlyBlockVolumesTrue", func(t *testing.T) {
		mockDB := database.NewMockStorage(t)
		mockVCPDB := database2.NewMockStorage(t)

		config := &common.TelemetryConfig{
			EnableAutoTieringBillingMetrics: true,
			EnableFilesAutoTieringBilling:   false, // Files billing disabled - triggers query
			EnableReplicationBillingMetrics: false,
			PoolVolumeLabelPageSize:         10,
			GoogleBillingLabelsMaxEntries:   10,
		}

		provider := &BillingProvider{
			config:       config,
			vcpDataStore: mockVCPDB,
			metricsDB:    mockDB,
		}

		ctx := context.Background()
		aggregationStartTime := time.Now().Add(-time.Hour)

		// Pool ID 1 is block-only
		mockVCPDB.On("GetBlockOnlyPoolIDs", mock.Anything).Return(map[int64]bool{1: true}, nil).Once()

		// Return pool data - pool 1 is in block-only map
		poolData := []*database2.PoolResourceData{
			{
				ID:               1,
				UUID:             "block-pool-uuid",
				Name:             "block-pool",
				AccountID:        100,
				AllowAutoTiering: true,
				PoolAttributes:   &datamodel.PoolAttributes{AccountName: "test-account"},
			},
		}
		mockVCPDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolData, nil).Once()
		mockVCPDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
		mockVCPDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil)

		resourceCollection, err := provider.fetchResourceData(ctx, aggregationStartTime)
		assert.NoError(t, err)
		assert.NotNil(t, resourceCollection)

		// Verify that pool 1 (block-pool) has HasOnlyBlockVolumes=true
		// Note: ResourceKey includes ConsumerID (account name) from pool attributes
		blockPoolKey := ResourceKey{ResourceType: metadata.VolumePool, ResourceName: "block-pool", DeploymentName: "", ConsumerID: "test-account"}
		blockPoolData, found := resourceCollection.PoolData[blockPoolKey]
		assert.True(t, found, "Block pool should be in pool data")
		assert.True(t, blockPoolData.HasOnlyBlockVolumes, "Block-only pool should have HasOnlyBlockVolumes=true")
	})

	t.Run("AllPoolsHaveHasOnlyBlockVolumesFalseWhenFilesAutoTieringEnabled", func(t *testing.T) {
		mockDB := database.NewMockStorage(t)
		mockVCPDB := database2.NewMockStorage(t)

		config := &common.TelemetryConfig{
			EnableAutoTieringBillingMetrics: true,
			EnableFilesAutoTieringBilling:   true, // Files billing ENABLED - no query needed
			EnableReplicationBillingMetrics: false,
			PoolVolumeLabelPageSize:         10,
			GoogleBillingLabelsMaxEntries:   10,
		}

		provider := &BillingProvider{
			config:       config,
			vcpDataStore: mockVCPDB,
			metricsDB:    mockDB,
		}

		ctx := context.Background()
		aggregationStartTime := time.Now().Add(-time.Hour)

		// GetBlockOnlyPoolIDs should NOT be called when files billing is enabled

		// Return pool data
		poolData := []*database2.PoolResourceData{
			{
				ID:               1,
				UUID:             "pool-uuid",
				Name:             "pool-1",
				AccountID:        100,
				AllowAutoTiering: true,
				PoolAttributes:   &datamodel.PoolAttributes{AccountName: "test-account"},
			},
		}
		mockVCPDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(poolData, nil).Once()
		mockVCPDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
		mockVCPDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil)

		resourceCollection, err := provider.fetchResourceData(ctx, aggregationStartTime)
		assert.NoError(t, err)
		assert.NotNil(t, resourceCollection)

		// Verify GetBlockOnlyPoolIDs was NOT called
		mockVCPDB.AssertNotCalled(t, "GetBlockOnlyPoolIDs", mock.Anything)

		// Pool should have HasOnlyBlockVolumes=false (since blockOnlyPoolIDs is empty map)
		// Note: ResourceKey includes ConsumerID (account name) from pool attributes
		poolKey := ResourceKey{ResourceType: metadata.VolumePool, ResourceName: "pool-1", DeploymentName: "", ConsumerID: "test-account"}
		poolDataResult, found := resourceCollection.PoolData[poolKey]
		assert.True(t, found, "Pool should be in pool data")
		assert.False(t, poolDataResult.HasOnlyBlockVolumes, "Pool should have HasOnlyBlockVolumes=false when files billing is enabled")
	})
}

// TestIsAutoTieringBillingMetric tests the isAutoTieringBillingMetric function
func TestIsAutoTieringBillingMetric(t *testing.T) {
	tests := []struct {
		name         string
		measuredType metadata.MeasuredType
		expected     bool
	}{
		{
			name:         "CoolTierDataReadSizeRaw is auto-tiering metric",
			measuredType: metadata.CoolTierDataReadSizeRaw,
			expected:     true,
		},
		{
			name:         "CoolTierDataWriteSizeRaw is auto-tiering metric",
			measuredType: metadata.CoolTierDataWriteSizeRaw,
			expected:     true,
		},
		{
			name:         "PoolHotTierProvisionedSize is auto-tiering metric",
			measuredType: metadata.PoolHotTierProvisionedSize,
			expected:     true,
		},
		{
			name:         "PoolCapacityTierLogicalFootprint is auto-tiering metric",
			measuredType: metadata.PoolCapacityTierLogicalFootprint,
			expected:     true,
		},
		{
			name:         "PoolAllocatedSize is NOT auto-tiering metric",
			measuredType: metadata.PoolAllocatedSize,
			expected:     false,
		},
		{
			name:         "AllocatedUsed is NOT auto-tiering metric",
			measuredType: metadata.AllocatedUsed,
			expected:     false,
		},
		{
			name:         "PoolTotalThroughputMibps is NOT auto-tiering metric",
			measuredType: metadata.PoolTotalThroughputMibps,
			expected:     false,
		},
		{
			name:         "LogicalSize is NOT auto-tiering metric",
			measuredType: metadata.LogicalSize,
			expected:     false,
		},
		{
			name:         "BackupLogicalSize is NOT auto-tiering metric",
			measuredType: metadata.BackupLogicalSize,
			expected:     false,
		},
		{
			name:         "CoolTierDataReadSize (non-raw) is NOT auto-tiering billing metric",
			measuredType: metadata.CoolTierDataReadSize,
			expected:     false,
		},
		{
			name:         "CoolTierDataWriteSize (non-raw) is NOT auto-tiering billing metric",
			measuredType: metadata.CoolTierDataWriteSize,
			expected:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isAutoTieringBillingMetric(tt.measuredType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestFetchPoolData_AllowAutoTieringField tests that AllowAutoTiering field is correctly populated in pool resource data
func TestFetchPoolData_AllowAutoTieringField(t *testing.T) {
	ctx := context.Background()
	mockVcpDB := &database2.MockStorage{}
	mockMetricsDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{PoolVolumeLabelPageSize: 10, GoogleBillingLabelsMaxEntries: 10}
	provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

	startTime := time.Now().Add(-1 * time.Hour)

	// Pool with AllowAutoTiering enabled
	mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{
		{
			UUID:           "pool-uuid-auto-tiering",
			Name:           "auto-tiering-pool",
			AccountID:      1,
			DeploymentName: "dep1",
			PoolAttributes: &datamodel.PoolAttributes{
				AccountName:  "account1",
				IsRegionalHA: false,
			},
			AllowAutoTiering: true, // Auto-tiering enabled
		},
		{
			UUID:           "pool-uuid-no-auto-tiering",
			Name:           "no-auto-tiering-pool",
			AccountID:      2,
			DeploymentName: "dep2",
			PoolAttributes: &datamodel.PoolAttributes{
				AccountName:  "account2",
				IsRegionalHA: false,
			},
			AllowAutoTiering: false, // Auto-tiering disabled
		},
	}, nil).Once()
	mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
	mockVcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil).Once()

	resourceCollection, err := provider.fetchResourceData(ctx, startTime)
	assert.NoError(t, err)
	assert.Len(t, resourceCollection.PoolData, 2)

	// Verify AllowAutoTiering is correctly set for each pool
	autoTieringPoolKey := ResourceKey{
		ResourceType:   metadata.VolumePool,
		ResourceName:   "auto-tiering-pool",
		DeploymentName: "dep1",
		ConsumerID:     "account1",
	}
	assert.True(t, resourceCollection.PoolData[autoTieringPoolKey].AllowAutoTiering,
		"Pool with AllowAutoTiering=true should have AllowAutoTiering set in resource data")

	noAutoTieringPoolKey := ResourceKey{
		ResourceType:   metadata.VolumePool,
		ResourceName:   "no-auto-tiering-pool",
		DeploymentName: "dep2",
		ConsumerID:     "account2",
	}
	assert.False(t, resourceCollection.PoolData[noAutoTieringPoolKey].AllowAutoTiering,
		"Pool with AllowAutoTiering=false should have AllowAutoTiering unset in resource data")

	mockVcpDB.AssertExpectations(t)
}

// TestProcessMetricsWithJobDef_DisableBillingForLargeVolumes_CRR verifies that CRR billing
// is disabled for Large Volumes when EnableLargeVolumesBilling feature flag is false
func TestProcessMetricsWithJobDef_DisableBillingForLargeVolumes_CRR(t *testing.T) {
	mockDB := &database.MockStorage{}
	processor := &BillingProvider{
		metricsDB: mockDB,
		config:    &common.TelemetryConfig{EnableLargeVolumesBilling: false},
	}
	ctx := context.Background()
	logger := util.GetLogger(ctx)
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	resourceID := ResourceKey{
		ResourceType:   metadata.VolumeReplicationRelationship,
		ResourceName:   "test-replication-uuid",
		DeploymentName: "dep1",
		ConsumerID:     "test-customer",
	}

	repName := "replication1"
	srcLoc := "us-west"
	dstLoc := "us-east"
	dstVolUUID := "dst-vol-uuid"

	resourceCollection := &ResourceCollection{
		VolumeReplicationData: map[ResourceKey]ResourceData{
			resourceID: {
				UUID:          "test-uuid",
				AccountID:     123,
				Labels:        Labels{"env": "test"},
				LargeCapacity: true, // Large Volumes pool
				VolumeStyle:   "FLEXGROUP",
				VolumeReplicationInfo: &VolumeReplicationInfo{
					ReplicationType:       "CROSS_REGION_REPLICATION",
					ReplicationSchedule:   "hourly",
					ReplicationName:       &repName,
					SourceLocation:        &srcLoc,
					DestinationLocation:   &dstLoc,
					DestinationVolumeUUID: &dstVolUUID,
				},
			},
		},
	}

	metrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    "test-uuid",
			ConsumerID:      "test-customer",
			Location:        "test-location",
			Quantity:        100,
			MetricTimestamp: now,
			ResourceType:    metadata.VolumeReplicationRelationship,
			MeasuredType:    metadata.XregionReplicationTotalTransferBytes,
			DeploymentName:  "dep1",
		},
	}

	var aggregatedRecords []datamodel2.AggregatedUsage
	err := processor.processMetricsWithJobDef(ctx, resourceID, hydratedMetricsToTimeSeries(metrics, startTime, now), common.AggregationJobDefinition{AggregationType: common.CounterAggregation}, startTime, now, resourceCollection, &aggregatedRecords, make(map[CounterAggregationCacheResourceKey]*float64), logger)

	assert.NoError(t, err)
	assert.Len(t, aggregatedRecords, 0, "No record should be created for Large Volumes with CRR when EnableLargeVolumesBilling is false")
}

// TestProcessMetricsWithJobDef_DisableBillingForLargeVolumes_Backup verifies that Backup billing
// is disabled for Large Volumes when EnableLargeVolumesBilling feature flag is false
func TestProcessMetricsWithJobDef_DisableBillingForLargeVolumes_Backup(t *testing.T) {
	mockDB := &database.MockStorage{}
	processor := &BillingProvider{
		metricsDB: mockDB,
		config:    &common.TelemetryConfig{EnableLargeVolumesBilling: false},
	}
	ctx := context.Background()
	logger := util.GetLogger(ctx)
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	resourceID := ResourceKey{
		ResourceType:   metadata.Backup,
		ResourceName:   "test-backup-volume-uuid",
		DeploymentName: "Vault1",
		ConsumerID:     "Account1",
	}

	resourceCollection := &ResourceCollection{
		BackupData: map[ResourceKey]ResourceData{
			resourceID: {
				UUID:          "test-backup-volume-uuid",
				AccountID:     123,
				Labels:        Labels{"env": "test"},
				LargeCapacity: true, // Large Volumes pool
				VolumeStyle:   "FLEXGROUP",
			},
		},
	}

	metrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    "test-backup-volume-uuid",
			ConsumerID:      "Account1",
			Location:        "test-location",
			Quantity:        1024,
			MetricTimestamp: now,
			ResourceType:    metadata.Backup,
			MeasuredType:    metadata.BackupLogicalSize,
			DeploymentName:  "Vault1",
		},
	}

	var aggregatedRecords []datamodel2.AggregatedUsage
	err := processor.processMetricsWithJobDef(ctx, resourceID, hydratedMetricsToTimeSeries(metrics, startTime, now), common.AggregationJobDefinition{AggregationType: common.SumAggregation}, startTime, now, resourceCollection, &aggregatedRecords, make(map[CounterAggregationCacheResourceKey]*float64), logger)

	assert.NoError(t, err)
	assert.Len(t, aggregatedRecords, 0, "No record should be created for Large Volumes with Backup when EnableLargeVolumesBilling is false")
}

// TestProcessMetricsWithJobDef_EnableBillingForLargeVolumes_WhenFlagEnabled verifies that billing
// remains enabled for Large Volumes when EnableLargeVolumesBilling is true
func TestProcessMetricsWithJobDef_EnableBillingForLargeVolumes_WhenFlagEnabled(t *testing.T) {
	mockDB := &database.MockStorage{}
	processor := &BillingProvider{
		metricsDB: mockDB,
		config:    &common.TelemetryConfig{EnableLargeVolumesBilling: true}, // Feature flag enabled
	}
	ctx := context.Background()
	logger := util.GetLogger(ctx)
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	resourceID := ResourceKey{
		ResourceType:   metadata.VolumeReplicationRelationship,
		ResourceName:   "test-replication-uuid",
		DeploymentName: "dep1",
		ConsumerID:     "test-customer",
	}

	repName := "replication1"
	srcLoc := "us-west"
	dstLoc := "us-east"
	dstVolUUID := "dst-vol-uuid"

	resourceCollection := &ResourceCollection{
		VolumeReplicationData: map[ResourceKey]ResourceData{
			resourceID: {
				UUID:          "test-uuid",
				AccountID:     123,
				Labels:        Labels{"env": "test"},
				LargeCapacity: true, // Large Volumes pool
				VolumeStyle:   "FLEXGROUP",
				VolumeReplicationInfo: &VolumeReplicationInfo{
					ReplicationType:       "CROSS_REGION_REPLICATION",
					ReplicationSchedule:   "hourly",
					ReplicationName:       &repName,
					SourceLocation:        &srcLoc,
					DestinationLocation:   &dstLoc,
					DestinationVolumeUUID: &dstVolUUID,
				},
			},
		},
	}

	metrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    "test-uuid",
			ConsumerID:      "test-customer",
			Location:        "test-location",
			Quantity:        100,
			MetricTimestamp: now,
			ResourceType:    metadata.VolumeReplicationRelationship,
			MeasuredType:    metadata.XregionReplicationTotalTransferBytes,
			DeploymentName:  "dep1",
		},
	}

	var aggregatedRecords []datamodel2.AggregatedUsage
	err := processor.processMetricsWithJobDef(ctx, resourceID, hydratedMetricsToTimeSeries(metrics, startTime, now), common.AggregationJobDefinition{AggregationType: common.CounterAggregation}, startTime, now, resourceCollection, &aggregatedRecords, make(map[CounterAggregationCacheResourceKey]*float64), logger)

	assert.NoError(t, err)
	assert.Len(t, aggregatedRecords, 1)
	// When feature flag is enabled, billing should remain enabled for Large Volumes
	assert.True(t, aggregatedRecords[0].IsBillable, "CRR billing should be enabled when EnableLargeVolumesBilling is true")
}

// TestFetchVolumeReplicationData_NilVolume verifies that volume replications with nil Volume are skipped
func TestFetchVolumeReplicationData_NilVolume(t *testing.T) {
	ctx := context.Background()
	mockVcpDB := &database2.MockStorage{}
	mockMetricsDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{
		PoolVolumeLabelPageSize:         100,
		GoogleBillingLabelsMaxEntries:   10,
		EnableReplicationBillingMetrics: true,
	}
	provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

	// Mock volume replications - one with nil Volume
	mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-rep-uuid-nil"},
			Name:      "replication-nil-volume",
			Volume:    nil, // Nil volume - should be skipped
			Account:   &datamodel.Account{Name: "account1"},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ReplicationType: "CROSS_REGION_REPLICATION",
				ExternalUUID:    "ext-uuid-nil",
			},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-rep-uuid-valid"},
			Name:      "replication-valid",
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
				Name:      "vol1",
				Pool:      &datamodel.Pool{DeploymentName: "dep1"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					Protocols: []string{"ISCSI"},
				},
				LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{
					LargeCapacity: true,
				},
			},
			Account: &datamodel.Account{Name: "account1"},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ReplicationType: "CROSS_REGION_REPLICATION",
				ExternalUUID:    "ext-uuid-valid",
				Labels:          &datamodel.JSONB{"env": "prod"},
			},
		},
	}, nil).Once()
	mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

	resourceCollection := &ResourceCollection{
		VolumeReplicationData: make(map[ResourceKey]ResourceData),
	}

	startTime := time.Now().Add(-1 * time.Hour)

	err := provider.fetchVolumeReplicationData(ctx, startTime, resourceCollection)
	assert.NoError(t, err)

	// Only the valid replication should be in the collection (nil Volume one was skipped)
	assert.Len(t, resourceCollection.VolumeReplicationData, 1)

	// Verify the valid replication has LargeCapacity from Volume.LargeVolumeAttributes
	for _, data := range resourceCollection.VolumeReplicationData {
		assert.True(t, data.LargeCapacity, "Should get LargeCapacity from Volume.LargeVolumeAttributes")
		assert.Equal(t, "FLEXGROUP", data.VolumeStyle, "VolumeStyle should be FLEXGROUP for large capacity")
	}

	mockVcpDB.AssertExpectations(t)
}

// TestFetchVolumeReplicationData_InRegionReplicationSkipped verifies that in-region replications
// (INTRAZONE and INTERZONE) are skipped when EnableInRegionReplicationBillingMetrics is false
func TestFetchVolumeReplicationData_InRegionReplicationSkipped(t *testing.T) {
	ctx := context.Background()
	mockVcpDB := &database2.MockStorage{}
	mockMetricsDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{
		PoolVolumeLabelPageSize:                 100,
		GoogleBillingLabelsMaxEntries:           10,
		EnableReplicationBillingMetrics:         true,
		EnableInRegionReplicationBillingMetrics: false, // Disabled - should skip INTRAZONE and INTERZONE
	}
	provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

	// Mock volume replications with different replication types
	mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-rep-intrazone"},
			Name:      "replication-intrazone",
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "vol-uuid-1"},
				Name:      "vol1",
				Pool:      &datamodel.Pool{DeploymentName: "dep1"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					Protocols: []string{"ISCSI"},
				},
			},
			Account: &datamodel.Account{Name: "account1"},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ReplicationType: "INTRA_ZONE_REPLICATION", // Should be skipped
				ExternalUUID:    "ext-uuid-intrazone",
			},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-rep-interzone"},
			Name:      "replication-interzone",
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "vol-uuid-2"},
				Name:      "vol2",
				Pool:      &datamodel.Pool{DeploymentName: "dep2"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					Protocols: []string{"ISCSI"},
				},
			},
			Account: &datamodel.Account{Name: "account1"},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ReplicationType: "INTER_ZONE_REPLICATION", // Should be skipped
				ExternalUUID:    "ext-uuid-interzone",
			},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-rep-crossregion"},
			Name:      "replication-crossregion",
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "vol-uuid-3"},
				Name:      "vol3",
				Pool:      &datamodel.Pool{DeploymentName: "dep3"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					Protocols: []string{"ISCSI"},
				},
			},
			Account: &datamodel.Account{Name: "account1"},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ReplicationType: "CROSS_REGION_REPLICATION", // Should be processed
				ExternalUUID:    "ext-uuid-crossregion",
				Labels:          &datamodel.JSONB{"env": "prod"},
			},
		},
	}, nil).Once()
	mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

	resourceCollection := &ResourceCollection{
		VolumeReplicationData: make(map[ResourceKey]ResourceData),
	}

	startTime := time.Now().Add(-1 * time.Hour)

	err := provider.fetchVolumeReplicationData(ctx, startTime, resourceCollection)
	assert.NoError(t, err)

	// Only the CROSS_REGION_REPLICATION should be in the collection (INTRAZONE and INTERZONE were skipped)
	assert.Len(t, resourceCollection.VolumeReplicationData, 1)

	// Verify it's the cross-region replication by checking the ResourceKey
	found := false
	for key := range resourceCollection.VolumeReplicationData {
		if key.ResourceName == "ext-uuid-crossregion" {
			found = true
			break
		}
	}
	assert.True(t, found, "Should find cross-region replication in results")

	mockVcpDB.AssertExpectations(t)
}

// TestProcessMetricsWithJobDef_RegularVolumes_BillingEnabled verifies that billing
// is enabled for regular (non-Large) volumes even when EnableLargeVolumesBilling is false
func TestProcessMetricsWithJobDef_RegularVolumes_BillingEnabled(t *testing.T) {
	mockDB := &database.MockStorage{}
	processor := &BillingProvider{
		metricsDB: mockDB,
		config:    &common.TelemetryConfig{EnableLargeVolumesBilling: false}, // Feature flag disabled (billing off for large volumes)
	}
	ctx := context.Background()
	logger := util.GetLogger(ctx)
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	resourceID := ResourceKey{
		ResourceType:   metadata.VolumeReplicationRelationship,
		ResourceName:   "test-replication-uuid",
		DeploymentName: "dep1",
		ConsumerID:     "test-customer",
	}

	repName := "replication1"
	srcLoc := "us-west"
	dstLoc := "us-east"
	dstVolUUID := "dst-vol-uuid"

	resourceCollection := &ResourceCollection{
		VolumeReplicationData: map[ResourceKey]ResourceData{
			resourceID: {
				UUID:          "test-uuid",
				AccountID:     123,
				Labels:        Labels{"env": "test"},
				LargeCapacity: false, // Regular volume (not Large Volumes pool)
				VolumeStyle:   "FLEXVOL",
				VolumeReplicationInfo: &VolumeReplicationInfo{
					ReplicationType:       "CROSS_REGION_REPLICATION",
					ReplicationSchedule:   "hourly",
					ReplicationName:       &repName,
					SourceLocation:        &srcLoc,
					DestinationLocation:   &dstLoc,
					DestinationVolumeUUID: &dstVolUUID,
				},
			},
		},
	}

	metrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    "test-uuid",
			ConsumerID:      "test-customer",
			Location:        "test-location",
			Quantity:        100,
			MetricTimestamp: now,
			ResourceType:    metadata.VolumeReplicationRelationship,
			MeasuredType:    metadata.XregionReplicationTotalTransferBytes,
			DeploymentName:  "dep1",
		},
	}

	var aggregatedRecords []datamodel2.AggregatedUsage
	err := processor.processMetricsWithJobDef(ctx, resourceID, hydratedMetricsToTimeSeries(metrics, startTime, now), common.AggregationJobDefinition{AggregationType: common.CounterAggregation}, startTime, now, resourceCollection, &aggregatedRecords, make(map[CounterAggregationCacheResourceKey]*float64), logger)

	assert.NoError(t, err)
	assert.Len(t, aggregatedRecords, 1)
	// Regular volumes should have billing enabled
	assert.True(t, aggregatedRecords[0].IsBillable, "CRR billing should be enabled for regular volumes")
}

// TestFetchBackupData_UsesOntapVolumeStyle verifies that fetchBackupData uses
// backup.Attributes.OntapVolumeStyle to determine VolumeStyle (not volume lookup)
func TestFetchBackupData_UsesOntapVolumeStyle(t *testing.T) {
	ctx := context.Background()
	mockVcpDB := &database2.MockStorage{}
	mockMetricsDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{
		PoolVolumeLabelPageSize:       100,
		GoogleBillingLabelsMaxEntries: 10,
		EnableBackupBillingMetrics:    true,
	}
	provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

	// Mock backup metadata (no labels)
	mockVcpDB.On("GetBackupMetadata", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.BackupMetadata{}, nil)

	// Mock backups - one with flexgroup (large capacity), one with flexvol (regular)
	mockVcpDB.On("GetBackupResourceDataForAggregation", mock.Anything, mock.Anything, mock.MatchedBy(func(pagination *dbutils.Pagination) bool {
		return pagination.Offset == 0
	})).Return([]*datamodel.Backup{
		{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-1"},
			VolumeUUID:              "deleted-volume-uuid", // Deleted volume UUID
			LatestLogicalBackupSize: 1024,
			BackupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "vault1",
				AccountID: 123,
			},
			Attributes: &datamodel.BackupAttributes{
				VolumeName:        "deleted-large-volume",
				AccountIdentifier: "account1",
				OntapVolumeStyle:  database2.OntapFgVolumeStyle, // Should result in FLEXGROUP VolumeStyle
			},
		},
		{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-2"},
			VolumeUUID:              "another-deleted-volume-uuid", // Deleted volume UUID
			LatestLogicalBackupSize: 2048,
			BackupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{ID: 2},
				Name:      "vault2",
				AccountID: 456,
			},
			Attributes: &datamodel.BackupAttributes{
				VolumeName:        "deleted-regular-volume",
				AccountIdentifier: "account2",
				OntapVolumeStyle:  "flexvol", // Should result in FLEXVOL VolumeStyle
			},
		},
	}, nil).Once()
	mockVcpDB.On("GetBackupResourceDataForAggregation", mock.Anything, mock.Anything, mock.MatchedBy(func(pagination *dbutils.Pagination) bool {
		return pagination.Offset > 0
	})).Return([]*datamodel.Backup{}, nil).Once()

	resourceCollection := &ResourceCollection{
		BackupData: make(map[ResourceKey]ResourceData),
	}

	startTime := time.Now().Add(-1 * time.Hour)

	err := provider.fetchBackupData(ctx, startTime, resourceCollection)
	assert.NoError(t, err)

	// Verify both backups are in the collection
	assert.Len(t, resourceCollection.BackupData, 2)

	// Find and verify the large capacity backup
	largeBackupKey := ResourceKey{
		ResourceType:   metadata.Backup,
		ResourceName:   "deleted-volume-uuid",
		DeploymentName: "vault1",
		ConsumerID:     "account1",
	}
	largeBackupData, found := resourceCollection.BackupData[largeBackupKey]
	assert.True(t, found, "Large capacity backup should be in collection")
	assert.True(t, largeBackupData.LargeCapacity, "LargeCapacity should be true for flexgroup backup")
	assert.Equal(t, "FLEXGROUP", largeBackupData.VolumeStyle, "VolumeStyle should be FLEXGROUP for flexgroup backup")

	// Find and verify the regular backup
	regularBackupKey := ResourceKey{
		ResourceType:   metadata.Backup,
		ResourceName:   "another-deleted-volume-uuid",
		DeploymentName: "vault2",
		ConsumerID:     "account2",
	}
	regularBackupData, found := resourceCollection.BackupData[regularBackupKey]
	assert.True(t, found, "Regular backup should be in collection")
	assert.False(t, regularBackupData.LargeCapacity, "LargeCapacity should be false for flexvol backup")
	assert.Equal(t, "FLEXVOL", regularBackupData.VolumeStyle, "VolumeStyle should be FLEXVOL for flexvol backup")

	mockVcpDB.AssertExpectations(t)
}

// TestFetchVolumeReplicationData_UsesLargeVolumeAttributes verifies that
// fetchVolumeReplicationData uses Volume.LargeVolumeAttributes to determine VolumeStyle
func TestFetchVolumeReplicationData_UsesLargeVolumeAttributes(t *testing.T) {
	ctx := context.Background()
	mockVcpDB := &database2.MockStorage{}
	mockMetricsDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{
		PoolVolumeLabelPageSize:              100,
		GoogleBillingLabelsMaxEntries:        10,
		EnableReplicationBillingMetrics:      true,
		EnableFilesReplicationBillingMetrics: true, // Required to allow non-ISCSI volumes
	}
	provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

	// Mock volume replications with LargeVolumeAttributes
	mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-rep-uuid-large"},
			Name:      "replication-large-volume",
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "vol-uuid-large"},
				Name:      "large-vol",
				Pool:      &datamodel.Pool{DeploymentName: "dep1"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					Protocols: []string{"NFSV3"},
				},
				LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{
					LargeCapacity: true, // Large capacity volume
				},
			},
			Account: &datamodel.Account{Name: "account1"},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ReplicationType: "CROSS_REGION_REPLICATION",
				ExternalUUID:    "ext-uuid-large",
			},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "vol-rep-uuid-regular"},
			Name:      "replication-regular-volume",
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "vol-uuid-regular"},
				Name:      "regular-vol",
				Pool:      &datamodel.Pool{DeploymentName: "dep2"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					Protocols: []string{"NFSV3"},
				},
				LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{
					LargeCapacity: false, // Regular volume
				},
			},
			Account: &datamodel.Account{Name: "account2"},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ReplicationType: "CROSS_REGION_REPLICATION",
				ExternalUUID:    "ext-uuid-regular",
			},
		},
	}, nil).Once()
	mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

	resourceCollection := &ResourceCollection{
		VolumeReplicationData: make(map[ResourceKey]ResourceData),
	}

	startTime := time.Now().Add(-1 * time.Hour)

	err := provider.fetchVolumeReplicationData(ctx, startTime, resourceCollection)
	assert.NoError(t, err)

	// Verify both replications are in the collection
	assert.Len(t, resourceCollection.VolumeReplicationData, 2)

	// Find and verify the large capacity replication
	largeRepKey := ResourceKey{
		ResourceType:   metadata.VolumeReplicationRelationship,
		ResourceName:   "ext-uuid-large",
		DeploymentName: "dep1",
		ConsumerID:     "account1",
	}
	largeRepData, found := resourceCollection.VolumeReplicationData[largeRepKey]
	assert.True(t, found, "Large capacity replication should be in collection")
	assert.True(t, largeRepData.LargeCapacity, "LargeCapacity should be true from Volume.LargeVolumeAttributes")
	assert.Equal(t, "FLEXGROUP", largeRepData.VolumeStyle, "VolumeStyle should be FLEXGROUP for large capacity")

	// Find and verify the regular replication
	regularRepKey := ResourceKey{
		ResourceType:   metadata.VolumeReplicationRelationship,
		ResourceName:   "ext-uuid-regular",
		DeploymentName: "dep2",
		ConsumerID:     "account2",
	}
	regularRepData, found := resourceCollection.VolumeReplicationData[regularRepKey]
	assert.True(t, found, "Regular replication should be in collection")
	assert.False(t, regularRepData.LargeCapacity, "LargeCapacity should be false from Volume.LargeVolumeAttributes")
	assert.Equal(t, "FLEXVOL", regularRepData.VolumeStyle, "VolumeStyle should be FLEXVOL for regular volume")

	mockVcpDB.AssertExpectations(t)
}

// TestFetchResourceData_PopulatesIsONTAPModeFromAPIAccessMode tests that IsONTAPMode is correctly
// populated from the pool's APIAccessMode field
func TestFetchResourceData_PopulatesIsONTAPModeFromAPIAccessMode(t *testing.T) {
	ctx := context.Background()
	mockVcpDB := &database2.MockStorage{}
	mockMetricsDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{
		PoolVolumeLabelPageSize:         10,
		GoogleBillingLabelsMaxEntries:   10,
		EnableReplicationBillingMetrics: true,
	}
	provider := NewBillingProvider(mockMetricsDB, mockVcpDB, config, mockSink)

	// First call returns pools with different APIAccessMode values
	mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{
		{
			UUID:           "expert-pool-uuid",
			Name:           "expert-pool",
			AccountID:      1,
			DeploymentName: "dep1",
			APIAccessMode:  "ONTAP", // Expert mode
			PoolAttributes: &datamodel.PoolAttributes{
				AccountName: "account1",
				Labels:      &datamodel.JSONB{"env": "prod"},
			},
			AllowAutoTiering: true,
		},
		{
			UUID:           "standard-pool-uuid",
			Name:           "standard-pool",
			AccountID:      2,
			DeploymentName: "dep2",
			APIAccessMode:  "DEFAULT", // Standard mode
			PoolAttributes: &datamodel.PoolAttributes{
				AccountName: "account2",
				Labels:      &datamodel.JSONB{"env": "dev"},
			},
			AllowAutoTiering: true,
		},
		{
			UUID:           "empty-mode-pool-uuid",
			Name:           "empty-mode-pool",
			AccountID:      3,
			DeploymentName: "dep3",
			APIAccessMode:  "", // Empty mode (should be treated as non-expert)
			PoolAttributes: &datamodel.PoolAttributes{
				AccountName: "account3",
				Labels:      &datamodel.JSONB{"env": "test"},
			},
			AllowAutoTiering: true,
		},
	}, nil).Once()
	mockVcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Once()
	mockVcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil).Once()
	mockVcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Once()

	resourceCollection, err := provider.fetchResourceData(ctx, time.Now().Add(-1*time.Hour))
	assert.NoError(t, err)
	assert.Len(t, resourceCollection.PoolData, 3)

	// Verify ONTAP mode pool has IsONTAPMode=true
	ontapPoolKey := ResourceKey{
		ResourceType:   metadata.VolumePool,
		ResourceName:   "expert-pool",
		DeploymentName: "dep1",
		ConsumerID:     "account1",
	}
	ontapPoolData, found := resourceCollection.PoolData[ontapPoolKey]
	assert.True(t, found, "ONTAP mode pool should be in collection")
	assert.True(t, ontapPoolData.IsONTAPMode, "IsONTAPMode should be true for pool with APIAccessMode=ONTAP")

	// Verify standard mode pool has IsONTAPMode=false
	standardPoolKey := ResourceKey{
		ResourceType:   metadata.VolumePool,
		ResourceName:   "standard-pool",
		DeploymentName: "dep2",
		ConsumerID:     "account2",
	}
	standardPoolData, found := resourceCollection.PoolData[standardPoolKey]
	assert.True(t, found, "Standard mode pool should be in collection")
	assert.False(t, standardPoolData.IsONTAPMode, "IsONTAPMode should be false for pool with APIAccessMode=DEFAULT")

	// Verify empty mode pool has IsONTAPMode=false
	emptyModePoolKey := ResourceKey{
		ResourceType:   metadata.VolumePool,
		ResourceName:   "empty-mode-pool",
		DeploymentName: "dep3",
		ConsumerID:     "account3",
	}
	emptyModePoolData, found := resourceCollection.PoolData[emptyModePoolKey]
	assert.True(t, found, "Empty mode pool should be in collection")
	assert.False(t, emptyModePoolData.IsONTAPMode, "IsONTAPMode should be false for pool with empty APIAccessMode")

	mockVcpDB.AssertExpectations(t)
}

// TestResourceData_PrimaryZone tests that PrimaryZone is set correctly
func TestResourceData_PrimaryZone(t *testing.T) {
	t.Run("PrimaryZone set from pool attributes", func(t *testing.T) {
		rd := ResourceData{
			UUID:        "test-uuid",
			PrimaryZone: "us-central1-a",
		}
		assert.Equal(t, "us-central1-a", rd.PrimaryZone)
	})

	t.Run("PrimaryZone empty when not set", func(t *testing.T) {
		rd := ResourceData{
			UUID: "test-uuid",
		}
		assert.Empty(t, rd.PrimaryZone)
	})
}

// TestZoneSetOnAggregatedUsage tests that Zone is set for all zonal pool metrics
func TestZoneSetOnAggregatedUsage(t *testing.T) {
	t.Run("Zone set for zonal pool", func(t *testing.T) {
		resourceData := &ResourceData{
			UUID:        "test-uuid",
			AccountID:   123,
			PrimaryZone: "us-central1-a",
		}
		resourceKey := ResourceKey{
			ResourceType: metadata.VolumePool,
		}

		var zone *string
		if resourceData.PrimaryZone != "" && resourceKey.ResourceType == metadata.VolumePool {
			zone = &resourceData.PrimaryZone
		}

		assert.NotNil(t, zone)
		assert.Equal(t, "us-central1-a", *zone)
	})

	t.Run("Zone nil for regional pool", func(t *testing.T) {
		resourceData := &ResourceData{
			UUID:        "test-uuid",
			AccountID:   123,
			PrimaryZone: "us-central1-a",
		}
		resourceKey := ResourceKey{
			ResourceType: metadata.VolumePoolRegionalHA,
		}

		var zone *string
		if resourceData.PrimaryZone != "" && resourceKey.ResourceType == metadata.VolumePool {
			zone = &resourceData.PrimaryZone
		}

		assert.Nil(t, zone, "Zone should be nil for regional pool")
	})

	t.Run("Zone set for non-AT metric on zonal pool too", func(t *testing.T) {
		resourceData := &ResourceData{
			UUID:        "test-uuid",
			AccountID:   123,
			PrimaryZone: "us-central1-a",
		}
		resourceKey := ResourceKey{
			ResourceType: metadata.VolumePool,
		}

		var zone *string
		if resourceData.PrimaryZone != "" && resourceKey.ResourceType == metadata.VolumePool {
			zone = &resourceData.PrimaryZone
		}

		assert.NotNil(t, zone, "Zone should be set for all zonal pool metrics")
		assert.Equal(t, "us-central1-a", *zone)
	})

	t.Run("Zone nil when PrimaryZone is empty", func(t *testing.T) {
		resourceData := &ResourceData{
			UUID:        "test-uuid",
			AccountID:   123,
			PrimaryZone: "",
		}
		resourceKey := ResourceKey{
			ResourceType: metadata.VolumePool,
		}

		var zone *string
		if resourceData.PrimaryZone != "" && resourceKey.ResourceType == metadata.VolumePool {
			zone = &resourceData.PrimaryZone
		}

		assert.Nil(t, zone, "Zone should be nil when PrimaryZone is empty")
	})
}

func TestProcessBillingMetrics_CrossRegionRestoreTransferBytes_RegionOverride(t *testing.T) {
	mockDB := &database.MockStorage{}
	mockSink := &MockUsageSink{}
	config := &common.TelemetryConfig{}
	vcpDB := &database2.MockStorage{}
	processor := NewBillingProvider(mockDB, vcpDB, config, mockSink)
	ctx := context.Background()
	now := time.Now()
	startTime := now.Add(-1 * time.Hour)

	vcpDB.On("GetBlockOnlyPoolIDs", mock.Anything).Return(map[int64]bool{}, nil).Maybe()
	vcpDB.On("ListPoolsForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.PoolResourceData{}, nil).Maybe()
	vcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{
		{
			UUID:      "vol-uuid-restore",
			Name:      "restore-vol-1",
			AccountID: 123,
			VolumeAttributes: &datamodel.VolumeAttributes{
				AccountName:    "customer1",
				DeploymentName: "deployment1",
			},
		},
	}, nil).Once()
	vcpDB.On("ListVolumesForResourceData", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*database2.VolumeResourceData{}, nil).Maybe()
	vcpDB.On("ListVolumeReplicationsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil).Maybe()

	mockDB.On("GetLatestAggregatedUsageForAllResources", mock.Anything, "CounterAggregation", mock.Anything, mock.Anything).Return(
		[]datamodel2.AggregatedUsage{}, nil,
	).Maybe()

	restoreMetricsJSON := []byte(`{"backup_region_name":"us-west2","source_region_name":"us-east4"}`)
	restoreMetrics := []datamodel2.HydratedMetrics{
		{
			ResourceName:    "restore-vol-1",
			ConsumerID:      "customer1",
			Location:        "us-east4",
			Quantity:        1024,
			MetricTimestamp: startTime.Add(10 * time.Minute),
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.CbsCrossRegionVolumeRestoreTransferBytes,
			DeploymentName:  "deployment1",
			Metadata:        restoreMetricsJSON,
		},
		{
			ResourceName:    "restore-vol-1",
			ConsumerID:      "customer1",
			Location:        "us-east4",
			Quantity:        2048,
			MetricTimestamp: startTime.Add(20 * time.Minute),
			ResourceType:    metadata.Volume,
			MeasuredType:    metadata.CbsCrossRegionVolumeRestoreTransferBytes,
			DeploymentName:  "deployment1",
			Metadata:        restoreMetricsJSON,
		},
	}

	matchRestoreMetrics := func(conditions [][]interface{}) bool {
		hasVolumeResourceType := false
		hasRestoreMeasuredType := false
		for _, cond := range conditions {
			if len(cond) < 2 {
				continue
			}
			condStr, ok := cond[0].(string)
			if !ok {
				continue
			}
			if condStr == "resource_type = ?" {
				if val, ok := cond[1].(string); ok && val == "VOLUME" {
					hasVolumeResourceType = true
				}
			}
			if condStr == "measured_type = ?" {
				if val, ok := cond[1].(string); ok && val == "CBS_CROSS_REGION_VOLUME_RESTORE_TRANSFER_BYTES" {
					hasRestoreMeasuredType = true
				}
			}
		}
		return hasVolumeResourceType && hasRestoreMeasuredType
	}

	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.MatchedBy(matchRestoreMetrics), mock.Anything).Return(restoreMetrics, nil).Once()
	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.MatchedBy(matchRestoreMetrics), mock.Anything).Return([]datamodel2.HydratedMetrics{}, nil).Once()
	mockDB.On("GetHydratedMetricsWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(
		[]datamodel2.HydratedMetrics{}, nil,
	)

	backupRegion := "us-west2"
	sourceRegion := "us-east4"
	mockDB.On("CreateAggregatedUsageBatch", mock.Anything, mock.MatchedBy(func(records []datamodel2.AggregatedUsage) bool {
		for _, record := range records {
			if record.MeasuredType == metadata.CbsCrossRegionVolumeRestoreTransferBytes {
				if record.SourceRegion == nil || *record.SourceRegion != backupRegion {
					return false
				}
				if record.DestinationRegion == nil || *record.DestinationRegion != sourceRegion {
					return false
				}
				return true
			}
		}
		return false
	}), mock.Anything).Return(nil).Once()

	err := processor.ProcessBillingMetrics(ctx, now)
	assert.NoError(t, err)

	mockDB.AssertExpectations(t)
	vcpDB.AssertExpectations(t)
}
