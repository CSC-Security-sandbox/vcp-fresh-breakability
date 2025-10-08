package collector

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
)

type mockStorage struct {
	mock.Mock
	database.Storage
}

func (m *mockStorage) ListPools(ctx context.Context, filter *utils.Filter) ([]*datamodel.PoolView, error) {
	args := m.Called(ctx, filter)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*datamodel.PoolView), args.Error(1)
}

func Test_GetPoolMetrics_ReturnsMetrics(t *testing.T) {
	m := new(mockStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

	var pools []*datamodel.PoolView
	pools = append(
		pools,
		&datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "pool-uuid-1",
				},
				Name:           "Pool1",
				SizeInBytes:    1000,
				UsedBytes:      500,
				DeploymentName: "gcp-deployment-1",
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{
						UUID: "account-uuid-1",
					},
					Name: "Account1",
				},
				PoolAttributes: &datamodel.PoolAttributes{
					ThroughputMibps: 100,
				},
			},
			Throughput:   100.0,
			QuotaInBytes: 500,
		},
	)

	m.On("ListPools", mock.Anything, mock.Anything).Return(pools, nil)

	result, err := GetPoolMetrics(ctx, m, config, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetrics, 2)          // Should have 2 metrics: PoolAllocatedSize and AllocatedUsed
	assert.Len(t, result.HydratedMetricsDataModel, 2) // Should have 2 hydrated metrics (both PoolAllocatedSize and AllocatedUsed)

	// Test new PoolMetadataMap field
	assert.NotNil(t, result.PoolMetadataMap, "PoolMetadataMap should not be nil")
	assert.Len(t, result.PoolMetadataMap, 1, "PoolMetadataMap should contain one entry")

	// Check first metric (PoolAllocatedSize)
	assert.Equal(t, metadata.PoolAllocatedSize, result.HydratedMetrics[0].MeasuredType)
	assert.Equal(t, float64(1000), result.HydratedMetrics[0].Quantity)
	assert.Equal(t, "Pool1", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "us-east-1", derefString(result.HydratedMetrics[0].Metadata.RegionName))
	assert.Equal(t, "Account1", derefString(result.HydratedMetrics[0].Metadata.AccountName))

	// Check second metric (AllocatedUsed)
	assert.Equal(t, metadata.AllocatedUsed, result.HydratedMetrics[1].MeasuredType)
	assert.Equal(t, float64(500), result.HydratedMetrics[1].Quantity)

	// Check hydrated metrics data model - first hydrated metric (PoolAllocatedSize)
	assert.Equal(t, metadata.PoolAllocatedSize, result.HydratedMetricsDataModel[0].MeasuredType)
	assert.Equal(t, metadata.VolumePool, result.HydratedMetricsDataModel[0].ResourceType)
	assert.Equal(t, "Account1", result.HydratedMetricsDataModel[0].ConsumerID)
	assert.Equal(t, "Pool1", result.HydratedMetricsDataModel[0].ResourceName)
	assert.Equal(t, "us-east-1", result.HydratedMetricsDataModel[0].Location)
	assert.Equal(t, float64(1000), result.HydratedMetricsDataModel[0].Quantity)

	// Check second hydrated metric (AllocatedUsed)
	assert.Equal(t, metadata.AllocatedUsed, result.HydratedMetricsDataModel[1].MeasuredType)
	assert.Equal(t, metadata.VolumePool, result.HydratedMetricsDataModel[1].ResourceType)
	assert.Equal(t, "Account1", result.HydratedMetricsDataModel[1].ConsumerID)
	assert.Equal(t, "Pool1", result.HydratedMetricsDataModel[1].ResourceName)
	assert.Equal(t, "us-east-1", result.HydratedMetricsDataModel[1].Location)
	assert.Equal(t, float64(500), result.HydratedMetricsDataModel[1].Quantity)

	// Verify the type is correct
	assert.IsType(t, datamodel2.HydratedMetrics{}, result.HydratedMetricsDataModel[0])
	assert.IsType(t, datamodel2.HydratedMetrics{}, result.HydratedMetricsDataModel[1])
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func Test_GetPoolMetrics_MultiplePools(t *testing.T) {
	m := new(mockStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

	pools := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-1"},
				Name:           "Pool1",
				SizeInBytes:    1000,
				UsedBytes:      300,
				DeploymentName: "gcp-deployment-1",
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
					Name:      "Account1",
				},
				PoolAttributes: &datamodel.PoolAttributes{
					ThroughputMibps: 0,
				},
			},
			QuotaInBytes: 300,
		},
		{
			Pool: datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-2"},
				Name:           "Pool2",
				SizeInBytes:    2000,
				UsedBytes:      800,
				DeploymentName: "gcp-deployment-2",
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "account-uuid-2"},
					Name:      "Account2",
				},
				PoolAttributes: &datamodel.PoolAttributes{
					ThroughputMibps: 0,
				},
			},
			QuotaInBytes: 800,
		},
	}

	m.On("ListPools", mock.Anything, mock.Anything).Return(pools, nil)

	result, err := GetPoolMetrics(ctx, m, config, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetrics, 4)          // Should have 4 metrics: 2 pools * 2 metric types each
	assert.Len(t, result.HydratedMetricsDataModel, 4) // Should have 4 hydrated metrics (2 per pool for both PoolAllocatedSize and AllocatedUsed)

	// Check first pool metrics
	assert.Equal(t, metadata.PoolAllocatedSize, result.HydratedMetrics[0].MeasuredType)
	assert.Equal(t, float64(1000), result.HydratedMetrics[0].Quantity)
	assert.Equal(t, "Pool1", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "us-east-1", derefString(result.HydratedMetrics[0].Metadata.RegionName))
	assert.Equal(t, "Account1", derefString(result.HydratedMetrics[0].Metadata.AccountName))

	assert.Equal(t, metadata.AllocatedUsed, result.HydratedMetrics[1].MeasuredType)
	assert.Equal(t, float64(300), result.HydratedMetrics[1].Quantity)

	// Check second pool metrics
	assert.Equal(t, metadata.PoolAllocatedSize, result.HydratedMetrics[2].MeasuredType)
	assert.Equal(t, float64(2000), result.HydratedMetrics[2].Quantity)
	assert.Equal(t, "Pool2", derefString(result.HydratedMetrics[2].Metadata.ResourceName))
	assert.Equal(t, "Account2", derefString(result.HydratedMetrics[2].Metadata.AccountName))

	assert.Equal(t, metadata.AllocatedUsed, result.HydratedMetrics[3].MeasuredType)
	assert.Equal(t, float64(800), result.HydratedMetrics[3].Quantity)

	// Check hydrated metrics - Pool1 PoolAllocatedSize
	assert.Equal(t, "Account1", result.HydratedMetricsDataModel[0].ConsumerID)
	assert.Equal(t, "Pool1", result.HydratedMetricsDataModel[0].ResourceName)
	assert.Equal(t, metadata.PoolAllocatedSize, result.HydratedMetricsDataModel[0].MeasuredType)
	assert.Equal(t, float64(1000), result.HydratedMetricsDataModel[0].Quantity)

	// Check hydrated metrics - Pool1 AllocatedUsed
	assert.Equal(t, "Account1", result.HydratedMetricsDataModel[1].ConsumerID)
	assert.Equal(t, "Pool1", result.HydratedMetricsDataModel[1].ResourceName)
	assert.Equal(t, metadata.AllocatedUsed, result.HydratedMetricsDataModel[1].MeasuredType)
	assert.Equal(t, float64(300), result.HydratedMetricsDataModel[1].Quantity)

	// Check hydrated metrics - Pool2 PoolAllocatedSize
	assert.Equal(t, "Account2", result.HydratedMetricsDataModel[2].ConsumerID)
	assert.Equal(t, "Pool2", result.HydratedMetricsDataModel[2].ResourceName)
	assert.Equal(t, metadata.PoolAllocatedSize, result.HydratedMetricsDataModel[2].MeasuredType)
	assert.Equal(t, float64(2000), result.HydratedMetricsDataModel[2].Quantity)

	// Check hydrated metrics - Pool2 AllocatedUsed
	assert.Equal(t, "Account2", result.HydratedMetricsDataModel[3].ConsumerID)
	assert.Equal(t, "Pool2", result.HydratedMetricsDataModel[3].ResourceName)
	assert.Equal(t, metadata.AllocatedUsed, result.HydratedMetricsDataModel[3].MeasuredType)
	assert.Equal(t, float64(800), result.HydratedMetricsDataModel[3].Quantity)
}

func Test_GetPoolMetrics_EmptyPools(t *testing.T) {
	m := new(mockStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	m.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	result, err := GetPoolMetrics(ctx, m, config, time.Now())
	assert.Error(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetrics)
	assert.Empty(t, result.HydratedMetricsDataModel)
}

func Test_GetPoolMetrics_ListPoolsError(t *testing.T) {
	m := new(mockStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	m.On("ListPools", mock.Anything, mock.Anything).Return(nil, assert.AnError)

	result, err := GetPoolMetrics(ctx, m, config, time.Now())
	assert.Error(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetrics)
	assert.Empty(t, result.HydratedMetricsDataModel)
}

// Test for the new setupHydratedMetricsDataModel function
func TestSetupHydratedMetricsDataModel(t *testing.T) {
	// Create test metadata
	resourceMetadata := metadata.ResourceMetadata{}
	resourceName := "test-pool"
	regionName := "us-west-2"
	deploymentName := "test-deployment"
	sizeInBytes := int64(2048)

	resourceMetadata.SetResourceName(resourceName)
	resourceMetadata.SetRegionName(regionName)
	resourceMetadata.SetDeploymentName(deploymentName)
	resourceMetadata.SetSizeInBytes(sizeInBytes)

	// Test parameters
	measuredType := metadata.PoolAllocatedSize
	resourceType := metadata.VolumePool
	projectID := "test-project-123"
	timestamp := time.Now()

	// Call the function
	result := setupHydratedMetricsDataModel(measuredType, resourceType, projectID, resourceMetadata, timestamp, float64(sizeInBytes))

	// Assertions
	assert.Equal(t, timestamp, result.MetricTimestamp)
	assert.Equal(t, measuredType, result.MeasuredType)
	assert.Equal(t, projectID, result.ConsumerID)
	assert.Equal(t, resourceType, result.ResourceType)
	assert.Equal(t, resourceName, result.ResourceName)
	assert.Equal(t, regionName, result.Location)
	assert.Equal(t, float64(sizeInBytes), result.Quantity)
}

func TestSetupHydratedMetricsDataModel_DifferentMetricTypes(t *testing.T) {
	tests := []struct {
		name         string
		measuredType metadata.MeasuredType
		resourceType metadata.ResourceType
		projectID    string
	}{
		{
			name:         "PoolAllocatedSize metric",
			measuredType: metadata.PoolAllocatedSize,
			resourceType: metadata.VolumePool,
			projectID:    "project-1",
		},
		{
			name:         "AllocatedUsed metric",
			measuredType: metadata.AllocatedUsed,
			resourceType: metadata.VolumePool,
			projectID:    "project-2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test metadata
			resourceMetadata := metadata.ResourceMetadata{}
			resourceName := "test-pool-" + tt.projectID
			regionName := "eu-central-1"
			deploymentName := "test-deployment-" + tt.projectID
			sizeInBytes := int64(4096)

			resourceMetadata.SetResourceName(resourceName)
			resourceMetadata.SetRegionName(regionName)
			resourceMetadata.SetDeploymentName(deploymentName)
			resourceMetadata.SetSizeInBytes(sizeInBytes)

			timestamp := time.Now()

			// Call the function
			result := setupHydratedMetricsDataModel(tt.measuredType, tt.resourceType, tt.projectID, resourceMetadata, timestamp, float64(sizeInBytes))

			// Assertions
			assert.Equal(t, timestamp, result.MetricTimestamp)
			assert.Equal(t, tt.measuredType, result.MeasuredType)
			assert.Equal(t, tt.projectID, result.ConsumerID)
			assert.Equal(t, tt.resourceType, result.ResourceType)
			assert.Equal(t, resourceName, result.ResourceName)
			assert.Equal(t, regionName, result.Location)
			assert.Equal(t, float64(sizeInBytes), result.Quantity)
		})
	}
}

// Test that verifies the integration between GetPoolMetrics and setupHydratedMetricsDataModel
func TestGetPoolMetrics_HydratedMetricsDataModelIntegration(t *testing.T) {
	m := new(mockStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "ap-south-1"}

	pools := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-integration"},
				Name:           "IntegrationPool",
				SizeInBytes:    5000,
				UsedBytes:      1500,
				DeploymentName: "gcp-integration-deployment",
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "account-uuid-integration"},
					Name:      "IntegrationAccount",
				},
				PoolAttributes: &datamodel.PoolAttributes{
					ThroughputMibps: 0,
				},
			},
			QuotaInBytes: 1500,
		},
	}

	m.On("ListPools", mock.Anything, mock.Anything).Return(pools, nil)

	result, err := GetPoolMetrics(ctx, m, config, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify that both PoolAllocatedSize and AllocatedUsed metrics are converted to HydratedMetrics
	assert.Len(t, result.HydratedMetricsDataModel, 2)

	// Find the PoolAllocatedSize metric in the metrics slice
	var poolAllocatedSizeMetric *entity.HydratedMetric
	for i := range result.HydratedMetrics {
		if result.HydratedMetrics[i].MeasuredType == metadata.PoolAllocatedSize {
			poolAllocatedSizeMetric = &result.HydratedMetrics[i]
			break
		}
	}
	assert.NotNil(t, poolAllocatedSizeMetric)

	// Verify the HydratedMetrics data model is correctly populated - PoolAllocatedSize
	hmPoolAllocated := result.HydratedMetricsDataModel[0]
	assert.Equal(t, metadata.PoolAllocatedSize, hmPoolAllocated.MeasuredType)
	assert.Equal(t, metadata.VolumePool, hmPoolAllocated.ResourceType)
	assert.Equal(t, "IntegrationAccount", hmPoolAllocated.ConsumerID)
	assert.Equal(t, "IntegrationPool", hmPoolAllocated.ResourceName)
	assert.Equal(t, "ap-south-1", hmPoolAllocated.Location)
	assert.Equal(t, float64(5000), hmPoolAllocated.Quantity)

	// Verify the HydratedMetrics data model is correctly populated - AllocatedUsed
	hmAllocatedUsed := result.HydratedMetricsDataModel[1]
	assert.Equal(t, metadata.AllocatedUsed, hmAllocatedUsed.MeasuredType)
	assert.Equal(t, metadata.VolumePool, hmAllocatedUsed.ResourceType)
	assert.Equal(t, "IntegrationAccount", hmAllocatedUsed.ConsumerID)
	assert.Equal(t, "IntegrationPool", hmAllocatedUsed.ResourceName)
	assert.Equal(t, "ap-south-1", hmAllocatedUsed.Location)
	assert.Equal(t, float64(1500), hmAllocatedUsed.Quantity)

	// Verify timestamp is recent (within last minute)
	timeDiff := time.Since(hmPoolAllocated.MetricTimestamp)
	assert.True(t, timeDiff < time.Minute, "Timestamp should be recent")
	timeDiff2 := time.Since(hmAllocatedUsed.MetricTimestamp)
	assert.True(t, timeDiff2 < time.Minute, "Timestamp should be recent")
}

// Test for new throughput functionality and PoolMetadataMap
func Test_GetPoolMetrics_WithThroughputAndMetadataMap(t *testing.T) {
	m := new(mockStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

	var pools []*datamodel.PoolView
	pools = append(
		pools,
		&datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{
					ID:   42,
					UUID: "pool-uuid-throughput",
				},
				Name:           "ThroughputPool",
				SizeInBytes:    2048,
				UsedBytes:      1024,
				DeploymentName: "gcp-deployment-throughput",
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{
						UUID: "account-uuid-throughput",
					},
					Name: "ThroughputAccount",
				},
				PoolAttributes: &datamodel.PoolAttributes{
					ThroughputMibps: 150,
				},
			},
			Throughput:   150.5,
			QuotaInBytes: 1024,
		},
	)

	m.On("ListPools", mock.Anything, mock.Anything).Return(pools, nil)

	result, err := GetPoolMetrics(ctx, m, config, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Test new PoolMetadataMap field
	assert.NotNil(t, result.PoolMetadataMap, "PoolMetadataMap should not be nil")
	assert.Len(t, result.PoolMetadataMap, 1, "PoolMetadataMap should contain one entry")

	poolMetadata, exists := result.PoolMetadataMap[42]
	assert.True(t, exists, "Pool metadata should exist for pool ID 42")

	// Test throughput metadata
	assert.Equal(t, 150.0, *poolMetadata.Throughput, "Throughput should be set correctly")
	assert.Equal(t, int64(42), *poolMetadata.ResourceID, "ResourceID should be set correctly")
	assert.Equal(t, "pool-uuid-throughput", *poolMetadata.ResourceUUID, "ResourceUUID should match")
	assert.Equal(t, "ThroughputPool", *poolMetadata.ResourceName, "ResourceName should match")
	assert.Equal(t, "gcp-deployment-throughput", *poolMetadata.DeploymentName, "DeploymentName should match")
	assert.Equal(t, metadata.VolumePool, poolMetadata.ResourceType, "ResourceType should be VolumePool")
	assert.Equal(t, "us-east-1", *poolMetadata.RegionName, "RegionName should match config")
	assert.Equal(t, "ThroughputAccount", *poolMetadata.AccountName, "AccountName should match")
	assert.Equal(t, int64(2048), *poolMetadata.SizeInBytes, "SizeInBytes should match")
}

func Test_GetPoolMetrics_MultiplePoolsWithDifferentThroughput(t *testing.T) {
	m := new(mockStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-west-2"}

	pools := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 100, UUID: "pool-uuid-1"},
				Name:           "Pool1",
				SizeInBytes:    1000,
				UsedBytes:      300,
				DeploymentName: "gcp-deployment-1",
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
					Name:      "Account1",
				},
				PoolAttributes: &datamodel.PoolAttributes{
					ThroughputMibps: 200,
				},
			},
			Throughput:   200.0,
			QuotaInBytes: 300,
		},
		{
			Pool: datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 200, UUID: "pool-uuid-2"},
				Name:           "Pool2",
				SizeInBytes:    2000,
				UsedBytes:      800,
				DeploymentName: "gcp-deployment-2",
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "account-uuid-2"},
					Name:      "Account2",
				},
				PoolAttributes: &datamodel.PoolAttributes{
					ThroughputMibps: 350,
				},
			},
			Throughput:   350.5,
			QuotaInBytes: 800,
		},
	}

	m.On("ListPools", mock.Anything, mock.Anything).Return(pools, nil)

	result, err := GetPoolMetrics(ctx, m, config, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Test PoolMetadataMap contains both pools
	assert.Len(t, result.PoolMetadataMap, 2, "PoolMetadataMap should contain two entries")

	// Test first pool metadata
	pool1Metadata, exists := result.PoolMetadataMap[100]
	assert.True(t, exists, "Pool metadata should exist for pool ID 100")
	assert.Equal(t, 200.0, *pool1Metadata.Throughput, "Pool1 throughput should be 200.0")
	assert.Equal(t, int64(100), *pool1Metadata.ResourceID, "Pool1 ResourceID should be 100")

	// Test second pool metadata
	pool2Metadata, exists := result.PoolMetadataMap[200]
	assert.True(t, exists, "Pool metadata should exist for pool ID 200")
	assert.Equal(t, 350.0, *pool2Metadata.Throughput, "Pool2 throughput should be 350.0")
	assert.Equal(t, int64(200), *pool2Metadata.ResourceID, "Pool2 ResourceID should be 200")
}

func Test_GetPoolMetrics_ZeroThroughput(t *testing.T) {
	m := new(mockStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

	var pools []*datamodel.PoolView
	pools = append(
		pools,
		&datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "pool-uuid-zero-throughput",
				},
				Name:           "ZeroThroughputPool",
				SizeInBytes:    1000,
				UsedBytes:      500,
				DeploymentName: "gcp-deployment-zero",
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{
						UUID: "account-uuid-zero",
					},
					Name: "ZeroAccount",
				},
				PoolAttributes: &datamodel.PoolAttributes{
					ThroughputMibps: 0,
				},
			},
			Throughput:   0.0, // Zero throughput
			QuotaInBytes: 500,
		},
	)

	m.On("ListPools", mock.Anything, mock.Anything).Return(pools, nil)

	result, err := GetPoolMetrics(ctx, m, config, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Test PoolMetadataMap with zero throughput
	poolMetadata, exists := result.PoolMetadataMap[1]
	assert.True(t, exists, "Pool metadata should exist for pool ID 1")
	assert.Equal(t, 0.0, *poolMetadata.Throughput, "Throughput should be 0.0")
	assert.Equal(t, int64(1), *poolMetadata.ResourceID, "ResourceID should be set")
}

// Test pool metadata includes throughput and resource ID
func Test_GetPoolMetrics_IncludesThroughputAndResourceID(t *testing.T) {
	m := new(mockStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-west-2"}

	throughput := 500.75
	var pools []*datamodel.PoolView
	pools = append(
		pools,
		&datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{
					ID:   42,
					UUID: "pool-uuid-throughput",
				},
				Name:           "ThroughputPool",
				SizeInBytes:    5000,
				UsedBytes:      2500,
				DeploymentName: "throughput-deployment",
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{
						UUID: "account-uuid-throughput",
					},
					Name: "ThroughputAccount",
				},
				PoolAttributes: &datamodel.PoolAttributes{
					ThroughputMibps: 500,
				},
			},
			Throughput:   throughput,
			QuotaInBytes: 2500,
		},
	)

	m.On("ListPools", mock.Anything, mock.Anything).Return(pools, nil)

	result, err := GetPoolMetrics(ctx, m, config, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetrics, 2)          // Should have 2 metrics: PoolAllocatedSize and AllocatedUsed
	assert.Len(t, result.HydratedMetricsDataModel, 2) // Should have 2 hydrated metrics

	// Test new PoolMetadataMap functionality
	assert.NotNil(t, result.PoolMetadataMap, "PoolMetadataMap should not be nil")
	assert.Len(t, result.PoolMetadataMap, 1, "PoolMetadataMap should contain one entry")

	// Verify the pool metadata contains throughput and resource ID
	poolMetadata, exists := result.PoolMetadataMap[42]
	assert.True(t, exists, "Pool metadata should exist for pool ID 42")
	assert.NotNil(t, poolMetadata.Throughput, "Pool throughput should be set")
	assert.Equal(t, 500.0, *poolMetadata.Throughput, "Pool throughput should match")
	assert.NotNil(t, poolMetadata.ResourceID, "Pool resource ID should be set")
	assert.Equal(t, int64(42), *poolMetadata.ResourceID, "Pool resource ID should match")

	// Verify other metadata fields
	assert.Equal(t, "pool-uuid-throughput", derefString(poolMetadata.ResourceUUID))
	assert.Equal(t, "ThroughputPool", derefString(poolMetadata.ResourceName))
	assert.Equal(t, "ThroughputAccount", derefString(poolMetadata.AccountName))
	assert.Equal(t, metadata.VolumePool, poolMetadata.ResourceType)

	// Check metric metadata also has throughput and resource ID
	assert.NotNil(t, result.HydratedMetrics[0].Metadata.Throughput)
	assert.Equal(t, 500.0, *result.HydratedMetrics[0].Metadata.Throughput)
	assert.NotNil(t, result.HydratedMetrics[0].Metadata.ResourceID)
	assert.Equal(t, int64(42), *result.HydratedMetrics[0].Metadata.ResourceID)
}

// Test assemblePoolMetadata function with throughput and resource ID
func Test_AssemblePoolMetadata_WithThroughputAndResourceID(t *testing.T) {
	config := &common.TelemetryConfig{RegionName: "eu-west-1"}
	throughput := 1000.5

	pool := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   123,
				UUID: "test-pool-uuid",
			},
			Name:           "TestPool",
			SizeInBytes:    8192,
			DeploymentName: "test-deployment",
			Account: &datamodel.Account{
				Name: "TestAccount",
			},
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 1000,
			},
		},
		Throughput: throughput,
	}

	result := assemblePoolMetadata(pool, config)

	// Verify all metadata fields
	assert.Equal(t, "test-pool-uuid", derefString(result.ResourceUUID))
	assert.Equal(t, "TestPool", derefString(result.ResourceName))
	assert.Equal(t, "TestPool", derefString(result.ResourceDisplayName))
	assert.Equal(t, metadata.VolumePool, result.ResourceType)
	assert.Equal(t, int64(8192), derefInt64(result.SizeInBytes))
	assert.Equal(t, "eu-west-1", derefString(result.RegionName))
	assert.Equal(t, "TestAccount", derefString(result.AccountName))
	assert.Equal(t, "test-deployment", derefString(result.DeploymentName))

	// Verify new fields
	assert.NotNil(t, result.Throughput, "Throughput should be set")
	assert.Equal(t, 1000.0, *result.Throughput, "Throughput should match")
	assert.NotNil(t, result.ResourceID, "ResourceID should be set")
	assert.Equal(t, int64(123), *result.ResourceID, "ResourceID should match")
}

func Test_GetPoolMetrics_NilPoolAttributes(t *testing.T) {
	m := new(mockStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

	// Pool with nil PoolAttributes
	pools := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 123, UUID: "pool-uuid-1"},
				Name:           "Pool1",
				SizeInBytes:    1000,
				DeploymentName: "test-deployment",
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{ID: 1},
					Name:      "Account1",
				},
				PoolAttributes: nil, // This should trigger the skip condition
			},
			QuotaInBytes: 500,
		},
	}

	m.On("ListPools", mock.Anything, mock.Anything).Return(pools, nil)

	result, err := GetPoolMetrics(ctx, m, config, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Should be empty since pool was skipped due to nil PoolAttributes
	assert.Len(t, result.HydratedMetrics, 0)
	assert.Len(t, result.HydratedMetricsDataModel, 0)
	assert.Len(t, result.PoolMetadataMap, 0)

	m.AssertExpectations(t)
}
