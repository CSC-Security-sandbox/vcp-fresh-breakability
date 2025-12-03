package collector

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
)

type mockVolumeStorage struct {
	mock.Mock
	database.Storage
}

func (m *mockVolumeStorage) ListVolumesWithAccounts(ctx context.Context) ([]*datamodel.Volume, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*datamodel.Volume), args.Error(1)
}

func (m *mockVolumeStorage) GetSfrMetricsByTimeRange(ctx context.Context, startTime, endTime time.Time) (map[string]datamodel.SfrMetricsAggregate, error) {
	args := m.Called(ctx, startTime, endTime)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]datamodel.SfrMetricsAggregate), args.Error(1)
}

func Test_GetVolumeMetrics_ReturnsMetrics(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

	// Create poolMetadataMap for testing
	poolMetadataMap := make(map[int64]metadata.ResourceMetadata)
	poolMetadataMap[1] = metadata.ResourceMetadata{
		ResourceType: metadata.Volume,
	}

	backupChainBytes := int64(1024)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-1"},
			Name:        "Volume1",
			SizeInBytes: 2048,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
				Name:      "Account1",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-1"},
				DeploymentName: "test-deployment",
			},
			PoolID: 1,
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	config.EnableBackupBillingMetrics = true // Enable backup billing metrics for test

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// VolumeAllocatedThroughput should be in separate field
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)
	// BackupEnabledVolumeAllocatedSize should be in HydratedMetrics when EnableBackupBillingMetrics is true
	assert.Len(t, result.HydratedMetrics, 1)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check BackupEnabledVolumeAllocatedSize metric (in regular field)
	assert.Equal(t, metadata.BackupEnabledVolumeAllocatedSize, result.HydratedMetrics[0].MeasuredType)
	assert.Equal(t, float64(2048), result.HydratedMetrics[0].Quantity)
	assert.Equal(t, "volume-uuid-1", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, metadata.Volume, result.HydratedMetrics[0].Metadata.ResourceType)
	assert.Equal(t, "Volume1", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "us-east-1", derefString(result.HydratedMetrics[0].Metadata.RegionName))
	assert.Equal(t, "Account1", derefString(result.HydratedMetrics[0].Metadata.AccountName))

	// Check hydrated metrics data model
	assert.Equal(t, metadata.BackupEnabledVolumeAllocatedSize, result.HydratedMetricsDataModel[0].MeasuredType)
	assert.Equal(t, metadata.Volume, result.HydratedMetricsDataModel[0].ResourceType)
	assert.Equal(t, "Account1", result.HydratedMetricsDataModel[0].ConsumerID)
	assert.Equal(t, "Volume1", result.HydratedMetricsDataModel[0].ResourceName)
	assert.Equal(t, "us-east-1", result.HydratedMetricsDataModel[0].Location)
	assert.Equal(t, float64(2048), result.HydratedMetricsDataModel[0].Quantity)

	// Verify the type is correct
	assert.IsType(t, datamodel2.HydratedMetrics{}, result.HydratedMetricsDataModel[0])
}

func Test_GetVolumeMetrics_MultipleVolumes(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

	// Create poolMetadataMap for testing
	poolMetadataMap := make(map[int64]metadata.ResourceMetadata)

	backupChainBytes1 := int64(1024)
	backupChainBytes2 := int64(2048)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-1"},
			Name:        "Volume1",
			SizeInBytes: 2048,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
				Name:      "Account1",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-1"},
				DeploymentName: "test-deployment-1",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes1,
			},
		},
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-2"},
			Name:        "Volume2",
			SizeInBytes: 4096,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-2"},
				Name:      "Account2",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-2"},
				DeploymentName: "test-deployment-2",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes2,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	config.EnableBackupBillingMetrics = true // Enable backup billing metrics for test

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// VolumeAllocatedThroughput should be in separate field (2 volumes)
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 0)
	// BackupEnabledVolumeAllocatedSize should be in HydratedMetrics when EnableBackupBillingMetrics is true (2 volumes)
	assert.Len(t, result.HydratedMetrics, 2)
	assert.Len(t, result.HydratedMetricsDataModel, 2)

	// Check first volume BackupEnabledVolumeAllocatedSize metric (in regular field)
	assert.Equal(t, metadata.BackupEnabledVolumeAllocatedSize, result.HydratedMetrics[0].MeasuredType)
	assert.Equal(t, float64(2048), result.HydratedMetrics[0].Quantity)
	assert.Equal(t, "volume-uuid-1", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, "Volume1", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "Account1", derefString(result.HydratedMetrics[0].Metadata.AccountName))

	// Check second volume BackupEnabledVolumeAllocatedSize metric (in regular field)
	assert.Equal(t, metadata.BackupEnabledVolumeAllocatedSize, result.HydratedMetrics[1].MeasuredType)
	assert.Equal(t, float64(4096), result.HydratedMetrics[1].Quantity)
	assert.Equal(t, "volume-uuid-2", derefString(result.HydratedMetrics[1].Metadata.ResourceUUID))
	assert.Equal(t, "Volume2", derefString(result.HydratedMetrics[1].Metadata.ResourceName))
	assert.Equal(t, "Account2", derefString(result.HydratedMetrics[1].Metadata.AccountName))

	// Check hydrated metrics - Volume1
	assert.Equal(t, "Account1", result.HydratedMetricsDataModel[0].ConsumerID)
	assert.Equal(t, "Volume1", result.HydratedMetricsDataModel[0].ResourceName)
	assert.Equal(t, metadata.BackupEnabledVolumeAllocatedSize, result.HydratedMetricsDataModel[0].MeasuredType)
	assert.Equal(t, float64(2048), result.HydratedMetricsDataModel[0].Quantity)

	// Check hydrated metrics - Volume2
	assert.Equal(t, "Account2", result.HydratedMetricsDataModel[1].ConsumerID)
	assert.Equal(t, "Volume2", result.HydratedMetricsDataModel[1].ResourceName)
	assert.Equal(t, metadata.BackupEnabledVolumeAllocatedSize, result.HydratedMetricsDataModel[1].MeasuredType)
	assert.Equal(t, float64(4096), result.HydratedMetricsDataModel[1].Quantity)
}

func Test_GetVolumeMetrics_EmptyVolumes(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	poolMetadataMap := make(map[int64]metadata.ResourceMetadata)
	m.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetrics)
	assert.Empty(t, result.HydratedMetricsDataModel)
}

func Test_GetVolumeMetrics_ListVolumesWithAccountsError(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	poolMetadataMap := make(map[int64]metadata.ResourceMetadata)
	m.On("ListVolumesWithAccounts", mock.Anything).Return(nil, assert.AnError)

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.Error(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetrics)
	assert.Empty(t, result.HydratedMetricsDataModel)
}

func Test_GetVolumeMetrics_FiltersVolumesWithZeroBackupChainBytes(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	poolMetadataMap := make(map[int64]metadata.ResourceMetadata)

	zeroBackupChainBytes := int64(0)
	positiveBackupChainBytes := int64(1024)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-1"},
			Name:        "Volume1",
			SizeInBytes: 2048,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
				Name:      "Account1",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-1"},
				DeploymentName: "test-deployment-1",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &zeroBackupChainBytes, // Should be filtered out
			},
		},
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-2"},
			Name:        "Volume2",
			SizeInBytes: 4096,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-2"},
				Name:      "Account2",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-2"},
				DeploymentName: "test-deployment-2",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &positiveBackupChainBytes, // Should be included
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	config.EnableBackupBillingMetrics = true // Enable backup billing metrics for test

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// VolumeAllocatedThroughput metrics should be generated for both volumes (2 total)
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 0)

	// Only one volume should be processed for BackupEnabledVolumeAllocatedSize (the one with positive backup chain bytes)
	assert.Len(t, result.HydratedMetrics, 1)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check that only the second volume is processed for backup billing
	assert.Equal(t, "volume-uuid-2", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, "Volume2", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "Account2", derefString(result.HydratedMetrics[0].Metadata.AccountName))
	assert.Equal(t, float64(4096), result.HydratedMetrics[0].Quantity)
}

func Test_GetVolumeMetrics_ProcessesVolumesWithNilDataProtection(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	poolMetadataMap := make(map[int64]metadata.ResourceMetadata)

	positiveBackupChainBytes := int64(1024)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-1"},
			Name:        "Volume1",
			SizeInBytes: 2048,
			Throughput:  100, // Add throughput so VolumeAllocatedThroughput metric is generated
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
				Name:      "Account1",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-1"},
				DeploymentName: "test-deployment-1",
			},
			DataProtection: nil, // Should be processed (not filtered out)
		},
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-2"},
			Name:        "Volume2",
			SizeInBytes: 4096,
			Throughput:  200, // Add throughput so VolumeAllocatedThroughput metric is generated
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-2"},
				Name:      "Account2",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-2"},
				DeploymentName: "test-deployment-2",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &positiveBackupChainBytes, // Should be included
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	config.EnableBackupBillingMetrics = true // Enable backup billing metrics for test

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Both volumes should generate VolumeAllocatedThroughput metrics
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 2)

	// Both volumes should be processed for backup billing (nil DataProtection is not filtered out)
	assert.Len(t, result.HydratedMetrics, 2)
	assert.Len(t, result.HydratedMetricsDataModel, 2)

	// Check first volume (with nil DataProtection)
	assert.Equal(t, "volume-uuid-1", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, "Volume1", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "Account1", derefString(result.HydratedMetrics[0].Metadata.AccountName))
	assert.Equal(t, float64(2048), result.HydratedMetrics[0].Quantity)

	// Check second volume (with valid DataProtection)
	assert.Equal(t, "volume-uuid-2", derefString(result.HydratedMetrics[1].Metadata.ResourceUUID))
	assert.Equal(t, "Volume2", derefString(result.HydratedMetrics[1].Metadata.ResourceName))
	assert.Equal(t, "Account2", derefString(result.HydratedMetrics[1].Metadata.AccountName))
	assert.Equal(t, float64(4096), result.HydratedMetrics[1].Quantity)
}

func Test_GetVolumeMetrics_FiltersVolumesWithNilAccount(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	poolMetadataMap := make(map[int64]metadata.ResourceMetadata)

	backupChainBytes := int64(1024)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-1"},
			Name:        "Volume1",
			SizeInBytes: 2048,
			Throughput:  100, // Won't matter since account is nil, but added for consistency
			Account:     nil, // Nil account should be filtered out
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-1"},
				DeploymentName: "test-deployment-1",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-2"},
			Name:        "Volume2",
			SizeInBytes: 4096,
			Throughput:  200, // Add throughput so VolumeAllocatedThroughput metric is generated
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-2"},
				Name:      "Account2",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-2"},
				DeploymentName: "test-deployment-2",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	config.EnableBackupBillingMetrics = true // Enable backup billing metrics for test

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Only one volume should generate VolumeAllocatedThroughput (the one with valid account)
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)

	// Only one volume should be processed (the one with valid account)
	assert.Len(t, result.HydratedMetrics, 1)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check that only the second volume is processed
	assert.Equal(t, "volume-uuid-2", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, "Volume2", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "Account2", derefString(result.HydratedMetrics[0].Metadata.AccountName))
	assert.Equal(t, float64(4096), result.HydratedMetrics[0].Quantity)
}

func Test_GetVolumeMetrics_FiltersVolumesWithMissingUUID(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	poolMetadataMap := make(map[int64]metadata.ResourceMetadata)

	backupChainBytes := int64(1024)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: ""}, // Missing UUID
			Name:        "Volume1",
			SizeInBytes: 2048,
			Throughput:  100, // Won't matter since UUID is missing, but added for consistency
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
				Name:      "Account1",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-1"},
				DeploymentName: "test-deployment-1",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-2"},
			Name:        "Volume2",
			SizeInBytes: 4096,
			Throughput:  200, // Add throughput so VolumeAllocatedThroughput metric is generated
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-2"},
				Name:      "Account2",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-2"},
				DeploymentName: "test-deployment-2",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	config.EnableBackupBillingMetrics = true // Enable backup billing metrics for test

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Only one volume should generate VolumeAllocatedThroughput (the one with valid UUID)
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)

	// Only one volume should be processed (the one with valid UUID)
	assert.Len(t, result.HydratedMetrics, 1)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check that only the second volume is processed
	assert.Equal(t, "volume-uuid-2", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, "Volume2", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "Account2", derefString(result.HydratedMetrics[0].Metadata.AccountName))
	assert.Equal(t, float64(4096), result.HydratedMetrics[0].Quantity)
}

func Test_GetVolumeMetrics_FiltersVolumesWithMissingName(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	poolMetadataMap := make(map[int64]metadata.ResourceMetadata)

	backupChainBytes := int64(1024)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-1"},
			Name:        "", // Missing name
			SizeInBytes: 2048,
			Throughput:  100, // Won't matter since name is missing, but added for consistency
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
				Name:      "Account1",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-1"},
				DeploymentName: "test-deployment-1",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-2"},
			Name:        "Volume2",
			SizeInBytes: 4096,
			Throughput:  200, // Add throughput so VolumeAllocatedThroughput metric is generated
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-2"},
				Name:      "Account2",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-2"},
				DeploymentName: "test-deployment-2",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	config.EnableBackupBillingMetrics = true // Enable backup billing metrics for test

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Only one volume should generate VolumeAllocatedThroughput (the one with valid name)
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)

	// Only one volume should be processed (the one with valid name)
	assert.Len(t, result.HydratedMetrics, 1)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check that only the second volume is processed
	assert.Equal(t, "volume-uuid-2", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, "Volume2", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "Account2", derefString(result.HydratedMetrics[0].Metadata.AccountName))
	assert.Equal(t, float64(4096), result.HydratedMetrics[0].Quantity)
}

func Test_GetVolumeMetrics_FiltersVolumesWithMissingAccountName(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	poolMetadataMap := make(map[int64]metadata.ResourceMetadata)

	backupChainBytes := int64(1024)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-1"},
			Name:        "Volume1",
			SizeInBytes: 2048,
			Throughput:  100, // Won't matter since account name is missing, but added for consistency
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
				Name:      "", // Missing account name
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-1"},
				DeploymentName: "test-deployment-1",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-2"},
			Name:        "Volume2",
			SizeInBytes: 4096,
			Throughput:  200, // Add throughput so VolumeAllocatedThroughput metric is generated
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-2"},
				Name:      "Account2",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-2"},
				DeploymentName: "test-deployment-2",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	config.EnableBackupBillingMetrics = true // Enable backup billing metrics for test

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Only one volume should generate VolumeAllocatedThroughput (the one with valid account name)
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)

	// Only one volume should be processed (the one with valid account name)
	assert.Len(t, result.HydratedMetrics, 1)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check that only the second volume is processed
	assert.Equal(t, "volume-uuid-2", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, "Volume2", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "Account2", derefString(result.HydratedMetrics[0].Metadata.AccountName))
	assert.Equal(t, float64(4096), result.HydratedMetrics[0].Quantity)
}

// Test for the assembleVolumeMetadata function
func TestAssembleVolumeMetadata(t *testing.T) {
	// Create test volume
	backupChainBytes := int64(1024)
	volume := &datamodel.Volume{
		BaseModel:   datamodel.BaseModel{UUID: "test-volume-uuid"},
		Name:        "test-volume",
		SizeInBytes: 2048,
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test-account",
		},
		Pool: &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
			DeploymentName: "test-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupChainBytes: &backupChainBytes,
		},
	}

	// Create test config
	config := &common.TelemetryConfig{
		RegionName: "us-central1",
	}

	// Call the function
	resourceMetadata := assembleVolumeMetadata(volume, config)

	// Assertions
	assert.Equal(t, "test-volume-uuid", derefString(resourceMetadata.ResourceUUID))
	assert.Equal(t, metadata.Volume, resourceMetadata.ResourceType)
	assert.Equal(t, int64(2048), derefInt64(resourceMetadata.SizeInBytes))
	assert.Equal(t, "us-central1", derefString(resourceMetadata.RegionName))
	assert.Equal(t, "test-volume", derefString(resourceMetadata.ResourceName))
	assert.Equal(t, "test-volume", derefString(resourceMetadata.ResourceDisplayName))
	assert.Equal(t, "test-account", derefString(resourceMetadata.AccountName))
}

// Test that verifies the integration between GetVolumeMetrics and setupHydratedMetricsDataModel
func TestGetVolumeMetrics_HydratedMetricsDataModelIntegration(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "ap-south-1"}
	poolMetadataMap := make(map[int64]metadata.ResourceMetadata)
	poolMetadataMap[1] = metadata.ResourceMetadata{
		ResourceType: metadata.Volume,
	}

	backupChainBytes := int64(5000)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-integration"},
			Name:        "IntegrationVolume",
			SizeInBytes: 10000,
			Throughput:  150, // Add throughput so VolumeAllocatedThroughput metric is generated
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-integration"},
				Name:      "IntegrationAccount",
			},
			PoolID: 1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-integration"},
				DeploymentName: "integration-deployment",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	config.EnableBackupBillingMetrics = true // Enable backup billing metrics for test

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify that VolumeAllocatedThroughput metric is generated
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)

	// Verify that BackupEnabledVolumeAllocatedSize metric is converted to HydratedMetrics
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Find the BackupEnabledVolumeAllocatedSize metric in the metrics slice
	var BackupEnabledVolumeAllocatedSizeMetric *entity.HydratedMetric
	for i := range result.HydratedMetrics {
		if result.HydratedMetrics[i].MeasuredType == metadata.BackupEnabledVolumeAllocatedSize {
			BackupEnabledVolumeAllocatedSizeMetric = &result.HydratedMetrics[i]
			break
		}
	}
	assert.NotNil(t, BackupEnabledVolumeAllocatedSizeMetric)

	// Verify the HydratedMetrics data model is correctly populated
	hmBackupVolumeAllocated := result.HydratedMetricsDataModel[0]
	assert.Equal(t, metadata.BackupEnabledVolumeAllocatedSize, hmBackupVolumeAllocated.MeasuredType)
	assert.Equal(t, metadata.Volume, hmBackupVolumeAllocated.ResourceType)
	assert.Equal(t, "IntegrationAccount", hmBackupVolumeAllocated.ConsumerID)
	assert.Equal(t, "IntegrationVolume", hmBackupVolumeAllocated.ResourceName)
	assert.Equal(t, "ap-south-1", hmBackupVolumeAllocated.Location)
	assert.Equal(t, float64(10000), hmBackupVolumeAllocated.Quantity)

	// Verify timestamp is recent (within last minute)
	timeDiff := time.Since(hmBackupVolumeAllocated.MetricTimestamp)
	assert.True(t, timeDiff < time.Minute, "Timestamp should be recent")
}

// Test for new VolumeAllocatedThroughput functionality
func Test_GetVolumeMetrics_WithThroughputMapping(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

	// Create poolMetadataMap with throughput data
	poolThroughput := 250.5
	poolMetadata := metadata.ResourceMetadata{}
	poolMetadata.SetThroughput(poolThroughput)
	poolMetadata.SetResourceID(int64(10))
	poolMetadataMap := map[int64]metadata.ResourceMetadata{
		10: poolMetadata,
	}

	backupChainBytes := int64(1024)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-throughput"},
			Name:        "ThroughputVolume",
			SizeInBytes: 2048,
			Throughput:  100, // Volume has its own throughput, should use volume throughput
			PoolID:      10,  // Matches the pool in metadata map
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-throughput"},
				Name:      "ThroughputAccount",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-throughput"},
				DeploymentName: "throughput-deployment",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	config.EnableBackupBillingMetrics = true // Enable backup billing metrics for test

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// VolumeAllocatedThroughput should be in separate field
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)
	// BackupEnabledVolumeAllocatedSize should be in HydratedMetrics when EnableBackupBillingMetrics is true
	assert.Len(t, result.HydratedMetrics, 1)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check VolumeAllocatedThroughput metric (in separate field)
	assert.Equal(t, metadata.VolumeAllocatedThroughput, result.VolumeAllocatedThroughputHydratedMetrics[0].MeasuredType)
	assert.Equal(t, float64(100), result.VolumeAllocatedThroughputHydratedMetrics[0].Quantity) // Should use volume throughput when volume.Throughput != 0
	assert.Equal(t, "volume-uuid-throughput", derefString(result.VolumeAllocatedThroughputHydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, "ThroughputVolume", derefString(result.VolumeAllocatedThroughputHydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "ThroughputAccount", derefString(result.VolumeAllocatedThroughputHydratedMetrics[0].Metadata.AccountName))
	assert.Equal(t, metadata.Volume, result.VolumeAllocatedThroughputHydratedMetrics[0].Metadata.ResourceType)

	// Check BackupEnabledVolumeAllocatedSize metric (in regular field)
	assert.Equal(t, metadata.BackupEnabledVolumeAllocatedSize, result.HydratedMetrics[0].MeasuredType)
	assert.Equal(t, float64(2048), result.HydratedMetrics[0].Quantity)
	assert.Equal(t, "volume-uuid-throughput", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
}

func Test_GetVolumeMetrics_WithZeroVolumeThroughput(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

	// Create poolMetadataMap with pool throughput for PoolID 20
	poolMetadata := metadata.ResourceMetadata{}
	poolMetadata.SetThroughput(300.0) // Set pool throughput
	poolMetadata.SetResourceID(int64(20))
	poolMetadataMap := map[int64]metadata.ResourceMetadata{
		20: poolMetadata,
	}

	backupChainBytes := int64(1024)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-zero-throughput"},
			Name:        "ZeroThroughputVolume",
			SizeInBytes: 2048,
			Throughput:  0, // Zero throughput should use pool throughput
			PoolID:      20,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-zero"},
				Name:      "ZeroAccount",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-zero"},
				DeploymentName: "zero-deployment",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	config.EnableBackupBillingMetrics = true // Enable backup billing metrics for test

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// VolumeAllocatedThroughput should be in separate field
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)
	// BackupEnabledVolumeAllocatedSize should be in HydratedMetrics when EnableBackupBillingMetrics is true
	assert.Len(t, result.HydratedMetrics, 1)

	// Check VolumeAllocatedThroughput metric with zero volume throughput (should use pool throughput)
	assert.Equal(t, metadata.VolumeAllocatedThroughput, result.VolumeAllocatedThroughputHydratedMetrics[0].MeasuredType)
	assert.Equal(t, float64(300), result.VolumeAllocatedThroughputHydratedMetrics[0].Quantity) // Should use pool throughput
	assert.Equal(t, "volume-uuid-zero-throughput", derefString(result.VolumeAllocatedThroughputHydratedMetrics[0].Metadata.ResourceUUID))
}

func Test_GetVolumeMetrics_WithNilPoolThroughput(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

	// Create poolMetadataMap with nil throughput (should default to 0.0)
	poolMetadata := metadata.ResourceMetadata{}
	poolMetadata.SetResourceID(int64(30))
	// Don't set throughput, so it remains nil
	poolMetadataMap := map[int64]metadata.ResourceMetadata{
		30: poolMetadata,
	}

	backupChainBytes := int64(1024)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-nil-pool-throughput"},
			Name:        "NilPoolThroughputVolume",
			SizeInBytes: 2048,
			Throughput:  150, // Non-zero volume throughput, should use volume throughput
			PoolID:      30,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-nil"},
				Name:      "NilAccount",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-nil"},
				DeploymentName: "nil-deployment",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	config.EnableBackupBillingMetrics = true // Enable backup billing metrics for test

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// VolumeAllocatedThroughput should be in separate field
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)
	// BackupEnabledVolumeAllocatedSize should be in HydratedMetrics when EnableBackupBillingMetrics is true
	assert.Len(t, result.HydratedMetrics, 1)

	// Check VolumeAllocatedThroughput metric - should use volume throughput when volume.Throughput != 0
	assert.Equal(t, metadata.VolumeAllocatedThroughput, result.VolumeAllocatedThroughputHydratedMetrics[0].MeasuredType)
	assert.Equal(t, float64(150), result.VolumeAllocatedThroughputHydratedMetrics[0].Quantity) // Should use volume throughput (150)
	assert.Equal(t, "volume-uuid-nil-pool-throughput", derefString(result.VolumeAllocatedThroughputHydratedMetrics[0].Metadata.ResourceUUID))
}

// Test_GetVolumeMetrics_WithResourceTypeMapping tests the resource type mapping logic for zonal vs regional volumes
func Test_GetVolumeMetrics_WithResourceTypeMapping(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

	// Create poolMetadataMap with regional HA pool
	poolMetadata := metadata.ResourceMetadata{}
	poolMetadata.SetThroughput(200.0)
	poolMetadata.SetResourceID(int64(100))
	poolMetadata.SetResourceType(metadata.VolumePoolRegionalHA)
	poolMetadataMap := map[int64]metadata.ResourceMetadata{
		100: poolMetadata,
	}

	backupChainBytes := int64(1024)
	volume := &datamodel.Volume{
		BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-regional"},
		Name:        "RegionalVolume",
		SizeInBytes: 3000,
		Throughput:  150,
		PoolID:      100, // Maps to regional HA pool
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid-regional"},
			Name:      "RegionalAccount",
		},
		Pool: &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-regional"},
			DeploymentName: "regional-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupChainBytes: &backupChainBytes,
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{volume}, nil)

	config.EnableBackupBillingMetrics = true

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// VolumeAllocatedThroughput metric should be generated
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)

	// Check that resource type is correctly mapped to VolumeRegionalHA
	assert.Equal(t, metadata.VolumeRegionalHA, result.VolumeAllocatedThroughputHydratedMetrics[0].Metadata.ResourceType)
	assert.Equal(t, "RegionalVolume", derefString(result.VolumeAllocatedThroughputHydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, float64(150), result.VolumeAllocatedThroughputHydratedMetrics[0].Quantity)
}

// Test_GetVolumeMetrics_BackupChainBytesEdgeCases tests various edge cases for backup chain bytes filtering
func Test_GetVolumeMetrics_BackupChainBytesEdgeCases(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	poolMetadataMap := make(map[int64]metadata.ResourceMetadata)

	negativeBackupChainBytes := int64(-100)
	zeroBackupChainBytes := int64(0)
	positiveBackupChainBytes := int64(1)

	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-negative"},
			Name:        "VolumeNegative",
			SizeInBytes: 2000,
			Throughput:  100,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
				Name:      "Account1",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-1"},
				DeploymentName: "test-deployment-1",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &negativeBackupChainBytes, // Should be filtered out
			},
		},
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-zero"},
			Name:        "VolumeZero",
			SizeInBytes: 3000,
			Throughput:  150,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-2"},
				Name:      "Account2",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-2"},
				DeploymentName: "test-deployment-2",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &zeroBackupChainBytes, // Should be filtered out
			},
		},
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-one"},
			Name:        "VolumeOne",
			SizeInBytes: 4000,
			Throughput:  200,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-3"},
				Name:      "Account3",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-3"},
				DeploymentName: "test-deployment-3",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &positiveBackupChainBytes, // Should be included
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	config.EnableBackupBillingMetrics = true

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// All volumes should generate VolumeAllocatedThroughput metrics (throughput filtering is separate)
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 3)

	// Only volumes with positive backup chain bytes should be in backup billing metrics
	assert.Len(t, result.HydratedMetrics, 1)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check that only positive backup chain byte volume is included for backup billing
	assert.Equal(t, "VolumeOne", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
}

func Test_GetVolumeMetrics_SFRMetricsEnabled(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	config.SFRMetricsEnabled = true

	// Create poolMetadataMap for testing
	poolMetadataMap := make(map[int64]metadata.ResourceMetadata)
	poolMetadataMap[1] = metadata.ResourceMetadata{
		ResourceType: metadata.Volume,
	}

	backupChainBytes := int64(1024)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-1"},
			Name:        "Volume1",
			SizeInBytes: 2048,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
				Name:      "Account1",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-1"},
				DeploymentName: "test-deployment",
			},
			PoolID: 1,
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	// Mock GetSfrMetricsByTimeRange to return SFR metrics
	sfrMetricsMap := map[string]datamodel.SfrMetricsAggregate{
		"volume-uuid-1": {
			TotalSize:  10240,
			TotalCount: 25,
		},
	}
	m.On("GetSfrMetricsByTimeRange", mock.Anything, mock.Anything, mock.Anything).Return(sfrMetricsMap, nil)

	config.EnableBackupBillingMetrics = true

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify SFR metrics are included
	assert.Len(t, result.SFRHydratedMetrics, 2) // One for TotalSize, one for TotalCount

	// Check SFR Total Size Restored Bytes metric
	var sizeMetric *entity.HydratedMetric
	var countMetric *entity.HydratedMetric
	for i := range result.SFRHydratedMetrics {
		if result.SFRHydratedMetrics[i].MeasuredType == metadata.SFRTotalSizeRestoredBytes {
			sizeMetric = &result.SFRHydratedMetrics[i]
		}
		if result.SFRHydratedMetrics[i].MeasuredType == metadata.SFRTotalFilesRestoredCount {
			countMetric = &result.SFRHydratedMetrics[i]
		}
	}

	assert.NotNil(t, sizeMetric, "SFR Total Size Restored Bytes metric should be present")
	assert.Equal(t, float64(10240), sizeMetric.Quantity)
	assert.Equal(t, "volume-uuid-1", derefString(sizeMetric.Metadata.ResourceUUID))

	assert.NotNil(t, countMetric, "SFR Total Files Restored Count metric should be present")
	assert.Equal(t, float64(25), countMetric.Quantity)
	assert.Equal(t, "volume-uuid-1", derefString(countMetric.Metadata.ResourceUUID))
}

func Test_GetVolumeMetrics_SFRMetricsEnabled_Error(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	config.SFRMetricsEnabled = true

	// Create poolMetadataMap for testing
	poolMetadataMap := make(map[int64]metadata.ResourceMetadata)
	poolMetadataMap[1] = metadata.ResourceMetadata{
		ResourceType: metadata.Volume,
	}

	backupChainBytes := int64(1024)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-1"},
			Name:        "Volume1",
			SizeInBytes: 2048,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
				Name:      "Account1",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-1"},
				DeploymentName: "test-deployment",
			},
			PoolID: 1,
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	// Mock GetSfrMetricsByTimeRange to return error
	m.On("GetSfrMetricsByTimeRange", mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)

	config.EnableBackupBillingMetrics = true

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err) // Error is logged but doesn't fail the function
	assert.NotNil(t, result)

	// SFR metrics should be empty when error occurs
	assert.Empty(t, result.SFRHydratedMetrics)
}

func Test_GetVolumeMetrics_SFRMetricsEnabled_NoMetricsForVolume(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	config.SFRMetricsEnabled = true

	// Create poolMetadataMap for testing
	poolMetadataMap := make(map[int64]metadata.ResourceMetadata)
	poolMetadataMap[1] = metadata.ResourceMetadata{
		ResourceType: metadata.Volume,
	}

	backupChainBytes := int64(1024)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-1"},
			Name:        "Volume1",
			SizeInBytes: 2048,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
				Name:      "Account1",
			},
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-1"},
				DeploymentName: "test-deployment",
			},
			PoolID: 1,
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	// Mock GetSfrMetricsByTimeRange to return empty map (no metrics for this volume)
	sfrMetricsMap := map[string]datamodel.SfrMetricsAggregate{}
	m.On("GetSfrMetricsByTimeRange", mock.Anything, mock.Anything, mock.Anything).Return(sfrMetricsMap, nil)

	config.EnableBackupBillingMetrics = true

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// SFR metrics should be empty when volume not in map
	assert.Empty(t, result.SFRHydratedMetrics)
}
