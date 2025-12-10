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

func (m *mockVolumeStorage) GetBackupVault(ctx context.Context, backupVaultID string) (*datamodel.BackupVault, error) {
	args := m.Called(ctx, backupVaultID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*datamodel.BackupVault), args.Error(1)
}

func (m *mockVolumeStorage) GetSfrMetricsByTimeRange(ctx context.Context, startTime, endTime time.Time) (map[string]datamodel.SfrMetricsAggregate, error) {
	args := m.Called(ctx, startTime, endTime)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]datamodel.SfrMetricsAggregate), args.Error(1)
}

func (m *mockVolumeStorage) GetMultipleBackupVaults(ctx context.Context, conditions [][]interface{}) ([]*datamodel.BackupVault, error) {
	args := m.Called(ctx, conditions)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*datamodel.BackupVault), args.Error(1)
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

	m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)

	config.EnableBackupBillingMetrics = true
	config.EnableFilesBackupBilling = true // Enable files backup billing to include in HydratedMetricsDataModel

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// VolumeAllocatedThroughput should be in separate field
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)
	// BackupEnabledVolumeAllocatedSize should only be in HydratedMetricsDataModel when EnableBackupBillingMetrics is true
	assert.Len(t, result.HydratedMetrics, 0)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

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

	m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)

	config.EnableBackupBillingMetrics = true
	config.EnableFilesBackupBilling = true // Enable files backup billing to include in HydratedMetricsDataModel

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// VolumeAllocatedThroughput should be in separate field (2 volumes)
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 0)
	// BackupEnabledVolumeAllocatedSize should only be in HydratedMetricsDataModel when EnableBackupBillingMetrics is true (2 volumes)
	assert.Len(t, result.HydratedMetrics, 0)
	assert.Len(t, result.HydratedMetricsDataModel, 2)

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

	m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)

	config.EnableBackupBillingMetrics = true
	config.EnableFilesBackupBilling = true // Enable files backup billing to include in HydratedMetricsDataModel

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// VolumeAllocatedThroughput metrics should be generated for both volumes (2 total)
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 0)

	// Only one volume should be processed for BackupEnabledVolumeAllocatedSize (the one with positive backup chain bytes)
	assert.Len(t, result.HydratedMetrics, 0)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check that only the second volume is processed for backup billing
	// Metrics are only in HydratedMetricsDataModel, not in HydratedMetrics
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

	m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)

	config.EnableBackupBillingMetrics = true
	config.EnableFilesBackupBilling = true // Enable files backup billing to include in HydratedMetricsDataModel

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Both volumes should generate VolumeAllocatedThroughput metrics
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 2)

	// Only volume with DataProtection and BackupChainBytes > 0 should be processed for backup billing
	// Volume1 has nil DataProtection, so only Volume2 should be included
	assert.Len(t, result.HydratedMetrics, 0)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Verify that Volume2 (with valid DataProtection) is the one included
	assert.Equal(t, "Account2", result.HydratedMetricsDataModel[0].ConsumerID)
	assert.Equal(t, "Volume2", result.HydratedMetricsDataModel[0].ResourceName)
	assert.Equal(t, float64(4096), result.HydratedMetricsDataModel[0].Quantity)
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

	m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)

	config.EnableBackupBillingMetrics = true
	config.EnableFilesBackupBilling = true // Enable files backup billing to include in HydratedMetricsDataModel

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Only one volume should generate VolumeAllocatedThroughput (the one with valid account)
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)

	// Only one volume should be processed (the one with valid account)
	assert.Len(t, result.HydratedMetrics, 0)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check that only the second volume is processed
	assert.Equal(t, "Account2", result.HydratedMetricsDataModel[0].ConsumerID)
	assert.Equal(t, "Volume2", result.HydratedMetricsDataModel[0].ResourceName)
	assert.Equal(t, float64(4096), result.HydratedMetricsDataModel[0].Quantity)
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

	m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)

	config.EnableBackupBillingMetrics = true
	config.EnableFilesBackupBilling = true // Enable files backup billing to include in HydratedMetricsDataModel

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Only one volume should generate VolumeAllocatedThroughput (the one with valid UUID)
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)

	// Only one volume should be processed (the one with valid UUID)
	assert.Len(t, result.HydratedMetrics, 0)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check that only the second volume is processed
	assert.Equal(t, "Account2", result.HydratedMetricsDataModel[0].ConsumerID)
	assert.Equal(t, "Volume2", result.HydratedMetricsDataModel[0].ResourceName)
	assert.Equal(t, float64(4096), result.HydratedMetricsDataModel[0].Quantity)
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

	m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)

	config.EnableBackupBillingMetrics = true
	config.EnableFilesBackupBilling = true // Enable files backup billing to include in HydratedMetricsDataModel

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Only one volume should generate VolumeAllocatedThroughput (the one with valid name)
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)

	// Only one volume should be processed (the one with valid name)
	assert.Len(t, result.HydratedMetrics, 0)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check that only the second volume is processed
	assert.Equal(t, "Account2", result.HydratedMetricsDataModel[0].ConsumerID)
	assert.Equal(t, "Volume2", result.HydratedMetricsDataModel[0].ResourceName)
	assert.Equal(t, float64(4096), result.HydratedMetricsDataModel[0].Quantity)
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

	m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)

	config.EnableBackupBillingMetrics = true
	config.EnableFilesBackupBilling = true // Enable files backup billing to include in HydratedMetricsDataModel

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Only one volume should generate VolumeAllocatedThroughput (the one with valid account name)
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)

	// Only one volume should be processed (the one with valid account name)
	assert.Len(t, result.HydratedMetrics, 0)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check that only the second volume is processed
	assert.Equal(t, "Account2", result.HydratedMetricsDataModel[0].ConsumerID)
	assert.Equal(t, "Volume2", result.HydratedMetricsDataModel[0].ResourceName)
	assert.Equal(t, float64(4096), result.HydratedMetricsDataModel[0].Quantity)
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

	m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)

	config.EnableBackupBillingMetrics = true
	config.EnableFilesBackupBilling = true // Enable files backup billing to include in HydratedMetricsDataModel

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify that VolumeAllocatedThroughput metric is generated
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)

	// Verify that BackupEnabledVolumeAllocatedSize metric is converted to HydratedMetrics
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Metrics are only in HydratedMetricsDataModel, not in HydratedMetrics

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

	m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)

	config.EnableBackupBillingMetrics = true
	config.EnableFilesBackupBilling = true // Enable files backup billing to include in HydratedMetricsDataModel

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// VolumeAllocatedThroughput should be in separate field
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)
	// BackupEnabledVolumeAllocatedSize should only be in HydratedMetricsDataModel when EnableBackupBillingMetrics is true
	assert.Len(t, result.HydratedMetrics, 0)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check VolumeAllocatedThroughput metric (in separate field)
	assert.Equal(t, metadata.VolumeAllocatedThroughput, result.VolumeAllocatedThroughputHydratedMetrics[0].MeasuredType)
	assert.Equal(t, float64(100), result.VolumeAllocatedThroughputHydratedMetrics[0].Quantity) // Should use volume throughput when volume.Throughput != 0
	assert.Equal(t, "volume-uuid-throughput", derefString(result.VolumeAllocatedThroughputHydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, "ThroughputVolume", derefString(result.VolumeAllocatedThroughputHydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "ThroughputAccount", derefString(result.VolumeAllocatedThroughputHydratedMetrics[0].Metadata.AccountName))
	assert.Equal(t, metadata.Volume, result.VolumeAllocatedThroughputHydratedMetrics[0].Metadata.ResourceType)

	// BackupEnabledVolumeAllocatedSize metric is only in HydratedMetricsDataModel, not in HydratedMetrics
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

	m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)

	config.EnableBackupBillingMetrics = true
	config.EnableFilesBackupBilling = true // Enable files backup billing to include in HydratedMetricsDataModel

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// VolumeAllocatedThroughput should be in separate field
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)
	// BackupEnabledVolumeAllocatedSize should only be in HydratedMetricsDataModel when EnableBackupBillingMetrics is true
	assert.Len(t, result.HydratedMetrics, 0)

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

	m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)

	config.EnableBackupBillingMetrics = true
	config.EnableFilesBackupBilling = true // Enable files backup billing to include in HydratedMetricsDataModel

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// VolumeAllocatedThroughput should be in separate field
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 1)
	// BackupEnabledVolumeAllocatedSize should only be in HydratedMetricsDataModel when EnableBackupBillingMetrics is true
	assert.Len(t, result.HydratedMetrics, 0)

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

	m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)

	config.EnableBackupBillingMetrics = true
	config.EnableFilesBackupBilling = true // Enable files backup billing to include in HydratedMetricsDataModel

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

	m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)

	config.EnableBackupBillingMetrics = true
	config.EnableFilesBackupBilling = true // Enable files backup billing to include in HydratedMetricsDataModel

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// All volumes should generate VolumeAllocatedThroughput metrics (throughput filtering is separate)
	assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, 3)

	// Only volumes with positive backup chain bytes should be in backup billing metrics
	assert.Len(t, result.HydratedMetrics, 0)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check that only positive backup chain byte volume is included for backup billing
	assert.Equal(t, "VolumeOne", result.HydratedMetricsDataModel[0].ResourceName)
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

	// Mock GetMultipleBackupVaults for backup billing metrics
	m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)

	// Mock GetSfrMetricsByTimeRange to return SFR metrics
	sfrMetricsMap := map[string]datamodel.SfrMetricsAggregate{
		"volume-uuid-1": {
			TotalSize:  10240,
			TotalCount: 25,
		},
	}
	m.On("GetSfrMetricsByTimeRange", mock.Anything, mock.Anything, mock.Anything).Return(sfrMetricsMap, nil)

	config.EnableBackupBillingMetrics = true
	config.EnableFilesBackupBilling = true // Enable files backup billing to include in HydratedMetricsDataModel

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

	// Mock GetMultipleBackupVaults for backup billing metrics
	m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)

	// Mock GetSfrMetricsByTimeRange to return error
	m.On("GetSfrMetricsByTimeRange", mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)

	config.EnableBackupBillingMetrics = true
	config.EnableFilesBackupBilling = true // Enable files backup billing to include in HydratedMetricsDataModel

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

	// Mock GetMultipleBackupVaults for backup billing metrics
	m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)

	// Mock GetSfrMetricsByTimeRange to return empty map (no metrics for this volume)
	sfrMetricsMap := map[string]datamodel.SfrMetricsAggregate{}
	m.On("GetSfrMetricsByTimeRange", mock.Anything, mock.Anything, mock.Anything).Return(sfrMetricsMap, nil)

	config.EnableBackupBillingMetrics = true
	config.EnableFilesBackupBilling = true // Enable files backup billing to include in HydratedMetricsDataModel

	result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// SFR metrics should be empty when volume not in map
	assert.Empty(t, result.SFRHydratedMetrics)
}

func Test_GetVolumeMetrics_Skip_CRB_BMF_Billing_Metrics(t *testing.T) {
	tests := []struct {
		name                                  string
		enableCrossRegionBackupBillingMetrics bool
		volumes                               []*datamodel.Volume
		backupVault                           *datamodel.BackupVault
		backupVaultError                      error
		expectedHydratedMetricsCount          int
		expectedDataModelMetricsCount         int
		expectedThroughputMetricsCount        int
		description                           string
	}{
		{
			name:                                  "Flag disabled - skip cross-region volume BMF billing metrics",
			enableCrossRegionBackupBillingMetrics: false,
			volumes: []*datamodel.Volume{
				{
					BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-1"},
					Name:        "CrossRegionVolume1",
					SizeInBytes: 2048,
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
						BackupChainBytes: intPtr(1024),
						BackupVaultID:    "backup-vault-1",
					},
				},
			},
			backupVault: &datamodel.BackupVault{
				BaseModel:        datamodel.BaseModel{UUID: "backup-vault-1"},
				Name:             "BackupVault1",
				SourceRegionName: stringPtr("us-east-1"),
				BackupRegionName: stringPtr("us-west-1"), // Different region
			},
			expectedHydratedMetricsCount:   0, // HydratedMetrics is not created for backup billing metrics
			expectedDataModelMetricsCount:  0, // HydratedMetricsDataModel should be skipped
			expectedThroughputMetricsCount: 1, // Throughput metric is independent
			description:                    "Cross-region volume should skip HydratedMetricsDataModel",
		},
		{
			name:                                  "Flag enabled - include cross-region volume BMF billing metrics",
			enableCrossRegionBackupBillingMetrics: true,
			volumes: []*datamodel.Volume{
				{
					BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-2"},
					Name:        "CrossRegionVolume2",
					SizeInBytes: 3072,
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
						BackupChainBytes: intPtr(2048),
						BackupVaultID:    "backup-vault-2",
					},
				},
			},
			backupVault: &datamodel.BackupVault{
				BaseModel:        datamodel.BaseModel{UUID: "backup-vault-2"},
				Name:             "BackupVault2",
				SourceRegionName: stringPtr("us-east-1"),
				BackupRegionName: stringPtr("eu-west-1"), // Different region
			},
			expectedHydratedMetricsCount:   0, // HydratedMetrics is not created for backup billing metrics
			expectedDataModelMetricsCount:  1, // HydratedMetricsDataModel should be included
			expectedThroughputMetricsCount: 1,
			description:                    "Cross-region volume should create HydratedMetricsDataModel when flag is enabled",
		},
		{
			name:                                  "Flag disabled - same region volume BMF billing metrics still included",
			enableCrossRegionBackupBillingMetrics: false,
			volumes: []*datamodel.Volume{
				{
					BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-3"},
					Name:        "SameRegionVolume",
					SizeInBytes: 4096,
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
						BackupChainBytes: intPtr(3072),
						BackupVaultID:    "backup-vault-3",
					},
				},
			},
			backupVault: &datamodel.BackupVault{
				BaseModel:        datamodel.BaseModel{UUID: "backup-vault-3"},
				Name:             "BackupVault3",
				SourceRegionName: stringPtr("us-east-1"),
				BackupRegionName: stringPtr("us-east-1"), // Same region
			},
			expectedHydratedMetricsCount:   0, // HydratedMetrics is not created for backup billing metrics
			expectedDataModelMetricsCount:  1, // Should be included even with flag disabled
			expectedThroughputMetricsCount: 1,
			description:                    "Same region volume should always create HydratedMetricsDataModel",
		},
		{
			name:                                  "Flag disabled - nil BackupVaultID should include billing metrics",
			enableCrossRegionBackupBillingMetrics: false,
			volumes: []*datamodel.Volume{
				{
					BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-4"},
					Name:        "NoVaultVolume",
					SizeInBytes: 5120,
					Throughput:  250,
					Account: &datamodel.Account{
						BaseModel: datamodel.BaseModel{UUID: "account-uuid-4"},
						Name:      "Account4",
					},
					Pool: &datamodel.Pool{
						BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-4"},
						DeploymentName: "test-deployment-4",
					},
					DataProtection: &datamodel.DataProtection{
						BackupChainBytes: intPtr(4096),
						BackupVaultID:    "", // No backup vault
					},
				},
			},
			expectedHydratedMetricsCount:   0, // HydratedMetrics is not created for backup billing metrics
			expectedDataModelMetricsCount:  1, // Should be included (no vault to check)
			expectedThroughputMetricsCount: 1,
			description:                    "Volume without BackupVaultID should create HydratedMetricsDataModel",
		},
		{
			name:                                  "Flag disabled - nil DataProtection should include billing metrics",
			enableCrossRegionBackupBillingMetrics: false,
			volumes: []*datamodel.Volume{
				{
					BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-5"},
					Name:        "NoDataProtectionVolume",
					SizeInBytes: 6144,
					Throughput:  300,
					Account: &datamodel.Account{
						BaseModel: datamodel.BaseModel{UUID: "account-uuid-5"},
						Name:      "Account5",
					},
					Pool: &datamodel.Pool{
						BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-5"},
						DeploymentName: "test-deployment-5",
					},
					DataProtection: &datamodel.DataProtection{
						BackupChainBytes: intPtr(5120),
						// No BackupVaultID
					},
				},
			},
			expectedHydratedMetricsCount:   0, // HydratedMetrics is not created for backup billing metrics
			expectedDataModelMetricsCount:  1, // Should be included (no vault ID to check)
			expectedThroughputMetricsCount: 1,
			description:                    "Volume without BackupVaultID should create HydratedMetricsDataModel",
		},
		{
			name:                                  "Flag disabled - GetBackupVault error should skip BMF billing metrics",
			enableCrossRegionBackupBillingMetrics: false,
			volumes: []*datamodel.Volume{
				{
					BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-6"},
					Name:        "ErrorVaultVolume",
					SizeInBytes: 7168,
					Throughput:  350,
					Account: &datamodel.Account{
						BaseModel: datamodel.BaseModel{UUID: "account-uuid-6"},
						Name:      "Account6",
					},
					Pool: &datamodel.Pool{
						BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-6"},
						DeploymentName: "test-deployment-6",
					},
					DataProtection: &datamodel.DataProtection{
						BackupChainBytes: intPtr(6144),
						BackupVaultID:    "backup-vault-error",
					},
				},
			},
			backupVaultError:               assert.AnError,
			expectedHydratedMetricsCount:   0, // HydratedMetrics is not created for backup billing metrics
			expectedDataModelMetricsCount:  0, // Should be skipped due to error
			expectedThroughputMetricsCount: 1,
			description:                    "GetBackupVault error should skip HydratedMetricsDataModel",
		},
		{
			name:                                  "Flag disabled - nil region names should include billing metrics",
			enableCrossRegionBackupBillingMetrics: false,
			volumes: []*datamodel.Volume{
				{
					BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-7"},
					Name:        "NilRegionVolume",
					SizeInBytes: 8192,
					Throughput:  400,
					Account: &datamodel.Account{
						BaseModel: datamodel.BaseModel{UUID: "account-uuid-7"},
						Name:      "Account7",
					},
					Pool: &datamodel.Pool{
						BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-7"},
						DeploymentName: "test-deployment-7",
					},
					DataProtection: &datamodel.DataProtection{
						BackupChainBytes: intPtr(7168),
						BackupVaultID:    "backup-vault-7",
					},
				},
			},
			backupVault: &datamodel.BackupVault{
				BaseModel:        datamodel.BaseModel{UUID: "backup-vault-7"},
				Name:             "BackupVault7",
				SourceRegionName: nil, // Nil region
				BackupRegionName: stringPtr("us-west-1"),
			},
			expectedHydratedMetricsCount:   0, // HydratedMetrics is not created for backup billing metrics
			expectedDataModelMetricsCount:  1, // Should be included (cannot determine cross-region)
			expectedThroughputMetricsCount: 1,
			description:                    "Nil SourceRegionName should create HydratedMetricsDataModel",
		},
		{
			name:                                  "Flag disabled - mixed cross-region and same-region volumes",
			enableCrossRegionBackupBillingMetrics: false,
			volumes: []*datamodel.Volume{
				{
					BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-8"},
					Name:        "SameRegionVolume1",
					SizeInBytes: 9216,
					Throughput:  450,
					Account: &datamodel.Account{
						BaseModel: datamodel.BaseModel{UUID: "account-uuid-8"},
						Name:      "Account8",
					},
					Pool: &datamodel.Pool{
						BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-8"},
						DeploymentName: "test-deployment-8",
					},
					DataProtection: &datamodel.DataProtection{
						BackupChainBytes: intPtr(8192),
						BackupVaultID:    "backup-vault-8",
					},
				},
				{
					BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-9"},
					Name:        "CrossRegionVolume2",
					SizeInBytes: 10240,
					Throughput:  500,
					Account: &datamodel.Account{
						BaseModel: datamodel.BaseModel{UUID: "account-uuid-9"},
						Name:      "Account9",
					},
					Pool: &datamodel.Pool{
						BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-9"},
						DeploymentName: "test-deployment-9",
					},
					DataProtection: &datamodel.DataProtection{
						BackupChainBytes: intPtr(9216),
						BackupVaultID:    "backup-vault-9",
					},
				},
			},
			backupVault: &datamodel.BackupVault{
				BaseModel:        datamodel.BaseModel{UUID: "backup-vault-9"},
				Name:             "BackupVault9",
				SourceRegionName: stringPtr("us-east-1"),
				BackupRegionName: stringPtr("ap-south-1"), // Different region for second volume
			},
			expectedHydratedMetricsCount:   0, // HydratedMetrics is not created for backup billing metrics
			expectedDataModelMetricsCount:  1, // Only same-region creates HydratedMetricsDataModel
			expectedThroughputMetricsCount: 2,
			description:                    "Mixed volumes should filter cross-region from HydratedMetricsDataModel",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := new(mockVolumeStorage)
			ctx := context.Background()
			config := &common.TelemetryConfig{
				RegionName:                            "us-east-1",
				EnableBackupBillingMetrics:            true,
				EnableFilesBackupBilling:              true, // Enable files backup billing to include in metrics
				EnableCrossRegionBackupBillingMetrics: tt.enableCrossRegionBackupBillingMetrics,
			}
			poolMetadataMap := make(map[int64]metadata.ResourceMetadata)
			m.On("ListVolumesWithAccounts", mock.Anything).Return(tt.volumes, nil)

			// Mock GetMultipleBackupVaults call - fetches all backup vaults at once
			if tt.backupVaultError != nil {
				m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return(nil, tt.backupVaultError)
			} else if tt.backupVault != nil {
				// For mixed volumes test, return both vaults
				if tt.name == "Flag disabled - mixed cross-region and same-region volumes" {
					sameRegionVault := &datamodel.BackupVault{
						BaseModel:        datamodel.BaseModel{UUID: "backup-vault-8"},
						Name:             "BackupVault8",
						SourceRegionName: stringPtr("us-east-1"),
						BackupRegionName: stringPtr("us-east-1"), // Same region
					}
					backupVaults := []*datamodel.BackupVault{sameRegionVault, tt.backupVault}
					m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return(backupVaults, nil)
				} else {
					backupVaults := []*datamodel.BackupVault{tt.backupVault}
					m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return(backupVaults, nil)
				}
			} else {
				// No backup vault needed - return empty slice
				m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)
			}

			result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
			assert.NoError(t, err)
			assert.NotNil(t, result)

			// Verify counts
			assert.Len(t, result.HydratedMetrics, tt.expectedHydratedMetricsCount,
				"HydratedMetrics count mismatch: %s", tt.description)
			assert.Len(t, result.HydratedMetricsDataModel, tt.expectedDataModelMetricsCount,
				"HydratedMetricsDataModel count mismatch: %s", tt.description)
			assert.Len(t, result.VolumeAllocatedThroughputHydratedMetrics, tt.expectedThroughputMetricsCount,
				"VolumeAllocatedThroughputHydratedMetrics count mismatch: %s", tt.description)

			// Additional validations for HydratedMetrics (BackupEnabledVolumeAllocatedSize)
			for i, metric := range result.HydratedMetrics {
				assert.Equal(t, metadata.BackupEnabledVolumeAllocatedSize, metric.MeasuredType,
					"HydratedMetrics[%d] should have BackupEnabledVolumeAllocatedSize type", i)
				assert.NotEmpty(t, derefString(metric.Metadata.ResourceUUID),
					"HydratedMetrics[%d] should have ResourceUUID", i)
				assert.NotEmpty(t, derefString(metric.Metadata.ResourceName),
					"HydratedMetrics[%d] should have ResourceName", i)
			}

			// Additional validations for HydratedMetricsDataModel
			if tt.expectedDataModelMetricsCount > 0 {
				for i, dataMetric := range result.HydratedMetricsDataModel {
					assert.Equal(t, metadata.BackupEnabledVolumeAllocatedSize, dataMetric.MeasuredType,
						"HydratedMetricsDataModel[%d] should have BackupEnabledVolumeAllocatedSize type", i)
					assert.NotEmpty(t, dataMetric.ConsumerID,
						"HydratedMetricsDataModel[%d] should have ConsumerID", i)
					assert.NotEmpty(t, dataMetric.ResourceName,
						"HydratedMetricsDataModel[%d] should have ResourceName", i)
				}
			}

			// Verify throughput metrics are always generated
			for i, throughputMetric := range result.VolumeAllocatedThroughputHydratedMetrics {
				assert.Equal(t, metadata.VolumeAllocatedThroughput, throughputMetric.MeasuredType,
					"ThroughputMetrics[%d] should have VolumeAllocatedThroughput type", i)
			}
		})
	}
}

// Helper function for int64 pointers
func intPtr(i int64) *int64 {
	return &i
}

// Test_GetVolumeMetrics_CRB_With_SFR_Metrics tests that SFR performance metrics are collected
// even when CRB billing metrics are skipped
func Test_GetVolumeMetrics_CRB_With_SFR_Metrics(t *testing.T) {
	tests := []struct {
		name                                  string
		enableCrossRegionBackupBillingMetrics bool
		enableSFRMetrics                      bool
		volumes                               []*datamodel.Volume
		backupVault                           *datamodel.BackupVault
		sfrMetricsMap                         map[string]datamodel.SfrMetricsAggregate
		expectedHydratedMetricsCount          int
		expectedDataModelMetricsCount         int
		expectedSFRMetricsCount               int
		description                           string
	}{
		{
			name:                                  "CRB volume - billing metrics skipped but SFR metrics collected",
			enableCrossRegionBackupBillingMetrics: false,
			enableSFRMetrics:                      true,
			volumes: []*datamodel.Volume{
				{
					BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-crb-sfr"},
					Name:        "CRBVolumeWithSFR",
					SizeInBytes: 2048,
					Account: &datamodel.Account{
						BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
						Name:      "Account1",
					},
					Pool: &datamodel.Pool{
						BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-1"},
						DeploymentName: "test-deployment-1",
					},
					VolumeAttributes: &datamodel.VolumeAttributes{
						Protocols: []string{"ISCSI"}, // SAN protocol volume
					},
					DataProtection: &datamodel.DataProtection{
						BackupChainBytes: intPtr(1024),
						BackupVaultID:    "backup-vault-crb",
					},
				},
			},
			backupVault: &datamodel.BackupVault{
				BaseModel:        datamodel.BaseModel{UUID: "backup-vault-crb"},
				Name:             "BackupVaultCRB",
				SourceRegionName: stringPtr("us-east-1"),
				BackupRegionName: stringPtr("us-west-1"), // Different region
			},
			sfrMetricsMap: map[string]datamodel.SfrMetricsAggregate{
				"volume-uuid-crb-sfr": {
					TotalSize:  5120,
					TotalCount: 10,
				},
			},
			expectedHydratedMetricsCount:  0, // No billing HydratedMetrics (BackupEnabledVolumeAllocatedSize doesn't create them)
			expectedDataModelMetricsCount: 0, // DataModel metrics should be skipped for CRB
			expectedSFRMetricsCount:       2, // SFR metrics should STILL be collected (size + count)
			description:                   "CRB volume should skip billing but collect SFR performance metrics",
		},
		{
			name:                                  "CRB volume - SFR disabled, billing metrics skipped",
			enableCrossRegionBackupBillingMetrics: false,
			enableSFRMetrics:                      false,
			volumes: []*datamodel.Volume{
				{
					BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-crb-no-sfr"},
					Name:        "CRBVolumeNoSFR",
					SizeInBytes: 2048,
					Account: &datamodel.Account{
						BaseModel: datamodel.BaseModel{UUID: "account-uuid-2"},
						Name:      "Account2",
					},
					Pool: &datamodel.Pool{
						BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-2"},
						DeploymentName: "test-deployment-2",
					},
					VolumeAttributes: &datamodel.VolumeAttributes{
						Protocols: []string{"ISCSI"}, // SAN protocol volume
					},
					DataProtection: &datamodel.DataProtection{
						BackupChainBytes: intPtr(1024),
						BackupVaultID:    "backup-vault-crb-2",
					},
				},
			},
			backupVault: &datamodel.BackupVault{
				BaseModel:        datamodel.BaseModel{UUID: "backup-vault-crb-2"},
				Name:             "BackupVaultCRB2",
				SourceRegionName: stringPtr("us-east-1"),
				BackupRegionName: stringPtr("eu-west-1"), // Different region
			},
			sfrMetricsMap:                 map[string]datamodel.SfrMetricsAggregate{}, // Empty, no SFR data
			expectedHydratedMetricsCount:  0,                                          // No billing HydratedMetrics
			expectedDataModelMetricsCount: 0,                                          // Skipped for CRB
			expectedSFRMetricsCount:       0,                                          // No SFR metrics since disabled
			description:                   "CRB volume with SFR disabled should skip both billing and SFR metrics",
		},
		{
			name:                                  "Same region volume - billing and SFR metrics both collected",
			enableCrossRegionBackupBillingMetrics: false,
			enableSFRMetrics:                      true,
			volumes: []*datamodel.Volume{
				{
					BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-same-region-sfr"},
					Name:        "SameRegionVolumeWithSFR",
					SizeInBytes: 3072,
					Account: &datamodel.Account{
						BaseModel: datamodel.BaseModel{UUID: "account-uuid-3"},
						Name:      "Account3",
					},
					Pool: &datamodel.Pool{
						BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-3"},
						DeploymentName: "test-deployment-3",
					},
					VolumeAttributes: &datamodel.VolumeAttributes{
						Protocols: []string{"ISCSI"}, // SAN protocol volume
					},
					DataProtection: &datamodel.DataProtection{
						BackupChainBytes: intPtr(2048),
						BackupVaultID:    "backup-vault-same",
					},
				},
			},
			backupVault: &datamodel.BackupVault{
				BaseModel:        datamodel.BaseModel{UUID: "backup-vault-same"},
				Name:             "BackupVaultSame",
				SourceRegionName: stringPtr("us-east-1"),
				BackupRegionName: stringPtr("us-east-1"), // Same region
			},
			sfrMetricsMap: map[string]datamodel.SfrMetricsAggregate{
				"volume-uuid-same-region-sfr": {
					TotalSize:  8192,
					TotalCount: 15,
				},
			},
			expectedHydratedMetricsCount:  0, // No billing HydratedMetrics
			expectedDataModelMetricsCount: 1, // Should be included for same-region
			expectedSFRMetricsCount:       2, // SFR metrics should be collected
			description:                   "Same region volume should collect both billing and SFR metrics",
		},
		{
			name:                                  "CRB flag enabled - billing and SFR metrics both collected",
			enableCrossRegionBackupBillingMetrics: true,
			enableSFRMetrics:                      true,
			volumes: []*datamodel.Volume{
				{
					BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-crb-enabled"},
					Name:        "CRBEnabledVolume",
					SizeInBytes: 4096,
					Account: &datamodel.Account{
						BaseModel: datamodel.BaseModel{UUID: "account-uuid-4"},
						Name:      "Account4",
					},
					Pool: &datamodel.Pool{
						BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-4"},
						DeploymentName: "test-deployment-4",
					},
					VolumeAttributes: &datamodel.VolumeAttributes{
						Protocols: []string{"ISCSI"}, // SAN protocol volume
					},
					DataProtection: &datamodel.DataProtection{
						BackupChainBytes: intPtr(3072),
						BackupVaultID:    "backup-vault-crb-enabled",
					},
				},
			},
			backupVault: &datamodel.BackupVault{
				BaseModel:        datamodel.BaseModel{UUID: "backup-vault-crb-enabled"},
				Name:             "BackupVaultCRBEnabled",
				SourceRegionName: stringPtr("us-east-1"),
				BackupRegionName: stringPtr("ap-south-1"), // Different region
			},
			sfrMetricsMap: map[string]datamodel.SfrMetricsAggregate{
				"volume-uuid-crb-enabled": {
					TotalSize:  12288,
					TotalCount: 20,
				},
			},
			expectedHydratedMetricsCount:  0, // No billing HydratedMetrics
			expectedDataModelMetricsCount: 1, // Should be included when flag is enabled
			expectedSFRMetricsCount:       2, // SFR metrics should be collected
			description:                   "CRB enabled flag should collect both billing and SFR metrics",
		},
		{
			name:                                  "Mixed volumes - CRB skips billing but all collect SFR",
			enableCrossRegionBackupBillingMetrics: false,
			enableSFRMetrics:                      true,
			volumes: []*datamodel.Volume{
				{
					BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-mixed-1"},
					Name:        "MixedVolume1CRB",
					SizeInBytes: 2048,
					Account: &datamodel.Account{
						BaseModel: datamodel.BaseModel{UUID: "account-uuid-5"},
						Name:      "Account5",
					},
					Pool: &datamodel.Pool{
						BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-5"},
						DeploymentName: "test-deployment-5",
					},
					VolumeAttributes: &datamodel.VolumeAttributes{
						Protocols: []string{"ISCSI"}, // SAN protocol volume
					},
					DataProtection: &datamodel.DataProtection{
						BackupChainBytes: intPtr(1024),
						BackupVaultID:    "backup-vault-mixed-crb",
					},
				},
				{
					BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-mixed-2"},
					Name:        "MixedVolume2Same",
					SizeInBytes: 3072,
					Account: &datamodel.Account{
						BaseModel: datamodel.BaseModel{UUID: "account-uuid-6"},
						Name:      "Account6",
					},
					Pool: &datamodel.Pool{
						BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-6"},
						DeploymentName: "test-deployment-6",
					},
					VolumeAttributes: &datamodel.VolumeAttributes{
						Protocols: []string{"ISCSI"}, // SAN protocol volume
					},
					DataProtection: &datamodel.DataProtection{
						BackupChainBytes: intPtr(2048),
						BackupVaultID:    "backup-vault-mixed-same",
					},
				},
			},
			backupVault: &datamodel.BackupVault{
				BaseModel:        datamodel.BaseModel{UUID: "backup-vault-mixed-crb"},
				Name:             "BackupVaultMixedCRB",
				SourceRegionName: stringPtr("us-east-1"),
				BackupRegionName: stringPtr("eu-west-1"), // Different region for first volume
			},
			sfrMetricsMap: map[string]datamodel.SfrMetricsAggregate{
				"volume-uuid-mixed-1": {
					TotalSize:  2048,
					TotalCount: 5,
				},
				"volume-uuid-mixed-2": {
					TotalSize:  4096,
					TotalCount: 8,
				},
			},
			expectedHydratedMetricsCount:  0, // No billing HydratedMetrics
			expectedDataModelMetricsCount: 1, // Only same-region volume
			expectedSFRMetricsCount:       4, // Both volumes should have SFR metrics (2 metrics each)
			description:                   "Mixed volumes: CRB skips billing but both collect SFR metrics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := new(mockVolumeStorage)
			ctx := context.Background()
			config := &common.TelemetryConfig{
				RegionName:                            "us-east-1",
				EnableBackupBillingMetrics:            true,
				EnableCrossRegionBackupBillingMetrics: tt.enableCrossRegionBackupBillingMetrics,
				SFRMetricsEnabled:                     tt.enableSFRMetrics,
			}
			poolMetadataMap := make(map[int64]metadata.ResourceMetadata)

			m.On("ListVolumesWithAccounts", mock.Anything).Return(tt.volumes, nil)

			// Mock GetMultipleBackupVaults
			if tt.backupVault != nil {
				if tt.name == "Mixed volumes - CRB skips billing but all collect SFR" {
					// For mixed test, return both vaults
					sameRegionVault := &datamodel.BackupVault{
						BaseModel:        datamodel.BaseModel{UUID: "backup-vault-mixed-same"},
						Name:             "BackupVaultMixedSame",
						SourceRegionName: stringPtr("us-east-1"),
						BackupRegionName: stringPtr("us-east-1"), // Same region
					}
					backupVaults := []*datamodel.BackupVault{tt.backupVault, sameRegionVault}
					m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return(backupVaults, nil)
				} else {
					backupVaults := []*datamodel.BackupVault{tt.backupVault}
					m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return(backupVaults, nil)
				}
			} else {
				m.On("GetMultipleBackupVaults", mock.Anything, mock.Anything).Return([]*datamodel.BackupVault{}, nil)
			}

			// Mock SFR metrics if enabled
			if tt.enableSFRMetrics {
				m.On("GetSfrMetricsByTimeRange", mock.Anything, mock.Anything, mock.Anything).Return(tt.sfrMetricsMap, nil)
			}

			result, err := GetVolumeMetrics(ctx, m, config, poolMetadataMap, time.Now())
			assert.NoError(t, err)
			assert.NotNil(t, result)

			// Verify counts
			assert.Len(t, result.HydratedMetrics, tt.expectedHydratedMetricsCount,
				"HydratedMetrics count mismatch: %s", tt.description)
			assert.Len(t, result.HydratedMetricsDataModel, tt.expectedDataModelMetricsCount,
				"HydratedMetricsDataModel count mismatch: %s", tt.description)
			assert.Len(t, result.SFRHydratedMetrics, tt.expectedSFRMetricsCount,
				"SFRHydratedMetrics count mismatch: %s", tt.description)

			// Verify SFR metrics have correct types
			if tt.expectedSFRMetricsCount > 0 {
				sizeMetricFound := false
				countMetricFound := false
				for _, sfrMetric := range result.SFRHydratedMetrics {
					if sfrMetric.MeasuredType == metadata.SFRTotalSizeRestoredBytes {
						sizeMetricFound = true
						assert.Greater(t, sfrMetric.Quantity, float64(0), "SFR size metric should have positive quantity")
					}
					if sfrMetric.MeasuredType == metadata.SFRTotalFilesRestoredCount {
						countMetricFound = true
						assert.Greater(t, sfrMetric.Quantity, float64(0), "SFR count metric should have positive quantity")
					}
				}
				assert.True(t, sizeMetricFound, "SFR Total Size Restored Bytes metric should be present")
				assert.True(t, countMetricFound, "SFR Total Files Restored Count metric should be present")
			}
		})
	}
}
