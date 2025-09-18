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

func Test_GetVolumeMetrics_ReturnsMetrics(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

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
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	result, err := GetVolumeMetrics(ctx, m, config)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetrics, 1)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check metric
	assert.Equal(t, metadata.BackupVolumeAllocatedSize, result.HydratedMetrics[0].MeasuredType)
	assert.Equal(t, float64(2048), result.HydratedMetrics[0].Quantity)
	assert.Equal(t, "volume-uuid-1", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, metadata.Volume, result.HydratedMetrics[0].Metadata.ResourceType)
	assert.Equal(t, "Volume1", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "us-east-1", derefString(result.HydratedMetrics[0].Metadata.RegionName))
	assert.Equal(t, "Account1", derefString(result.HydratedMetrics[0].Metadata.AccountName))

	// Check hydrated metrics data model
	assert.Equal(t, metadata.BackupVolumeAllocatedSize, result.HydratedMetricsDataModel[0].MeasuredType)
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
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes2,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	result, err := GetVolumeMetrics(ctx, m, config)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetrics, 2)
	assert.Len(t, result.HydratedMetricsDataModel, 2)

	// Check first volume metric
	assert.Equal(t, metadata.BackupVolumeAllocatedSize, result.HydratedMetrics[0].MeasuredType)
	assert.Equal(t, float64(2048), result.HydratedMetrics[0].Quantity)
	assert.Equal(t, "volume-uuid-1", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, "Volume1", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "Account1", derefString(result.HydratedMetrics[0].Metadata.AccountName))

	// Check second volume metric
	assert.Equal(t, metadata.BackupVolumeAllocatedSize, result.HydratedMetrics[1].MeasuredType)
	assert.Equal(t, float64(4096), result.HydratedMetrics[1].Quantity)
	assert.Equal(t, "volume-uuid-2", derefString(result.HydratedMetrics[1].Metadata.ResourceUUID))
	assert.Equal(t, "Volume2", derefString(result.HydratedMetrics[1].Metadata.ResourceName))
	assert.Equal(t, "Account2", derefString(result.HydratedMetrics[1].Metadata.AccountName))

	// Check hydrated metrics - Volume1
	assert.Equal(t, "Account1", result.HydratedMetricsDataModel[0].ConsumerID)
	assert.Equal(t, "Volume1", result.HydratedMetricsDataModel[0].ResourceName)
	assert.Equal(t, metadata.BackupVolumeAllocatedSize, result.HydratedMetricsDataModel[0].MeasuredType)
	assert.Equal(t, float64(2048), result.HydratedMetricsDataModel[0].Quantity)

	// Check hydrated metrics - Volume2
	assert.Equal(t, "Account2", result.HydratedMetricsDataModel[1].ConsumerID)
	assert.Equal(t, "Volume2", result.HydratedMetricsDataModel[1].ResourceName)
	assert.Equal(t, metadata.BackupVolumeAllocatedSize, result.HydratedMetricsDataModel[1].MeasuredType)
	assert.Equal(t, float64(4096), result.HydratedMetricsDataModel[1].Quantity)
}

func Test_GetVolumeMetrics_EmptyVolumes(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	m.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)

	result, err := GetVolumeMetrics(ctx, m, config)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetrics)
	assert.Empty(t, result.HydratedMetricsDataModel)
}

func Test_GetVolumeMetrics_ListVolumesWithAccountsError(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	m.On("ListVolumesWithAccounts", mock.Anything).Return(nil, assert.AnError)

	result, err := GetVolumeMetrics(ctx, m, config)
	assert.Error(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetrics)
	assert.Empty(t, result.HydratedMetricsDataModel)
}

func Test_GetVolumeMetrics_FiltersVolumesWithZeroBackupChainBytes(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

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
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &positiveBackupChainBytes, // Should be included
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	result, err := GetVolumeMetrics(ctx, m, config)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	// Only one volume should be processed (the one with positive backup chain bytes)
	assert.Len(t, result.HydratedMetrics, 1)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check that only the second volume is processed
	assert.Equal(t, "volume-uuid-2", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, "Volume2", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "Account2", derefString(result.HydratedMetrics[0].Metadata.AccountName))
	assert.Equal(t, float64(4096), result.HydratedMetrics[0].Quantity)
}

func Test_GetVolumeMetrics_ProcessesVolumesWithNilDataProtection(t *testing.T) {
	m := new(mockVolumeStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

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
			DataProtection: nil, // Should be processed (not filtered out)
		},
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-2"},
			Name:        "Volume2",
			SizeInBytes: 4096,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-2"},
				Name:      "Account2",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &positiveBackupChainBytes, // Should be included
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	result, err := GetVolumeMetrics(ctx, m, config)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	// Both volumes should be processed (nil DataProtection is not filtered out)
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

	backupChainBytes := int64(1024)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-1"},
			Name:        "Volume1",
			SizeInBytes: 2048,
			Account:     nil, // Nil account should be filtered out
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
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
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	result, err := GetVolumeMetrics(ctx, m, config)
	assert.NoError(t, err)
	assert.NotNil(t, result)
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

	backupChainBytes := int64(1024)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: ""}, // Missing UUID
			Name:        "Volume1",
			SizeInBytes: 2048,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
				Name:      "Account1",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
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
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	result, err := GetVolumeMetrics(ctx, m, config)
	assert.NoError(t, err)
	assert.NotNil(t, result)
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

	backupChainBytes := int64(1024)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-1"},
			Name:        "", // Missing name
			SizeInBytes: 2048,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
				Name:      "Account1",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
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
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	result, err := GetVolumeMetrics(ctx, m, config)
	assert.NoError(t, err)
	assert.NotNil(t, result)
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

	backupChainBytes := int64(1024)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-1"},
			Name:        "Volume1",
			SizeInBytes: 2048,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
				Name:      "", // Missing account name
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
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
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	result, err := GetVolumeMetrics(ctx, m, config)
	assert.NoError(t, err)
	assert.NotNil(t, result)
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

	backupChainBytes := int64(5000)
	volumes := []*datamodel.Volume{
		{
			BaseModel:   datamodel.BaseModel{UUID: "volume-uuid-integration"},
			Name:        "IntegrationVolume",
			SizeInBytes: 10000,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-integration"},
				Name:      "IntegrationAccount",
			},
			DataProtection: &datamodel.DataProtection{
				BackupChainBytes: &backupChainBytes,
			},
		},
	}

	m.On("ListVolumesWithAccounts", mock.Anything).Return(volumes, nil)

	result, err := GetVolumeMetrics(ctx, m, config)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify that BackupVolumeAllocatedSize metric is converted to HydratedMetrics
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Find the BackupVolumeAllocatedSize metric in the metrics slice
	var backupVolumeAllocatedSizeMetric *entity.HydratedMetric
	for i := range result.HydratedMetrics {
		if result.HydratedMetrics[i].MeasuredType == metadata.BackupVolumeAllocatedSize {
			backupVolumeAllocatedSizeMetric = &result.HydratedMetrics[i]
			break
		}
	}
	assert.NotNil(t, backupVolumeAllocatedSizeMetric)

	// Verify the HydratedMetrics data model is correctly populated
	hmBackupVolumeAllocated := result.HydratedMetricsDataModel[0]
	assert.Equal(t, metadata.BackupVolumeAllocatedSize, hmBackupVolumeAllocated.MeasuredType)
	assert.Equal(t, metadata.Volume, hmBackupVolumeAllocated.ResourceType)
	assert.Equal(t, "IntegrationAccount", hmBackupVolumeAllocated.ConsumerID)
	assert.Equal(t, "IntegrationVolume", hmBackupVolumeAllocated.ResourceName)
	assert.Equal(t, "ap-south-1", hmBackupVolumeAllocated.Location)
	assert.Equal(t, float64(10000), hmBackupVolumeAllocated.Quantity)

	// Verify timestamp is recent (within last minute)
	timeDiff := time.Since(hmBackupVolumeAllocated.MetricTimestamp)
	assert.True(t, timeDiff < time.Minute, "Timestamp should be recent")
}
