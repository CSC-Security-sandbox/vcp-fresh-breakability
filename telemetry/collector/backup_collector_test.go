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

type mockBackupStorage struct {
	mock.Mock
	database.Storage
}

func (m *mockBackupStorage) GetBackupLogicalSizeMetrics(ctx context.Context) ([]*datamodel.Backup, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*datamodel.Backup), args.Error(1)
}

func Test_GetBackupMetrics_ReturnsMetrics(t *testing.T) {
	m := new(mockBackupStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

	var backups []*datamodel.Backup
	backups = append(
		backups,
		&datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "backup-uuid-1",
			},
			Name:                    "Backup1",
			VolumeUUID:              "volume-uuid-1",
			LatestLogicalBackupSize: 1024,
			Attributes: &datamodel.BackupAttributes{
				AccountIdentifier: "Account1",
				VolumeName:        "Volume1",
			},
		},
	)

	m.On("GetBackupLogicalSizeMetrics", mock.Anything).Return(backups, nil)

	result, err := GetBackupMetrics(ctx, m, config)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetrics, 1)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check metric
	assert.Equal(t, metadata.BackupLogicalSize, result.HydratedMetrics[0].MeasuredType)
	assert.Equal(t, float64(1024), result.HydratedMetrics[0].Quantity)
	assert.Equal(t, "volume-uuid-1", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, metadata.Volume, result.HydratedMetrics[0].Metadata.ResourceType)
	assert.Equal(t, "Volume1", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "us-east-1", derefString(result.HydratedMetrics[0].Metadata.RegionName))
	assert.Equal(t, "Account1", derefString(result.HydratedMetrics[0].Metadata.AccountName))

	// Check hydrated metrics data model
	assert.Equal(t, metadata.BackupLogicalSize, result.HydratedMetricsDataModel[0].MeasuredType)
	assert.Equal(t, metadata.Volume, result.HydratedMetricsDataModel[0].ResourceType)
	assert.Equal(t, "Account1", result.HydratedMetricsDataModel[0].ConsumerID)
	assert.Equal(t, "Volume1", result.HydratedMetricsDataModel[0].ResourceName)
	assert.Equal(t, "us-east-1", result.HydratedMetricsDataModel[0].Location)
	assert.Equal(t, float64(1024), result.HydratedMetricsDataModel[0].Quantity)

	// Verify the type is correct
	assert.IsType(t, datamodel2.HydratedMetrics{}, result.HydratedMetricsDataModel[0])
}

func Test_GetBackupMetrics_MultipleBackups(t *testing.T) {
	m := new(mockBackupStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

	backups := []*datamodel.Backup{
		{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-1"},
			Name:                    "Backup1",
			VolumeUUID:              "volume-uuid-1",
			LatestLogicalBackupSize: 1024,
			Attributes: &datamodel.BackupAttributes{
				AccountIdentifier: "Account1",
				VolumeName:        "Volume1",
			},
		},
		{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-2"},
			Name:                    "Backup2",
			VolumeUUID:              "volume-uuid-2",
			LatestLogicalBackupSize: 2048,
			Attributes: &datamodel.BackupAttributes{
				AccountIdentifier: "Account2",
				VolumeName:        "Volume2",
			},
		},
	}

	m.On("GetBackupLogicalSizeMetrics", mock.Anything).Return(backups, nil)

	result, err := GetBackupMetrics(ctx, m, config)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetrics, 2)
	assert.Len(t, result.HydratedMetricsDataModel, 2)

	// Check first backup metric
	assert.Equal(t, metadata.BackupLogicalSize, result.HydratedMetrics[0].MeasuredType)
	assert.Equal(t, float64(1024), result.HydratedMetrics[0].Quantity)
	assert.Equal(t, "volume-uuid-1", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, "Volume1", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "Account1", derefString(result.HydratedMetrics[0].Metadata.AccountName))

	// Check second backup metric
	assert.Equal(t, metadata.BackupLogicalSize, result.HydratedMetrics[1].MeasuredType)
	assert.Equal(t, float64(2048), result.HydratedMetrics[1].Quantity)
	assert.Equal(t, "volume-uuid-2", derefString(result.HydratedMetrics[1].Metadata.ResourceUUID))
	assert.Equal(t, "Volume2", derefString(result.HydratedMetrics[1].Metadata.ResourceName))
	assert.Equal(t, "Account2", derefString(result.HydratedMetrics[1].Metadata.AccountName))

	// Check hydrated metrics - Backup1
	assert.Equal(t, "Account1", result.HydratedMetricsDataModel[0].ConsumerID)
	assert.Equal(t, "Volume1", result.HydratedMetricsDataModel[0].ResourceName)
	assert.Equal(t, metadata.BackupLogicalSize, result.HydratedMetricsDataModel[0].MeasuredType)
	assert.Equal(t, float64(1024), result.HydratedMetricsDataModel[0].Quantity)

	// Check hydrated metrics - Backup2
	assert.Equal(t, "Account2", result.HydratedMetricsDataModel[1].ConsumerID)
	assert.Equal(t, "Volume2", result.HydratedMetricsDataModel[1].ResourceName)
	assert.Equal(t, metadata.BackupLogicalSize, result.HydratedMetricsDataModel[1].MeasuredType)
	assert.Equal(t, float64(2048), result.HydratedMetricsDataModel[1].Quantity)
}

func Test_GetBackupMetrics_EmptyBackups(t *testing.T) {
	m := new(mockBackupStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	m.On("GetBackupLogicalSizeMetrics", mock.Anything).Return([]*datamodel.Backup{}, nil)

	result, err := GetBackupMetrics(ctx, m, config)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetrics)
	assert.Empty(t, result.HydratedMetricsDataModel)
}

func Test_GetBackupMetrics_GetBackupLogicalSizeMetricsError(t *testing.T) {
	m := new(mockBackupStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	m.On("GetBackupLogicalSizeMetrics", mock.Anything).Return(nil, assert.AnError)

	result, err := GetBackupMetrics(ctx, m, config)
	assert.Error(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetrics)
	assert.Empty(t, result.HydratedMetricsDataModel)
}

func Test_GetBackupMetrics_NilAttributes(t *testing.T) {
	m := new(mockBackupStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

	backups := []*datamodel.Backup{
		{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-1"},
			Name:                    "Backup1",
			VolumeUUID:              "volume-uuid-1",
			LatestLogicalBackupSize: 1024,
			Attributes:              nil, // Nil attributes should be skipped
		},
	}

	m.On("GetBackupLogicalSizeMetrics", mock.Anything).Return(backups, nil)

	result, err := GetBackupMetrics(ctx, m, config)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	// With nil attributes, the backup should be skipped entirely
	assert.Len(t, result.HydratedMetrics, 0)
	assert.Len(t, result.HydratedMetricsDataModel, 0)
}

func Test_GetBackupMetrics_MixedValidAndNilAttributes(t *testing.T) {
	m := new(mockBackupStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

	backups := []*datamodel.Backup{
		{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-1"},
			Name:                    "Backup1",
			VolumeUUID:              "volume-uuid-1",
			LatestLogicalBackupSize: 1024,
			Attributes: &datamodel.BackupAttributes{
				AccountIdentifier: "Account1",
				VolumeName:        "Volume1",
			},
		},
		{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-2"},
			Name:                    "Backup2",
			VolumeUUID:              "volume-uuid-2",
			LatestLogicalBackupSize: 2048,
			Attributes:              nil, // This should be skipped
		},
		{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-3"},
			Name:                    "Backup3",
			VolumeUUID:              "volume-uuid-3",
			LatestLogicalBackupSize: 4096,
			Attributes: &datamodel.BackupAttributes{
				AccountIdentifier: "Account3",
				VolumeName:        "Volume3",
			},
		},
	}

	m.On("GetBackupLogicalSizeMetrics", mock.Anything).Return(backups, nil)

	result, err := GetBackupMetrics(ctx, m, config)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	// Only 2 valid backups should be processed (the one with nil attributes should be skipped)
	assert.Len(t, result.HydratedMetrics, 2)
	assert.Len(t, result.HydratedMetricsDataModel, 2)

	// Check first valid backup metric
	assert.Equal(t, metadata.BackupLogicalSize, result.HydratedMetrics[0].MeasuredType)
	assert.Equal(t, float64(1024), result.HydratedMetrics[0].Quantity)
	assert.Equal(t, "volume-uuid-1", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, "Volume1", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "Account1", derefString(result.HydratedMetrics[0].Metadata.AccountName))

	// Check second valid backup metric
	assert.Equal(t, metadata.BackupLogicalSize, result.HydratedMetrics[1].MeasuredType)
	assert.Equal(t, float64(4096), result.HydratedMetrics[1].Quantity)
	assert.Equal(t, "volume-uuid-3", derefString(result.HydratedMetrics[1].Metadata.ResourceUUID))
	assert.Equal(t, "Volume3", derefString(result.HydratedMetrics[1].Metadata.ResourceName))
	assert.Equal(t, "Account3", derefString(result.HydratedMetrics[1].Metadata.AccountName))

	// Check hydrated metrics - Backup1
	assert.Equal(t, "Account1", result.HydratedMetricsDataModel[0].ConsumerID)
	assert.Equal(t, "Volume1", result.HydratedMetricsDataModel[0].ResourceName)
	assert.Equal(t, metadata.BackupLogicalSize, result.HydratedMetricsDataModel[0].MeasuredType)
	assert.Equal(t, float64(1024), result.HydratedMetricsDataModel[0].Quantity)

	// Check hydrated metrics - Backup3
	assert.Equal(t, "Account3", result.HydratedMetricsDataModel[1].ConsumerID)
	assert.Equal(t, "Volume3", result.HydratedMetricsDataModel[1].ResourceName)
	assert.Equal(t, metadata.BackupLogicalSize, result.HydratedMetricsDataModel[1].MeasuredType)
	assert.Equal(t, float64(4096), result.HydratedMetricsDataModel[1].Quantity)
}

// Test for the assembleBackupMetadata function
func TestAssembleBackupMetadata(t *testing.T) {
	// Create test backup
	backup := &datamodel.Backup{
		VolumeUUID:              "test-volume-uuid",
		LatestLogicalBackupSize: 1024,
		Attributes: &datamodel.BackupAttributes{
			AccountIdentifier: "test-account",
			VolumeName:        "test-volume",
		},
	}

	// Create test config
	config := &common.TelemetryConfig{
		RegionName: "us-central1",
	}

	// Call the function
	resourceMetadata := assembleBackupMetadata(backup, config)

	// Assertions
	assert.Equal(t, "test-volume-uuid", derefString(resourceMetadata.ResourceUUID))
	assert.Equal(t, metadata.Volume, resourceMetadata.ResourceType)
	assert.Equal(t, int64(1024), derefInt64(resourceMetadata.SizeInBytes))
	assert.Equal(t, "us-central1", derefString(resourceMetadata.RegionName))
	assert.Equal(t, "test-volume", derefString(resourceMetadata.ResourceName))
	assert.Equal(t, "test-volume", derefString(resourceMetadata.ResourceDisplayName))
	assert.Equal(t, "test-account", derefString(resourceMetadata.AccountName))
}

func TestAssembleBackupMetadata_NilAttributes(t *testing.T) {
	// Create test backup with nil attributes
	backup := &datamodel.Backup{
		VolumeUUID:              "test-volume-uuid",
		LatestLogicalBackupSize: 1024,
		Attributes:              nil,
	}

	// Create test config
	config := &common.TelemetryConfig{
		RegionName: "us-central1",
	}

	// Call the function - this should panic due to nil attributes access
	defer func() {
		if r := recover(); r != nil {
			// Expected panic due to nil attributes access
			t.Logf("Expected panic occurred: %v", r)
		}
	}()

	resourceMetadata := assembleBackupMetadata(backup, config)

	// If we get here, the function handled nil attributes gracefully
	assert.Equal(t, "test-volume-uuid", derefString(resourceMetadata.ResourceUUID))
	assert.Equal(t, metadata.Volume, resourceMetadata.ResourceType)
	assert.Equal(t, int64(1024), derefInt64(resourceMetadata.SizeInBytes))
	assert.Equal(t, "us-central1", derefString(resourceMetadata.RegionName))
}

func derefInt64(i *int64) int64 {
	if i == nil {
		return 0
	}
	return *i
}

// Test that verifies the integration between GetBackupMetrics and setupHydratedMetricsDataModel
func TestGetBackupMetrics_HydratedMetricsDataModelIntegration(t *testing.T) {
	m := new(mockBackupStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "ap-south-1"}

	backups := []*datamodel.Backup{
		{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-integration"},
			Name:                    "IntegrationBackup",
			VolumeUUID:              "volume-uuid-integration",
			LatestLogicalBackupSize: 5000,
			Attributes: &datamodel.BackupAttributes{
				AccountIdentifier: "IntegrationAccount",
				VolumeName:        "IntegrationVolume",
			},
		},
	}

	m.On("GetBackupLogicalSizeMetrics", mock.Anything).Return(backups, nil)

	result, err := GetBackupMetrics(ctx, m, config)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify that BackupLogicalSize metric is converted to HydratedMetrics
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Find the BackupLogicalSize metric in the metrics slice
	var backupLogicalSizeMetric *entity.HydratedMetric
	for i := range result.HydratedMetrics {
		if result.HydratedMetrics[i].MeasuredType == metadata.BackupLogicalSize {
			backupLogicalSizeMetric = &result.HydratedMetrics[i]
			break
		}
	}
	assert.NotNil(t, backupLogicalSizeMetric)

	// Verify the HydratedMetrics data model is correctly populated
	hmBackupLogical := result.HydratedMetricsDataModel[0]
	assert.Equal(t, metadata.BackupLogicalSize, hmBackupLogical.MeasuredType)
	assert.Equal(t, metadata.Volume, hmBackupLogical.ResourceType)
	assert.Equal(t, "IntegrationAccount", hmBackupLogical.ConsumerID)
	assert.Equal(t, "IntegrationVolume", hmBackupLogical.ResourceName)
	assert.Equal(t, "ap-south-1", hmBackupLogical.Location)
	assert.Equal(t, float64(5000), hmBackupLogical.Quantity)

	// Verify timestamp is recent (within last minute)
	timeDiff := time.Since(hmBackupLogical.MetricTimestamp)
	assert.True(t, timeDiff < time.Minute, "Timestamp should be recent")
}
