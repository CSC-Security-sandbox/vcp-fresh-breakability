package collector

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
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

func (m *mockBackupStorage) GetBackupMetrics(ctx context.Context, conditions [][]interface{}, pagination *dbutils.Pagination) ([]*datamodel.Backup, error) {
	args := m.Called(ctx, conditions, pagination)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*datamodel.Backup), args.Error(1)
}

func Test_GetBackupMetrics_ReturnsMetrics(t *testing.T) {
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
			BackupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
				Name:      "BackupVault1",
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
					Name:      "Account1",
				},
			},
		},
	}

	// Mock the first call to return backups, subsequent calls return empty
	m.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(pagination *dbutils.Pagination) bool {
		return pagination.Offset == 0
	})).Return(backups, nil)
	m.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(pagination *dbutils.Pagination) bool {
		return pagination.Offset > 0
	})).Return([]*datamodel.Backup{}, nil)

	result, err := GetBackupMetrics(ctx, m, config, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetrics, 1)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Check metric
	assert.Equal(t, metadata.BackupLogicalSize, result.HydratedMetrics[0].MeasuredType)
	assert.Equal(t, float64(1024), result.HydratedMetrics[0].Quantity)
	assert.Equal(t, "volume-uuid-1", derefString(result.HydratedMetrics[0].Metadata.ResourceUUID))
	assert.Equal(t, metadata.Backup, result.HydratedMetrics[0].Metadata.ResourceType)
	assert.Equal(t, "Volume1", derefString(result.HydratedMetrics[0].Metadata.ResourceName))
	assert.Equal(t, "us-east-1", derefString(result.HydratedMetrics[0].Metadata.RegionName))
	assert.Equal(t, "Account1", derefString(result.HydratedMetrics[0].Metadata.AccountName))

	// Check hydrated metrics data model
	assert.Equal(t, metadata.BackupLogicalSize, result.HydratedMetricsDataModel[0].MeasuredType)
	assert.Equal(t, metadata.Backup, result.HydratedMetricsDataModel[0].ResourceType)
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
			BackupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
				Name:      "BackupVault1",
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
					Name:      "Account1",
				},
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
			BackupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{UUID: "vault-uuid-2"},
				Name:      "BackupVault2",
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "account-uuid-2"},
					Name:      "Account2",
				},
			},
		},
	}

	// Mock the first call to return backups, subsequent calls return empty
	m.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(pagination *dbutils.Pagination) bool {
		return pagination.Offset == 0
	})).Return(backups, nil)
	m.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(pagination *dbutils.Pagination) bool {
		return pagination.Offset > 0
	})).Return([]*datamodel.Backup{}, nil)

	result, err := GetBackupMetrics(ctx, m, config, time.Now())
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
	// Mock to return empty results immediately
	m.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Backup{}, nil)

	result, err := GetBackupMetrics(ctx, m, config, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetrics)
	assert.Empty(t, result.HydratedMetricsDataModel)
}

func Test_GetBackupMetrics_GetBackupMetricsError(t *testing.T) {
	m := new(mockBackupStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	m.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)

	result, err := GetBackupMetrics(ctx, m, config, time.Now())
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

	// Mock the first call to return backups, subsequent calls return empty
	m.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(pagination *dbutils.Pagination) bool {
		return pagination.Offset == 0
	})).Return(backups, nil)
	m.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(pagination *dbutils.Pagination) bool {
		return pagination.Offset > 0
	})).Return([]*datamodel.Backup{}, nil)

	result, err := GetBackupMetrics(ctx, m, config, time.Now())
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
			BackupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
				Name:      "BackupVault1",
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
					Name:      "Account1",
				},
			},
		},
		{
			BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-2"},
			Name:                    "Backup2",
			VolumeUUID:              "volume-uuid-2",
			LatestLogicalBackupSize: 2048,
			Attributes:              nil, // This should be skipped
			BackupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{UUID: "vault-uuid-2"},
				Name:      "BackupVault2",
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "account-uuid-2"},
					Name:      "Account2",
				},
			},
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
			BackupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{UUID: "vault-uuid-3"},
				Name:      "BackupVault3",
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "account-uuid-3"},
					Name:      "Account3",
				},
			},
		},
	}

	// Mock the first call to return backups, subsequent calls return empty
	m.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(pagination *dbutils.Pagination) bool {
		return pagination.Offset == 0
	})).Return(backups, nil)
	m.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(pagination *dbutils.Pagination) bool {
		return pagination.Offset > 0
	})).Return([]*datamodel.Backup{}, nil)

	result, err := GetBackupMetrics(ctx, m, config, time.Now())
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
	assert.Equal(t, metadata.Backup, resourceMetadata.ResourceType)
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
			BackupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{UUID: "vault-uuid-integration"},
				Name:      "IntegrationBackupVault",
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "account-uuid-integration"},
					Name:      "IntegrationAccount",
				},
			},
		},
	}

	// Mock the first call to return backups, subsequent calls return empty
	m.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(pagination *dbutils.Pagination) bool {
		return pagination.Offset == 0
	})).Return(backups, nil)
	m.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(pagination *dbutils.Pagination) bool {
		return pagination.Offset > 0
	})).Return([]*datamodel.Backup{}, nil)

	result, err := GetBackupMetrics(ctx, m, config, time.Now())
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify that BackupLogicalSize metric is converted to HydratedMetrics
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	// Find the BackupLogicalSize metric in the metrics slice
	var backupMetric *entity.HydratedMetric
	for i := range result.HydratedMetrics {
		if result.HydratedMetrics[i].MeasuredType == metadata.BackupLogicalSize {
			backupMetric = &result.HydratedMetrics[i]
			break
		}
	}
	assert.NotNil(t, backupMetric)

	// Verify the HydratedMetrics data model is correctly populated
	hmBackup := result.HydratedMetricsDataModel[0]
	assert.Equal(t, metadata.BackupLogicalSize, hmBackup.MeasuredType)
	assert.Equal(t, metadata.Backup, hmBackup.ResourceType)
	assert.Equal(t, "IntegrationAccount", hmBackup.ConsumerID)
	assert.Equal(t, "IntegrationVolume", hmBackup.ResourceName)
	assert.Equal(t, "ap-south-1", hmBackup.Location)
	assert.Equal(t, float64(5000), hmBackup.Quantity)

	// Verify timestamp is recent (within last minute)
	timeDiff := time.Since(hmBackup.MetricTimestamp)
	assert.True(t, timeDiff < time.Minute, "Timestamp should be recent")
}

func TestGetBackupMetrics_Skipping_Cross_Region_Backups_Billing_Metrics(t *testing.T) {
	tests := []struct {
		name                                  string
		enableCrossRegionBackupBillingMetrics bool
		backups                               []*datamodel.Backup
		expectedHydratedMetricsCount          int
		expectedDataModelMetricsCount         int
		description                           string
	}{
		{
			name:                                  "Flag disabled - skip cross-region backup billing metrics",
			enableCrossRegionBackupBillingMetrics: false,
			backups: []*datamodel.Backup{
				{
					BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-1"},
					Name:                    "CrossRegionBackup1",
					VolumeUUID:              "volume-uuid-1",
					LatestLogicalBackupSize: 1024,
					Attributes: &datamodel.BackupAttributes{
						AccountIdentifier: "Account1",
						VolumeName:        "Volume1",
					},
					BackupVault: &datamodel.BackupVault{
						BaseModel:        datamodel.BaseModel{UUID: "vault-uuid-1"},
						Name:             "BackupVault1",
						BackupVaultType:  activities.CrossRegionBackupType, // Mark as cross-region
						SourceRegionName: stringPtr("us-east-1"),
						BackupRegionName: stringPtr("us-west-1"),
						Account: &datamodel.Account{
							BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
							Name:      "Account1",
						},
					},
				},
			},
			expectedHydratedMetricsCount:  1, // HydratedMetrics is always created
			expectedDataModelMetricsCount: 0, // HydratedMetricsDataModel should be skipped
			description:                   "Cross-region backup should create HydratedMetrics but skip HydratedMetricsDataModel",
		},
		{
			name:                                  "Flag enabled - include cross-region backup billing metrics",
			enableCrossRegionBackupBillingMetrics: true,
			backups: []*datamodel.Backup{
				{
					BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-2"},
					Name:                    "CrossRegionBackup2",
					VolumeUUID:              "volume-uuid-2",
					LatestLogicalBackupSize: 2048,
					Attributes: &datamodel.BackupAttributes{
						AccountIdentifier: "Account2",
						VolumeName:        "Volume2",
					},
					BackupVault: &datamodel.BackupVault{
						BaseModel:        datamodel.BaseModel{UUID: "vault-uuid-2"},
						Name:             "BackupVault2",
						BackupVaultType:  activities.CrossRegionBackupType, // Mark as cross-region
						SourceRegionName: stringPtr("us-east-1"),
						BackupRegionName: stringPtr("eu-west-1"),
						Account: &datamodel.Account{
							BaseModel: datamodel.BaseModel{UUID: "account-uuid-2"},
							Name:      "Account2",
						},
					},
				},
			},
			expectedHydratedMetricsCount:  1,
			expectedDataModelMetricsCount: 1, // HydratedMetricsDataModel should be included
			description:                   "Cross-region backup should create both HydratedMetrics and HydratedMetricsDataModel",
		},
		{
			name:                                  "Flag disabled - same region backup billing metrics still included",
			enableCrossRegionBackupBillingMetrics: false,
			backups: []*datamodel.Backup{
				{
					BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-3"},
					Name:                    "SameRegionBackup",
					VolumeUUID:              "volume-uuid-3",
					LatestLogicalBackupSize: 3072,
					Attributes: &datamodel.BackupAttributes{
						AccountIdentifier: "Account3",
						VolumeName:        "Volume3",
					},
					BackupVault: &datamodel.BackupVault{
						BaseModel:        datamodel.BaseModel{UUID: "vault-uuid-3"},
						Name:             "BackupVault3",
						BackupVaultType:  "IN_REGION", // Not cross-region
						SourceRegionName: stringPtr("us-east-1"),
						BackupRegionName: stringPtr("us-east-1"),
						Account: &datamodel.Account{
							BaseModel: datamodel.BaseModel{UUID: "account-uuid-3"},
							Name:      "Account3",
						},
					},
				},
			},
			expectedHydratedMetricsCount:  1,
			expectedDataModelMetricsCount: 1, // Should be included even with flag disabled
			description:                   "Same region backup should always create both metrics",
		},
		{
			name:                                  "Flag disabled - nil BackupVault should include billing metrics",
			enableCrossRegionBackupBillingMetrics: false,
			backups: []*datamodel.Backup{
				{
					BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-4"},
					Name:                    "NilVaultBackup",
					VolumeUUID:              "volume-uuid-4",
					LatestLogicalBackupSize: 4096,
					Attributes: &datamodel.BackupAttributes{
						AccountIdentifier: "Account4",
						VolumeName:        "Volume4",
					},
					BackupVault: nil, // Nil vault - cannot determine type, so billing is included
				},
			},
			expectedHydratedMetricsCount:  1,
			expectedDataModelMetricsCount: 1, // Should be included (cannot determine cross-region)
			description:                   "Nil BackupVault should create both metrics",
		},
		{
			name:                                  "Flag disabled - non-cross-region backup with type should include billing metrics",
			enableCrossRegionBackupBillingMetrics: false,
			backups: []*datamodel.Backup{
				{
					BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-5"},
					Name:                    "StandardBackup",
					VolumeUUID:              "volume-uuid-5",
					LatestLogicalBackupSize: 5120,
					Attributes: &datamodel.BackupAttributes{
						AccountIdentifier: "Account5",
						VolumeName:        "Volume5",
					},
					BackupVault: &datamodel.BackupVault{
						BaseModel:        datamodel.BaseModel{UUID: "vault-uuid-5"},
						Name:             "BackupVault5",
						BackupVaultType:  "IN_REGION", // Not cross-region
						SourceRegionName: nil,
						BackupRegionName: stringPtr("us-west-1"),
						Account: &datamodel.Account{
							BaseModel: datamodel.BaseModel{UUID: "account-uuid-5"},
							Name:      "Account5",
						},
					},
				},
			},
			expectedHydratedMetricsCount:  1,
			expectedDataModelMetricsCount: 1, // Should be included (not cross-region type)
			description:                   "Standard backup vault type should create both metrics",
		},
		{
			name:                                  "Flag disabled - mixed cross-region and standard backups",
			enableCrossRegionBackupBillingMetrics: false,
			backups: []*datamodel.Backup{
				{
					BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-6"},
					Name:                    "StandardBackup1",
					VolumeUUID:              "volume-uuid-6",
					LatestLogicalBackupSize: 6144,
					Attributes: &datamodel.BackupAttributes{
						AccountIdentifier: "Account6",
						VolumeName:        "Volume6",
					},
					BackupVault: &datamodel.BackupVault{
						BaseModel:        datamodel.BaseModel{UUID: "vault-uuid-6"},
						Name:             "BackupVault6",
						BackupVaultType:  "IN_REGION", // Not cross-region
						SourceRegionName: stringPtr("us-east-1"),
						BackupRegionName: stringPtr("us-east-1"),
						Account: &datamodel.Account{
							BaseModel: datamodel.BaseModel{UUID: "account-uuid-6"},
							Name:      "Account6",
						},
					},
				},
				{
					BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-7"},
					Name:                    "CrossRegionBackup2",
					VolumeUUID:              "volume-uuid-7",
					LatestLogicalBackupSize: 7168,
					Attributes: &datamodel.BackupAttributes{
						AccountIdentifier: "Account7",
						VolumeName:        "Volume7",
					},
					BackupVault: &datamodel.BackupVault{
						BaseModel:        datamodel.BaseModel{UUID: "vault-uuid-7"},
						Name:             "BackupVault7",
						BackupVaultType:  activities.CrossRegionBackupType, // Cross-region
						SourceRegionName: stringPtr("us-east-1"),
						BackupRegionName: stringPtr("ap-south-1"),
						Account: &datamodel.Account{
							BaseModel: datamodel.BaseModel{UUID: "account-uuid-7"},
							Name:      "Account7",
						},
					},
				},
				{
					BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-8"},
					Name:                    "StandardBackup2",
					VolumeUUID:              "volume-uuid-8",
					LatestLogicalBackupSize: 8192,
					Attributes: &datamodel.BackupAttributes{
						AccountIdentifier: "Account8",
						VolumeName:        "Volume8",
					},
					BackupVault: &datamodel.BackupVault{
						BaseModel:        datamodel.BaseModel{UUID: "vault-uuid-8"},
						Name:             "BackupVault8",
						BackupVaultType:  "IN_REGION", // Not cross-region
						SourceRegionName: stringPtr("us-west-2"),
						BackupRegionName: stringPtr("us-west-2"),
						Account: &datamodel.Account{
							BaseModel: datamodel.BaseModel{UUID: "account-uuid-8"},
							Name:      "Account8",
						},
					},
				},
			},
			expectedHydratedMetricsCount:  3, // All create HydratedMetrics
			expectedDataModelMetricsCount: 2, // Only standard backups create HydratedMetricsDataModel
			description:                   "Mixed backups should filter cross-region from HydratedMetricsDataModel",
		},
		{
			name:                                  "Flag enabled - mixed cross-region and standard backups all included",
			enableCrossRegionBackupBillingMetrics: true,
			backups: []*datamodel.Backup{
				{
					BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-9"},
					Name:                    "StandardBackup3",
					VolumeUUID:              "volume-uuid-9",
					LatestLogicalBackupSize: 9216,
					Attributes: &datamodel.BackupAttributes{
						AccountIdentifier: "Account9",
						VolumeName:        "Volume9",
					},
					BackupVault: &datamodel.BackupVault{
						BaseModel:        datamodel.BaseModel{UUID: "vault-uuid-9"},
						Name:             "BackupVault9",
						BackupVaultType:  "IN_REGION", // Not cross-region
						SourceRegionName: stringPtr("eu-west-1"),
						BackupRegionName: stringPtr("eu-west-1"),
						Account: &datamodel.Account{
							BaseModel: datamodel.BaseModel{UUID: "account-uuid-9"},
							Name:      "Account9",
						},
					},
				},
				{
					BaseModel:               datamodel.BaseModel{UUID: "backup-uuid-10"},
					Name:                    "CrossRegionBackup3",
					VolumeUUID:              "volume-uuid-10",
					LatestLogicalBackupSize: 10240,
					Attributes: &datamodel.BackupAttributes{
						AccountIdentifier: "Account10",
						VolumeName:        "Volume10",
					},
					BackupVault: &datamodel.BackupVault{
						BaseModel:        datamodel.BaseModel{UUID: "vault-uuid-10"},
						Name:             "BackupVault10",
						BackupVaultType:  activities.CrossRegionBackupType, // Cross-region
						SourceRegionName: stringPtr("eu-west-1"),
						BackupRegionName: stringPtr("us-east-1"),
						Account: &datamodel.Account{
							BaseModel: datamodel.BaseModel{UUID: "account-uuid-10"},
							Name:      "Account10",
						},
					},
				},
			},
			expectedHydratedMetricsCount:  2,
			expectedDataModelMetricsCount: 2, // All backups create HydratedMetricsDataModel when flag is enabled
			description:                   "All backups should create both metrics when flag is enabled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := new(mockBackupStorage)
			ctx := context.Background()
			config := &common.TelemetryConfig{
				RegionName:                            "us-east-1",
				EnableCrossRegionBackupBillingMetrics: tt.enableCrossRegionBackupBillingMetrics,
			}

			// Mock the first call to return backups, subsequent calls return empty
			m.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(pagination *dbutils.Pagination) bool {
				return pagination.Offset == 0
			})).Return(tt.backups, nil)
			m.On("GetBackupMetrics", mock.Anything, mock.Anything, mock.MatchedBy(func(pagination *dbutils.Pagination) bool {
				return pagination.Offset > 0
			})).Return([]*datamodel.Backup{}, nil)

			result, err := GetBackupMetrics(ctx, m, config, time.Now())
			assert.NoError(t, err)
			assert.NotNil(t, result)

			// Verify counts
			assert.Len(t, result.HydratedMetrics, tt.expectedHydratedMetricsCount,
				"HydratedMetrics count mismatch: %s", tt.description)
			assert.Len(t, result.HydratedMetricsDataModel, tt.expectedDataModelMetricsCount,
				"HydratedMetricsDataModel count mismatch: %s", tt.description)

			// Additional validations for HydratedMetrics (should always be created)
			for i, metric := range result.HydratedMetrics {
				assert.Equal(t, metadata.BackupLogicalSize, metric.MeasuredType,
					"HydratedMetrics[%d] should have BackupLogicalSize type", i)
				assert.Equal(t, tt.backups[i].VolumeUUID, derefString(metric.Metadata.ResourceUUID),
					"HydratedMetrics[%d] should have correct VolumeUUID", i)
				assert.Equal(t, float64(tt.backups[i].LatestLogicalBackupSize), metric.Quantity,
					"HydratedMetrics[%d] should have correct quantity", i)
			}

			// Additional validations for HydratedMetricsDataModel
			if tt.expectedDataModelMetricsCount > 0 {
				for i, dataMetric := range result.HydratedMetricsDataModel {
					assert.Equal(t, metadata.BackupLogicalSize, dataMetric.MeasuredType,
						"HydratedMetricsDataModel[%d] should have BackupLogicalSize type", i)
					assert.Equal(t, metadata.Backup, dataMetric.ResourceType,
						"HydratedMetricsDataModel[%d] should have Backup resource type", i)
					assert.NotEmpty(t, dataMetric.ConsumerID,
						"HydratedMetricsDataModel[%d] should have ConsumerID", i)
					assert.NotEmpty(t, dataMetric.ResourceName,
						"HydratedMetricsDataModel[%d] should have ResourceName", i)
				}
			}
		})
	}
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}
