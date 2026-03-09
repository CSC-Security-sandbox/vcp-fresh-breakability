package collector

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	metricsdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// --- mock for vcp Storage (cross-region restore tests) ---

type mockCrossRegionVCPStorage struct {
	mock.Mock
	database.Storage
}

func (m *mockCrossRegionVCPStorage) GetJobsWithCondition(ctx context.Context, filter dbutils.Filter) ([]*datamodel.Job, error) {
	args := m.Called(ctx, filter)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*datamodel.Job), args.Error(1)
}

func (m *mockCrossRegionVCPStorage) ListVolumesWithPagination(ctx context.Context, conditions [][]interface{}, pagination *dbutils.Pagination) ([]*datamodel.Volume, error) {
	args := m.Called(ctx, conditions, pagination)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*datamodel.Volume), args.Error(1)
}

func (m *mockCrossRegionVCPStorage) GetBackupWithVaultByUUID(ctx context.Context, backupUUID string) (*datamodel.Backup, error) {
	args := m.Called(ctx, backupUUID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*datamodel.Backup), args.Error(1)
}

func (m *mockCrossRegionVCPStorage) GetBackupVaultByNameAndOwnerID(ctx context.Context, backupVaultName, ownerID string) (*datamodel.BackupVault, error) {
	args := m.Called(ctx, backupVaultName, ownerID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*datamodel.BackupVault), args.Error(1)
}

func (m *mockCrossRegionVCPStorage) GetBackupByNameAndBackupVaultID(ctx context.Context, backupName string, backupVaultID int64) (*datamodel.Backup, error) {
	args := m.Called(ctx, backupName, backupVaultID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*datamodel.Backup), args.Error(1)
}

func (m *mockCrossRegionVCPStorage) GetSfrMetadataByJobID(ctx context.Context, jobID int64) (*datamodel.SfrMetadata, error) {
	args := m.Called(ctx, jobID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*datamodel.SfrMetadata), args.Error(1)
}

// --- mock for metrics Storage (cross-region restore tests) ---

type mockCrossRegionMetricsStorage struct {
	mock.Mock
	metricsdb.Storage
}

func (m *mockCrossRegionMetricsStorage) GetRestoreTimestamp(ctx context.Context) (*datamodel2.RestoreTimestamp, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*datamodel2.RestoreTimestamp), args.Error(1)
}

func (m *mockCrossRegionMetricsStorage) UpdateRestoreTimestamp(ctx context.Context, lastProcessedAt time.Time) error {
	args := m.Called(ctx, lastProcessedAt)
	return args.Error(0)
}

// --- helpers ---

func strPtr(s string) *string {
	return &s
}

func newTestJob(uuid, jobType, state, resourceName string, accountID int64, updatedAt time.Time) *datamodel.Job {
	return &datamodel.Job{
		BaseModel:    datamodel.BaseModel{UUID: uuid, UpdatedAt: updatedAt},
		Type:         jobType,
		State:        state,
		ResourceName: resourceName,
		AccountID:    sql.NullInt64{Int64: accountID, Valid: true},
	}
}

func newCrossRegionBackup(uuid, name string, sizeInBytes int64, backupRegionName string) *datamodel.Backup {
	return &datamodel.Backup{
		BaseModel:   datamodel.BaseModel{UUID: uuid},
		Name:        name,
		SizeInBytes: sizeInBytes,
		BackupVault: &datamodel.BackupVault{
			BackupVaultType:  activities.CrossRegionBackupType,
			BackupRegionName: strPtr(backupRegionName),
		},
	}
}

func newVolume(uuid, name string, accountName, deploymentName string, restoredBackupID string) *datamodel.Volume {
	return &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: uuid},
		Name:      name,
		Account:   &datamodel.Account{Name: accountName},
		Pool:      &datamodel.Pool{DeploymentName: deploymentName},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupID: restoredBackupID,
			AccountName:      accountName,
			DeploymentName:   deploymentName,
			Protocols:        []string{"NFSV3"},
		},
	}
}

func defaultConfig() *common.TelemetryConfig {
	return &common.TelemetryConfig{
		RegionName:                            "us-east4",
		EnableCrossRegionBackupBillingMetrics: true,
		EnableSFRCrossRegionRestoreBilling:    false,
		EnableFilesBackupBilling:              true,
	}
}

// =============================================================================
// ProcessRestoreBillingMetrics tests
// =============================================================================

func TestProcessRestoreBillingMetrics_GetRestoreTimestampError(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	metricsDB := new(mockCrossRegionMetricsStorage)
	ctx := context.Background()
	config := defaultConfig()

	metricsDB.On("GetRestoreTimestamp", mock.Anything).Return(nil, errors.New("db error"))

	result, err := ProcessRestoreBillingMetrics(ctx, vcpDB, metricsDB, config, time.Now())
	assert.Error(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetricsDataModel)
}

func TestProcessRestoreBillingMetrics_NoJobsFound(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	metricsDB := new(mockCrossRegionMetricsStorage)
	ctx := context.Background()
	config := defaultConfig()
	now := time.Now()

	metricsDB.On("GetRestoreTimestamp", mock.Anything).Return(nil, nil)
	metricsDB.On("UpdateRestoreTimestamp", mock.Anything, now).Return(nil)
	vcpDB.On("GetJobsWithCondition", mock.Anything, mock.Anything).Return([]*datamodel.Job{}, nil)

	result, err := ProcessRestoreBillingMetrics(ctx, vcpDB, metricsDB, config, now)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetricsDataModel)
	metricsDB.AssertCalled(t, "UpdateRestoreTimestamp", mock.Anything, now)
}

func TestProcessRestoreBillingMetrics_GetJobsError(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	metricsDB := new(mockCrossRegionMetricsStorage)
	ctx := context.Background()
	config := defaultConfig()
	now := time.Now()

	metricsDB.On("GetRestoreTimestamp", mock.Anything).Return(nil, nil)
	metricsDB.On("UpdateRestoreTimestamp", mock.Anything, now).Return(nil)
	vcpDB.On("GetJobsWithCondition", mock.Anything, mock.Anything).Return(nil, errors.New("job query failed"))

	result, err := ProcessRestoreBillingMetrics(ctx, vcpDB, metricsDB, config, now)
	assert.Error(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetricsDataModel)
}

func TestProcessRestoreBillingMetrics_ExistingTimestamp(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	metricsDB := new(mockCrossRegionMetricsStorage)
	ctx := context.Background()
	config := defaultConfig()
	now := time.Now()
	lastProcessed := now.Add(-30 * time.Minute)

	metricsDB.On("GetRestoreTimestamp", mock.Anything).Return(&datamodel2.RestoreTimestamp{LastProcessedAt: lastProcessed}, nil)
	metricsDB.On("UpdateRestoreTimestamp", mock.Anything, now).Return(nil)
	vcpDB.On("GetJobsWithCondition", mock.Anything, mock.Anything).Return([]*datamodel.Job{}, nil)

	result, err := ProcessRestoreBillingMetrics(ctx, vcpDB, metricsDB, config, now)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetricsDataModel)
	metricsDB.AssertCalled(t, "UpdateRestoreTimestamp", mock.Anything, now)
}

func TestProcessRestoreBillingMetrics_FullRestore_CrossRegion_Success(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	metricsDB := new(mockCrossRegionMetricsStorage)
	ctx := context.Background()
	config := defaultConfig()
	now := time.Now()
	jobUpdatedAt := now.Add(-5 * time.Minute)

	job := newTestJob("job-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "restored-vol", 100, jobUpdatedAt)

	metricsDB.On("GetRestoreTimestamp", mock.Anything).Return(nil, nil)
	metricsDB.On("UpdateRestoreTimestamp", mock.Anything, now).Return(nil)
	vcpDB.On("GetJobsWithCondition", mock.Anything, mock.Anything).Return([]*datamodel.Job{job}, nil)

	volume := newVolume("vol-1", "restored-vol", "acct-1", "deploy-1", "backup-1")
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{volume}, nil)

	backup := newCrossRegionBackup("backup-1", "backup-1", 1024*1024, "us-west2")
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "backup-1").Return(backup, nil)

	result, err := ProcessRestoreBillingMetrics(ctx, vcpDB, metricsDB, config, now)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	hm := result.HydratedMetricsDataModel[0]
	assert.Equal(t, metadata.CbsCrossRegionVolumeRestoreTransferBytes, hm.MeasuredType)
	assert.Equal(t, float64(1024*1024), hm.Quantity)
	assert.Equal(t, "acct-1", hm.ConsumerID)
	assert.Equal(t, metadata.Volume, hm.ResourceType)

	var extra map[string]string
	assert.NoError(t, json.Unmarshal(hm.Metadata, &extra))
	assert.Equal(t, "us-west2", extra["backup_region_name"])
	assert.Equal(t, "us-east4", extra["source_region_name"])

	metricsDB.AssertCalled(t, "UpdateRestoreTimestamp", mock.Anything, now)
}

func TestProcessRestoreBillingMetrics_UpdateTimestampError(t *testing.T) {
	metricsDB := new(mockCrossRegionMetricsStorage)
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()
	now := time.Now()

	metricsDB.On("GetRestoreTimestamp", mock.Anything).Return(nil, nil)
	metricsDB.On("UpdateRestoreTimestamp", mock.Anything, now).Return(errors.New("update failed"))

	result, err := ProcessRestoreBillingMetrics(ctx, vcpDB, metricsDB, config, now)
	assert.Error(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetricsDataModel)
}

func TestProcessRestoreBillingMetrics_MultipleJobs_TimestampAdvancedToNow(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	metricsDB := new(mockCrossRegionMetricsStorage)
	ctx := context.Background()
	config := defaultConfig()
	now := time.Now()

	job1Updated := now.Add(-10 * time.Minute)
	job2Updated := now.Add(-5 * time.Minute)

	job1 := newTestJob("job-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "vol-1", 100, job1Updated)
	job2 := newTestJob("job-2", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "vol-2", 100, job2Updated)

	metricsDB.On("GetRestoreTimestamp", mock.Anything).Return(nil, nil)
	metricsDB.On("UpdateRestoreTimestamp", mock.Anything, now).Return(nil)
	vcpDB.On("GetJobsWithCondition", mock.Anything, mock.Anything).Return([]*datamodel.Job{job1, job2}, nil)

	vol1 := newVolume("vol-uuid-1", "vol-1", "acct-1", "deploy-1", "backup-1")
	vol2 := newVolume("vol-uuid-2", "vol-2", "acct-1", "deploy-1", "backup-2")

	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.MatchedBy(func(c [][]interface{}) bool {
		if len(c) > 0 && len(c[0]) > 0 {
			s, ok := c[0][0].(string)
			if ok && s == "name = ? AND account_id = ?" {
				return c[0][1] == "vol-1"
			}
		}
		return false
	}), mock.Anything).Return([]*datamodel.Volume{vol1}, nil)

	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.MatchedBy(func(c [][]interface{}) bool {
		if len(c) > 0 && len(c[0]) > 0 {
			s, ok := c[0][0].(string)
			if ok && s == "name = ? AND account_id = ?" {
				return c[0][1] == "vol-2"
			}
		}
		return false
	}), mock.Anything).Return([]*datamodel.Volume{vol2}, nil)

	backup1 := newCrossRegionBackup("backup-1", "backup-1", 500, "us-west2")
	backup2 := newCrossRegionBackup("backup-2", "backup-2", 1000, "us-west2")
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "backup-1").Return(backup1, nil)
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "backup-2").Return(backup2, nil)

	result, err := ProcessRestoreBillingMetrics(ctx, vcpDB, metricsDB, config, now)
	assert.NoError(t, err)
	assert.Len(t, result.HydratedMetricsDataModel, 2)

	metricsDB.AssertCalled(t, "UpdateRestoreTimestamp", mock.Anything, now)
}

// =============================================================================
// processRestoreJob tests
// =============================================================================

func TestProcessRestoreJob_InvalidAccountID(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "job-no-acct"},
		AccountID: sql.NullInt64{Valid: false},
	}

	result := processRestoreJob(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestProcessRestoreJob_FailedFullRestore_Skipped(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-fail", string(models.JobTypeRestoreBackup), string(models.JobsStateERROR), "vol-fail", 100, time.Now())

	result := processRestoreJob(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestProcessRestoreJob_SFRJob_Dispatches(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()
	now := time.Now()

	job := newTestJob("job-sfr", string(models.JobTypeRestoreFilesBackup), string(models.JobsStateDONE), "vol-sfr", 100, now)
	job.BaseModel.ID = 42

	sfrMeta := &datamodel.SfrMetadata{
		FilesSize:  2048,
		FileCount:  5,
		BackupUUID: "sfr-backup-1",
		VolumeUUID: "sfr-vol-uuid",
	}
	vcpDB.On("GetSfrMetadataByJobID", mock.Anything, int64(42)).Return(sfrMeta, nil)

	backup := newCrossRegionBackup("sfr-backup-1", "sfr-backup-1", 10000, "eu-west1")
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "sfr-backup-1").Return(backup, nil)

	vol := newVolume("sfr-vol-uuid", "sfr-vol", "sfr-acct", "sfr-deploy", "")
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	result := processRestoreJob(ctx, vcpDB, config, job, now)
	assert.NotNil(t, result)
	assert.Equal(t, float64(2048), result.Quantity)
}

// =============================================================================
// createCrossRegionRestoreMetrics tests
// =============================================================================

func TestCreateCrossRegionRestoreMetrics_VolumeNotFound(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "missing-vol", 100, time.Now())
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil)

	result := createCrossRegionRestoreMetrics(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestCreateCrossRegionRestoreMetrics_VolumeQueryError(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "err-vol", 100, time.Now())
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("db err"))

	result := createCrossRegionRestoreMetrics(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestCreateCrossRegionRestoreMetrics_BackupNotFound(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "vol-1", 100, time.Now())
	vol := newVolume("vol-uuid", "vol-1", "acct", "deploy", "backup-missing")
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "backup-missing").Return(nil, errors.New("not found"))

	result := createCrossRegionRestoreMetrics(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestCreateCrossRegionRestoreMetrics_BackupVaultNil(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "vol-1", 100, time.Now())
	vol := newVolume("vol-uuid", "vol-1", "acct", "deploy", "backup-1")
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := &datamodel.Backup{
		BaseModel:   datamodel.BaseModel{UUID: "backup-1"},
		SizeInBytes: 1024,
		BackupVault: nil,
	}
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "backup-1").Return(backup, nil)

	result := createCrossRegionRestoreMetrics(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestCreateCrossRegionRestoreMetrics_NotCrossRegion(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "vol-1", 100, time.Now())
	vol := newVolume("vol-uuid", "vol-1", "acct", "deploy", "backup-1")
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := &datamodel.Backup{
		BaseModel:   datamodel.BaseModel{UUID: "backup-1"},
		SizeInBytes: 1024,
		BackupVault: &datamodel.BackupVault{
			BackupVaultType: "IN_REGION",
		},
	}
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "backup-1").Return(backup, nil)

	result := createCrossRegionRestoreMetrics(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestCreateCrossRegionRestoreMetrics_BackupRegionNil(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "vol-1", 100, time.Now())
	vol := newVolume("vol-uuid", "vol-1", "acct", "deploy", "backup-1")
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := &datamodel.Backup{
		BaseModel:   datamodel.BaseModel{UUID: "backup-1"},
		SizeInBytes: 1024,
		BackupVault: &datamodel.BackupVault{
			BackupVaultType:  activities.CrossRegionBackupType,
			BackupRegionName: nil,
		},
	}
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "backup-1").Return(backup, nil)

	result := createCrossRegionRestoreMetrics(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestCreateCrossRegionRestoreMetrics_BackupRegionMatchesCurrentRegion(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "vol-1", 100, time.Now())
	vol := newVolume("vol-uuid", "vol-1", "acct", "deploy", "backup-1")
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := newCrossRegionBackup("backup-1", "backup-1", 1024, config.RegionName)
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "backup-1").Return(backup, nil)

	result := createCrossRegionRestoreMetrics(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestCreateCrossRegionRestoreMetrics_ZeroBackupSize(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "vol-1", 100, time.Now())
	vol := newVolume("vol-uuid", "vol-1", "acct", "deploy", "backup-1")
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := newCrossRegionBackup("backup-1", "backup-1", 0, "us-west2")
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "backup-1").Return(backup, nil)

	result := createCrossRegionRestoreMetrics(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestCreateCrossRegionRestoreMetrics_Success(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()
	now := time.Now()

	job := newTestJob("job-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "vol-1", 100, now)
	vol := newVolume("vol-uuid", "vol-1", "acct", "deploy", "backup-1")
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := newCrossRegionBackup("backup-1", "backup-1", 5000, "us-west2")
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "backup-1").Return(backup, nil)

	result := createCrossRegionRestoreMetrics(ctx, vcpDB, config, job, now)
	assert.NotNil(t, result)
	assert.Equal(t, metadata.CbsCrossRegionVolumeRestoreTransferBytes, result.MeasuredType)
	assert.Equal(t, float64(5000), result.Quantity)
	assert.Equal(t, "acct", result.ConsumerID)
	assert.Equal(t, metadata.Volume, result.ResourceType)
}

// =============================================================================
// createCrossRegionRestoreMetrics — protocol / EnableFilesBackupBilling gating
// =============================================================================

func TestCreateCrossRegionRestoreMetrics_NilProtocols_Skipped(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()
	now := time.Now()

	job := newTestJob("job-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "vol-1", 100, now)
	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		Name:      "vol-1",
		Account:   &datamodel.Account{Name: "acct"},
		Pool:      &datamodel.Pool{DeploymentName: "deploy"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupID: "backup-1",
			AccountName:      "acct",
		},
	}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := newCrossRegionBackup("backup-1", "backup-1", 5000, "us-west2")
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "backup-1").Return(backup, nil)

	result := createCrossRegionRestoreMetrics(ctx, vcpDB, config, job, now)
	assert.Nil(t, result)
}

func TestCreateCrossRegionRestoreMetrics_SANProtocol_BillingDisabled_EmitsMetric(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()
	config.EnableFilesBackupBilling = false
	now := time.Now()

	job := newTestJob("job-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "vol-1", 100, now)
	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		Name:      "vol-1",
		Account:   &datamodel.Account{Name: "acct"},
		Pool:      &datamodel.Pool{DeploymentName: "deploy"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupID: "backup-1",
			AccountName:      "acct",
			Protocols:        []string{"ISCSI"},
		},
	}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := newCrossRegionBackup("backup-1", "backup-1", 5000, "us-west2")
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "backup-1").Return(backup, nil)

	result := createCrossRegionRestoreMetrics(ctx, vcpDB, config, job, now)
	assert.NotNil(t, result)
	assert.Equal(t, float64(5000), result.Quantity)
	assert.Equal(t, metadata.CbsCrossRegionVolumeRestoreTransferBytes, result.MeasuredType)
}

func TestCreateCrossRegionRestoreMetrics_NASProtocol_BillingEnabled_EmitsMetric(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()
	config.EnableFilesBackupBilling = true
	now := time.Now()

	job := newTestJob("job-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "vol-1", 100, now)
	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		Name:      "vol-1",
		Account:   &datamodel.Account{Name: "acct"},
		Pool:      &datamodel.Pool{DeploymentName: "deploy"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupID: "backup-1",
			AccountName:      "acct",
			Protocols:        []string{"NFSV3"},
		},
	}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := newCrossRegionBackup("backup-1", "backup-1", 5000, "us-west2")
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "backup-1").Return(backup, nil)

	result := createCrossRegionRestoreMetrics(ctx, vcpDB, config, job, now)
	assert.NotNil(t, result)
	assert.Equal(t, float64(5000), result.Quantity)
	assert.Equal(t, metadata.CbsCrossRegionVolumeRestoreTransferBytes, result.MeasuredType)
}

func TestCreateCrossRegionRestoreMetrics_NASProtocol_BillingDisabled_Skipped(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()
	config.EnableFilesBackupBilling = false
	now := time.Now()

	job := newTestJob("job-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "vol-1", 100, now)
	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		Name:      "vol-1",
		Account:   &datamodel.Account{Name: "acct"},
		Pool:      &datamodel.Pool{DeploymentName: "deploy"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupID: "backup-1",
			AccountName:      "acct",
			Protocols:        []string{"NFSV3"},
		},
	}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := newCrossRegionBackup("backup-1", "backup-1", 5000, "us-west2")
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "backup-1").Return(backup, nil)

	result := createCrossRegionRestoreMetrics(ctx, vcpDB, config, job, now)
	assert.Nil(t, result)
}

// =============================================================================
// createSfrCrossRegionRestoreMetrics tests
// =============================================================================

func TestCreateSfrCrossRegionRestoreMetrics_SfrMetadataNotFound(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-sfr-1", string(models.JobTypeRestoreFilesBackup), string(models.JobsStateDONE), "vol-sfr", 100, time.Now())
	job.BaseModel.ID = 10

	vcpDB.On("GetSfrMetadataByJobID", mock.Anything, int64(10)).Return(nil, errors.New("not found"))

	result := createSfrCrossRegionRestoreMetrics(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestCreateSfrCrossRegionRestoreMetrics_ZeroFileSize(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-sfr-2", string(models.JobTypeRestoreFilesBackup), string(models.JobsStateDONE), "vol-sfr", 100, time.Now())
	job.BaseModel.ID = 11

	sfrMeta := &datamodel.SfrMetadata{FilesSize: 0, BackupUUID: "b-1", VolumeUUID: "v-1"}
	vcpDB.On("GetSfrMetadataByJobID", mock.Anything, int64(11)).Return(sfrMeta, nil)

	result := createSfrCrossRegionRestoreMetrics(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestCreateSfrCrossRegionRestoreMetrics_BackupNotFound(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-sfr-3", string(models.JobTypeRestoreFilesBackup), string(models.JobsStateDONE), "vol-sfr", 100, time.Now())
	job.BaseModel.ID = 12

	sfrMeta := &datamodel.SfrMetadata{FilesSize: 100, BackupUUID: "b-missing", VolumeUUID: "v-1"}
	vcpDB.On("GetSfrMetadataByJobID", mock.Anything, int64(12)).Return(sfrMeta, nil)

	vol := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{UUID: "v-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{},
	}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "b-missing").Return(nil, errors.New("not found"))

	result := createSfrCrossRegionRestoreMetrics(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestCreateSfrCrossRegionRestoreMetrics_BackupNotFound_HardError(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-sfr-3b", string(models.JobTypeRestoreFilesBackup), string(models.JobsStateDONE), "vol-sfr", 100, time.Now())
	job.BaseModel.ID = 30

	sfrMeta := &datamodel.SfrMetadata{FilesSize: 100, BackupUUID: "b-err", VolumeUUID: "v-1"}
	vcpDB.On("GetSfrMetadataByJobID", mock.Anything, int64(30)).Return(sfrMeta, nil)

	vol := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "v-1"}}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "b-err").Return(nil, errors.New("connection refused"))

	result := createSfrCrossRegionRestoreMetrics(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestCreateSfrCrossRegionRestoreMetrics_BackupVaultNil(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-sfr-4", string(models.JobTypeRestoreFilesBackup), string(models.JobsStateDONE), "vol-sfr", 100, time.Now())
	job.BaseModel.ID = 13

	sfrMeta := &datamodel.SfrMetadata{FilesSize: 100, BackupUUID: "b-1", VolumeUUID: "v-1"}
	vcpDB.On("GetSfrMetadataByJobID", mock.Anything, int64(13)).Return(sfrMeta, nil)

	vol := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "v-1"}}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: "b-1"}, BackupVault: nil}
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "b-1").Return(backup, nil)

	result := createSfrCrossRegionRestoreMetrics(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestCreateSfrCrossRegionRestoreMetrics_NotCrossRegion(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-sfr-5", string(models.JobTypeRestoreFilesBackup), string(models.JobsStateDONE), "vol-sfr", 100, time.Now())
	job.BaseModel.ID = 14

	sfrMeta := &datamodel.SfrMetadata{FilesSize: 100, BackupUUID: "b-1", VolumeUUID: "v-1"}
	vcpDB.On("GetSfrMetadataByJobID", mock.Anything, int64(14)).Return(sfrMeta, nil)

	vol := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "v-1"}}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := &datamodel.Backup{
		BaseModel:   datamodel.BaseModel{UUID: "b-1"},
		BackupVault: &datamodel.BackupVault{BackupVaultType: "IN_REGION"},
	}
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "b-1").Return(backup, nil)

	result := createSfrCrossRegionRestoreMetrics(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestCreateSfrCrossRegionRestoreMetrics_BackupRegionNil(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-sfr-nil-region", string(models.JobTypeRestoreFilesBackup), string(models.JobsStateDONE), "vol-sfr", 100, time.Now())
	job.BaseModel.ID = 20

	sfrMeta := &datamodel.SfrMetadata{FilesSize: 100, BackupUUID: "b-1", VolumeUUID: "v-1"}
	vcpDB.On("GetSfrMetadataByJobID", mock.Anything, int64(20)).Return(sfrMeta, nil)

	vol := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "v-1"}}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := &datamodel.Backup{
		BaseModel: datamodel.BaseModel{UUID: "b-1"},
		BackupVault: &datamodel.BackupVault{
			BackupVaultType:  activities.CrossRegionBackupType,
			BackupRegionName: nil,
		},
	}
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "b-1").Return(backup, nil)

	result := createSfrCrossRegionRestoreMetrics(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestCreateSfrCrossRegionRestoreMetrics_BackupRegionMatchesCurrentRegion(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-sfr-same-region", string(models.JobTypeRestoreFilesBackup), string(models.JobsStateDONE), "vol-sfr", 100, time.Now())
	job.BaseModel.ID = 21

	sfrMeta := &datamodel.SfrMetadata{FilesSize: 100, BackupUUID: "b-1", VolumeUUID: "v-1"}
	vcpDB.On("GetSfrMetadataByJobID", mock.Anything, int64(21)).Return(sfrMeta, nil)

	vol := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "v-1"}}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := newCrossRegionBackup("b-1", "b-1", 5000, config.RegionName)
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "b-1").Return(backup, nil)

	result := createSfrCrossRegionRestoreMetrics(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestCreateSfrCrossRegionRestoreMetrics_VolumeNotFound(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-sfr-6", string(models.JobTypeRestoreFilesBackup), string(models.JobsStateDONE), "vol-sfr", 100, time.Now())
	job.BaseModel.ID = 15

	sfrMeta := &datamodel.SfrMetadata{FilesSize: 100, BackupUUID: "b-1", VolumeUUID: "v-missing"}
	vcpDB.On("GetSfrMetadataByJobID", mock.Anything, int64(15)).Return(sfrMeta, nil)

	backup := newCrossRegionBackup("b-1", "b-1", 5000, "eu-west1")
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "b-1").Return(backup, nil)

	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil)

	result := createSfrCrossRegionRestoreMetrics(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestCreateSfrCrossRegionRestoreMetrics_Success(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()
	now := time.Now()

	job := newTestJob("job-sfr-ok", string(models.JobTypeRestoreFilesBackup), string(models.JobsStateDONE), "vol-sfr", 100, now)
	job.BaseModel.ID = 16

	sfrMeta := &datamodel.SfrMetadata{FilesSize: 4096, FileCount: 10, BackupUUID: "sfr-b-1", VolumeUUID: "sfr-v-uuid"}
	vcpDB.On("GetSfrMetadataByJobID", mock.Anything, int64(16)).Return(sfrMeta, nil)

	backup := newCrossRegionBackup("sfr-b-1", "sfr-b-1", 10000, "eu-west1")
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "sfr-b-1").Return(backup, nil)

	vol := newVolume("sfr-v-uuid", "sfr-vol", "sfr-acct", "sfr-deploy", "")
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	result := createSfrCrossRegionRestoreMetrics(ctx, vcpDB, config, job, now)
	assert.NotNil(t, result)
	assert.Equal(t, float64(4096), result.Quantity)
	assert.Equal(t, metadata.CbsCrossRegionVolumeRestoreTransferBytes, result.MeasuredType)
	assert.Equal(t, "sfr-acct", result.ConsumerID)
}

// =============================================================================
// createSfrCrossRegionRestoreMetrics — protocol / EnableFilesBackupBilling gating
// =============================================================================

func TestCreateSfrCrossRegionRestoreMetrics_NilProtocols_Skipped(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	job := newTestJob("job-sfr-np", string(models.JobTypeRestoreFilesBackup), string(models.JobsStateDONE), "vol-sfr", 100, time.Now())
	job.BaseModel.ID = 50

	sfrMeta := &datamodel.SfrMetadata{FilesSize: 4096, BackupUUID: "sfr-b-1", VolumeUUID: "sfr-v-uuid"}
	vcpDB.On("GetSfrMetadataByJobID", mock.Anything, int64(50)).Return(sfrMeta, nil)

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "sfr-v-uuid"},
		Name:      "sfr-vol",
		Account:   &datamodel.Account{Name: "sfr-acct"},
		Pool:      &datamodel.Pool{DeploymentName: "sfr-deploy"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			AccountName: "sfr-acct",
		},
	}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := newCrossRegionBackup("sfr-b-1", "sfr-b-1", 10000, "eu-west1")
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "sfr-b-1").Return(backup, nil)

	result := createSfrCrossRegionRestoreMetrics(ctx, vcpDB, config, job, time.Now())
	assert.Nil(t, result)
}

func TestCreateSfrCrossRegionRestoreMetrics_SANProtocol_BillingDisabled_EmitsMetric(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()
	config.EnableFilesBackupBilling = false
	now := time.Now()

	job := newTestJob("job-sfr-san", string(models.JobTypeRestoreFilesBackup), string(models.JobsStateDONE), "vol-sfr", 100, now)
	job.BaseModel.ID = 51

	sfrMeta := &datamodel.SfrMetadata{FilesSize: 4096, FileCount: 5, BackupUUID: "sfr-b-1", VolumeUUID: "sfr-v-uuid"}
	vcpDB.On("GetSfrMetadataByJobID", mock.Anything, int64(51)).Return(sfrMeta, nil)

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "sfr-v-uuid"},
		Name:      "sfr-vol",
		Account:   &datamodel.Account{Name: "sfr-acct"},
		Pool:      &datamodel.Pool{DeploymentName: "sfr-deploy"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			AccountName: "sfr-acct",
			Protocols:   []string{"ISCSI"},
		},
	}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := newCrossRegionBackup("sfr-b-1", "sfr-b-1", 10000, "eu-west1")
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "sfr-b-1").Return(backup, nil)

	result := createSfrCrossRegionRestoreMetrics(ctx, vcpDB, config, job, now)
	assert.NotNil(t, result)
	assert.Equal(t, float64(4096), result.Quantity)
	assert.Equal(t, metadata.CbsCrossRegionVolumeRestoreTransferBytes, result.MeasuredType)
}

func TestCreateSfrCrossRegionRestoreMetrics_NASProtocol_BillingEnabled_EmitsMetric(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()
	config.EnableFilesBackupBilling = true
	now := time.Now()

	job := newTestJob("job-sfr-nas-en", string(models.JobTypeRestoreFilesBackup), string(models.JobsStateDONE), "vol-sfr", 100, now)
	job.BaseModel.ID = 52

	sfrMeta := &datamodel.SfrMetadata{FilesSize: 4096, FileCount: 5, BackupUUID: "sfr-b-1", VolumeUUID: "sfr-v-uuid"}
	vcpDB.On("GetSfrMetadataByJobID", mock.Anything, int64(52)).Return(sfrMeta, nil)

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "sfr-v-uuid"},
		Name:      "sfr-vol",
		Account:   &datamodel.Account{Name: "sfr-acct"},
		Pool:      &datamodel.Pool{DeploymentName: "sfr-deploy"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			AccountName: "sfr-acct",
			Protocols:   []string{"NFSV3"},
		},
	}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := newCrossRegionBackup("sfr-b-1", "sfr-b-1", 10000, "eu-west1")
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "sfr-b-1").Return(backup, nil)

	result := createSfrCrossRegionRestoreMetrics(ctx, vcpDB, config, job, now)
	assert.NotNil(t, result)
	assert.Equal(t, float64(4096), result.Quantity)
	assert.Equal(t, metadata.CbsCrossRegionVolumeRestoreTransferBytes, result.MeasuredType)
}

func TestCreateSfrCrossRegionRestoreMetrics_NASProtocol_BillingDisabled_Skipped(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()
	config.EnableFilesBackupBilling = false
	now := time.Now()

	job := newTestJob("job-sfr-nas-dis", string(models.JobTypeRestoreFilesBackup), string(models.JobsStateDONE), "vol-sfr", 100, now)
	job.BaseModel.ID = 53

	sfrMeta := &datamodel.SfrMetadata{FilesSize: 4096, FileCount: 5, BackupUUID: "sfr-b-1", VolumeUUID: "sfr-v-uuid"}
	vcpDB.On("GetSfrMetadataByJobID", mock.Anything, int64(53)).Return(sfrMeta, nil)

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "sfr-v-uuid"},
		Name:      "sfr-vol",
		Account:   &datamodel.Account{Name: "sfr-acct"},
		Pool:      &datamodel.Pool{DeploymentName: "sfr-deploy"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			AccountName: "sfr-acct",
			Protocols:   []string{"NFSV3"},
		},
	}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := newCrossRegionBackup("sfr-b-1", "sfr-b-1", 10000, "eu-west1")
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "sfr-b-1").Return(backup, nil)

	result := createSfrCrossRegionRestoreMetrics(ctx, vcpDB, config, job, now)
	assert.Nil(t, result)
}

// =============================================================================
// FetchSourceBackupForRestoredVolumeUsingIDOrBackupPath tests
// =============================================================================

func TestFetchSourceBackup_NilVolumeAttributes(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	vol := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "v-1"}, VolumeAttributes: nil}
	job := newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "v-1", 100, time.Now())

	backup, err := FetchSourceBackupForRestoredVolumeUsingIDOrBackupPath(ctx, vcpDB, vol, job, config)
	assert.NoError(t, err)
	assert.Nil(t, backup)
}

func TestFetchSourceBackup_ByRestoredBackupID_Success(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	vol := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{UUID: "v-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{RestoredBackupID: "b-by-id"},
	}
	job := newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "v-1", 100, time.Now())

	expectedBackup := &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: "b-by-id"}}
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "b-by-id").Return(expectedBackup, nil)

	backup, err := FetchSourceBackupForRestoredVolumeUsingIDOrBackupPath(ctx, vcpDB, vol, job, config)
	assert.NoError(t, err)
	assert.Equal(t, expectedBackup, backup)
}

func TestFetchSourceBackup_ByRestoredBackupID_NotFound_FallsThrough(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	vol := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{UUID: "v-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{RestoredBackupID: "b-missing"},
	}
	job := newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "v-1", 100, time.Now())

	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "b-missing").Return(nil, errors.New("not found"))

	backup, err := FetchSourceBackupForRestoredVolumeUsingIDOrBackupPath(ctx, vcpDB, vol, job, config)
	assert.NoError(t, err)
	assert.Nil(t, backup)
}

func TestFetchSourceBackup_ByRestoredBackupID_HardError(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	vol := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{UUID: "v-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{RestoredBackupID: "b-err"},
	}
	job := newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "v-1", 100, time.Now())

	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "b-err").Return(nil, errors.New("connection refused"))

	backup, err := FetchSourceBackupForRestoredVolumeUsingIDOrBackupPath(ctx, vcpDB, vol, job, config)
	assert.Error(t, err)
	assert.Nil(t, backup)
}

func TestFetchSourceBackup_NoIDOrPath(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	config := defaultConfig()

	vol := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{UUID: "v-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{},
	}
	job := newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "v-1", 100, time.Now())

	backup, err := FetchSourceBackupForRestoredVolumeUsingIDOrBackupPath(ctx, vcpDB, vol, job, config)
	assert.NoError(t, err)
	assert.Nil(t, backup)
}

// =============================================================================
// FetchSourceBackupByResourcePath tests (via google-proxy)
// =============================================================================

func setupGProxyMocks(t *testing.T, mockInvoker *googleproxyclient.MockInvoker) func() {
	t.Helper()
	origGetRemoteRegionConfig := getRemoteRegionConfig
	origGetGProxyClient := getGProxyClient

	getRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
		return "localhost:8080", "test-jwt-token", nil
	}
	getGProxyClient = func(basePath, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
		return &googleproxyclient.ProxyClient{Invoker: mockInvoker}
	}

	return func() {
		getRemoteRegionConfig = origGetRemoteRegionConfig
		getGProxyClient = origGetGProxyClient
	}
}

func TestFetchSourceBackupByResourcePath_InvalidPath(t *testing.T) {
	ctx := context.Background()
	config := defaultConfig()

	vol := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{UUID: "v-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{RestoredBackupPath: "invalid/path"},
	}
	job := newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "v-1", 100, time.Now())

	backup, err := FetchSourceBackupByResourcePath(ctx, vol, job, config)
	assert.Error(t, err)
	assert.Nil(t, backup)
	assert.Contains(t, err.Error(), "cannot parse backup path")
}

func TestFetchSourceBackupByResourcePath_GetRemoteRegionConfigError(t *testing.T) {
	ctx := context.Background()
	config := defaultConfig()

	origGetRemoteRegionConfig := getRemoteRegionConfig
	defer func() { getRemoteRegionConfig = origGetRemoteRegionConfig }()

	getRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
		return "", "", errors.New("region config unavailable")
	}

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "v-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupPath: "projects/1088371202435/locations/us-central1/backupVaults/vcp-crb-bv-22-feb/backups/test-backup",
		},
	}
	job := newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "v-1", 100, time.Now())

	backup, err := FetchSourceBackupByResourcePath(ctx, vol, job, config)
	assert.Error(t, err)
	assert.Nil(t, backup)
	assert.Contains(t, err.Error(), "region config")
}

func TestFetchSourceBackupByResourcePath_ListBackupVaultsError(t *testing.T) {
	ctx := context.Background()
	config := defaultConfig()
	mockInvoker := new(googleproxyclient.MockInvoker)
	cleanup := setupGProxyMocks(t, mockInvoker)
	defer cleanup()

	mockInvoker.On("V1betaListBackupVaults", mock.Anything, mock.Anything).
		Return(nil, errors.New("google-proxy unavailable"))

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "v-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupPath: "projects/1088371202435/locations/us-east4/backupVaults/vcp-crb-bv-22-feb-destination-8178/backups/test-backup",
		},
	}
	job := newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "v-1", 100, time.Now())

	backup, err := FetchSourceBackupByResourcePath(ctx, vol, job, config)
	assert.Error(t, err)
	assert.Nil(t, backup)
	assert.Contains(t, err.Error(), "ListBackupVaults failed")
}

func TestFetchSourceBackupByResourcePath_VaultNotFound(t *testing.T) {
	ctx := context.Background()
	config := defaultConfig()
	mockInvoker := new(googleproxyclient.MockInvoker)
	cleanup := setupGProxyMocks(t, mockInvoker)
	defer cleanup()

	mockInvoker.On("V1betaListBackupVaults", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupVaultsOK{
			BackupVaults: []googleproxyclient.BackupVaultV1beta{},
		}, nil)

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "v-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupPath: "projects/p/locations/l/backupVaults/missing-vault/backups/b",
		},
	}
	job := newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "v-1", 100, time.Now())

	backup, err := FetchSourceBackupByResourcePath(ctx, vol, job, config)
	assert.Error(t, err)
	assert.Nil(t, backup)
	assert.Contains(t, err.Error(), "not found via google-proxy")
}

func TestFetchSourceBackupByResourcePath_VaultFoundByResourceId_BackupFound(t *testing.T) {
	ctx := context.Background()
	config := defaultConfig()
	mockInvoker := new(googleproxyclient.MockInvoker)
	cleanup := setupGProxyMocks(t, mockInvoker)
	defer cleanup()

	mockInvoker.On("V1betaListBackupVaults", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupVaultsOK{
			BackupVaults: []googleproxyclient.BackupVaultV1beta{
				{
					BackupVaultId:          googleproxyclient.NewOptString("72f15309-7aa7-d7a0-4c85-8a22fe55bfc1"),
					ResourceId:             "vcp-crb-bv-22-feb-destination-8178",
					BackupRegion:           googleproxyclient.NewOptString("us-east4"),
					SourceRegion:           googleproxyclient.NewOptString("us-central1"),
					BackupVaultType:        googleproxyclient.NewOptBackupVaultV1betaBackupVaultType(googleproxyclient.BackupVaultV1betaBackupVaultTypeCROSSREGION),
					State:                  googleproxyclient.NewOptBackupVaultV1betaState(googleproxyclient.BackupVaultV1betaStateREADY),
					DestinationBackupVault: googleproxyclient.NewOptString("projects/1088371202435/locations/us-central1/backupVaults/vcp-crb-bv-22-feb"),
				},
			},
		}, nil)

	mockInvoker.On("V1betaListBackups", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupsOK{
			Backups: []googleproxyclient.BackupV1beta{
				{
					ResourceId:       googleproxyclient.NewOptString("test-backup"),
					BackupId:         googleproxyclient.NewOptString("backup-uuid-123"),
					VolumeUsageBytes: googleproxyclient.NewOptInt64(1024 * 1024),
					State:            googleproxyclient.NewOptBackupV1betaState(googleproxyclient.BackupV1betaStateREADY),
					BackupType:       googleproxyclient.NewOptBackupV1betaBackupType(googleproxyclient.BackupV1betaBackupTypeMANUAL),
				},
			},
		}, nil)

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "v-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupPath: "projects/1088371202435/locations/us-east4/backupVaults/vcp-crb-bv-22-feb-destination-8178/backups/test-backup",
		},
	}
	job := newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "v-1", 100, time.Now())

	backup, err := FetchSourceBackupByResourcePath(ctx, vol, job, config)
	assert.NoError(t, err)
	assert.NotNil(t, backup)
	assert.Equal(t, "backup-uuid-123", backup.UUID)
	assert.Equal(t, "test-backup", backup.Name)
	assert.Equal(t, int64(1024*1024), backup.SizeInBytes)
	assert.NotNil(t, backup.BackupVault)
	assert.Equal(t, "vcp-crb-bv-22-feb-destination-8178", backup.BackupVault.Name)
	assert.Equal(t, "CROSS_REGION", backup.BackupVault.BackupVaultType)
}

func TestFetchSourceBackupByResourcePath_VaultFoundByCrossRegionVaultName(t *testing.T) {
	ctx := context.Background()
	config := defaultConfig()
	mockInvoker := new(googleproxyclient.MockInvoker)
	cleanup := setupGProxyMocks(t, mockInvoker)
	defer cleanup()

	// Path points to source vault (vcp-crb-bv-22-feb in us-central1),
	// but we're running from us-east4. The local vault's DestinationBackupVault
	// matches the full path of the source vault.
	mockInvoker.On("V1betaListBackupVaults", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupVaultsOK{
			BackupVaults: []googleproxyclient.BackupVaultV1beta{
				{
					BackupVaultId:          googleproxyclient.NewOptString("72f15309-7aa7-d7a0-4c85-8a22fe55bfc1"),
					ResourceId:             "vcp-crb-bv-22-feb-destination-8178",
					BackupRegion:           googleproxyclient.NewOptString("us-east4"),
					SourceRegion:           googleproxyclient.NewOptString("us-central1"),
					BackupVaultType:        googleproxyclient.NewOptBackupVaultV1betaBackupVaultType(googleproxyclient.BackupVaultV1betaBackupVaultTypeCROSSREGION),
					State:                  googleproxyclient.NewOptBackupVaultV1betaState(googleproxyclient.BackupVaultV1betaStateREADY),
					DestinationBackupVault: googleproxyclient.NewOptString("projects/1088371202435/locations/us-central1/backupVaults/vcp-crb-bv-22-feb"),
				},
			},
		}, nil)

	mockInvoker.On("V1betaListBackups", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupsOK{
			Backups: []googleproxyclient.BackupV1beta{
				{
					ResourceId:       googleproxyclient.NewOptString("my-backup"),
					BackupId:         googleproxyclient.NewOptString("bkp-uuid-456"),
					VolumeUsageBytes: googleproxyclient.NewOptInt64(2048),
					State:            googleproxyclient.NewOptBackupV1betaState(googleproxyclient.BackupV1betaStateREADY),
					BackupType:       googleproxyclient.NewOptBackupV1betaBackupType(googleproxyclient.BackupV1betaBackupTypeMANUAL),
				},
			},
		}, nil)

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "v-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupPath: "projects/1088371202435/locations/us-central1/backupVaults/vcp-crb-bv-22-feb/backups/my-backup",
		},
	}
	job := newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "v-1", 100, time.Now())

	backup, err := FetchSourceBackupByResourcePath(ctx, vol, job, config)
	assert.NoError(t, err)
	assert.NotNil(t, backup)
	assert.Equal(t, "bkp-uuid-456", backup.UUID)
	assert.Equal(t, "my-backup", backup.Name)
}

func TestFetchSourceBackupByResourcePath_ListBackupsError(t *testing.T) {
	ctx := context.Background()
	config := defaultConfig()
	mockInvoker := new(googleproxyclient.MockInvoker)
	cleanup := setupGProxyMocks(t, mockInvoker)
	defer cleanup()

	mockInvoker.On("V1betaListBackupVaults", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupVaultsOK{
			BackupVaults: []googleproxyclient.BackupVaultV1beta{
				{
					BackupVaultId: googleproxyclient.NewOptString("vault-uuid"),
					ResourceId:    "the-vault",
					State:         googleproxyclient.NewOptBackupVaultV1betaState(googleproxyclient.BackupVaultV1betaStateREADY),
				},
			},
		}, nil)

	mockInvoker.On("V1betaListBackups", mock.Anything, mock.Anything).
		Return(nil, errors.New("backup list unavailable"))

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "v-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupPath: "projects/p/locations/l/backupVaults/the-vault/backups/some-backup",
		},
	}
	job := newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "v-1", 100, time.Now())

	backup, err := FetchSourceBackupByResourcePath(ctx, vol, job, config)
	assert.Error(t, err)
	assert.Nil(t, backup)
	assert.Contains(t, err.Error(), "ListBackups failed")
}

func TestFetchSourceBackupByResourcePath_BackupNotFoundInVault(t *testing.T) {
	ctx := context.Background()
	config := defaultConfig()
	mockInvoker := new(googleproxyclient.MockInvoker)
	cleanup := setupGProxyMocks(t, mockInvoker)
	defer cleanup()

	mockInvoker.On("V1betaListBackupVaults", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupVaultsOK{
			BackupVaults: []googleproxyclient.BackupVaultV1beta{
				{
					BackupVaultId: googleproxyclient.NewOptString("vault-uuid"),
					ResourceId:    "the-vault",
					State:         googleproxyclient.NewOptBackupVaultV1betaState(googleproxyclient.BackupVaultV1betaStateREADY),
				},
			},
		}, nil)

	mockInvoker.On("V1betaListBackups", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupsOK{
			Backups: []googleproxyclient.BackupV1beta{
				{
					ResourceId: googleproxyclient.NewOptString("other-backup"),
					BackupId:   googleproxyclient.NewOptString("other-uuid"),
				},
			},
		}, nil)

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "v-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupPath: "projects/p/locations/l/backupVaults/the-vault/backups/missing-backup",
		},
	}
	job := newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "v-1", 100, time.Now())

	backup, err := FetchSourceBackupByResourcePath(ctx, vol, job, config)
	assert.Error(t, err)
	assert.Nil(t, backup)
	assert.Contains(t, err.Error(), "not found in vault")
}

func TestFetchSourceBackupByResourcePath_UnexpectedListVaultsResponse(t *testing.T) {
	ctx := context.Background()
	config := defaultConfig()
	mockInvoker := new(googleproxyclient.MockInvoker)
	cleanup := setupGProxyMocks(t, mockInvoker)
	defer cleanup()

	mockInvoker.On("V1betaListBackupVaults", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupVaultsBadRequest{}, nil)

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "v-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupPath: "projects/p/locations/l/backupVaults/the-vault/backups/b",
		},
	}
	job := newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "v-1", 100, time.Now())

	backup, err := FetchSourceBackupByResourcePath(ctx, vol, job, config)
	assert.Error(t, err)
	assert.Nil(t, backup)
	assert.Contains(t, err.Error(), "unexpected response")
}

func TestFetchSourceBackupByResourcePath_UnexpectedListBackupsResponse(t *testing.T) {
	ctx := context.Background()
	config := defaultConfig()
	mockInvoker := new(googleproxyclient.MockInvoker)
	cleanup := setupGProxyMocks(t, mockInvoker)
	defer cleanup()

	mockInvoker.On("V1betaListBackupVaults", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupVaultsOK{
			BackupVaults: []googleproxyclient.BackupVaultV1beta{
				{
					BackupVaultId: googleproxyclient.NewOptString("vault-uuid"),
					ResourceId:    "the-vault",
					State:         googleproxyclient.NewOptBackupVaultV1betaState(googleproxyclient.BackupVaultV1betaStateREADY),
				},
			},
		}, nil)

	mockInvoker.On("V1betaListBackups", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupsBadRequest{}, nil)

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "v-1"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupPath: "projects/p/locations/l/backupVaults/the-vault/backups/b",
		},
	}
	job := newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "v-1", 100, time.Now())

	backup, err := FetchSourceBackupByResourcePath(ctx, vol, job, config)
	assert.Error(t, err)
	assert.Nil(t, backup)
	assert.Contains(t, err.Error(), "unexpected response")
}

// =============================================================================
// parseRestoredBackupPath tests
// =============================================================================

func TestParseRestoredBackupPath_Valid(t *testing.T) {
	vault, vaultResourcePath, backup, err := parseRestoredBackupPath("projects/p/locations/l/backupVaults/myVault/backups/myBackup")
	assert.NoError(t, err)
	assert.Equal(t, "myVault", vault)
	assert.Equal(t, "projects/p/locations/l/backupVaults/myVault", vaultResourcePath)
	assert.Equal(t, "myBackup", backup)
}

func TestParseRestoredBackupPath_Invalid(t *testing.T) {
	_, _, _, err := parseRestoredBackupPath("invalid/path/without/vaults")
	assert.Error(t, err)
}

func TestParseRestoredBackupPath_MissingBackups(t *testing.T) {
	_, _, _, err := parseRestoredBackupPath("projects/p/locations/l/backupVaults/myVault")
	assert.Error(t, err)
}

// =============================================================================
// fetchRestoreJobs tests
// =============================================================================

func TestFetchRestoreJobs_WithoutSFR(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	now := time.Now()
	since := now.Add(-1 * time.Hour)

	jobs := []*datamodel.Job{
		newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "v-1", 1, now),
	}
	vcpDB.On("GetJobsWithCondition", mock.Anything, mock.Anything).Return(jobs, nil)

	result, err := fetchRestoreJobs(ctx, vcpDB, since, now, false)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
}

func TestFetchRestoreJobs_WithSFR(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()
	now := time.Now()
	since := now.Add(-1 * time.Hour)

	jobs := []*datamodel.Job{
		newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "v-1", 1, now),
		newTestJob("j-2", string(models.JobTypeRestoreFilesBackup), string(models.JobsStateDONE), "v-2", 1, now),
	}
	vcpDB.On("GetJobsWithCondition", mock.Anything, mock.Anything).Return(jobs, nil)

	result, err := fetchRestoreJobs(ctx, vcpDB, since, now, true)
	assert.NoError(t, err)
	assert.Len(t, result, 2)
}

// =============================================================================
// assembleMetadata tests
// =============================================================================

func TestAssembleMetadata_WithPool(t *testing.T) {
	config := defaultConfig()
	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		Name:      "test-vol",
		Account:   &datamodel.Account{Name: "acct-name"},
		Pool:      &datamodel.Pool{DeploymentName: "pool-deploy"},
	}
	backup := newCrossRegionBackup("b-1", "b-1", 9999, "us-west2")

	met := assembleMetadata(vol, backup, config)
	assert.Equal(t, "vol-uuid", *met.ResourceUUID)
	assert.Equal(t, metadata.Volume, met.ResourceType)
	assert.Equal(t, "test-vol", *met.ResourceName)
	assert.Equal(t, "us-east4", *met.RegionName)
	assert.Equal(t, int64(9999), *met.SizeInBytes)
	assert.Equal(t, "pool-deploy", *met.DeploymentName)
	assert.Equal(t, "acct-name", *met.AccountName)
	assert.Equal(t, "us-west2", *met.BackupRegionName)
	assert.Equal(t, "us-east4", *met.SourceRegionName)
}

func TestAssembleMetadata_WithVolumeAttributesDeployment(t *testing.T) {
	config := defaultConfig()
	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		Name:      "test-vol",
		Pool:      nil,
		VolumeAttributes: &datamodel.VolumeAttributes{
			DeploymentName: "attr-deploy",
			AccountName:    "attr-acct",
		},
	}
	backup := newCrossRegionBackup("b-1", "b-1", 100, "eu-west1")

	met := assembleMetadata(vol, backup, config)
	assert.Equal(t, "attr-deploy", *met.DeploymentName)
}

func TestAssembleMetadata_EmptyDeployment(t *testing.T) {
	config := defaultConfig()
	vol := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{UUID: "vol-uuid"},
		Name:             "test-vol",
		Pool:             nil,
		VolumeAttributes: nil,
	}
	backup := newCrossRegionBackup("b-1", "b-1", 100, "eu-west1")

	met := assembleMetadata(vol, backup, config)
	assert.Equal(t, EmptyDeploymentName, *met.DeploymentName)
}

func TestAssembleMetadata_NoBackupRegion(t *testing.T) {
	config := defaultConfig()
	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		Name:      "test-vol",
		Pool:      &datamodel.Pool{DeploymentName: "d"},
	}
	backup := &datamodel.Backup{
		BaseModel:   datamodel.BaseModel{UUID: "b-1"},
		SizeInBytes: 100,
		BackupVault: &datamodel.BackupVault{
			BackupVaultType:  activities.CrossRegionBackupType,
			BackupRegionName: nil,
		},
	}

	met := assembleMetadata(vol, backup, config)
	assert.Nil(t, met.BackupRegionName)
	assert.Equal(t, "us-east4", *met.SourceRegionName)
}

// =============================================================================
// getVolumeAccountName tests
// =============================================================================

func TestGetVolumeAccountName_FromAccount(t *testing.T) {
	vol := &datamodel.Volume{Account: &datamodel.Account{Name: "from-account"}}
	assert.Equal(t, "from-account", getVolumeAccountName(vol))
}

func TestGetVolumeAccountName_FromVolumeAttributes(t *testing.T) {
	vol := &datamodel.Volume{
		Account:          nil,
		VolumeAttributes: &datamodel.VolumeAttributes{AccountName: "from-attrs"},
	}
	assert.Equal(t, "from-attrs", getVolumeAccountName(vol))
}

func TestGetVolumeAccountName_Empty(t *testing.T) {
	vol := &datamodel.Volume{Account: nil, VolumeAttributes: nil}
	assert.Equal(t, "", getVolumeAccountName(vol))
}

func TestGetVolumeAccountName_EmptyAccountName(t *testing.T) {
	vol := &datamodel.Volume{
		Account:          &datamodel.Account{Name: ""},
		VolumeAttributes: &datamodel.VolumeAttributes{AccountName: "fallback"},
	}
	assert.Equal(t, "fallback", getVolumeAccountName(vol))
}

// =============================================================================
// setRegionMetadataForBilling tests
// =============================================================================

func TestSetRegionMetadataForBilling_NilHM(t *testing.T) {
	rm := metadata.ResourceMetadata{}
	rm.SetBackupRegionName("region")
	setRegionMetadataForBilling(nil, rm)
}

func TestSetRegionMetadataForBilling_NoBothRegions(t *testing.T) {
	hm := &datamodel2.HydratedMetrics{}
	rm := metadata.ResourceMetadata{}
	setRegionMetadataForBilling(hm, rm)
	assert.Nil(t, hm.Metadata)
}

func TestSetRegionMetadataForBilling_BackupRegionOnly(t *testing.T) {
	hm := &datamodel2.HydratedMetrics{}
	rm := metadata.ResourceMetadata{}
	rm.SetBackupRegionName("us-west2")
	setRegionMetadataForBilling(hm, rm)

	var extra map[string]string
	assert.NoError(t, json.Unmarshal(hm.Metadata, &extra))
	assert.Equal(t, "us-west2", extra["backup_region_name"])
	_, ok := extra["source_region_name"]
	assert.False(t, ok)
}

func TestSetRegionMetadataForBilling_SourceRegionOnly(t *testing.T) {
	hm := &datamodel2.HydratedMetrics{}
	rm := metadata.ResourceMetadata{}
	rm.SetSourceRegionName("us-east4")
	setRegionMetadataForBilling(hm, rm)

	var extra map[string]string
	assert.NoError(t, json.Unmarshal(hm.Metadata, &extra))
	assert.Equal(t, "us-east4", extra["source_region_name"])
}

func TestSetRegionMetadataForBilling_BothRegions(t *testing.T) {
	hm := &datamodel2.HydratedMetrics{}
	rm := metadata.ResourceMetadata{}
	rm.SetBackupRegionName("us-west2")
	rm.SetSourceRegionName("us-east4")
	setRegionMetadataForBilling(hm, rm)

	var extra map[string]string
	assert.NoError(t, json.Unmarshal(hm.Metadata, &extra))
	assert.Equal(t, "us-west2", extra["backup_region_name"])
	assert.Equal(t, "us-east4", extra["source_region_name"])
}

// =============================================================================
// getRestoredVolume / getSfrRestoredVolume tests
// =============================================================================

func TestGetRestoredVolume_Success(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()

	job := newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "vol-name", 100, time.Now())
	vol := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid"}}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	result, err := getRestoredVolume(ctx, vcpDB, job)
	assert.NoError(t, err)
	assert.Equal(t, "vol-uuid", result.UUID)
}

func TestGetRestoredVolume_NotFound(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()

	job := newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "missing", 100, time.Now())
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil)

	result, err := getRestoredVolume(ctx, vcpDB, job)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestGetRestoredVolume_Error(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()

	job := newTestJob("j-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "err-vol", 100, time.Now())
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("db error"))

	result, err := getRestoredVolume(ctx, vcpDB, job)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestGetSfrRestoredVolume_Success(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()

	vol := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "sfr-vol-uuid"}}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	result, err := getSfrRestoredVolume(ctx, vcpDB, "sfr-vol-uuid")
	assert.NoError(t, err)
	assert.Equal(t, "sfr-vol-uuid", result.UUID)
}

func TestGetSfrRestoredVolume_NotFound(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()

	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil)

	result, err := getSfrRestoredVolume(ctx, vcpDB, "missing-uuid")
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestGetSfrRestoredVolume_Error(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	ctx := context.Background()

	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("err"))

	result, err := getSfrRestoredVolume(ctx, vcpDB, "err-uuid")
	assert.Error(t, err)
	assert.Nil(t, result)
}

// =============================================================================
// Full SFR E2E through ProcessRestoreBillingMetrics
// =============================================================================

func TestProcessRestoreBillingMetrics_SFREnabled_Success(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	metricsDB := new(mockCrossRegionMetricsStorage)
	ctx := context.Background()
	config := defaultConfig()
	config.EnableSFRCrossRegionRestoreBilling = true
	now := time.Now()
	jobUpdatedAt := now.Add(-3 * time.Minute)

	sfrJob := newTestJob("sfr-job-e2e", string(models.JobTypeRestoreFilesBackup), string(models.JobsStateDONE), "sfr-vol", 200, jobUpdatedAt)
	sfrJob.BaseModel.ID = 99

	metricsDB.On("GetRestoreTimestamp", mock.Anything).Return(nil, nil)
	metricsDB.On("UpdateRestoreTimestamp", mock.Anything, now).Return(nil)
	vcpDB.On("GetJobsWithCondition", mock.Anything, mock.Anything).Return([]*datamodel.Job{sfrJob}, nil)

	sfrMeta := &datamodel.SfrMetadata{FilesSize: 8192, FileCount: 3, BackupUUID: "sfr-bkp", VolumeUUID: "sfr-vol-uuid"}
	vcpDB.On("GetSfrMetadataByJobID", mock.Anything, int64(99)).Return(sfrMeta, nil)

	backup := newCrossRegionBackup("sfr-bkp", "sfr-bkp", 50000, "asia-east1")
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "sfr-bkp").Return(backup, nil)

	vol := newVolume("sfr-vol-uuid", "sfr-vol", "sfr-acct", "sfr-deploy", "")
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	result, err := ProcessRestoreBillingMetrics(ctx, vcpDB, metricsDB, config, now)
	assert.NoError(t, err)
	assert.Len(t, result.HydratedMetricsDataModel, 1)
	assert.Equal(t, float64(8192), result.HydratedMetricsDataModel[0].Quantity)
	metricsDB.AssertCalled(t, "UpdateRestoreTimestamp", mock.Anything, now)
}

// =============================================================================
// Edge cases: volume with VolumeAttributes but empty DeploymentName
// =============================================================================

func TestAssembleMetadata_VolumeAttrsEmptyDeployment(t *testing.T) {
	config := defaultConfig()
	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		Name:      "test-vol",
		Pool:      nil,
		VolumeAttributes: &datamodel.VolumeAttributes{
			DeploymentName: "",
			AccountName:    "acct",
		},
	}
	backup := newCrossRegionBackup("b-1", "b-1", 100, "eu-west1")

	met := assembleMetadata(vol, backup, config)
	assert.Equal(t, EmptyDeploymentName, *met.DeploymentName)
}

// =============================================================================
// parseRestoredBackupPath — comprehensive path scenarios
// =============================================================================

func TestParseRestoredBackupPath_FullCrossRegionPath(t *testing.T) {
	vaultName, vaultResourcePath, backupName, err := parseRestoredBackupPath(
		"projects/1088371202435/locations/us-east4/backupVaults/vcp-crb-bv-22-feb-destination-8178/backups/my-backup-001",
	)
	assert.NoError(t, err)
	assert.Equal(t, "vcp-crb-bv-22-feb-destination-8178", vaultName)
	assert.Equal(t, "projects/1088371202435/locations/us-east4/backupVaults/vcp-crb-bv-22-feb-destination-8178", vaultResourcePath)
	assert.Equal(t, "my-backup-001", backupName)
}

func TestParseRestoredBackupPath_SourceRegionPath(t *testing.T) {
	vaultName, vaultResourcePath, backupName, err := parseRestoredBackupPath(
		"projects/1088371202435/locations/us-central1/backupVaults/vcp-crb-bv-22-feb/backups/daily-backup-2026-03-01",
	)
	assert.NoError(t, err)
	assert.Equal(t, "vcp-crb-bv-22-feb", vaultName)
	assert.Equal(t, "projects/1088371202435/locations/us-central1/backupVaults/vcp-crb-bv-22-feb", vaultResourcePath)
	assert.Equal(t, "daily-backup-2026-03-01", backupName)
}

func TestParseRestoredBackupPath_EmptyString(t *testing.T) {
	_, _, _, err := parseRestoredBackupPath("")
	assert.Error(t, err)
}

func TestParseRestoredBackupPath_MissingBackupVaults(t *testing.T) {
	_, _, _, err := parseRestoredBackupPath("projects/p/locations/l/vaults/myVault/backups/b")
	assert.Error(t, err)
}

func TestParseRestoredBackupPath_TrailingSlashAfterBackupVaults(t *testing.T) {
	_, _, _, err := parseRestoredBackupPath("projects/p/locations/l/backupVaults/")
	assert.Error(t, err)
}

// =============================================================================
// matchesVaultName — all matching patterns
// =============================================================================

func TestMatchesVaultName_DirectResourceIdMatch(t *testing.T) {
	bv := googleproxyclient.BackupVaultV1beta{
		ResourceId: "vcp-crb-bv-22-feb-destination-8178",
	}
	assert.True(t, matchesVaultName(bv, "vcp-crb-bv-22-feb-destination-8178", ""))
}

func TestMatchesVaultName_DestinationBackupVaultShortNameMatch(t *testing.T) {
	bv := googleproxyclient.BackupVaultV1beta{
		ResourceId:             "vcp-crb-bv-22-feb-destination-8178",
		DestinationBackupVault: googleproxyclient.NewOptString("vcp-crb-bv-22-feb"),
	}
	assert.True(t, matchesVaultName(bv, "vcp-crb-bv-22-feb", ""))
}

func TestMatchesVaultName_DestinationBackupVaultFullPathMatch(t *testing.T) {
	bv := googleproxyclient.BackupVaultV1beta{
		ResourceId:             "vcp-crb-bv-22-feb-destination-8178",
		DestinationBackupVault: googleproxyclient.NewOptString("projects/1088371202435/locations/us-central1/backupVaults/vcp-crb-bv-22-feb"),
	}
	assert.True(t, matchesVaultName(
		bv,
		"vcp-crb-bv-22-feb",
		"projects/1088371202435/locations/us-central1/backupVaults/vcp-crb-bv-22-feb",
	))
}

func TestMatchesVaultName_NoMatch(t *testing.T) {
	bv := googleproxyclient.BackupVaultV1beta{
		ResourceId:             "some-other-vault",
		DestinationBackupVault: googleproxyclient.NewOptString("another-vault"),
	}
	assert.False(t, matchesVaultName(bv, "missing-vault", "projects/p/locations/l/backupVaults/missing-vault"))
}

func TestMatchesVaultName_DestinationNotSet(t *testing.T) {
	bv := googleproxyclient.BackupVaultV1beta{
		ResourceId: "some-vault",
	}
	assert.False(t, matchesVaultName(bv, "different-vault", ""))
}

func TestMatchesVaultName_EmptyVaultResourcePath(t *testing.T) {
	bv := googleproxyclient.BackupVaultV1beta{
		ResourceId:             "local-vault",
		DestinationBackupVault: googleproxyclient.NewOptString("projects/p/locations/l/backupVaults/source-vault"),
	}
	// dest is the full path, so short name "source-vault" does NOT match when vaultResourcePath is empty
	assert.False(t, matchesVaultName(bv, "source-vault", ""))
	// but with full resource path provided, it matches
	assert.True(t, matchesVaultName(bv, "source-vault", "projects/p/locations/l/backupVaults/source-vault"))
	assert.False(t, matchesVaultName(bv, "other-vault", ""))
}

// =============================================================================
// findVaultByNameOrCrossRegionVaultName — using real DB record patterns
// =============================================================================

// buildSourceRegionVaults returns vaults as seen by the source-region (us-central1) google-proxy.
// The source vault has backup_region=us-east4, meaning backups are stored in us-east4.
// Cross-region billing fires from the source region where backup_region != config.RegionName.
func buildSourceRegionVaults() []googleproxyclient.BackupVaultV1beta {
	return []googleproxyclient.BackupVaultV1beta{
		{
			BackupVaultId:          googleproxyclient.NewOptString("8178187e-f4b7-a0dd-d042-90385ed6e6e3"),
			ResourceId:             "vcp-crb-bv-22-feb",
			BackupRegion:           googleproxyclient.NewOptString("us-east4"),
			SourceRegion:           googleproxyclient.NewOptString("us-central1"),
			BackupVaultType:        googleproxyclient.NewOptBackupVaultV1betaBackupVaultType(googleproxyclient.BackupVaultV1betaBackupVaultTypeCROSSREGION),
			State:                  googleproxyclient.NewOptBackupVaultV1betaState(googleproxyclient.BackupVaultV1betaStateREADY),
			StateDetails:           googleproxyclient.NewOptString("Available for use"),
			DestinationBackupVault: googleproxyclient.NewOptString("projects/1088371202435/locations/us-east4/backupVaults/vcp-crb-bv-22-feb-destination-8178"),
		},
	}
}

// buildDestinationRegionVaults returns vaults as seen by the destination-region (us-east4) google-proxy.
func buildDestinationRegionVaults() []googleproxyclient.BackupVaultV1beta {
	return []googleproxyclient.BackupVaultV1beta{
		{
			BackupVaultId:          googleproxyclient.NewOptString("72f15309-7aa7-d7a0-4c85-8a22fe55bfc1"),
			ResourceId:             "vcp-crb-bv-22-feb-destination-8178",
			BackupRegion:           googleproxyclient.NewOptString("us-east4"),
			SourceRegion:           googleproxyclient.NewOptString("us-central1"),
			BackupVaultType:        googleproxyclient.NewOptBackupVaultV1betaBackupVaultType(googleproxyclient.BackupVaultV1betaBackupVaultTypeCROSSREGION),
			State:                  googleproxyclient.NewOptBackupVaultV1betaState(googleproxyclient.BackupVaultV1betaStateREADY),
			StateDetails:           googleproxyclient.NewOptString("Available for use"),
			DestinationBackupVault: googleproxyclient.NewOptString("projects/1088371202435/locations/us-central1/backupVaults/vcp-crb-bv-22-feb"),
		},
	}
}

func sourceRegionConfig() *common.TelemetryConfig {
	return &common.TelemetryConfig{
		RegionName:                            "us-central1",
		EnableCrossRegionBackupBillingMetrics: true,
		EnableSFRCrossRegionRestoreBilling:    false,
		EnableFilesBackupBilling:              true,
	}
}

func TestFindVault_DirectMatch_DestinationVault(t *testing.T) {
	vaults := buildDestinationRegionVaults()
	bv, err := findVaultByNameOrCrossRegionVaultName(
		vaults,
		"vcp-crb-bv-22-feb-destination-8178",
		"projects/1088371202435/locations/us-east4/backupVaults/vcp-crb-bv-22-feb-destination-8178",
		"us-east4",
	)
	assert.NoError(t, err)
	assert.NotNil(t, bv)
	assert.Equal(t, "vcp-crb-bv-22-feb-destination-8178", bv.Name)
}

func TestFindVault_CrossRegionMatch_SourceVaultNameInPath(t *testing.T) {
	vaults := buildDestinationRegionVaults()
	bv, err := findVaultByNameOrCrossRegionVaultName(
		vaults,
		"vcp-crb-bv-22-feb",
		"projects/1088371202435/locations/us-central1/backupVaults/vcp-crb-bv-22-feb",
		"us-east4",
	)
	assert.NoError(t, err)
	assert.NotNil(t, bv)
	assert.Equal(t, "vcp-crb-bv-22-feb-destination-8178", bv.Name)
	assert.Equal(t, "CROSS_REGION", bv.BackupVaultType)
}

func TestFindVault_NoMatch(t *testing.T) {
	vaults := buildDestinationRegionVaults()
	bv, err := findVaultByNameOrCrossRegionVaultName(
		vaults,
		"nonexistent-vault",
		"projects/p/locations/l/backupVaults/nonexistent-vault",
		"us-east4",
	)
	assert.Error(t, err)
	assert.Nil(t, bv)
	assert.Contains(t, err.Error(), "not found via google-proxy")
}

func TestFindVault_EmptyVaultList(t *testing.T) {
	bv, err := findVaultByNameOrCrossRegionVaultName(
		[]googleproxyclient.BackupVaultV1beta{},
		"any-vault",
		"",
		"us-east4",
	)
	assert.Error(t, err)
	assert.Nil(t, bv)
}

func TestFindVault_MultipleVaults_CorrectOneSelected(t *testing.T) {
	vaults := []googleproxyclient.BackupVaultV1beta{
		{
			BackupVaultId:   googleproxyclient.NewOptString("in-region-uuid"),
			ResourceId:      "local-in-region-vault",
			BackupVaultType: googleproxyclient.NewOptBackupVaultV1betaBackupVaultType(googleproxyclient.BackupVaultV1betaBackupVaultTypeINREGION),
			State:           googleproxyclient.NewOptBackupVaultV1betaState(googleproxyclient.BackupVaultV1betaStateREADY),
		},
		{
			BackupVaultId:          googleproxyclient.NewOptString("cross-region-uuid"),
			ResourceId:             "vcp-crb-bv-22-feb-destination-8178",
			BackupRegion:           googleproxyclient.NewOptString("us-east4"),
			SourceRegion:           googleproxyclient.NewOptString("us-central1"),
			BackupVaultType:        googleproxyclient.NewOptBackupVaultV1betaBackupVaultType(googleproxyclient.BackupVaultV1betaBackupVaultTypeCROSSREGION),
			State:                  googleproxyclient.NewOptBackupVaultV1betaState(googleproxyclient.BackupVaultV1betaStateREADY),
			DestinationBackupVault: googleproxyclient.NewOptString("projects/1088371202435/locations/us-central1/backupVaults/vcp-crb-bv-22-feb"),
		},
	}
	bv, err := findVaultByNameOrCrossRegionVaultName(
		vaults,
		"vcp-crb-bv-22-feb",
		"projects/1088371202435/locations/us-central1/backupVaults/vcp-crb-bv-22-feb",
		"us-east4",
	)
	assert.NoError(t, err)
	assert.NotNil(t, bv)
	assert.Equal(t, "vcp-crb-bv-22-feb-destination-8178", bv.Name)
	assert.Equal(t, "CROSS_REGION", bv.BackupVaultType)
}

// =============================================================================
// convertGProxyBackupToDataModel tests
// =============================================================================

func TestConvertGProxyBackupToDataModel_AllFields(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	vault := &datamodel.BackupVault{
		BaseModel:       datamodel.BaseModel{ID: 44},
		Name:            "test-vault",
		BackupVaultType: "CROSS_REGION",
	}

	b := googleproxyclient.BackupV1beta{
		ResourceId:       googleproxyclient.NewOptString("backup-name"),
		BackupId:         googleproxyclient.NewOptString("backup-uuid"),
		VolumeId:         googleproxyclient.NewOptString("vol-uuid"),
		VolumeUsageBytes: googleproxyclient.NewOptInt64(5000),
		BackupChainBytes: googleproxyclient.NewOptInt64(3000),
		State:            googleproxyclient.NewOptBackupV1betaState(googleproxyclient.BackupV1betaStateREADY),
		BackupType:       googleproxyclient.NewOptBackupV1betaBackupType(googleproxyclient.BackupV1betaBackupTypeMANUAL),
		Description:      googleproxyclient.NewOptString("test backup"),
		Created:          googleproxyclient.NewOptDateTime(now),
		BucketName:       googleproxyclient.NewOptString("bucket-1"),
		SnapshotName:     googleproxyclient.NewOptString("snap-1"),
		SourceVolume:     googleproxyclient.NewOptString("source-vol"),
	}

	result := convertGProxyBackupToDataModel(b, vault)
	assert.Equal(t, "backup-uuid", result.UUID)
	assert.Equal(t, "backup-name", result.Name)
	assert.Equal(t, "test backup", result.Description)
	assert.Equal(t, "READY", result.State)
	assert.Equal(t, "MANUAL", result.Type)
	assert.Equal(t, "vol-uuid", result.VolumeUUID)
	assert.Equal(t, int64(5000), result.SizeInBytes)
	assert.Equal(t, int64(3000), result.LatestLogicalBackupSize)
	assert.Equal(t, now, result.CreatedAt)
	assert.NotNil(t, result.Attributes)
	assert.Equal(t, "bucket-1", result.Attributes.BucketName)
	assert.Equal(t, "snap-1", result.Attributes.SnapshotName)
	assert.Equal(t, "source-vol", result.Attributes.VolumeName)
	assert.Equal(t, vault, result.BackupVault)
	assert.Equal(t, int64(44), result.BackupVaultID)
}

func TestConvertGProxyBackupToDataModel_FallbackToBackupChainBytes(t *testing.T) {
	vault := &datamodel.BackupVault{BaseModel: datamodel.BaseModel{ID: 1}}

	b := googleproxyclient.BackupV1beta{
		ResourceId:       googleproxyclient.NewOptString("bkp"),
		BackupId:         googleproxyclient.NewOptString("uuid"),
		BackupChainBytes: googleproxyclient.NewOptInt64(7000),
		State:            googleproxyclient.NewOptBackupV1betaState(googleproxyclient.BackupV1betaStateREADY),
		BackupType:       googleproxyclient.NewOptBackupV1betaBackupType(googleproxyclient.BackupV1betaBackupTypeMANUAL),
	}

	result := convertGProxyBackupToDataModel(b, vault)
	assert.Equal(t, int64(7000), result.SizeInBytes)
	assert.Equal(t, int64(7000), result.LatestLogicalBackupSize)
}

func TestConvertGProxyBackupToDataModel_NoSizeFields(t *testing.T) {
	vault := &datamodel.BackupVault{BaseModel: datamodel.BaseModel{ID: 1}}

	b := googleproxyclient.BackupV1beta{
		ResourceId: googleproxyclient.NewOptString("bkp"),
		BackupId:   googleproxyclient.NewOptString("uuid"),
		State:      googleproxyclient.NewOptBackupV1betaState(googleproxyclient.BackupV1betaStateREADY),
		BackupType: googleproxyclient.NewOptBackupV1betaBackupType(googleproxyclient.BackupV1betaBackupTypeMANUAL),
	}

	result := convertGProxyBackupToDataModel(b, vault)
	assert.Equal(t, int64(0), result.SizeInBytes)
	assert.Equal(t, int64(0), result.LatestLogicalBackupSize)
	assert.Nil(t, result.Attributes)
}

// =============================================================================
// convertGProxyVaultToDataModel tests
// =============================================================================

func TestConvertGProxyVaultToDataModel_CrossRegionVault(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	bv := googleproxyclient.BackupVaultV1beta{
		BackupVaultId:          googleproxyclient.NewOptString("72f15309-7aa7-d7a0-4c85-8a22fe55bfc1"),
		ResourceId:             "vcp-crb-bv-22-feb-destination-8178",
		BackupRegion:           googleproxyclient.NewOptString("us-east4"),
		SourceRegion:           googleproxyclient.NewOptString("us-central1"),
		BackupVaultType:        googleproxyclient.NewOptBackupVaultV1betaBackupVaultType(googleproxyclient.BackupVaultV1betaBackupVaultTypeCROSSREGION),
		State:                  googleproxyclient.NewOptBackupVaultV1betaState(googleproxyclient.BackupVaultV1betaStateREADY),
		StateDetails:           googleproxyclient.NewOptString("Available for use"),
		DestinationBackupVault: googleproxyclient.NewOptString("projects/1088371202435/locations/us-central1/backupVaults/vcp-crb-bv-22-feb"),
		CreatedAt:              googleproxyclient.NewOptDateTime(now),
	}

	result := convertGProxyVaultToDataModel(bv, "us-east4")
	assert.Equal(t, "72f15309-7aa7-d7a0-4c85-8a22fe55bfc1", result.UUID)
	assert.Equal(t, "vcp-crb-bv-22-feb-destination-8178", result.Name)
	assert.Equal(t, "CROSS_REGION", result.BackupVaultType)
	assert.Equal(t, "READY", result.LifeCycleState)
	assert.Equal(t, "Available for use", result.LifeCycleStateDetails)
	assert.NotNil(t, result.BackupRegionName)
	assert.Equal(t, "us-east4", *result.BackupRegionName)
	assert.NotNil(t, result.SourceRegionName)
	assert.Equal(t, "us-central1", *result.SourceRegionName)
	assert.NotNil(t, result.CrossRegionBackupVaultName)
	assert.Equal(t, "projects/1088371202435/locations/us-central1/backupVaults/vcp-crb-bv-22-feb", *result.CrossRegionBackupVaultName)
	assert.Equal(t, models.ServiceTypeGCNV, result.ServiceType)
}

func TestConvertGProxyVaultToDataModel_InRegionVault_NoSourceRegion(t *testing.T) {
	bv := googleproxyclient.BackupVaultV1beta{
		BackupVaultId:   googleproxyclient.NewOptString("in-region-uuid"),
		ResourceId:      "local-vault",
		BackupRegion:    googleproxyclient.NewOptString("us-east4"),
		BackupVaultType: googleproxyclient.NewOptBackupVaultV1betaBackupVaultType(googleproxyclient.BackupVaultV1betaBackupVaultTypeINREGION),
		State:           googleproxyclient.NewOptBackupVaultV1betaState(googleproxyclient.BackupVaultV1betaStateREADY),
	}

	result := convertGProxyVaultToDataModel(bv, "us-east4")
	assert.Equal(t, "IN_REGION", result.BackupVaultType)
	assert.NotNil(t, result.SourceRegionName)
	assert.Equal(t, "us-east4", *result.SourceRegionName)
	assert.Nil(t, result.CrossRegionBackupVaultName)
}

// =============================================================================
// E2E: BackupPath flow through ProcessRestoreBillingMetrics (cross-region)
//
// Cross-region billing fires from the SOURCE region where the vault's
// BackupRegionName differs from config.RegionName.
// e.g. config.RegionName="us-central1", vault.BackupRegion="us-east4" → billable
//
// The source-region VCP (us-central1) has the source vault "vcp-crb-bv-22-feb"
// with backup_region="us-east4" and destination_backup_vault pointing to
// the destination vault in us-east4.
// =============================================================================

func TestProcessRestoreBillingMetrics_BackupPath_CrossRegion_DirectSourceVaultMatch(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	metricsDB := new(mockCrossRegionMetricsStorage)
	ctx := context.Background()
	config := sourceRegionConfig()
	now := time.Now()
	jobUpdatedAt := now.Add(-5 * time.Minute)
	mockInvoker := new(googleproxyclient.MockInvoker)
	cleanup := setupGProxyMocks(t, mockInvoker)
	defer cleanup()

	job := newTestJob("job-path-1", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "restored-vol", 1, jobUpdatedAt)

	metricsDB.On("GetRestoreTimestamp", mock.Anything).Return(nil, nil)
	metricsDB.On("UpdateRestoreTimestamp", mock.Anything, now).Return(nil)
	vcpDB.On("GetJobsWithCondition", mock.Anything, mock.Anything).Return([]*datamodel.Job{job}, nil)

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid-1"},
		Name:      "restored-vol",
		Account:   &datamodel.Account{Name: "test-account"},
		Pool:      &datamodel.Pool{DeploymentName: "test-deploy"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupPath: "projects/1088371202435/locations/us-central1/backupVaults/vcp-crb-bv-22-feb/backups/daily-2026-03-01",
			AccountName:        "test-account",
			DeploymentName:     "test-deploy",
			Protocols:          []string{"NFSV3"},
		},
	}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	mockInvoker.On("V1betaListBackupVaults", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupVaultsOK{
			BackupVaults: buildSourceRegionVaults(),
		}, nil)

	mockInvoker.On("V1betaListBackups", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupsOK{
			Backups: []googleproxyclient.BackupV1beta{
				{
					ResourceId:       googleproxyclient.NewOptString("daily-2026-03-01"),
					BackupId:         googleproxyclient.NewOptString("bkp-uuid-direct"),
					VolumeUsageBytes: googleproxyclient.NewOptInt64(2 * 1024 * 1024),
					State:            googleproxyclient.NewOptBackupV1betaState(googleproxyclient.BackupV1betaStateREADY),
					BackupType:       googleproxyclient.NewOptBackupV1betaBackupType(googleproxyclient.BackupV1betaBackupTypeSCHEDULED),
				},
			},
		}, nil)

	result, err := ProcessRestoreBillingMetrics(ctx, vcpDB, metricsDB, config, now)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	hm := result.HydratedMetricsDataModel[0]
	assert.Equal(t, metadata.CbsCrossRegionVolumeRestoreTransferBytes, hm.MeasuredType)
	assert.Equal(t, float64(2*1024*1024), hm.Quantity)
	assert.Equal(t, "test-account", hm.ConsumerID)

	var extra map[string]string
	assert.NoError(t, json.Unmarshal(hm.Metadata, &extra))
	assert.Equal(t, "us-east4", extra["backup_region_name"])
	assert.Equal(t, "us-central1", extra["source_region_name"])
}

func TestProcessRestoreBillingMetrics_BackupPath_CrossRegion_DestVaultPathMatchedViaCrossRegionName(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	metricsDB := new(mockCrossRegionMetricsStorage)
	ctx := context.Background()
	config := sourceRegionConfig()
	now := time.Now()
	jobUpdatedAt := now.Add(-5 * time.Minute)
	mockInvoker := new(googleproxyclient.MockInvoker)
	cleanup := setupGProxyMocks(t, mockInvoker)
	defer cleanup()

	job := newTestJob("job-path-2", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "restored-vol-2", 1, jobUpdatedAt)

	metricsDB.On("GetRestoreTimestamp", mock.Anything).Return(nil, nil)
	metricsDB.On("UpdateRestoreTimestamp", mock.Anything, now).Return(nil)
	vcpDB.On("GetJobsWithCondition", mock.Anything, mock.Anything).Return([]*datamodel.Job{job}, nil)

	// Path points to destination vault in us-east4, but we're running from us-central1.
	// The source vault's DestinationBackupVault matches the full path.
	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid-2"},
		Name:      "restored-vol-2",
		Account:   &datamodel.Account{Name: "acct-cross"},
		Pool:      &datamodel.Pool{DeploymentName: "deploy-cross"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupPath: "projects/1088371202435/locations/us-east4/backupVaults/vcp-crb-bv-22-feb-destination-8178/backups/adhoc-backup-001",
			AccountName:        "acct-cross",
			DeploymentName:     "deploy-cross",
			Protocols:          []string{"NFSV3"},
		},
	}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	mockInvoker.On("V1betaListBackupVaults", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupVaultsOK{
			BackupVaults: buildSourceRegionVaults(),
		}, nil)

	mockInvoker.On("V1betaListBackups", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupsOK{
			Backups: []googleproxyclient.BackupV1beta{
				{
					ResourceId:       googleproxyclient.NewOptString("adhoc-backup-001"),
					BackupId:         googleproxyclient.NewOptString("bkp-uuid-cross"),
					VolumeUsageBytes: googleproxyclient.NewOptInt64(500000),
					State:            googleproxyclient.NewOptBackupV1betaState(googleproxyclient.BackupV1betaStateREADY),
					BackupType:       googleproxyclient.NewOptBackupV1betaBackupType(googleproxyclient.BackupV1betaBackupTypeMANUAL),
				},
			},
		}, nil)

	result, err := ProcessRestoreBillingMetrics(ctx, vcpDB, metricsDB, config, now)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetricsDataModel, 1)

	hm := result.HydratedMetricsDataModel[0]
	assert.Equal(t, float64(500000), hm.Quantity)
	assert.Equal(t, "acct-cross", hm.ConsumerID)
}

func TestProcessRestoreBillingMetrics_BackupPath_InRegionVault_SkippedAsNotCrossRegion(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	metricsDB := new(mockCrossRegionMetricsStorage)
	ctx := context.Background()
	config := sourceRegionConfig()
	now := time.Now()
	jobUpdatedAt := now.Add(-5 * time.Minute)
	mockInvoker := new(googleproxyclient.MockInvoker)
	cleanup := setupGProxyMocks(t, mockInvoker)
	defer cleanup()

	job := newTestJob("job-path-ir", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "restored-vol-ir", 1, jobUpdatedAt)

	metricsDB.On("GetRestoreTimestamp", mock.Anything).Return(nil, nil)
	metricsDB.On("UpdateRestoreTimestamp", mock.Anything, now).Return(nil)
	vcpDB.On("GetJobsWithCondition", mock.Anything, mock.Anything).Return([]*datamodel.Job{job}, nil)

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid-ir"},
		Name:      "restored-vol-ir",
		Account:   &datamodel.Account{Name: "acct-ir"},
		Pool:      &datamodel.Pool{DeploymentName: "deploy-ir"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupPath: "projects/1088371202435/locations/us-central1/backupVaults/local-in-region-vault/backups/ir-backup",
			AccountName:        "acct-ir",
			DeploymentName:     "deploy-ir",
		},
	}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	mockInvoker.On("V1betaListBackupVaults", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupVaultsOK{
			BackupVaults: []googleproxyclient.BackupVaultV1beta{
				{
					BackupVaultId:   googleproxyclient.NewOptString("in-region-vault-uuid"),
					ResourceId:      "local-in-region-vault",
					BackupRegion:    googleproxyclient.NewOptString("us-central1"),
					BackupVaultType: googleproxyclient.NewOptBackupVaultV1betaBackupVaultType(googleproxyclient.BackupVaultV1betaBackupVaultTypeINREGION),
					State:           googleproxyclient.NewOptBackupVaultV1betaState(googleproxyclient.BackupVaultV1betaStateREADY),
				},
			},
		}, nil)

	mockInvoker.On("V1betaListBackups", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupsOK{
			Backups: []googleproxyclient.BackupV1beta{
				{
					ResourceId:       googleproxyclient.NewOptString("ir-backup"),
					BackupId:         googleproxyclient.NewOptString("ir-bkp-uuid"),
					VolumeUsageBytes: googleproxyclient.NewOptInt64(10000),
					State:            googleproxyclient.NewOptBackupV1betaState(googleproxyclient.BackupV1betaStateREADY),
					BackupType:       googleproxyclient.NewOptBackupV1betaBackupType(googleproxyclient.BackupV1betaBackupTypeMANUAL),
				},
			},
		}, nil)

	result, err := ProcessRestoreBillingMetrics(ctx, vcpDB, metricsDB, config, now)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetricsDataModel)
}

func TestProcessRestoreBillingMetrics_BackupPath_SameRegionCrossRegionVault_SkippedAsSameRegion(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	metricsDB := new(mockCrossRegionMetricsStorage)
	ctx := context.Background()
	config := defaultConfig() // us-east4
	now := time.Now()
	jobUpdatedAt := now.Add(-5 * time.Minute)
	mockInvoker := new(googleproxyclient.MockInvoker)
	cleanup := setupGProxyMocks(t, mockInvoker)
	defer cleanup()

	job := newTestJob("job-path-sr", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "restored-vol-sr", 1, jobUpdatedAt)

	metricsDB.On("GetRestoreTimestamp", mock.Anything).Return(nil, nil)
	metricsDB.On("UpdateRestoreTimestamp", mock.Anything, now).Return(nil)
	vcpDB.On("GetJobsWithCondition", mock.Anything, mock.Anything).Return([]*datamodel.Job{job}, nil)

	// vault backup_region = us-east4 = config.RegionName → not cross-region
	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid-sr"},
		Name:      "restored-vol-sr",
		Account:   &datamodel.Account{Name: "acct-sr"},
		Pool:      &datamodel.Pool{DeploymentName: "deploy-sr"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupPath: "projects/p/locations/us-east4/backupVaults/same-region-vault/backups/sr-backup",
			AccountName:        "acct-sr",
			DeploymentName:     "deploy-sr",
		},
	}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	mockInvoker.On("V1betaListBackupVaults", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupVaultsOK{
			BackupVaults: []googleproxyclient.BackupVaultV1beta{
				{
					BackupVaultId:   googleproxyclient.NewOptString("sr-vault-uuid"),
					ResourceId:      "same-region-vault",
					BackupRegion:    googleproxyclient.NewOptString("us-east4"),
					BackupVaultType: googleproxyclient.NewOptBackupVaultV1betaBackupVaultType(googleproxyclient.BackupVaultV1betaBackupVaultTypeCROSSREGION),
					State:           googleproxyclient.NewOptBackupVaultV1betaState(googleproxyclient.BackupVaultV1betaStateREADY),
				},
			},
		}, nil)

	mockInvoker.On("V1betaListBackups", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupsOK{
			Backups: []googleproxyclient.BackupV1beta{
				{
					ResourceId:       googleproxyclient.NewOptString("sr-backup"),
					BackupId:         googleproxyclient.NewOptString("sr-bkp-uuid"),
					VolumeUsageBytes: googleproxyclient.NewOptInt64(30000),
					State:            googleproxyclient.NewOptBackupV1betaState(googleproxyclient.BackupV1betaStateREADY),
					BackupType:       googleproxyclient.NewOptBackupV1betaBackupType(googleproxyclient.BackupV1betaBackupTypeMANUAL),
				},
			},
		}, nil)

	result, err := ProcessRestoreBillingMetrics(ctx, vcpDB, metricsDB, config, now)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetricsDataModel)
}

func TestProcessRestoreBillingMetrics_BackupPath_ZeroSizeBackup_Skipped(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	metricsDB := new(mockCrossRegionMetricsStorage)
	ctx := context.Background()
	config := sourceRegionConfig()
	now := time.Now()
	jobUpdatedAt := now.Add(-5 * time.Minute)
	mockInvoker := new(googleproxyclient.MockInvoker)
	cleanup := setupGProxyMocks(t, mockInvoker)
	defer cleanup()

	job := newTestJob("job-path-zero", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "vol-zero", 1, jobUpdatedAt)

	metricsDB.On("GetRestoreTimestamp", mock.Anything).Return(nil, nil)
	metricsDB.On("UpdateRestoreTimestamp", mock.Anything, now).Return(nil)
	vcpDB.On("GetJobsWithCondition", mock.Anything, mock.Anything).Return([]*datamodel.Job{job}, nil)

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid-zero"},
		Name:      "vol-zero",
		Account:   &datamodel.Account{Name: "acct-z"},
		Pool:      &datamodel.Pool{DeploymentName: "deploy-z"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupPath: "projects/1088371202435/locations/us-central1/backupVaults/vcp-crb-bv-22-feb/backups/zero-bkp",
			AccountName:        "acct-z",
			DeploymentName:     "deploy-z",
			Protocols:          []string{"NFSV3"},
		},
	}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	mockInvoker.On("V1betaListBackupVaults", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupVaultsOK{
			BackupVaults: buildSourceRegionVaults(),
		}, nil)

	// Backup has no size fields set → SizeInBytes = 0
	mockInvoker.On("V1betaListBackups", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupsOK{
			Backups: []googleproxyclient.BackupV1beta{
				{
					ResourceId: googleproxyclient.NewOptString("zero-bkp"),
					BackupId:   googleproxyclient.NewOptString("zero-bkp-uuid"),
					State:      googleproxyclient.NewOptBackupV1betaState(googleproxyclient.BackupV1betaStateREADY),
					BackupType: googleproxyclient.NewOptBackupV1betaBackupType(googleproxyclient.BackupV1betaBackupTypeMANUAL),
				},
			},
		}, nil)

	result, err := ProcessRestoreBillingMetrics(ctx, vcpDB, metricsDB, config, now)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.HydratedMetricsDataModel)
}

func TestProcessRestoreBillingMetrics_BackupPath_FallsBackWhenRestoredBackupIDEmpty(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	metricsDB := new(mockCrossRegionMetricsStorage)
	ctx := context.Background()
	config := sourceRegionConfig()
	now := time.Now()
	jobUpdatedAt := now.Add(-5 * time.Minute)
	mockInvoker := new(googleproxyclient.MockInvoker)
	cleanup := setupGProxyMocks(t, mockInvoker)
	defer cleanup()

	job := newTestJob("job-fallback", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "vol-fallback", 1, jobUpdatedAt)

	metricsDB.On("GetRestoreTimestamp", mock.Anything).Return(nil, nil)
	metricsDB.On("UpdateRestoreTimestamp", mock.Anything, now).Return(nil)
	vcpDB.On("GetJobsWithCondition", mock.Anything, mock.Anything).Return([]*datamodel.Job{job}, nil)

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid-fb"},
		Name:      "vol-fallback",
		Account:   &datamodel.Account{Name: "acct-fb"},
		Pool:      &datamodel.Pool{DeploymentName: "deploy-fb"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupID:   "",
			RestoredBackupPath: "projects/1088371202435/locations/us-central1/backupVaults/vcp-crb-bv-22-feb/backups/fb-backup",
			AccountName:        "acct-fb",
			DeploymentName:     "deploy-fb",
			Protocols:          []string{"NFSV3"},
		},
	}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	mockInvoker.On("V1betaListBackupVaults", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupVaultsOK{
			BackupVaults: buildSourceRegionVaults(),
		}, nil)

	mockInvoker.On("V1betaListBackups", mock.Anything, mock.Anything).
		Return(&googleproxyclient.V1betaListBackupsOK{
			Backups: []googleproxyclient.BackupV1beta{
				{
					ResourceId:       googleproxyclient.NewOptString("fb-backup"),
					BackupId:         googleproxyclient.NewOptString("fb-bkp-uuid"),
					VolumeUsageBytes: googleproxyclient.NewOptInt64(75000),
					State:            googleproxyclient.NewOptBackupV1betaState(googleproxyclient.BackupV1betaStateREADY),
					BackupType:       googleproxyclient.NewOptBackupV1betaBackupType(googleproxyclient.BackupV1betaBackupTypeMANUAL),
				},
			},
		}, nil)

	result, err := ProcessRestoreBillingMetrics(ctx, vcpDB, metricsDB, config, now)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.HydratedMetricsDataModel, 1)
	assert.Equal(t, float64(75000), result.HydratedMetricsDataModel[0].Quantity)
}

func TestProcessRestoreBillingMetrics_BackupPath_RestoredBackupIDTakesPrecedence(t *testing.T) {
	vcpDB := new(mockCrossRegionVCPStorage)
	metricsDB := new(mockCrossRegionMetricsStorage)
	ctx := context.Background()
	config := defaultConfig()
	now := time.Now()
	jobUpdatedAt := now.Add(-5 * time.Minute)

	job := newTestJob("job-id-prio", string(models.JobTypeRestoreBackup), string(models.JobsStateDONE), "vol-prio", 100, jobUpdatedAt)

	metricsDB.On("GetRestoreTimestamp", mock.Anything).Return(nil, nil)
	metricsDB.On("UpdateRestoreTimestamp", mock.Anything, now).Return(nil)
	vcpDB.On("GetJobsWithCondition", mock.Anything, mock.Anything).Return([]*datamodel.Job{job}, nil)

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid-prio"},
		Name:      "vol-prio",
		Account:   &datamodel.Account{Name: "acct-prio"},
		Pool:      &datamodel.Pool{DeploymentName: "deploy-prio"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			RestoredBackupID:   "backup-by-id",
			RestoredBackupPath: "projects/p/locations/l/backupVaults/v/backups/should-not-use",
			AccountName:        "acct-prio",
			DeploymentName:     "deploy-prio",
			Protocols:          []string{"NFSV3"},
		},
	}
	vcpDB.On("ListVolumesWithPagination", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Volume{vol}, nil)

	backup := newCrossRegionBackup("backup-by-id", "backup-by-id", 9999, "us-west2")
	vcpDB.On("GetBackupWithVaultByUUID", mock.Anything, "backup-by-id").Return(backup, nil)

	result, err := ProcessRestoreBillingMetrics(ctx, vcpDB, metricsDB, config, now)
	assert.NoError(t, err)
	assert.Len(t, result.HydratedMetricsDataModel, 1)
	assert.Equal(t, float64(9999), result.HydratedMetricsDataModel[0].Quantity)
}
