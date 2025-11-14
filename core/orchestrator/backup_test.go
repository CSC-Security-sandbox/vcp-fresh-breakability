package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	vsaerror "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	workflow_engine_mock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"go.temporal.io/sdk/client"
)

func TestCreateBackup(t *testing.T) {
	temporal := workflow_engine_mock.NewMockTemporalTestClient(t)

	t.Run("WhenValidateCreateBackupParamsFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err, "Failed to ClearInMemoryDB")

		params := &common.CreateBackupParams{
			BackupName:    "testBackup",
			VolumeUUID:    "testVolumeUUID",
			BackupVaultID: "testVaultID",
			AccountName:   "testAccount",
		}
		originalValidateCreateBackupParams := validateCreateBackupParams
		validateCreateBackupParams = func(ctx context.Context, se database.Storage, params *common.CreateBackupParams) error {
			return errors.New("validation failed")
		}
		defer func() { validateCreateBackupParams = originalValidateCreateBackupParams }()

		_, _, err = createBackup(ctx, store, temporal, params)
		assert.EqualError(tt, err, "validation failed")
	})

	t.Run("WhenFailsWithVolumeNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err, "Failed to ClearInMemoryDB")

		params := &common.CreateBackupParams{
			BackupName:    "testBackup",
			VolumeUUID:    "testVolumeUUID",
			BackupVaultID: "testVaultID",
			AccountName:   "testAccount",
		}
		originalValidateCreateBackupParams := validateCreateBackupParams
		validateCreateBackupParams = func(ctx context.Context, se database.Storage, params *common.CreateBackupParams) error {
			return nil
		}
		defer func() { validateCreateBackupParams = originalValidateCreateBackupParams }()

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "testAccountUUID"}}, nil
		}
		_, _, err = createBackup(ctx, store, temporal, params)
		assertErrContainsOriginal(tt, err, "volume not found")
	})

	t.Run("WhenFailsWithValidation", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err, "Failed to ClearInMemoryDB")

		params := &common.CreateBackupParams{
			BackupName:    "testBackup",
			VolumeUUID:    "testVolumeUUID",
			BackupVaultID: "testVaultID",
			AccountName:   "testAccount",
		}
		originalValidateCreateBackupParams := validateCreateBackupParams
		validateCreateBackupParams = func(ctx context.Context, se database.Storage, params *common.CreateBackupParams) error {
			return errors.New("validation failed")
		}
		defer func() { validateCreateBackupParams = originalValidateCreateBackupParams }()

		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "testAccountUUID"}}, nil
		}
		_, _, err = createBackup(ctx, store, temporal, params)
		assert.EqualError(tt, err, "validation failed")
	})
}

func TestOrchestrator_CreateBackup(t *testing.T) {
	t.Run("CallsCreateBackup", func(tt *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(tt)
		temporal := workflow_engine_mock.NewMockTemporalTestClient(tt)
		params := &common.CreateBackupParams{}

		createBackup = func(ctx context.Context, se database.Storage, temporalClient client.Client, params *common.CreateBackupParams) (*models.Backup, string, error) {
			return &models.Backup{}, "job-id", nil
		}

		o := &Orchestrator{storage: store, temporal: temporal}
		backup, jobID, err := o.CreateBackup(ctx, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, backup)
		assert.Equal(tt, "job-id", jobID)
	})
}

func TestOrchestrator_ListBackups(t *testing.T) {
	t.Run("CallsGetBackups", func(tt *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(tt)
		params := &common.GetBackupsParams{}

		getBackups = func(ctx context.Context, se database.Storage, params *common.GetBackupsParams, filters [][]interface{}) ([]*datamodel.Backup, error) {
			return []*datamodel.Backup{}, nil
		}

		o := &Orchestrator{storage: store}
		backups, err := o.ListBackups(ctx, params.BackupVaultID, "account", nil)

		assert.NoError(tt, err)
		assert.NotNil(tt, backups)
	})
}

func Test_createBackup(t *testing.T) {
	t.Run("FailsValidation", func(tt *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(tt)
		temporal := workflow_engine_mock.NewMockTemporalTestClient(tt)
		params := &common.CreateBackupParams{}
		originalValidateCreateBackupParams := validateCreateBackupParams
		validateCreateBackupParams = func(ctx context.Context, se database.Storage, params *common.CreateBackupParams) error {
			return errors.New("validation failed")
		}
		defer func() { validateCreateBackupParams = originalValidateCreateBackupParams }()

		_, _, err := _createBackup(ctx, store, temporal, params)
		assert.EqualError(tt, err, "validation failed")
	})

	t.Run("FailsVolumeFetch", func(tt *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(tt)
		temporal := workflow_engine_mock.NewMockTemporalTestClient(tt)
		params := &common.CreateBackupParams{}
		originalValidateCreateBackupParams := validateCreateBackupParams
		validateCreateBackupParams = func(ctx context.Context, se database.Storage, params *common.CreateBackupParams) error {
			return nil
		}
		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		defer func() {
			validateCreateBackupParams = originalValidateCreateBackupParams
			getOrCreateAccount = _getOrCreateAccount
		}()

		store.On("GetVolumeWithAccountID", ctx, params.VolumeUUID, mock.Anything).Return(nil, errors.New("volume not found"))

		_, _, err := _createBackup(ctx, store, temporal, params)
		assert.EqualError(tt, err, "volume not found")
	})
}

func Test_validateCreateBackupParams(t *testing.T) {
	t.Run("FailsBackupCreatingState", func(tt *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(tt)
		params := &common.CreateBackupParams{}

		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(true, nil)

		err := _validateCreateBackupParams(ctx, store, params)
		assert.EqualError(tt, err, "A backup operation from the same volume is currently in progress. Please wait for it to complete before starting a new backup")
	})

	t.Run("FailsVolumeState", func(tt *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(tt)
		params := &common.CreateBackupParams{}

		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
		store.On("GetVolume", ctx, params.VolumeUUID).Return(&datamodel.Volume{State: "NOT_READY"}, nil)

		err := _validateCreateBackupParams(ctx, store, params)
		assert.EqualError(tt, err, "Volume is not in available state")
	})
}

func Test_getBackups(t *testing.T) {
	t.Run("RetrievesBackups", func(tt *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(tt)
		params := &common.GetBackupsParams{BackupVaultID: "BackupVaultID"}

		store.On("GetBackupsByBackupVaultOwnerIDAndFilter", ctx, params.BackupVaultID, int64(0), [][]interface{}(nil)).Return([]*datamodel.Backup{}, nil)

		backups, err := _getBackups(ctx, store, params, nil)
		assert.NoError(tt, err)
		assert.NotNil(tt, backups)
	})
}

func Test_createBackupEdgeCases(t *testing.T) {
	ctx := context.Background()
	params := &common.CreateBackupParams{
		BackupName:    "testBackup",
		VolumeUUID:    "testVolumeUUID",
		BackupVaultID: "testVaultID",
		Description:   "desc",
		BackupType:    "FULL",
	}

	t.Run("Success", func(t *testing.T) {
		store := database.NewMockStorage(t)
		temporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acc"}
		volume := &datamodel.Volume{
			Name:             "vol",
			Account:          account,
			VolumeAttributes: &datamodel.VolumeAttributes{Protocols: []string{"NFS"}},
			State:            "READY",
			DataProtection:   &datamodel.DataProtection{BackupVaultID: "testVaultID"},
		}
		backupVault := &datamodel.BackupVault{
			BaseModel:        datamodel.BaseModel{ID: 1, UUID: "testVaultID"},
			AccountID:        1,
			SourceRegionName: func() *string { s := "us-east1"; return &s }(),
		}
		job := &datamodel.Job{WorkflowID: "wf-id", BaseModel: datamodel.BaseModel{UUID: "job-uuid"}}
		backup := &datamodel.Backup{
			BaseModel:    datamodel.BaseModel{UUID: "backup-uuid"},
			Name:         params.BackupName,
			State:        models.LifeCycleStateCreating,
			StateDetails: models.LifeCycleStateCreatingDetails,
			BackupVault:  backupVault,
			Attributes:   &datamodel.BackupAttributes{VolumeName: "vol"},
		}
		validateCreateBackupParams = func(ctx context.Context, se database.Storage, params *common.CreateBackupParams) error {
			return nil
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateBackupParams = _validateCreateBackupParams
		}()
		// store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
		store.On("GetVolumeWithAccountID", ctx, params.VolumeUUID, int64(1)).Return(volume, nil)
		store.On("GetBackupVault", ctx, params.BackupVaultID).Return(backupVault, nil)
		store.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		store.On("CreateBackup", ctx, mock.Anything).Return(backup, nil)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()

		_, jobID, err := _createBackup(ctx, store, temporal, params)
		assert.NoError(t, err)
		assert.Equal(t, "job-uuid", jobID)
	})

	t.Run("FailsValidation", func(t *testing.T) {
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		validateCreateBackupParams = func(ctx context.Context, se database.Storage, params *common.CreateBackupParams) error {
			return errors.New("validation failed")
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acc"}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateBackupParams = _validateCreateBackupParams
		}()
		_, _, err := _createBackup(ctx, store, temporal, params)
		assert.EqualError(t, err, "validation failed")
	})

	t.Run("FailsBackupVaultFetch", func(t *testing.T) {
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}}
		volume := &datamodel.Volume{
			Name:             "vol",
			Account:          account,
			VolumeAttributes: &datamodel.VolumeAttributes{Protocols: []string{"NFS"}},
			State:            "READY",
			DataProtection:   &datamodel.DataProtection{BackupVaultID: "testVaultID"},
		}
		validateCreateBackupParams = func(ctx context.Context, se database.Storage, params *common.CreateBackupParams) error {
			return nil
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateBackupParams = _validateCreateBackupParams
		}()
		// store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
		store.On("GetVolumeWithAccountID", ctx, params.VolumeUUID, int64(1)).Return(volume, nil)
		store.On("GetBackupVault", ctx, params.BackupVaultID).Return(nil, errors.New("backup vault not found"))
		_, _, err := _createBackup(ctx, store, temporal, params)
		assert.EqualError(t, err, "backup vault not found")
	})

	t.Run("FailsJobCreation", func(t *testing.T) {
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acc"}
		volume := &datamodel.Volume{
			Name:             "vol",
			Account:          account,
			VolumeAttributes: &datamodel.VolumeAttributes{Protocols: []string{"NFS"}},
			State:            "READY",
			DataProtection:   &datamodel.DataProtection{BackupVaultID: "testVaultID"},
		}
		validateCreateBackupParams = func(ctx context.Context, se database.Storage, params *common.CreateBackupParams) error {
			return nil
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateBackupParams = _validateCreateBackupParams
		}()
		backupVault := &datamodel.BackupVault{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1, SourceRegionName: func() *string { s := "us-east1"; return &s }()}
		store.On("GetVolumeWithAccountID", ctx, params.VolumeUUID, int64(1)).Return(volume, nil)
		store.On("GetBackupVault", ctx, params.BackupVaultID).Return(backupVault, nil)
		store.On("CreateJob", ctx, mock.Anything).Return(nil, errors.New("job create failed"))
		_, _, err := _createBackup(ctx, store, temporal, params)
		assert.EqualError(t, err, "job create failed")
	})
}

func TestDeleteBackup(t *testing.T) {
	t.Run("onSuccess", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acc"}
		params := &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			AccountName:     "acc",
			BackupVaultUUID: "testVaultID",
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateBackupDeleteParams = func(ctx context.Context, se database.Storage, params *common.DeleteBackupParams) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateBackupDeleteParams = _validateBackupDeleteParams
		}()
		backup := &datamodel.Backup{
			BaseModel:   datamodel.BaseModel{UUID: params.BackupUUID},
			BackupVault: &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: params.BackupVaultUUID}},
		}
		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, account.Name).Return(backup, nil)
		conditions := [][]interface{}{{"volume_attributes->>'restored_backup_id' = ?", backup.UUID}, {"state = ?", "RESTORING"}}
		store.On("ListVolumes", ctx, conditions).Return(nil, nil)
		store.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "job-uuid"}}, nil)
		store.On("UpdateBackupState", ctx, mock.Anything).Return(backup, nil)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		_, jobUUID, err := deleteBackup(ctx, store, temporal, params)
		assert.NoError(t, err)
		assert.Equal(t, "job-uuid", jobUUID)
	})
	t.Run("onGetAccountError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		params := &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			AccountName:     "acc",
			BackupVaultUUID: "testVaultID",
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()

		_, _, err := deleteBackup(ctx, store, temporal, params)
		assert.EqualError(t, err, "account not found")
	})
	t.Run("onValidationError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		params := &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			AccountName:     "acc",
			BackupVaultUUID: "testVaultID",
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acc"}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateBackupDeleteParams = func(ctx context.Context, se database.Storage, params *common.DeleteBackupParams) error {
			return errors.New("validation failed")
		}
		defer func() { validateBackupDeleteParams = _validateBackupDeleteParams }()

		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, account.Name).Return(&datamodel.Backup{State: models.LifeCycleStateAvailable, Attributes: &datamodel.BackupAttributes{DeleteInitiated: false}}, nil)
		_, _, err := deleteBackup(ctx, store, temporal, params)
		assert.EqualError(t, err, "validation failed")
	})
	t.Run("onGetBackupError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		params := &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			AccountName:     "acc",
			BackupVaultUUID: "testVaultID",
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acc"}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateBackupDeleteParams = func(ctx context.Context, se database.Storage, params *common.DeleteBackupParams) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateBackupDeleteParams = _validateBackupDeleteParams
		}()

		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, account.Name).Return(nil, errors.New("backup not found"))

		_, _, err := deleteBackup(ctx, store, temporal, params)
		assert.EqualError(t, err, "backup not found")
	})
	t.Run("onBackupStateDeletingWithJobs", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		params := &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			AccountName:     "acc",
			BackupVaultUUID: "testVaultID",
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acc"}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: params.BackupUUID},
			State:     models.LifeCycleStateDeleting,
		}
		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, account.Name).Return(backup, nil)

		// Mock GetJobsWithCondition to return jobs
		mockJobs := []*datamodel.Job{
			{BaseModel: datamodel.BaseModel{UUID: "job-uuid-1"}, State: string(models.JobsStateNEW)},
			{BaseModel: datamodel.BaseModel{UUID: "job-uuid-2"}, State: string(models.JobsStatePROCESSING)},
		}
		store.On("GetJobsWithCondition", ctx, mock.Anything).Return(mockJobs, nil)

		_, jobUUID, err := deleteBackup(ctx, store, temporal, params)
		assert.NoError(t, err)
		assert.Equal(t, "job-uuid-1", jobUUID) // Should return first job
	})
	t.Run("onBackupStateDeletingWithNoJobs", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		params := &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			AccountName:     "acc",
			BackupVaultUUID: "testVaultID",
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acc"}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: params.BackupUUID},
			State:     models.LifeCycleStateDeleting,
		}
		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, account.Name).Return(backup, nil)

		// Mock GetJobsWithCondition to return empty jobs
		store.On("GetJobsWithCondition", ctx, mock.Anything).Return([]*datamodel.Job{}, nil)

		// Should continue to normal flow since no jobs found
		validateBackupDeleteParams = func(ctx context.Context, se database.Storage, params *common.DeleteBackupParams) error {
			return nil
		}
		conditions := [][]interface{}{{"volume_attributes->>'restored_backup_id' = ?", backup.UUID}, {"state = ?", "RESTORING"}}
		store.On("ListVolumes", ctx, conditions).Return(nil, nil)
		store.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "job-uuid"}}, nil)
		store.On("UpdateBackupState", ctx, mock.Anything).Return(backup, nil)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		defer func() { validateBackupDeleteParams = _validateBackupDeleteParams }()

		_, jobUUID, err := deleteBackup(ctx, store, temporal, params)
		assert.NoError(t, err)
		assert.Equal(t, "job-uuid", jobUUID)
	})
	t.Run("onBackupStateDeletingWithGetJobsError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		params := &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			AccountName:     "acc",
			BackupVaultUUID: "testVaultID",
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acc"}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: params.BackupUUID},
			State:     models.LifeCycleStateDeleting,
		}
		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, account.Name).Return(backup, nil)

		// Mock GetJobsWithCondition to return error
		store.On("GetJobsWithCondition", ctx, mock.Anything).Return(nil, errors.New("database error"))

		_, _, err := deleteBackup(ctx, store, temporal, params)
		assert.EqualError(t, err, "database error")
	})
	t.Run("onJobCreationError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		params := &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			AccountName:     "acc",
			BackupVaultUUID: "testVaultID",
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acc"}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateBackupDeleteParams = func(ctx context.Context, se database.Storage, params *common.DeleteBackupParams) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateBackupDeleteParams = _validateBackupDeleteParams
		}()
		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, account.Name).Return(&datamodel.Backup{}, nil)
		conditions := [][]interface{}{{"volume_attributes->>'restored_backup_id' = ?", ""}, {"state = ?", "RESTORING"}}
		store.On("ListVolumes", ctx, conditions).Return(nil, nil)
		store.On("CreateJob", ctx, mock.Anything).Return(nil, errors.New("job creation failed"))

		_, _, err := deleteBackup(ctx, store, temporal, params)
		assert.EqualError(t, err, "job creation failed")
	})
	t.Run("onUpdateBackupStateError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		params := &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			AccountName:     "acc",
			BackupVaultUUID: "testVaultID",
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acc"}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateBackupDeleteParams = func(ctx context.Context, se database.Storage, params *common.DeleteBackupParams) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateBackupDeleteParams = _validateBackupDeleteParams
		}()
		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, account.Name).Return(&datamodel.Backup{}, nil)
		conditions := [][]interface{}{{"volume_attributes->>'restored_backup_id' = ?", ""}, {"state = ?", "RESTORING"}}
		store.On("ListVolumes", ctx, conditions).Return(nil, nil)
		store.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{}, nil)
		store.On("UpdateBackupState", ctx, mock.Anything).Return(nil, errors.New("update state failed"))

		store.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		_, _, err := deleteBackup(ctx, store, temporal, params)
		assert.EqualError(t, err, "update state failed")
	})
	t.Run("onWorkflowExecutionError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		params := &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			AccountName:     "acc",
			BackupVaultUUID: "testVaultID",
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acc"}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateBackupDeleteParams = func(ctx context.Context, se database.Storage, params *common.DeleteBackupParams) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateBackupDeleteParams = _validateBackupDeleteParams
		}()
		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, account.Name).Return(&datamodel.Backup{}, nil)
		conditions := [][]interface{}{{"volume_attributes->>'restored_backup_id' = ?", ""}, {"state = ?", "RESTORING"}}
		store.On("ListVolumes", ctx, conditions).Return(nil, nil)
		store.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{}, nil)
		store.On("UpdateBackupState", ctx, mock.Anything).Return(&datamodel.Backup{}, nil)
		store.On("UpdateBackupState", ctx, mock.Anything).Return(&datamodel.Backup{}, nil)
		store.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow execution failed")).Once()

		_, _, err := deleteBackup(ctx, store, temporal, params)
		assert.EqualError(t, err, "workflow execution failed")
	})
}

// TestDeleteBackup_CrossRegionBackupVault tests the _deleteBackup function with CROSS_REGION backup vault
func TestDeleteBackup_CrossRegionBackupVault(t *testing.T) {
	t.Run("WhenRemoteBackupIsRestoring_ReturnsError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"}
		externalBackupVaultUUID := "external-vault-uuid"
		externalBackupUUID := "external-backup-uuid"
		backupRegionName := "us-central1"
		backup := &datamodel.Backup{
			BaseModel:    datamodel.BaseModel{UUID: "test-backup-uuid"},
			ExternalUUID: externalBackupUUID,
			State:        models.LifeCycleStateAvailable,
			Attributes:   &datamodel.BackupAttributes{DeleteInitiated: false},
			BackupVault: &datamodel.BackupVault{
				BaseModel:        datamodel.BaseModel{UUID: "test-vault-uuid"},
				BackupVaultType:  "CROSS_REGION",
				ExternalUUID:     &externalBackupVaultUUID,
				BackupRegionName: &backupRegionName,
			},
		}

		params := &common.DeleteBackupParams{
			BackupUUID:      backup.UUID,
			BackupVaultUUID: "test-vault-uuid",
			AccountName:     "test-account",
		}

		// Mock the validation function
		validateBackupDeleteParams = func(ctx context.Context, se database.Storage, params *common.DeleteBackupParams) error {
			return nil
		}
		defer func() { validateBackupDeleteParams = _validateBackupDeleteParams }()

		// Mock account lookup
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()

		// Mock backup retrieval
		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(backup, nil)

		// Mock ListVolumes to return empty (no local volumes restoring)
		conditions := [][]interface{}{{"volume_attributes->>'restored_backup_id' = ?", backup.UUID}, {"state = ?", models.LifeCycleStateRestoring}}
		store.On("ListVolumes", ctx, conditions).Return([]*datamodel.Volume{}, nil)

		// Mock fetchRemoteBackupFromVCP to return a backup that is restoring
		originalFetchRemoteBackup := fetchRemoteBackupFromVCP
		defer func() { fetchRemoteBackupFromVCP = originalFetchRemoteBackup }()

		remoteBackup := googleproxyclient.InternalBackupV1beta{
			IsRestoring: googleproxyclient.NewOptBool(true),
		}
		fetchRemoteBackupFromVCP = func(ctx context.Context, backupUUID, backupVaultUUID, projectNumber, region string) (googleproxyclient.InternalBackupV1beta, error) {
			return remoteBackup, nil
		}

		// Act
		result, jobID, err := deleteBackup(ctx, store, temporal, params)

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Cannot delete backup as restore is in progress for this backup in remote region")
		assert.Nil(t, result)
		assert.Equal(t, "", jobID)
		store.AssertExpectations(t)
	})

	t.Run("WhenRemoteBackupIsNotRestoring_Succeeds", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"}
		externalBackupVaultUUID := "external-vault-uuid"
		externalBackupUUID := "external-backup-uuid"
		backupRegionName := "us-central1"
		backup := &datamodel.Backup{
			BaseModel:    datamodel.BaseModel{UUID: "test-backup-uuid"},
			ExternalUUID: externalBackupUUID,
			State:        models.LifeCycleStateAvailable,
			Attributes:   &datamodel.BackupAttributes{DeleteInitiated: false},
			BackupVault: &datamodel.BackupVault{
				BaseModel:        datamodel.BaseModel{UUID: "test-vault-uuid"},
				BackupVaultType:  "CROSS_REGION",
				ExternalUUID:     &externalBackupVaultUUID,
				BackupRegionName: &backupRegionName,
			},
		}

		params := &common.DeleteBackupParams{
			BackupUUID:      backup.UUID,
			BackupVaultUUID: "test-vault-uuid",
			AccountName:     "test-account",
		}

		// Mock the validation function
		validateBackupDeleteParams = func(ctx context.Context, se database.Storage, params *common.DeleteBackupParams) error {
			return nil
		}
		defer func() { validateBackupDeleteParams = _validateBackupDeleteParams }()

		// Mock account lookup
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()

		// Mock backup retrieval
		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(backup, nil)

		// Mock ListVolumes to return empty (no local volumes restoring)
		conditions := [][]interface{}{{"volume_attributes->>'restored_backup_id' = ?", backup.UUID}, {"state = ?", models.LifeCycleStateRestoring}}
		store.On("ListVolumes", ctx, conditions).Return([]*datamodel.Volume{}, nil)

		// Mock fetchRemoteBackupFromVCP to return a backup that is NOT restoring
		originalFetchRemoteBackup := fetchRemoteBackupFromVCP
		defer func() { fetchRemoteBackupFromVCP = originalFetchRemoteBackup }()

		remoteBackup := googleproxyclient.InternalBackupV1beta{
			IsRestoring: googleproxyclient.NewOptBool(false),
		}
		fetchRemoteBackupFromVCP = func(ctx context.Context, backupUUID, backupVaultUUID, projectNumber, region string) (googleproxyclient.InternalBackupV1beta, error) {
			return remoteBackup, nil
		}

		// Mock job creation and workflow execution for successful deletion
		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
		}
		store.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		store.On("UpdateBackupState", ctx, mock.Anything).Return(backup, nil)
		temporal.EXPECT().ExecuteWorkflow(
			mock.Anything,
			mock.Anything,
			mock.Anything,
			params,
		).Return(nil, nil).Once()

		// Act
		result, jobID, err := deleteBackup(ctx, store, temporal, params)

		// Assert
		assert.NoError(t, err)
		assert.Nil(t, result)
		assert.Equal(t, "job-uuid", jobID)
		store.AssertExpectations(t)
		temporal.AssertExpectations(t)
	})

	t.Run("WhenFetchRemoteBackupFails_ReturnsError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"}
		externalBackupVaultUUID := "external-vault-uuid"
		externalBackupUUID := "external-backup-uuid"
		backupRegionName := "us-central1"
		backup := &datamodel.Backup{
			BaseModel:    datamodel.BaseModel{UUID: "test-backup-uuid"},
			ExternalUUID: externalBackupUUID,
			State:        models.LifeCycleStateAvailable,
			Attributes:   &datamodel.BackupAttributes{DeleteInitiated: false},
			BackupVault: &datamodel.BackupVault{
				BaseModel:        datamodel.BaseModel{UUID: "test-vault-uuid"},
				BackupVaultType:  "CROSS_REGION",
				ExternalUUID:     &externalBackupVaultUUID,
				BackupRegionName: &backupRegionName,
			},
		}

		params := &common.DeleteBackupParams{
			BackupUUID:      backup.UUID,
			BackupVaultUUID: "test-vault-uuid",
			AccountName:     "test-account",
		}

		// Mock the validation function
		validateBackupDeleteParams = func(ctx context.Context, se database.Storage, params *common.DeleteBackupParams) error {
			return nil
		}
		defer func() { validateBackupDeleteParams = _validateBackupDeleteParams }()

		// Mock account lookup
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()

		// Mock backup retrieval
		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(backup, nil)

		// Mock ListVolumes to return empty (no local volumes restoring)
		conditions := [][]interface{}{{"volume_attributes->>'restored_backup_id' = ?", backup.UUID}, {"state = ?", models.LifeCycleStateRestoring}}
		store.On("ListVolumes", ctx, conditions).Return([]*datamodel.Volume{}, nil)

		// Mock fetchRemoteBackupFromVCP to return an error
		originalFetchRemoteBackup := fetchRemoteBackupFromVCP
		defer func() { fetchRemoteBackupFromVCP = originalFetchRemoteBackup }()

		expectedError := errors.New("failed to fetch remote backup")
		fetchRemoteBackupFromVCP = func(ctx context.Context, backupUUID, backupVaultUUID, projectNumber, region string) (googleproxyclient.InternalBackupV1beta, error) {
			return googleproxyclient.InternalBackupV1beta{}, expectedError
		}

		// Act
		result, jobID, err := deleteBackup(ctx, store, temporal, params)

		// Assert
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		assert.Nil(t, result)
		assert.Equal(t, "", jobID)
		store.AssertExpectations(t)
	})
}

func TestValidateBackupDeleteParams(t *testing.T) {
	t.Run("OnSuccess", func(t *testing.T) {
		// Ensure immutable backup feature is disabled for this test
		originalValue := utils.IsImmutableBackupEnabled()
		defer utils.SetImmutableBackupEnabledForTest(originalValue)
		utils.SetImmutableBackupEnabledForTest(false)

		ctx := context.Background()
		store := database.NewMockStorage(t)
		backup := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "testBackupUUID"},
			VolumeUUID: "volumeUUID1",
			BackupVault: &datamodel.BackupVault{
				BackupVaultType: "IN_REGION",
			},
		}
		store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(backup, nil)
		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, backup.VolumeUUID).Return(false, nil)
		store.On("IsLatestBackup", ctx, backup.UUID, backup.VolumeUUID).Return(false, nil)
		store.On("BackupCountByVolumeID", ctx, backup.VolumeUUID).Return(int64(2), nil)
		err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			BackupVaultUUID: "testVaultID",
			AccountName:     "testAccount",
		})
		assert.Nil(t, err)
	})

	t.Run("OnSuccessWithImmutableBackupEnabled", func(t *testing.T) {
		// Enable immutable backup feature for this test
		originalValue := utils.IsImmutableBackupEnabled()
		defer utils.SetImmutableBackupEnabledForTest(originalValue)
		utils.SetImmutableBackupEnabledForTest(true)

		ctx := context.Background()
		store := database.NewMockStorage(t)
		backup := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "testBackupUUID"},
			VolumeUUID: "volumeUUID1",
			BackupVault: &datamodel.BackupVault{
				BackupVaultType: "IN_REGION",
			},
		}
		store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(backup, nil)
		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, backup.VolumeUUID).Return(false, nil)
		store.On("IsLatestBackup", ctx, backup.UUID, backup.VolumeUUID).Return(false, nil)
		store.On("BackupCountByVolumeID", ctx, backup.VolumeUUID).Return(int64(2), nil)
		store.On("GetBackupVault", ctx, "testVaultID").Return(&datamodel.BackupVault{
			ImmutableAttributes: nil, // No immutable attributes for this test
		}, nil)
		err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			BackupVaultUUID: "testVaultID",
			AccountName:     "testAccount",
		})
		assert.Nil(t, err)
	})
	t.Run("OnLatestBackupError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		backup := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "testBackupUUID"},
			VolumeUUID: "volumeUUID1",
			BackupVault: &datamodel.BackupVault{
				BackupVaultType: "IN_REGION",
			},
		}
		store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(backup, nil)
		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, backup.VolumeUUID).Return(false, nil)
		store.On("IsLatestBackup", ctx, backup.UUID, backup.VolumeUUID).Return(true, nil)
		store.On("BackupCountByVolumeID", ctx, backup.VolumeUUID).Return(int64(2), nil)
		err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			BackupVaultUUID: "testVaultID",
			AccountName:     "testAccount",
		})
		assert.NotNil(t, err)
		assert.EqualError(t, err, "Cannot delete latest backup")
	})
	t.Run("OnBackupNotFound", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(nil, vsaerror.NewNotFoundErr("backup", nil))
		err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			BackupVaultUUID: "testVaultID",
			AccountName:     "testAccount",
		})
		assert.NotNil(t, err)
		assert.EqualError(t, err, "Backup not found")
	})
	t.Run("OnGetBackupDBError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(nil, errors.New("failed"))
		err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			BackupVaultUUID: "testVaultID",
			AccountName:     "testAccount",
		})
		assert.NotNil(t, err)
		assert.EqualError(t, err, "failed")
	})
	t.Run("IsLatestBackupDBError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		backup := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "testBackupUUID"},
			VolumeUUID: "volumeUUID1",
			BackupVault: &datamodel.BackupVault{
				BackupVaultType: "IN_REGION",
			},
		}
		store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(backup, nil)
		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, backup.VolumeUUID).Return(false, nil)
		store.On("IsLatestBackup", ctx, backup.UUID, backup.VolumeUUID).Return(false, errors.New("error checking latest backup"))
		err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			BackupVaultUUID: "testVaultID",
			AccountName:     "testAccount",
		})
		assert.NotNil(t, err)
		assert.EqualError(t, err, "error checking latest backup")
	})
	t.Run("OnBackupCountByVolumeIDError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		backup := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "testBackupUUID"},
			VolumeUUID: "volumeUUID1",
			BackupVault: &datamodel.BackupVault{
				BackupVaultType: "IN_REGION",
			},
		}
		store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(backup, nil)
		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, backup.VolumeUUID).Return(false, nil)
		store.On("IsLatestBackup", ctx, backup.UUID, backup.VolumeUUID).Return(false, nil)
		store.On("BackupCountByVolumeID", ctx, backup.VolumeUUID).Return(int64(0), errors.New("error counting backups"))
		err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			BackupVaultUUID: "testVaultID",
			AccountName:     "testAccount",
		})
		assert.NotNil(t, err)
		assert.EqualError(t, err, "error counting backups")
	})
	t.Run("OnBackupInTransitionState", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		backup := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "testBackupUUID"},
			VolumeUUID: "volumeUUID1",
		}
		store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(backup, nil)
		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, backup.VolumeUUID).Return(true, nil)
		err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			BackupVaultUUID: "testVaultID",
			AccountName:     "testAccount",
		})
		assert.NotNil(t, err)
		assert.EqualError(t, err, "A backup operation from the same volume is currently in progress. Please wait for it to complete before starting a new backup")
	})
	t.Run("OnBackupInTransitionStateCheckError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		backup := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "testBackupUUID"},
			VolumeUUID: "volumeUUID1",
		}
		store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(backup, nil)
		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, backup.VolumeUUID).Return(false, errors.New("transition state check error"))
		err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			BackupVaultUUID: "testVaultID",
			AccountName:     "testAccount",
		})
		assert.NotNil(t, err)
		assert.EqualError(t, err, "transition state check error")
	})

	t.Run("OnBackupDeleteFromDestinationRegion", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		backup := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "testBackupUUID"},
			VolumeUUID: "volumeUUID1",
			BackupVault: &datamodel.BackupVault{
				BackupVaultType:  activities.CrossRegionBackupType,
				BackupRegionName: nillable.ToPointer("us-central1"),
			},
		}
		store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(backup, nil)
		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, backup.VolumeUUID).Return(false, nil)
		err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			BackupVaultUUID: "testVaultID",
			AccountName:     "testAccount",
			Region:          "us-central1",
		})
		assert.NotNil(t, err)
		assert.EqualError(t, err, "Cannot delete backup from the destination region")
	})

	// Immutable backup test cases
	t.Run("ImmutableBackupTests", func(t *testing.T) {
		t.Run("OnSuccessWithNonImmutableBackupVault", func(t *testing.T) {
			// Enable immutable backup feature for this test
			originalValue := utils.IsImmutableBackupEnabled()
			defer utils.SetImmutableBackupEnabledForTest(originalValue)
			utils.SetImmutableBackupEnabledForTest(true)

			ctx := context.Background()
			store := database.NewMockStorage(t)
			backup := &datamodel.Backup{
				BaseModel:  datamodel.BaseModel{UUID: "testBackupUUID"},
				VolumeUUID: "volumeUUID1",
				BackupVault: &datamodel.BackupVault{
					BackupVaultType: "IN_REGION",
				},
			}
			store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(backup, nil)
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, backup.VolumeUUID).Return(false, nil)
			store.On("IsLatestBackup", ctx, backup.UUID, backup.VolumeUUID).Return(false, nil)
			store.On("BackupCountByVolumeID", ctx, backup.VolumeUUID).Return(int64(2), nil)
			store.On("GetBackupVault", ctx, "testVaultID").Return(&datamodel.BackupVault{
				ImmutableAttributes: nil, // No immutable attributes
			}, nil)
			err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
				BackupUUID:      "testBackupUUID",
				BackupVaultUUID: "testVaultID",
				AccountName:     "testAccount",
			})
			assert.Nil(t, err)
		})

		t.Run("OnSuccessWithImmutableBackupVaultButNoRetentionPeriod", func(t *testing.T) {
			// Enable immutable backup feature for this test
			originalValue := utils.IsImmutableBackupEnabled()
			defer utils.SetImmutableBackupEnabledForTest(originalValue)
			utils.SetImmutableBackupEnabledForTest(true)

			ctx := context.Background()
			store := database.NewMockStorage(t)
			backup := &datamodel.Backup{
				BaseModel:  datamodel.BaseModel{UUID: "testBackupUUID"},
				VolumeUUID: "volumeUUID1",
				BackupVault: &datamodel.BackupVault{
					BackupVaultType: "IN_REGION",
				},
			}
			store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(backup, nil)
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, backup.VolumeUUID).Return(false, nil)
			store.On("IsLatestBackup", ctx, backup.UUID, backup.VolumeUUID).Return(false, nil)
			store.On("BackupCountByVolumeID", ctx, backup.VolumeUUID).Return(int64(2), nil)
			minRetDuration := int64(0)
			store.On("GetBackupVault", ctx, "testVaultID").Return(&datamodel.BackupVault{
				ImmutableAttributes: &datamodel.ImmutableAttributes{
					BackupMinimumEnforcedRetentionDuration: &minRetDuration,
					IsDailyBackupImmutable:                 false,
					IsWeeklyBackupImmutable:                false,
					IsMonthlyBackupImmutable:               false,
					IsAdhocBackupImmutable:                 false,
				},
			}, nil)
			err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
				BackupUUID:      "testBackupUUID",
				BackupVaultUUID: "testVaultID",
				AccountName:     "testAccount",
			})
			assert.Nil(t, err)
		})

		t.Run("OnFailureWithImmutableDailyBackupNotExpired", func(t *testing.T) {
			// Enable immutable backup feature for this test
			originalValue := utils.IsImmutableBackupEnabled()
			defer utils.SetImmutableBackupEnabledForTest(originalValue)
			utils.SetImmutableBackupEnabledForTest(true)

			ctx := context.Background()
			store := database.NewMockStorage(t)
			backupCreatedTime := time.Now().AddDate(0, 0, -5) // Created 5 days ago
			minRetDuration := int64(10)                       // 10 days retention (not expired)
			scheduleTag := common.ScheduleTagDaily
			backup := &datamodel.Backup{
				BaseModel:   datamodel.BaseModel{UUID: "testBackupUUID", CreatedAt: backupCreatedTime},
				VolumeUUID:  "volumeUUID1",
				Type:        utils.BackupTypeSCHEDULED,
				ScheduleTag: &scheduleTag,
				BackupVault: &datamodel.BackupVault{
					BackupVaultType: "IN_REGION",
				},
			}
			store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(backup, nil)
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, backup.VolumeUUID).Return(false, nil)
			store.On("IsLatestBackup", ctx, backup.UUID, backup.VolumeUUID).Return(false, nil)
			store.On("BackupCountByVolumeID", ctx, backup.VolumeUUID).Return(int64(2), nil)
			store.On("GetBackupVault", ctx, "testVaultID").Return(&datamodel.BackupVault{
				ImmutableAttributes: &datamodel.ImmutableAttributes{
					BackupMinimumEnforcedRetentionDuration: &minRetDuration,
					IsDailyBackupImmutable:                 true,
					IsWeeklyBackupImmutable:                false,
					IsMonthlyBackupImmutable:               false,
					IsAdhocBackupImmutable:                 false,
				},
			}, nil)
			err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
				BackupUUID:      "testBackupUUID",
				BackupVaultUUID: "testVaultID",
				AccountName:     "testAccount",
			})
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "Cannot delete backup before minimum retention period")
		})

		t.Run("OnSuccessWithImmutableDailyBackupExpired", func(t *testing.T) {
			// Enable immutable backup feature for this test
			originalValue := utils.IsImmutableBackupEnabled()
			defer utils.SetImmutableBackupEnabledForTest(originalValue)
			utils.SetImmutableBackupEnabledForTest(true)

			ctx := context.Background()
			store := database.NewMockStorage(t)
			backupCreatedTime := time.Now().AddDate(0, 0, -15) // Created 15 days ago
			minRetDuration := int64(10)                        // 10 days retention (expired)
			scheduleTag := common.ScheduleTagDaily
			backup := &datamodel.Backup{
				BaseModel:   datamodel.BaseModel{UUID: "testBackupUUID", CreatedAt: backupCreatedTime},
				VolumeUUID:  "volumeUUID1",
				Type:        utils.BackupTypeSCHEDULED,
				ScheduleTag: &scheduleTag,
				BackupVault: &datamodel.BackupVault{
					BackupVaultType: "IN_REGION",
				},
			}
			store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(backup, nil)
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, backup.VolumeUUID).Return(false, nil)
			store.On("IsLatestBackup", ctx, backup.UUID, backup.VolumeUUID).Return(false, nil)
			store.On("BackupCountByVolumeID", ctx, backup.VolumeUUID).Return(int64(2), nil)
			store.On("GetBackupVault", ctx, "testVaultID").Return(&datamodel.BackupVault{
				ImmutableAttributes: &datamodel.ImmutableAttributes{
					BackupMinimumEnforcedRetentionDuration: &minRetDuration,
					IsDailyBackupImmutable:                 true,
					IsWeeklyBackupImmutable:                false,
					IsMonthlyBackupImmutable:               false,
					IsAdhocBackupImmutable:                 false,
				},
			}, nil)
			err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
				BackupUUID:      "testBackupUUID",
				BackupVaultUUID: "testVaultID",
				AccountName:     "testAccount",
			})
			assert.Nil(t, err)
		})

		t.Run("OnSuccessWithNonImmutableBackupType", func(t *testing.T) {
			// Enable immutable backup feature for this test
			originalValue := utils.IsImmutableBackupEnabled()
			defer utils.SetImmutableBackupEnabledForTest(originalValue)
			utils.SetImmutableBackupEnabledForTest(true)

			ctx := context.Background()
			store := database.NewMockStorage(t)
			backupCreatedTime := time.Now().AddDate(0, 0, -5) // Created 5 days ago
			minRetDuration := int64(10)                       // 10 days retention
			scheduleTag := common.ScheduleTagWeekly           // Weekly backup but weekly immutable is disabled
			backup := &datamodel.Backup{
				BaseModel:   datamodel.BaseModel{UUID: "testBackupUUID", CreatedAt: backupCreatedTime},
				VolumeUUID:  "volumeUUID1",
				Type:        utils.BackupTypeSCHEDULED,
				ScheduleTag: &scheduleTag,
				BackupVault: &datamodel.BackupVault{
					BackupVaultType: "IN_REGION",
				},
			}
			store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(backup, nil)
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, backup.VolumeUUID).Return(false, nil)
			store.On("IsLatestBackup", ctx, backup.UUID, backup.VolumeUUID).Return(false, nil)
			store.On("BackupCountByVolumeID", ctx, backup.VolumeUUID).Return(int64(2), nil)
			store.On("GetBackupVault", ctx, "testVaultID").Return(&datamodel.BackupVault{
				ImmutableAttributes: &datamodel.ImmutableAttributes{
					BackupMinimumEnforcedRetentionDuration: &minRetDuration,
					IsDailyBackupImmutable:                 true,  // Daily is immutable
					IsWeeklyBackupImmutable:                false, // Weekly is not immutable
					IsMonthlyBackupImmutable:               false,
					IsAdhocBackupImmutable:                 false,
				},
			}, nil)
			err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
				BackupUUID:      "testBackupUUID",
				BackupVaultUUID: "testVaultID",
				AccountName:     "testAccount",
			})
			assert.Nil(t, err) // Should succeed because weekly backup is not immutable
		})

		t.Run("OnFailureWithImmutableManualBackupNotExpired", func(t *testing.T) {
			// Enable immutable backup feature for this test
			originalValue := utils.IsImmutableBackupEnabled()
			defer utils.SetImmutableBackupEnabledForTest(originalValue)
			utils.SetImmutableBackupEnabledForTest(true)

			ctx := context.Background()
			store := database.NewMockStorage(t)
			backupCreatedTime := time.Now().AddDate(0, 0, -5) // Created 5 days ago
			minRetDuration := int64(10)                       // 10 days retention (not expired)
			backup := &datamodel.Backup{
				BaseModel:  datamodel.BaseModel{UUID: "testBackupUUID", CreatedAt: backupCreatedTime},
				VolumeUUID: "volumeUUID1",
				Type:       utils.BackupTypeMANUAL,
				BackupVault: &datamodel.BackupVault{
					BackupVaultType: "IN_REGION",
				},
			}
			store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(backup, nil)
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, backup.VolumeUUID).Return(false, nil)
			store.On("IsLatestBackup", ctx, backup.UUID, backup.VolumeUUID).Return(false, nil)
			store.On("BackupCountByVolumeID", ctx, backup.VolumeUUID).Return(int64(2), nil)
			store.On("GetBackupVault", ctx, "testVaultID").Return(&datamodel.BackupVault{
				ImmutableAttributes: &datamodel.ImmutableAttributes{
					BackupMinimumEnforcedRetentionDuration: &minRetDuration,
					IsDailyBackupImmutable:                 false,
					IsWeeklyBackupImmutable:                false,
					IsMonthlyBackupImmutable:               false,
					IsAdhocBackupImmutable:                 true, // Manual backup is immutable
				},
			}, nil)
			err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
				BackupUUID:      "testBackupUUID",
				BackupVaultUUID: "testVaultID",
				AccountName:     "testAccount",
			})
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "Cannot delete backup before minimum retention period")
		})

		t.Run("OnBackupVaultNotFound", func(t *testing.T) {
			// Enable immutable backup feature for this test
			originalValue := utils.IsImmutableBackupEnabled()
			defer utils.SetImmutableBackupEnabledForTest(originalValue)
			utils.SetImmutableBackupEnabledForTest(true)

			ctx := context.Background()
			store := database.NewMockStorage(t)
			backup := &datamodel.Backup{
				BaseModel:  datamodel.BaseModel{UUID: "testBackupUUID"},
				VolumeUUID: "volumeUUID1",
				BackupVault: &datamodel.BackupVault{
					BackupVaultType: "IN_REGION",
				},
			}
			store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(backup, nil)
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, backup.VolumeUUID).Return(false, nil)
			store.On("IsLatestBackup", ctx, backup.UUID, backup.VolumeUUID).Return(false, nil)
			store.On("BackupCountByVolumeID", ctx, backup.VolumeUUID).Return(int64(2), nil)
			store.On("GetBackupVault", ctx, "testVaultID").Return(nil, vsaerror.NewNotFoundErr("backup vault", nil))
			err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
				BackupUUID:      "testBackupUUID",
				BackupVaultUUID: "testVaultID",
				AccountName:     "testAccount",
			})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "Backup vault not found")
		})

		t.Run("OnGetBackupVaultError", func(t *testing.T) {
			// Enable immutable backup feature for this test
			originalValue := utils.IsImmutableBackupEnabled()
			defer utils.SetImmutableBackupEnabledForTest(originalValue)
			utils.SetImmutableBackupEnabledForTest(true)

			ctx := context.Background()
			store := database.NewMockStorage(t)
			backup := &datamodel.Backup{
				BaseModel:  datamodel.BaseModel{UUID: "testBackupUUID"},
				VolumeUUID: "volumeUUID1",
				BackupVault: &datamodel.BackupVault{
					BackupVaultType: "IN_REGION",
				},
			}
			store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(backup, nil)
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, backup.VolumeUUID).Return(false, nil)
			store.On("IsLatestBackup", ctx, backup.UUID, backup.VolumeUUID).Return(false, nil)
			store.On("BackupCountByVolumeID", ctx, backup.VolumeUUID).Return(int64(2), nil)
			store.On("GetBackupVault", ctx, "testVaultID").Return(nil, errors.New("database error"))
			err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
				BackupUUID:      "testBackupUUID",
				BackupVaultUUID: "testVaultID",
				AccountName:     "testAccount",
			})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "database error")
		})
	})
}

func TestValidateBackupDeleteParams_ImmutabilityChecks(t *testing.T) {
	t.Run("OnSuccessWithNonImmutableBackupVault", func(t *testing.T) {
		// Enable immutable backup feature for this test
		utils.SetImmutableBackupEnabledForTest(true)
		defer utils.SetImmutableBackupEnabledForTest(false)

		ctx := context.Background()
		store := database.NewMockStorage(t)
		backup := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "testBackupUUID"},
			VolumeUUID: "volumeUUID1",
			Type:       utils.BackupTypeSCHEDULED,
			Name:       "daily-backup-20230101",
			BackupVault: &datamodel.BackupVault{
				BackupVaultType: "IN_REGION",
			},
		}
		backupVault := &datamodel.BackupVault{
			ImmutableAttributes: nil, // No immutable attributes
		}

		store.On("GetBackup", ctx, "testVaultID", "testBackupUUID", "testAccount").Return(backup, nil)
		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, backup.VolumeUUID).Return(false, nil)
		store.On("IsLatestBackup", ctx, backup.UUID, backup.VolumeUUID).Return(false, nil)
		store.On("BackupCountByVolumeID", ctx, backup.VolumeUUID).Return(int64(2), nil)
		store.On("GetBackupVault", ctx, "testVaultID").Return(backupVault, nil)

		err := validateBackupDeleteParams(ctx, store, &common.DeleteBackupParams{
			BackupUUID:      "testBackupUUID",
			BackupVaultUUID: "testVaultID",
			AccountName:     "testAccount",
		})
		assert.Nil(t, err)
	})
}

func TestGetBackupsUnderBackupVault(t *testing.T) {
	t.Run("WhenGetBackupsFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		orch := &Orchestrator{storage: mockStorage}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, ownerID string) (*datamodel.Account, error) {
			return &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}}, nil
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()

		getBackups = func(ctx context.Context, se database.Storage, params *common.GetBackupsParams, filters [][]interface{}) ([]*datamodel.Backup, error) {
			return nil, errors.New("failed to get backups")
		}
		defer func() { getBackups = _getBackups }()
		mockStorage.On("GetBackupsByBackupVaultOwnerIDAndFilter", ctx, "backupVaultID", int64(1), [][]interface{}{
			{"uuid in ?", []string{"backupUUID"}},
		}).Return(nil, errors.New("failed to get backups"))
		backups, err := orch.GetBackupsUnderBackupVault(ctx, "backupVaultID", "ownerID", []string{"backupUUID"})
		assert.Nil(tt, backups)
		assert.EqualError(tt, err, "failed to get backups")
	})

	t.Run("WhenGetBackupsSucceeds", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		orch := &Orchestrator{storage: mockStorage}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, ownerID string) (*datamodel.Account, error) {
			return &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}}, nil
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()

		expectedBackups := []*datamodel.Backup{
			{BaseModel: datamodel.BaseModel{UUID: "backupUUID1"}},
			{BaseModel: datamodel.BaseModel{UUID: "backupUUID2"}},
		}
		getBackups = func(ctx context.Context, se database.Storage, params *common.GetBackupsParams, filters [][]interface{}) ([]*datamodel.Backup, error) {
			return expectedBackups, nil
		}
		defer func() { getBackups = _getBackups }()
		// Set up the mock expectation for GetBackupsByBackupVaultOwnerIDAndFilter
		mockStorage.On("GetBackupsByBackupVaultOwnerIDAndFilter", ctx, "backupVaultID", int64(1), [][]interface{}{
			{"uuid in ?", []string{"backupUUID"}},
		}).Return(expectedBackups, nil)
		backups, err := orch.GetBackupsUnderBackupVault(ctx, "backupVaultID", "ownerID", []string{"backupUUID"})
		assert.NoError(tt, err)
		assert.Equal(tt, expectedBackups, backups)
	})
}

func TestUpdateBackup(t *testing.T) {
	t.Run("onGetAccountError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		params := &common.UpdateBackupParams{
			BackupUUID:      "testBackupUUID",
			AccountName:     "acc",
			BackupVaultUUID: "testVaultID",
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()

		_, _, err := updateBackup(ctx, store, temporal, params)
		assert.EqualError(t, err, "account not found")
	})

	t.Run("onGetBackupError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		params := &common.UpdateBackupParams{
			BackupUUID:      "testBackupUUID",
			AccountName:     "acc",
			BackupVaultUUID: "testVaultID",
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acc"}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()

		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, account.Name).Return(nil, errors.New("backup not found"))

		_, _, err := updateBackup(ctx, store, temporal, params)
		assert.EqualError(t, err, "backup not found")
	})

	t.Run("onWorkflowExecutionError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		params := &common.UpdateBackupParams{
			BackupUUID:      "testBackupUUID",
			AccountName:     "acc",
			BackupVaultUUID: "testVaultID",
			Description:     "Updated description",
		}

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acc"}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()
		backup := &datamodel.Backup{
			BaseModel:   datamodel.BaseModel{UUID: params.BackupUUID},
			BackupVault: &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: params.BackupVaultUUID}},
			State:       models.LifeCycleStateAvailable,
		}
		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, account.Name).Return(backup, nil)
		store.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "job-uuid"}}, nil)
		store.On("UpdateBackupState", ctx, mock.Anything).Return(backup, nil)
		store.On("UpdateBackupState", ctx, mock.Anything).Return(&datamodel.Backup{}, nil)
		store.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow execution failed")).Once()

		_, _, err := updateBackup(ctx, store, temporal, params)
		assert.EqualError(t, err, "workflow execution failed")
	})

	t.Run("onInvalidBackupState", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		params := &common.UpdateBackupParams{
			BackupUUID:      "testBackupUUID",
			AccountName:     "acc",
			BackupVaultUUID: "testVaultID",
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acc"}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()
		backup := &datamodel.Backup{
			BaseModel:   datamodel.BaseModel{UUID: params.BackupUUID},
			BackupVault: &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: params.BackupVaultUUID}},
			State:       models.LifeCycleStateCreating,
		}
		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, account.Name).Return(backup, nil)

		_, _, err := updateBackup(ctx, store, temporal, params)
		assert.EqualError(t, err, "Backup can only be updated when in AVAILABLE state, current state: CREATING")
	})

	t.Run("onUpdatingBackupFromDestinationRegion", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		params := &common.UpdateBackupParams{
			BackupUUID:      "testBackupUUID",
			AccountName:     "acc",
			BackupVaultUUID: "testVaultID",
			Region:          "us-central1",
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acc"}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()
		backup := &datamodel.Backup{
			BaseModel:   datamodel.BaseModel{UUID: params.BackupUUID},
			BackupVault: &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: params.BackupVaultUUID}, BackupVaultType: activities.CrossRegionBackupType, BackupRegionName: nillable.ToPointer("us-central1")},
			State:       models.LifeCycleStateAvailable,
		}
		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, account.Name).Return(backup, nil)

		_, _, err := updateBackup(ctx, store, temporal, params)
		assert.EqualError(t, err, "Cannot update backup from the destination region")
	})

	t.Run("onJobCreationError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		temporal := new(workflow_engine_mock.MockTemporalTestClient)
		params := &common.UpdateBackupParams{
			BackupUUID:      "testBackupUUID",
			AccountName:     "acc",
			BackupVaultUUID: "testVaultID",
			Description:     "Updated description",
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acc"}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()
		backup := &datamodel.Backup{
			BaseModel:   datamodel.BaseModel{UUID: params.BackupUUID},
			BackupVault: &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: params.BackupVaultUUID}},
			State:       models.LifeCycleStateAvailable,
		}
		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, account.Name).Return(backup, nil)
		store.On("CreateJob", ctx, mock.Anything).Return(nil, errors.New("job creation failed"))

		_, _, err := updateBackup(ctx, store, temporal, params)
		assert.EqualError(t, err, "job creation failed")
	})
}

func TestValidateSnapshotForBackup_SnapshotAlreadyUsed_Integration(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

	t.Run("SnapshotAlreadyUsedForAvailableBackup", func(t *testing.T) {
		store, err := database.NewTestStorage(mockLogger)
		assert.NoError(t, err, "Failed to create test storage")
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(t, err, "Failed to clear in-memory DB")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:      "test-account",
		}
		err = store.DB().Create(account).Error
		assert.NoError(t, err)

		backupVault := &datamodel.BackupVault{
			BaseModel:        datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:             "test-vault",
			AccountID:        account.ID,
			SourceRegionName: func() *string { s := "us-east1"; return &s }(),
		}
		err = store.DB().Create(backupVault).Error
		assert.NoError(t, err)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:      "test-volume",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: backupVault.UUID,
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFS"},
			},
		}
		err = store.DB().Create(volume).Error
		assert.NoError(t, err)

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:      "test-snapshot",
			VolumeID:  volume.ID,
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				ExternalUUID: "ext-snapshot-uuid",
			},
		}
		err = store.DB().Create(snapshot).Error
		assert.NoError(t, err)

		existingBackup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:          "existing-backup",
			VolumeUUID:    volume.UUID,
			BackupVaultID: backupVault.ID,
			State:         models.LifeCycleStateAvailable,
			Attributes: &datamodel.BackupAttributes{
				SnapshotID: snapshot.SnapshotAttributes.ExternalUUID,
			},
		}
		err = store.DB().Create(existingBackup).Error
		assert.NoError(t, err)

		params := &common.CreateBackupParams{
			BackupName:          "new-backup",
			VolumeUUID:          volume.UUID,
			BackupVaultID:       backupVault.UUID,
			UseExistingSnapshot: true,
			SnapshotID:          snapshot.UUID,
		}

		vol, err := store.GetVolume(ctx, volume.UUID)
		assert.NoError(t, err)

		err = _validateSnapshotForBackup(ctx, store, params, vol)
		assert.EqualError(t, err, "This snapshot has already been used to create a backup")
	})

	t.Run("SnapshotUsedForNonAvailableBackup_ShouldPass", func(t *testing.T) {
		store, err := database.NewTestStorage(mockLogger)
		assert.NoError(t, err, "Failed to create test storage")
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(t, err, "Failed to clear in-memory DB")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:      "test-account",
		}
		err = store.DB().Create(account).Error
		assert.NoError(t, err)

		backupVault := &datamodel.BackupVault{
			BaseModel:        datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:             "test-vault",
			AccountID:        account.ID,
			SourceRegionName: func() *string { s := "us-east1"; return &s }(),
		}
		err = store.DB().Create(backupVault).Error
		assert.NoError(t, err)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:      "test-volume",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: backupVault.UUID,
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFS"},
			},
		}
		err = store.DB().Create(volume).Error
		assert.NoError(t, err)

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:      "test-snapshot",
			VolumeID:  volume.ID,
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				ExternalUUID: "ext-snapshot-uuid",
			},
		}
		err = store.DB().Create(snapshot).Error
		assert.NoError(t, err)

		assert.NoError(t, err)

		params := &common.CreateBackupParams{
			BackupName:          "new-backup",
			VolumeUUID:          volume.UUID,
			BackupVaultID:       backupVault.UUID,
			UseExistingSnapshot: true,
			SnapshotID:          snapshot.UUID,
		}

		vol, err := store.GetVolume(ctx, volume.UUID)
		assert.NoError(t, err)

		err = _validateSnapshotForBackup(ctx, store, params, vol)
		assert.NoError(t, err)
	})

	t.Run("MultipleBackupsCreationAttempts_BlocksSecondAttempt", func(t *testing.T) {
		store, err := database.NewTestStorage(mockLogger)
		assert.NoError(t, err, "Failed to create test storage")
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(t, err, "Failed to clear in-memory DB")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:      "test-account",
		}
		err = store.DB().Create(account).Error
		assert.NoError(t, err)

		backupVault := &datamodel.BackupVault{
			BaseModel:        datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:             "test-vault",
			AccountID:        account.ID,
			SourceRegionName: func() *string { s := "us-east1"; return &s }(),
		}
		err = store.DB().Create(backupVault).Error
		assert.NoError(t, err)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:      "test-volume",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: backupVault.UUID,
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFS"},
			},
		}
		err = store.DB().Create(volume).Error
		assert.NoError(t, err)

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:      "test-snapshot",
			VolumeID:  volume.ID,
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				ExternalUUID: "ext-snapshot-uuid",
			},
		}
		err = store.DB().Create(snapshot).Error
		assert.NoError(t, err)

		firstBackup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:          "first-backup",
			VolumeUUID:    volume.UUID,
			BackupVaultID: backupVault.ID,
			State:         models.LifeCycleStateAvailable,
			Attributes: &datamodel.BackupAttributes{
				SnapshotID: snapshot.SnapshotAttributes.ExternalUUID,
			},
		}
		err = store.DB().Create(firstBackup).Error
		assert.NoError(t, err)

		params := &common.CreateBackupParams{
			BackupName:          "second-backup",
			VolumeUUID:          volume.UUID,
			BackupVaultID:       backupVault.UUID,
			UseExistingSnapshot: true,
			SnapshotID:          snapshot.UUID,
		}

		vol, err := store.GetVolume(ctx, volume.UUID)
		assert.NoError(t, err)

		err = _validateSnapshotForBackup(ctx, store, params, vol)
		assert.EqualError(t, err, "This snapshot has already been used to create a backup")
	})

	t.Run("DifferentSnapshots_AllowMultipleBackups", func(t *testing.T) {
		store, err := database.NewTestStorage(mockLogger)
		assert.NoError(t, err, "Failed to create test storage")
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(t, err, "Failed to clear in-memory DB")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:      "test-account",
		}
		err = store.DB().Create(account).Error
		assert.NoError(t, err)

		backupVault := &datamodel.BackupVault{
			BaseModel:        datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:             "test-vault",
			AccountID:        account.ID,
			SourceRegionName: func() *string { s := "us-east1"; return &s }(),
		}
		err = store.DB().Create(backupVault).Error
		assert.NoError(t, err)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:      "test-volume",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: backupVault.UUID,
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFS"},
			},
		}
		err = store.DB().Create(volume).Error
		assert.NoError(t, err)

		snapshot1 := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:      "test-snapshot-1",
			VolumeID:  volume.ID,
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				ExternalUUID: "ext-snapshot-uuid-1",
			},
		}
		err = store.DB().Create(snapshot1).Error
		assert.NoError(t, err)

		backup1 := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:          "backup-1",
			VolumeUUID:    volume.UUID,
			BackupVaultID: backupVault.ID,
			State:         models.LifeCycleStateAvailable,
			Attributes: &datamodel.BackupAttributes{
				SnapshotID: snapshot1.SnapshotAttributes.ExternalUUID,
			},
		}
		err = store.DB().Create(backup1).Error
		assert.NoError(t, err)

		snapshot2 := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: utils.RandomUUID()},
			Name:      "test-snapshot-2",
			VolumeID:  volume.ID,
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				ExternalUUID: "ext-snapshot-uuid-2",
			},
		}
		err = store.DB().Create(snapshot2).Error
		assert.NoError(t, err)

		params := &common.CreateBackupParams{
			BackupName:          "backup-2",
			VolumeUUID:          volume.UUID,
			BackupVaultID:       backupVault.UUID,
			UseExistingSnapshot: true,
			SnapshotID:          snapshot2.UUID,
		}

		vol, err := store.GetVolume(ctx, volume.UUID)
		assert.NoError(t, err)

		// This should succeed because snapshot2 has not been used for any backup yet
		err = _validateSnapshotForBackup(ctx, store, params, vol)
		assert.NoError(t, err)
	})
}

func TestValidateCreateBackupParams_VolumeValidations(t *testing.T) {
	ctx := context.Background()

	t.Run("VolumeNotInReadyState", func(t *testing.T) {
		store := database.NewMockStorage(t)
		params := &common.CreateBackupParams{
			VolumeUUID: "test-volume-uuid",
		}

		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
		store.On("GetVolume", ctx, params.VolumeUUID).Return(&datamodel.Volume{
			State: models.LifeCycleStateCreating,
		}, nil)

		err := _validateCreateBackupParams(ctx, store, params)
		assert.EqualError(t, err, "Volume is not in available state")
	})

	t.Run("VolumeWithoutDataProtection", func(t *testing.T) {
		store := database.NewMockStorage(t)
		params := &common.CreateBackupParams{
			VolumeUUID: "test-volume-uuid",
		}

		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
		store.On("GetVolume", ctx, params.VolumeUUID).Return(&datamodel.Volume{
			State:          models.LifeCycleStateREADY,
			DataProtection: nil,
		}, nil)

		err := _validateCreateBackupParams(ctx, store, params)
		assert.EqualError(t, err, "Volume does not have any backup vault associated with it")
	})

	t.Run("VolumeWithDifferentBackupVault", func(t *testing.T) {
		store := database.NewMockStorage(t)
		params := &common.CreateBackupParams{
			VolumeUUID:    "test-volume-uuid",
			BackupVaultID: "requested-vault-id",
		}

		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
		store.On("GetVolume", ctx, params.VolumeUUID).Return(&datamodel.Volume{
			State: models.LifeCycleStateREADY,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "different-vault-id",
			},
		}, nil)

		err := _validateCreateBackupParams(ctx, store, params)
		assert.EqualError(t, err, "Volume does not have the specified backup vault associated with it")
	})

	t.Run("DestinationVolumeWithoutSnapshotID", func(t *testing.T) {
		store := database.NewMockStorage(t)
		params := &common.CreateBackupParams{
			VolumeUUID:    "test-volume-uuid",
			BackupVaultID: "vault-id",
			SnapshotID:    "", // No snapshot ID provided
		}

		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
		store.On("GetVolume", ctx, params.VolumeUUID).Return(&datamodel.Volume{
			State: models.LifeCycleStateREADY,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "vault-id",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: true,
			},
		}, nil)

		err := _validateCreateBackupParams(ctx, store, params)
		assert.EqualError(t, err, "Backup creation is not supported for destination volumes without specifying an existing snapshot. Please use an existing snapshot to create backups or create a snapshot on the source volume and back that up on this volume once it has been replicated to this volume")
	})

	t.Run("BackupNameWithSnapmirrorPrefix", func(t *testing.T) {
		store := database.NewMockStorage(t)
		params := &common.CreateBackupParams{
			VolumeUUID:    "test-volume-uuid",
			BackupVaultID: "vault-id",
			BackupName:    "snapmirror.12345678.2023-01-01_010101",
			SnapshotID:    "snapshot-id",
		}

		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
		store.On("GetVolume", ctx, params.VolumeUUID).Return(&datamodel.Volume{
			State: models.LifeCycleStateREADY,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "vault-id",
			},
		}, nil)

		err := _validateCreateBackupParams(ctx, store, params)
		assert.EqualError(t, err, "Backups cannot be created from snapshots resulting from volume replication. Please use a non-replication snapshot and update the backup name to a non-replication snapshot name")
	})
}

func TestValidateCreateBackupParams_ExistingSnapshot(t *testing.T) {
	ctx := context.Background()

	t.Run("UseExistingSnapshotWithoutSnapshotID", func(t *testing.T) {
		store := database.NewMockStorage(t)
		params := &common.CreateBackupParams{
			VolumeUUID:          "test-volume-uuid",
			BackupVaultID:       "vault-id",
			BackupName:          "test-backup",
			UseExistingSnapshot: true,
			SnapshotID:          "", // Missing snapshot ID
		}

		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
		vol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
			State:     models.LifeCycleStateREADY,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "vault-id",
			},
			AccountID: 1,
		}
		store.On("GetVolume", ctx, params.VolumeUUID).Return(vol, nil)

		err := _validateCreateBackupParams(ctx, store, params)
		assert.EqualError(t, err, "Failed to validate snapshot for backup: Missing value for 'SnapshotID'")
	})

	t.Run("UseExistingSnapshotNotFound", func(t *testing.T) {
		store := database.NewMockStorage(t)
		params := &common.CreateBackupParams{
			VolumeUUID:          "test-volume-uuid",
			BackupVaultID:       "vault-id",
			BackupName:          "test-backup",
			UseExistingSnapshot: true,
			SnapshotID:          "snapshot-id",
		}

		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
		vol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
			State:     models.LifeCycleStateREADY,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "vault-id",
			},
			AccountID: 1,
		}
		store.On("GetVolume", ctx, params.VolumeUUID).Return(vol, nil)
		store.On("GetSnapshotByUUID", ctx, params.SnapshotID, vol.AccountID, vol.ID).Return(nil, vsaerror.NewNotFoundErr("snapshot", nil))

		err := _validateCreateBackupParams(ctx, store, params)
		assert.EqualError(t, err, "Failed to validate snapshot for backup: Snapshot not found")
	})

	t.Run("UseExistingSnapshotNotReady", func(t *testing.T) {
		store := database.NewMockStorage(t)
		params := &common.CreateBackupParams{
			VolumeUUID:          "test-volume-uuid",
			BackupVaultID:       "vault-id",
			BackupName:          "test-backup",
			UseExistingSnapshot: true,
			SnapshotID:          "snapshot-id",
		}

		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
		vol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
			State:     models.LifeCycleStateREADY,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "vault-id",
			},
			AccountID: 1,
		}
		store.On("GetVolume", ctx, params.VolumeUUID).Return(vol, nil)
		store.On("GetSnapshotByUUID", ctx, params.SnapshotID, vol.AccountID, vol.ID).Return(&datamodel.Snapshot{
			State: models.LifeCycleStateCreating,
		}, nil)

		err := _validateCreateBackupParams(ctx, store, params)
		assert.EqualError(t, err, "Failed to validate snapshot for backup: Snapshot is not in available state")
	})

	t.Run("UseExistingSnapshotWithSnapmirrorName", func(t *testing.T) {
		store := database.NewMockStorage(t)
		params := &common.CreateBackupParams{
			VolumeUUID:          "test-volume-uuid",
			BackupVaultID:       "vault-id",
			BackupName:          "test-backup",
			UseExistingSnapshot: true,
			SnapshotID:          "snapshot-id",
		}

		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
		vol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
			State:     models.LifeCycleStateREADY,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "vault-id",
			},
			AccountID: 1,
		}
		store.On("GetVolume", ctx, params.VolumeUUID).Return(vol, nil)
		store.On("GetSnapshotByUUID", ctx, params.SnapshotID, vol.AccountID, vol.ID).Return(&datamodel.Snapshot{
			State: models.LifeCycleStateREADY,
			Name:  "snapmirror.12345678.2023-01-01_010101",
		}, nil)

		err := _validateCreateBackupParams(ctx, store, params)
		assert.EqualError(t, err, "Failed to validate snapshot for backup: Backups cannot be created from snapshots resulting from volume replication. Please use a non-replication snapshot.")
	})
}

func TestValidateCreateBackupParams_NewSnapshot(t *testing.T) {
	ctx := context.Background()

	t.Run("NotUseExistingSnapshotWithSnapshotID", func(t *testing.T) {
		store := database.NewMockStorage(t)
		params := &common.CreateBackupParams{
			VolumeUUID:          "test-volume-uuid",
			BackupVaultID:       "vault-id",
			BackupName:          "test-backup",
			UseExistingSnapshot: false,
			SnapshotID:          "snapshot-id", // Should not be set
		}

		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
		vol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
			State:     models.LifeCycleStateREADY,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "vault-id",
			},
			AccountID: 1,
		}
		store.On("GetVolume", ctx, params.VolumeUUID).Return(vol, nil)

		err := _validateCreateBackupParams(ctx, store, params)
		assert.EqualError(t, err, "Failed to validate snapshot for backup: Cannot set Snapshot ID when useExistingSnapshot is false")
	})

	t.Run("BackupNameConflictsWithExistingSnapshot", func(t *testing.T) {
		store := database.NewMockStorage(t)
		params := &common.CreateBackupParams{
			VolumeUUID:          "test-volume-uuid",
			BackupVaultID:       "vault-id",
			BackupName:          "existing-snapshot-name",
			UseExistingSnapshot: false,
			SnapshotID:          "",
		}

		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
		vol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
			State:     models.LifeCycleStateREADY,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "vault-id",
			},
			AccountID: 1,
		}
		store.On("GetVolume", ctx, params.VolumeUUID).Return(vol, nil)
		store.On("GetSnapshotByNameAndVolumeId", ctx, params.BackupName, vol.AccountID, vol.ID).Return(&datamodel.Snapshot{
			Name: "existing-snapshot-name",
		}, nil)

		err := _validateCreateBackupParams(ctx, store, params)
		assert.EqualError(t, err, "Failed to validate snapshot for backup: Backup creation failed because the name conflicts with an existing snapshot. Please use the existing snapshot or choose a new backup name. A new name will create a new snapshot for the backup")
	})

	t.Run("ValidNewSnapshot", func(t *testing.T) {
		store := database.NewMockStorage(t)
		params := &common.CreateBackupParams{
			VolumeUUID:          "test-volume-uuid",
			BackupVaultID:       "vault-id",
			BackupName:          "new-backup",
			UseExistingSnapshot: false,
			SnapshotID:          "",
		}

		store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
		vol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
			State:     models.LifeCycleStateREADY,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "vault-id",
			},
			AccountID: 1,
		}
		store.On("GetVolume", ctx, params.VolumeUUID).Return(vol, nil)
		store.On("GetSnapshotByNameAndVolumeId", ctx, params.BackupName, vol.AccountID, vol.ID).Return(nil, vsaerror.NewNotFoundErr("snapshot", nil))

		err := _validateCreateBackupParams(ctx, store, params)
		assert.NoError(t, err)
	})
}

// TestBackupCreationIntegration covers comprehensive backup creation scenarios
// including validation, edge cases, and error conditions
func TestBackupCreationIntegration(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

	// Helper function to create common test objects
	createCommonTestObjects := func() (*datamodel.Account, *datamodel.Volume, *datamodel.BackupVault) {
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test-account",
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
			Name:      "test-volume",
			AccountID: 1,
			Account:   account,
			State:     models.LifeCycleStateREADY,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "test-vault-id",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols:        []string{"NFS"},
				IsDataProtection: false,
			},
		}

		backupVault := &datamodel.BackupVault{
			BaseModel:        datamodel.BaseModel{ID: 1, UUID: "test-vault-id"},
			Name:             "test-vault",
			AccountID:        1,
			SourceRegionName: func() *string { s := "us-east1"; return &s }(),
		}

		return account, volume, backupVault
	}

	t.Run("PositiveScenarios", func(t *testing.T) {
		t.Run("CreateBackupWithoutExistingSnapshot_Success", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, volume, backupVault := createCommonTestObjects()

			params := &common.CreateBackupParams{
				BackupName:          "test-backup",
				VolumeUUID:          volume.UUID,
				BackupVaultID:       backupVault.UUID,
				AccountName:         account.Name,
				Description:         "Test backup",
				BackupType:          "MANUAL",
				UseExistingSnapshot: false,
				SnapshotID:          "",
			}

			job := &datamodel.Job{
				BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
				WorkflowID: "workflow-id",
			}

			backup := &datamodel.Backup{
				BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
				Name:          params.BackupName,
				VolumeUUID:    params.VolumeUUID,
				BackupVaultID: backupVault.ID,
				BackupVault:   backupVault, // Add the BackupVault relationship
				State:         models.LifeCycleStateCreating,
				StateDetails:  models.LifeCycleStateCreatingDetails,
				Attributes: &datamodel.BackupAttributes{
					VolumeName:          volume.Name,
					AccountIdentifier:   account.Name,
					Protocols:           volume.VolumeAttributes.Protocols,
					UseExistingSnapshot: false,
				},
				Description: params.Description,
				Type:        params.BackupType,
			}

			// Mock the validation and creation flow
			validateCreateBackupParams = func(ctx context.Context, se database.Storage, params *common.CreateBackupParams) error {
				return nil
			}
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				validateCreateBackupParams = _validateCreateBackupParams
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock storage calls
			store.On("GetVolumeWithAccountID", ctx, params.VolumeUUID, account.ID).Return(volume, nil)
			store.On("GetBackupVault", ctx, params.BackupVaultID).Return(backupVault, nil)
			store.On("CreateJob", ctx, mock.Anything).Return(job, nil)
			store.On("CreateBackup", ctx, mock.Anything).Return(backup, nil)

			// Mock successful workflow execution
			temporal.EXPECT().ExecuteWorkflow(ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			// Act
			result, jobID, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, job.UUID, jobID)
			assert.Equal(t, backup.Name, result.Name)
			assert.Equal(t, backup.State, result.LifeCycleState)
			// Note: UseExistingSnapshot is not populated in the models.Backup conversion
		})

		t.Run("CreateBackupWithExistingSnapshot_ValidSnapshot_Success", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, volume, backupVault := createCommonTestObjects()

			existingSnapshot := &datamodel.Snapshot{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "snapshot-uuid"},
				Name:      "existing-snapshot",
				State:     models.LifeCycleStateREADY,
				VolumeID:  volume.ID,
				AccountID: account.ID,
				SnapshotAttributes: &datamodel.SnapshotAttributes{
					ExternalUUID: "external-snapshot-uuid",
				},
			}

			params := &common.CreateBackupParams{
				BackupName:          "test-backup-with-snapshot",
				VolumeUUID:          volume.UUID,
				BackupVaultID:       backupVault.UUID,
				AccountName:         account.Name,
				Description:         "Backup with existing snapshot",
				BackupType:          "MANUAL",
				UseExistingSnapshot: true,
				SnapshotID:          existingSnapshot.UUID,
			}

			job := &datamodel.Job{
				BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
				WorkflowID: "workflow-id",
			}

			backup := &datamodel.Backup{
				BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
				Name:          params.BackupName,
				VolumeUUID:    params.VolumeUUID,
				BackupVaultID: backupVault.ID,
				BackupVault:   backupVault, // Add the BackupVault relationship
				State:         models.LifeCycleStateCreating,
				StateDetails:  models.LifeCycleStateCreatingDetails,
				Attributes: &datamodel.BackupAttributes{
					VolumeName:          volume.Name,
					AccountIdentifier:   account.Name,
					Protocols:           volume.VolumeAttributes.Protocols,
					UseExistingSnapshot: true,
					SnapshotName:        existingSnapshot.Name,
					SnapshotID:          existingSnapshot.SnapshotAttributes.ExternalUUID,
				},
				Description: params.Description,
				Type:        params.BackupType,
			}

			// Mock functions
			validateCreateBackupParams = func(ctx context.Context, se database.Storage, params *common.CreateBackupParams) error {
				return nil
			}
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				validateCreateBackupParams = _validateCreateBackupParams
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock storage calls
			store.On("GetVolumeWithAccountID", ctx, params.VolumeUUID, account.ID).Return(volume, nil)
			store.On("GetBackupVault", ctx, params.BackupVaultID).Return(backupVault, nil)
			store.On("GetSnapshotByUUID", ctx, params.SnapshotID, account.ID, volume.ID).Return(existingSnapshot, nil)
			store.On("CreateJob", ctx, mock.Anything).Return(job, nil)
			store.On("CreateBackup", ctx, mock.Anything).Return(backup, nil)

			// Mock successful workflow execution
			temporal.EXPECT().ExecuteWorkflow(ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			// Act
			result, jobID, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, job.UUID, jobID)
			assert.Equal(t, backup.Name, result.Name)
			// Note: UseExistingSnapshot is not populated in the models.Backup conversion
			assert.Equal(t, existingSnapshot.Name, result.SnapshotName)
			// Note: models.Backup doesn't have SnapshotID field - it only has SnapshotName
		})
	})

	t.Run("NegativeScenarios", func(t *testing.T) {
		t.Run("CreateBackupWithExistingSnapshot_SnapshotNotAvailable", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, volume, backupVault := createCommonTestObjects()

			notAvailableSnapshot := &datamodel.Snapshot{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "snapshot-uuid"},
				Name:      "creating-snapshot",
				State:     models.LifeCycleStateCreating, // Not in READY state
				VolumeID:  volume.ID,
				AccountID: account.ID,
				SnapshotAttributes: &datamodel.SnapshotAttributes{
					ExternalUUID: "external-snapshot-uuid",
				},
			}

			params := &common.CreateBackupParams{
				BackupName:          "test-backup",
				VolumeUUID:          volume.UUID,
				BackupVaultID:       backupVault.UUID,
				AccountName:         account.Name,
				UseExistingSnapshot: true,
				SnapshotID:          notAvailableSnapshot.UUID,
			}

			// Mock validation functions
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock storage calls for validation
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
			store.On("GetVolume", ctx, params.VolumeUUID).Return(volume, nil)
			store.On("GetSnapshotByUUID", ctx, params.SnapshotID, account.ID, volume.ID).Return(notAvailableSnapshot, nil)

			// Act
			_, _, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "Snapshot is not in available state")
		})

		t.Run("CreateBackupWithoutExistingSnapshot_BackupNameConflictsWithSnapshot", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, volume, backupVault := createCommonTestObjects()

			conflictingSnapshot := &datamodel.Snapshot{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "conflicting-name",
			}

			params := &common.CreateBackupParams{
				BackupName:          "conflicting-name", // Same as existing snapshot
				VolumeUUID:          volume.UUID,
				BackupVaultID:       backupVault.UUID,
				AccountName:         account.Name,
				UseExistingSnapshot: false,
				SnapshotID:          "",
			}

			// Mock validation functions
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock storage calls for validation
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
			store.On("GetVolume", ctx, params.VolumeUUID).Return(volume, nil)
			store.On("GetSnapshotByNameAndVolumeId", ctx, params.BackupName, volume.AccountID, volume.ID).Return(conflictingSnapshot, nil)

			// Act
			_, _, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "Backup creation failed because the name conflicts with an existing snapshot")
		})

		t.Run("CreateBackupWithSnapmirrorPrefix_ShouldFail", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, volume, backupVault := createCommonTestObjects()

			params := &common.CreateBackupParams{
				BackupName:          "snapmirror.12345678.2023-01-01_010101", // Snapmirror prefix
				VolumeUUID:          volume.UUID,
				BackupVaultID:       backupVault.UUID,
				AccountName:         account.Name,
				UseExistingSnapshot: false,
			}

			// Mock validation functions
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock storage calls for validation
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
			store.On("GetVolume", ctx, params.VolumeUUID).Return(volume, nil)

			// Act
			_, _, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "Backups cannot be created from snapshots resulting from volume replication")
		})

		t.Run("CreateBackupWithExistingSnapshot_SnapshotNotFound", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, volume, backupVault := createCommonTestObjects()

			params := &common.CreateBackupParams{
				BackupName:          "test-backup",
				VolumeUUID:          volume.UUID,
				BackupVaultID:       backupVault.UUID,
				AccountName:         account.Name,
				UseExistingSnapshot: true,
				SnapshotID:          "nonexistent-snapshot-uuid",
			}

			// Mock validation functions
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock storage calls for validation
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
			store.On("GetVolume", ctx, params.VolumeUUID).Return(volume, nil)
			store.On("GetSnapshotByUUID", ctx, params.SnapshotID, account.ID, volume.ID).Return(nil, vsaerror.NewNotFoundErr("snapshot", nil))

			// Act
			_, _, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "Snapshot not found")
		})

		t.Run("CreateBackupWithExistingSnapshot_SnapshotWithSnapmirrorName", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, volume, backupVault := createCommonTestObjects()

			snapmirrorSnapshot := &datamodel.Snapshot{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "snapshot-uuid"},
				Name:      "snapmirror.12345678.2023-01-01_010101", // Snapmirror name
				State:     models.LifeCycleStateREADY,
				VolumeID:  volume.ID,
				AccountID: account.ID,
			}

			params := &common.CreateBackupParams{
				BackupName:          "test-backup",
				VolumeUUID:          volume.UUID,
				BackupVaultID:       backupVault.UUID,
				AccountName:         account.Name,
				UseExistingSnapshot: true,
				SnapshotID:          snapmirrorSnapshot.UUID,
			}

			// Mock validation functions
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock storage calls for validation
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
			store.On("GetVolume", ctx, params.VolumeUUID).Return(volume, nil)
			store.On("GetSnapshotByUUID", ctx, params.SnapshotID, account.ID, volume.ID).Return(snapmirrorSnapshot, nil)

			// Act
			_, _, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "Backups cannot be created from snapshots resulting from volume replication")
		})

		t.Run("CreateBackupWithExistingSnapshot_SnapshotInUseByActiveBackup", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, volume, backupVault := createCommonTestObjects()

			existingSnapshot := &datamodel.Snapshot{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "snapshot-uuid"},
				Name:      "existing-snapshot",
				State:     models.LifeCycleStateREADY,
				VolumeID:  volume.ID,
				AccountID: account.ID,
			}

			params := &common.CreateBackupParams{
				BackupName:          "test-backup",
				VolumeUUID:          volume.UUID,
				BackupVaultID:       backupVault.UUID,
				AccountName:         account.Name,
				UseExistingSnapshot: true,
				SnapshotID:          existingSnapshot.UUID,
			}

			// Mock validation functions
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock storage calls - simulate snapshot being used by active backup
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(true, nil)

			// Act
			_, _, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "A backup operation from the same volume is currently in progress")
		})

		t.Run("CreateBackupWithDataProtectionVolume_WithoutSnapshotID", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, _, backupVault := createCommonTestObjects()

			// Create data protection volume (replication destination)
			dataProtectionVolume := &datamodel.Volume{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
				Name:      "test-dp-volume",
				AccountID: 1,
				Account:   account,
				State:     models.LifeCycleStateREADY,
				DataProtection: &datamodel.DataProtection{
					BackupVaultID: "test-vault-id",
				},
				VolumeAttributes: &datamodel.VolumeAttributes{
					Protocols:        []string{"NFS"},
					IsDataProtection: true, // This is a data protection volume
				},
			}

			params := &common.CreateBackupParams{
				BackupName:          "test-backup",
				VolumeUUID:          dataProtectionVolume.UUID,
				BackupVaultID:       backupVault.UUID,
				AccountName:         account.Name,
				UseExistingSnapshot: false,
				SnapshotID:          "", // No snapshot ID provided for DP volume
			}

			// Mock validation functions
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock storage calls for validation
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
			store.On("GetVolume", ctx, params.VolumeUUID).Return(dataProtectionVolume, nil)

			// Act
			_, _, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "Backup creation is not supported for destination volumes without specifying an existing snapshot")
		})

		t.Run("CreateBackupWithoutExistingSnapshot_ButSnapshotIDProvided", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, volume, backupVault := createCommonTestObjects()

			params := &common.CreateBackupParams{
				BackupName:          "test-backup",
				VolumeUUID:          volume.UUID,
				BackupVaultID:       backupVault.UUID,
				AccountName:         account.Name,
				UseExistingSnapshot: false,           // Not using existing snapshot
				SnapshotID:          "snapshot-uuid", // But snapshot ID is provided
			}

			// Mock validation functions
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock storage calls for validation
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
			store.On("GetVolume", ctx, params.VolumeUUID).Return(volume, nil)

			// Act
			_, _, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "Cannot set Snapshot ID when useExistingSnapshot is false")
		})

		t.Run("CreateBackupWithExistingSnapshot_MissingSnapshotID", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, volume, backupVault := createCommonTestObjects()

			params := &common.CreateBackupParams{
				BackupName:          "test-backup",
				VolumeUUID:          volume.UUID,
				BackupVaultID:       backupVault.UUID,
				AccountName:         account.Name,
				UseExistingSnapshot: true, // Using existing snapshot
				SnapshotID:          "",   // But no snapshot ID provided
			}

			// Mock validation functions
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock storage calls for validation
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
			store.On("GetVolume", ctx, params.VolumeUUID).Return(volume, nil)

			// Act
			_, _, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "Missing value for 'SnapshotID'")
		})

		t.Run("CreateBackup_VolumeNotInReadyState", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, volume, backupVault := createCommonTestObjects()

			// Change volume state to not ready
			volume.State = models.LifeCycleStateCreating

			params := &common.CreateBackupParams{
				BackupName:          "test-backup",
				VolumeUUID:          volume.UUID,
				BackupVaultID:       backupVault.UUID,
				AccountName:         account.Name,
				UseExistingSnapshot: false,
			}

			// Mock validation functions
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock storage calls for validation
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
			store.On("GetVolume", ctx, params.VolumeUUID).Return(volume, nil)

			// Act
			_, _, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "Volume is not in available state")
		})

		t.Run("CreateBackup_VolumeWithoutDataProtection", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, volume, _ := createCommonTestObjects()

			// Remove data protection from volume
			volume.DataProtection = nil

			params := &common.CreateBackupParams{
				BackupName:          "test-backup",
				VolumeUUID:          volume.UUID,
				BackupVaultID:       "vault-id",
				AccountName:         account.Name,
				UseExistingSnapshot: false,
			}

			// Mock validation functions
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock storage calls for validation
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
			store.On("GetVolume", ctx, params.VolumeUUID).Return(volume, nil)

			// Act
			_, _, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "Volume does not have any backup vault associated with it")
		})

		t.Run("CreateBackup_VolumeWithDifferentBackupVault", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, volume, _ := createCommonTestObjects()

			// Set different backup vault ID in volume
			volume.DataProtection.BackupVaultID = "different-vault-id"

			params := &common.CreateBackupParams{
				BackupName:          "test-backup",
				VolumeUUID:          volume.UUID,
				BackupVaultID:       "requested-vault-id", // Different from volume's vault
				AccountName:         account.Name,
				UseExistingSnapshot: false,
			}

			// Mock validation functions
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock storage calls for validation
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
			store.On("GetVolume", ctx, params.VolumeUUID).Return(volume, nil)

			// Act
			_, _, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "Volume does not have the specified backup vault associated with it")
		})
	})

	t.Run("EdgeCases", func(t *testing.T) {
		t.Run("CreateBackup_AccountNotFound", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)

			params := &common.CreateBackupParams{
				BackupName:    "test-backup",
				VolumeUUID:    "volume-uuid",
				BackupVaultID: "vault-id",
				AccountName:   "nonexistent-account",
			}

			// Mock account not found
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return nil, vsaerror.NewNotFoundErr("account", nil)
			}
			defer func() {
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Act
			_, _, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.Error(t, err)
		})

		t.Run("CreateBackup_VolumeNotFound", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, _, _ := createCommonTestObjects()

			params := &common.CreateBackupParams{
				BackupName:    "test-backup",
				VolumeUUID:    "nonexistent-volume-uuid",
				BackupVaultID: "vault-id",
				AccountName:   account.Name,
			}

			// Mock functions
			validateCreateBackupParams = func(ctx context.Context, se database.Storage, params *common.CreateBackupParams) error {
				return nil
			}
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				validateCreateBackupParams = _validateCreateBackupParams
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock volume not found
			store.On("GetVolumeWithAccountID", ctx, params.VolumeUUID, account.ID).Return(nil, vsaerror.NewNotFoundErr("volume", nil))

			// Act
			_, _, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.Error(t, err)
		})

		t.Run("CreateBackup_BackupVaultNotFound", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, volume, _ := createCommonTestObjects()

			params := &common.CreateBackupParams{
				BackupName:    "test-backup",
				VolumeUUID:    volume.UUID,
				BackupVaultID: "nonexistent-vault-id",
				AccountName:   account.Name,
			}

			// Mock functions
			validateCreateBackupParams = func(ctx context.Context, se database.Storage, params *common.CreateBackupParams) error {
				return nil
			}
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				validateCreateBackupParams = _validateCreateBackupParams
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock storage calls
			store.On("GetVolumeWithAccountID", ctx, params.VolumeUUID, account.ID).Return(volume, nil)
			store.On("GetBackupVault", ctx, params.BackupVaultID).Return(nil, vsaerror.NewNotFoundErr("backup vault", nil))

			// Act
			_, _, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "Backup vault not found")
		})

		t.Run("CreateBackup_JobCreationFailure", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, volume, backupVault := createCommonTestObjects()

			params := &common.CreateBackupParams{
				BackupName:    "test-backup",
				VolumeUUID:    volume.UUID,
				BackupVaultID: backupVault.UUID,
				AccountName:   account.Name,
			}

			// Mock functions
			validateCreateBackupParams = func(ctx context.Context, se database.Storage, params *common.CreateBackupParams) error {
				return nil
			}
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				validateCreateBackupParams = _validateCreateBackupParams
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock storage calls
			store.On("GetVolumeWithAccountID", ctx, params.VolumeUUID, account.ID).Return(volume, nil)
			store.On("GetBackupVault", ctx, params.BackupVaultID).Return(backupVault, nil)
			store.On("CreateJob", ctx, mock.Anything).Return(nil, errors.New("failed to create job"))

			// Act
			_, _, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "failed to create job")
		})

		t.Run("CreateBackup_BackupCreationFailure", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, volume, backupVault := createCommonTestObjects()

			params := &common.CreateBackupParams{
				BackupName:    "test-backup",
				VolumeUUID:    volume.UUID,
				BackupVaultID: backupVault.UUID,
				AccountName:   account.Name,
			}

			job := &datamodel.Job{
				BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
				WorkflowID: "workflow-id",
			}

			// Mock functions
			validateCreateBackupParams = func(ctx context.Context, se database.Storage, params *common.CreateBackupParams) error {
				return nil
			}
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				validateCreateBackupParams = _validateCreateBackupParams
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock storage calls
			store.On("GetVolumeWithAccountID", ctx, params.VolumeUUID, account.ID).Return(volume, nil)
			store.On("GetBackupVault", ctx, params.BackupVaultID).Return(backupVault, nil)
			store.On("CreateJob", ctx, mock.Anything).Return(job, nil)
			store.On("CreateBackup", ctx, mock.Anything).Return(nil, errors.New("failed to create backup"))

			// Mock error handling
			store.On("UpdateJob", ctx, job.UUID, string(models.JobsStateERROR), 0, "failed to create backup").Return(nil, nil)

			// Act
			_, _, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "failed to create backup")
		})

		t.Run("CreateBackup_WorkflowExecutionFailure", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, volume, backupVault := createCommonTestObjects()

			params := &common.CreateBackupParams{
				BackupName:    "test-backup",
				VolumeUUID:    volume.UUID,
				BackupVaultID: backupVault.UUID,
				AccountName:   account.Name,
			}

			job := &datamodel.Job{
				BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
				WorkflowID: "workflow-id",
			}

			backup := &datamodel.Backup{
				BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
				Name:          params.BackupName,
				VolumeUUID:    params.VolumeUUID,
				BackupVaultID: backupVault.ID,
				BackupVault:   backupVault, // Add the BackupVault relationship
				State:         models.LifeCycleStateCreating,
				StateDetails:  models.LifeCycleStateCreatingDetails,
				Attributes: &datamodel.BackupAttributes{
					VolumeName:          volume.Name,
					AccountIdentifier:   account.Name,
					Protocols:           volume.VolumeAttributes.Protocols,
					UseExistingSnapshot: false,
				},
			}

			// Mock functions
			validateCreateBackupParams = func(ctx context.Context, se database.Storage, params *common.CreateBackupParams) error {
				return nil
			}
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				validateCreateBackupParams = _validateCreateBackupParams
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock storage calls
			store.On("GetVolumeWithAccountID", ctx, params.VolumeUUID, account.ID).Return(volume, nil)
			store.On("GetBackupVault", ctx, params.BackupVaultID).Return(backupVault, nil)
			store.On("CreateJob", ctx, mock.Anything).Return(job, nil)
			store.On("CreateBackup", ctx, mock.Anything).Return(backup, nil)

			// Mock workflow execution failure
			temporal.EXPECT().ExecuteWorkflow(ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow execution failed"))

			// Mock error handling
			store.On("UpdateBackupState", ctx, mock.MatchedBy(func(b *datamodel.Backup) bool {
				return b.State == models.LifeCycleStateError
			})).Return(backup, nil)
			store.On("UpdateJob", ctx, job.UUID, string(models.JobsStateERROR), 0, "workflow execution failed").Return(nil, nil)

			// Act
			_, _, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "workflow execution failed")
		})
	})

	t.Run("ValidationSequence", func(t *testing.T) {
		t.Run("CreateBackup_FullValidationSequence", func(t *testing.T) {
			store := database.NewMockStorage(t)
			temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
			account, volume, backupVault := createCommonTestObjects()

			params := &common.CreateBackupParams{
				BackupName:          "test-backup",
				VolumeUUID:          volume.UUID,
				BackupVaultID:       backupVault.UUID,
				AccountName:         account.Name,
				UseExistingSnapshot: false,
				SnapshotID:          "",
				Description:         "Integration test backup",
				BackupType:          "MANUAL",
			}

			job := &datamodel.Job{
				BaseModel:    datamodel.BaseModel{UUID: "job-uuid"},
				WorkflowID:   "workflow-id",
				Type:         string(models.JobTypeCreateBackup),
				State:        string(models.JobsStateNEW),
				ResourceName: params.BackupName,
				AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
			}

			backup := &datamodel.Backup{
				BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
				Name:          params.BackupName,
				VolumeUUID:    params.VolumeUUID,
				BackupVaultID: backupVault.ID,
				BackupVault:   backupVault, // Add the BackupVault relationship
				State:         models.LifeCycleStateCreating,
				StateDetails:  models.LifeCycleStateCreatingDetails,
				Attributes: &datamodel.BackupAttributes{
					VolumeName:          volume.Name,
					AccountIdentifier:   account.Name,
					Protocols:           volume.VolumeAttributes.Protocols,
					UseExistingSnapshot: false,
				},
				Description: params.Description,
				Type:        params.BackupType,
			}

			// Use real validation function to test the complete validation sequence
			getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
				return account, nil
			}
			defer func() {
				getOrCreateAccount = _getOrCreateAccount
			}()

			// Mock all the validation calls in sequence
			store.On("IsBackupInCreatingorDeletingStateByVolume", ctx, params.VolumeUUID).Return(false, nil)
			store.On("GetVolume", ctx, params.VolumeUUID).Return(volume, nil)
			store.On("GetSnapshotByNameAndVolumeId", ctx, params.BackupName, volume.AccountID, volume.ID).Return(nil, vsaerror.NewNotFoundErr("snapshot", nil))

			// Mock the creation calls
			store.On("GetVolumeWithAccountID", ctx, params.VolumeUUID, account.ID).Return(volume, nil)
			store.On("GetBackupVault", ctx, params.BackupVaultID).Return(backupVault, nil)
			store.On("CreateJob", ctx, mock.MatchedBy(func(j *datamodel.Job) bool {
				return j.Type == string(models.JobTypeCreateBackup) &&
					j.State == string(models.JobsStateNEW) &&
					j.ResourceName == params.BackupName
			})).Return(job, nil)
			store.On("CreateBackup", ctx, mock.MatchedBy(func(b *datamodel.Backup) bool {
				return b.Name == params.BackupName &&
					b.VolumeUUID == params.VolumeUUID &&
					b.State == models.LifeCycleStateCreating
			})).Return(backup, nil)

			// Mock successful workflow execution
			temporal.EXPECT().ExecuteWorkflow(
				ctx,
				mock.MatchedBy(func(opts client.StartWorkflowOptions) bool {
					return opts.TaskQueue != "" && opts.ID == job.WorkflowID
				}),
				mock.Anything, // workflow function
				params,        // workflow parameters
				backup,
				backupVault,
				volume,
			).Return(nil, nil)

			// Act
			result, jobID, err := _createBackup(ctx, store, temporal, params)

			// Assert
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, job.UUID, jobID)
			assert.Equal(t, backup.Name, result.Name)
			assert.Equal(t, backup.State, result.LifeCycleState)
			assert.Equal(t, backup.Description, *result.Description)
			assert.Equal(t, backup.Type, result.Type)

			// Verify all mocks were called as expected
			store.AssertExpectations(t)
			temporal.AssertExpectations(t)
		})
	})
}

// TestDeleteBackup_DeleteBackupFailure tests error handling when DeleteBackup fails for error state backup
func TestDeleteBackup_DeleteBackupFailure(t *testing.T) {
	ctx := context.Background()
	store := database.NewMockStorage(t)
	temporal := new(workflow_engine_mock.MockTemporalTestClient)

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "testAccountUUID"}, Name: "test-account"}
	backup := &datamodel.Backup{
		BaseModel: datamodel.BaseModel{UUID: "test-backup-uuid"},
		State:     models.LifeCycleStateError,
		Attributes: &datamodel.BackupAttributes{
			DeleteInitiated: false,
		},
	}

	params := &common.DeleteBackupParams{
		BackupUUID:      backup.UUID,
		BackupVaultUUID: "test-vault-uuid",
		AccountName:     "test-account",
	}

	// Mock the validation function
	validateBackupDeleteParams = func(ctx context.Context, se database.Storage, params *common.DeleteBackupParams) error {
		return nil
	}
	defer func() { validateBackupDeleteParams = _validateBackupDeleteParams }()

	// Mock account lookup
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = _getOrCreateAccount }()

	// Mock backup retrieval
	store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(backup, nil)

	// Mock DeleteBackup to fail
	expectedError := errors.New("failed to delete backup")
	store.On("DeleteBackup", ctx, backup.UUID).Return(nil, expectedError)

	// Act
	result, jobID, err := deleteBackup(ctx, store, temporal, params)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
	assert.Nil(t, result)
	assert.Equal(t, "", jobID)
	store.AssertExpectations(t)
}

// TestDeleteBackup_ListVolumesFailure tests error handling when ListVolumes fails
func TestDeleteBackup_ListVolumesFailure(t *testing.T) {
	ctx := context.Background()
	store := database.NewMockStorage(t)
	temporal := new(workflow_engine_mock.MockTemporalTestClient)

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "testAccountUUID"}, Name: "test-account"}
	backup := &datamodel.Backup{
		BaseModel: datamodel.BaseModel{UUID: "test-backup-uuid"},
		State:     models.LifeCycleStateAvailable,
		Attributes: &datamodel.BackupAttributes{
			DeleteInitiated: false,
		},
	}

	params := &common.DeleteBackupParams{
		BackupUUID:      backup.UUID,
		BackupVaultUUID: "test-vault-uuid",
		AccountName:     "test-account",
	}

	// Mock the validation function
	validateBackupDeleteParams = func(ctx context.Context, se database.Storage, params *common.DeleteBackupParams) error {
		return nil
	}
	defer func() { validateBackupDeleteParams = _validateBackupDeleteParams }()

	// Mock account lookup
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = _getOrCreateAccount }()

	// Mock backup retrieval
	store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(backup, nil)

	// Mock ListVolumes to fail
	expectedError := errors.New("failed to list volumes")
	store.On("ListVolumes", ctx, mock.AnythingOfType("[][]interface {}")).Return(nil, expectedError)

	// Act
	result, jobID, err := deleteBackup(ctx, store, temporal, params)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
	assert.Nil(t, result)
	assert.Equal(t, "", jobID)
	store.AssertExpectations(t)
}

func TestConvertDatastoreBackupToModel(t *testing.T) {
	t.Run("WhenBackupVaultIsNil", func(t *testing.T) {
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "test-backup-uuid",
			},
			Name:         "test-backup",
			VolumeUUID:   "test-volume-uuid",
			State:        "AVAILABLE",
			StateDetails: "Backup is available",
			Description:  "Test backup description",
			Type:         "MANUAL",
			Attributes: &datamodel.BackupAttributes{
				VolumeName: "test-volume",
			},
		}

		result := convertDatastoreBackupToModel(backup)

		assert.Equal(t, "test-backup-uuid", result.BackupID)
		assert.Equal(t, "test-backup", result.Name)
		assert.Equal(t, "test-volume-uuid", result.VolumeID)
		assert.Equal(t, "AVAILABLE", result.LifeCycleState)
		assert.Equal(t, "Backup is available", result.LifeCycleStateDetails)
		assert.Equal(t, "Test backup description", *result.Description)
		assert.Equal(t, "MANUAL", result.Type)
		assert.Equal(t, "test-volume", result.VolumeName)
		assert.Equal(t, int64(0), *result.MinimumEnforcedRetentionDuration)
		assert.False(t, result.IsBackupImmutable)
	})

	t.Run("WhenBackupVaultImmutableAttributesIsNil", func(t *testing.T) {
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "test-backup-uuid",
			},
			Name:         "test-backup",
			VolumeUUID:   "test-volume-uuid",
			State:        "AVAILABLE",
			StateDetails: "Backup is available",
			Description:  "Test backup description",
			Type:         "MANUAL",
			Attributes: &datamodel.BackupAttributes{
				VolumeName: "test-volume",
			},
			BackupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{
					UUID: "test-vault-uuid",
				},
				SourceRegionName: stringPtr("us-east1"),
			},
		}

		result := convertDatastoreBackupToModel(backup)

		assert.Equal(t, "test-backup-uuid", result.BackupID)
		assert.Equal(t, "test-backup", result.Name)
		assert.Equal(t, "test-volume-uuid", result.VolumeID)
		assert.Equal(t, "AVAILABLE", result.LifeCycleState)
		assert.Equal(t, "Backup is available", result.LifeCycleStateDetails)
		assert.Equal(t, "Test backup description", *result.Description)
		assert.Equal(t, "MANUAL", result.Type)
		assert.Equal(t, "test-volume", result.VolumeName)
		assert.Equal(t, "test-vault-uuid", result.BackupVaultID)
		assert.Equal(t, "us-east1", result.Region)
		assert.Equal(t, int64(0), *result.MinimumEnforcedRetentionDuration)
		assert.False(t, result.IsBackupImmutable)
	})

	t.Run("WhenBackupVaultHasImmutableAttributes", func(t *testing.T) {
		minRetentionDuration := int64(30)
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "test-backup-uuid",
			},
			Name:         "test-backup",
			VolumeUUID:   "test-volume-uuid",
			State:        "AVAILABLE",
			StateDetails: "Backup is available",
			Description:  "Test backup description",
			Type:         "MANUAL",
			Attributes: &datamodel.BackupAttributes{
				VolumeName: "test-volume",
			},
			BackupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{
					UUID: "test-vault-uuid",
				},
				SourceRegionName: stringPtr("us-east1"),
				ImmutableAttributes: &datamodel.ImmutableAttributes{
					BackupMinimumEnforcedRetentionDuration: &minRetentionDuration,
					IsAdhocBackupImmutable:                 true,
				},
			},
		}

		result := convertDatastoreBackupToModel(backup)

		assert.Equal(t, "test-backup-uuid", result.BackupID)
		assert.Equal(t, "test-backup", result.Name)
		assert.Equal(t, "test-volume-uuid", result.VolumeID)
		assert.Equal(t, "AVAILABLE", result.LifeCycleState)
		assert.Equal(t, "Backup is available", result.LifeCycleStateDetails)
		assert.Equal(t, "Test backup description", *result.Description)
		assert.Equal(t, "MANUAL", result.Type)
		assert.Equal(t, "test-volume", result.VolumeName)
		assert.Equal(t, "test-vault-uuid", result.BackupVaultID)
		assert.Equal(t, "us-east1", result.Region)
		assert.Equal(t, int64(30), *result.MinimumEnforcedRetentionDuration)
		assert.True(t, result.IsBackupImmutable)
	})

	t.Run("WhenBackupVaultHasImmutableAttributesButBackupIsNotImmutable", func(t *testing.T) {
		minRetentionDuration := int64(30)
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "test-backup-uuid",
			},
			Name:         "test-backup",
			VolumeUUID:   "test-volume-uuid",
			State:        "AVAILABLE",
			StateDetails: "Backup is available",
			Description:  "Test backup description",
			Type:         "MANUAL",
			Attributes: &datamodel.BackupAttributes{
				VolumeName: "test-volume",
			},
			BackupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{
					UUID: "test-vault-uuid",
				},
				SourceRegionName: stringPtr("us-east1"),
				ImmutableAttributes: &datamodel.ImmutableAttributes{
					BackupMinimumEnforcedRetentionDuration: &minRetentionDuration,
					IsAdhocBackupImmutable:                 false,
				},
			},
		}

		result := convertDatastoreBackupToModel(backup)

		assert.Equal(t, "test-backup-uuid", result.BackupID)
		assert.Equal(t, "test-backup", result.Name)
		assert.Equal(t, "test-volume-uuid", result.VolumeID)
		assert.Equal(t, "AVAILABLE", result.LifeCycleState)
		assert.Equal(t, "Backup is available", result.LifeCycleStateDetails)
		assert.Equal(t, "Test backup description", *result.Description)
		assert.Equal(t, "MANUAL", result.Type)
		assert.Equal(t, "test-volume", result.VolumeName)
		assert.Equal(t, "test-vault-uuid", result.BackupVaultID)
		assert.Equal(t, "us-east1", result.Region)
		assert.Equal(t, int64(30), *result.MinimumEnforcedRetentionDuration)
		assert.False(t, result.IsBackupImmutable)
	})

	t.Run("WhenBackupVaultHasImmutableAttributesWithZeroRetention", func(t *testing.T) {
		minRetentionDuration := int64(0)
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "test-backup-uuid",
			},
			Name:         "test-backup",
			VolumeUUID:   "test-volume-uuid",
			State:        "AVAILABLE",
			StateDetails: "Backup is available",
			Description:  "Test backup description",
			Type:         "MANUAL",
			Attributes: &datamodel.BackupAttributes{
				VolumeName: "test-volume",
			},
			BackupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{
					UUID: "test-vault-uuid",
				},
				SourceRegionName: stringPtr("us-east1"),
				ImmutableAttributes: &datamodel.ImmutableAttributes{
					BackupMinimumEnforcedRetentionDuration: &minRetentionDuration,
					IsAdhocBackupImmutable:                 true,
				},
			},
		}

		result := convertDatastoreBackupToModel(backup)

		assert.Equal(t, "test-backup-uuid", result.BackupID)
		assert.Equal(t, "test-backup", result.Name)
		assert.Equal(t, "test-volume-uuid", result.VolumeID)
		assert.Equal(t, "AVAILABLE", result.LifeCycleState)
		assert.Equal(t, "Backup is available", result.LifeCycleStateDetails)
		assert.Equal(t, "Test backup description", *result.Description)
		assert.Equal(t, "MANUAL", result.Type)
		assert.Equal(t, "test-volume", result.VolumeName)
		assert.Equal(t, "test-vault-uuid", result.BackupVaultID)
		assert.Equal(t, "us-east1", result.Region)
		assert.Equal(t, int64(0), *result.MinimumEnforcedRetentionDuration)
		assert.True(t, result.IsBackupImmutable)
	})
}

func TestDeleteBackupInternal(t *testing.T) {
	t.Run("WhenBackupDeletedSuccessfully_ReturnsNoError", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store := database.NewMockStorage(t)

		// Override hydrationEnabled to false
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = false
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		params := &common.DeleteBackupParams{
			BackupVaultUUID: "test-vault-uuid",
			BackupUUID:      "test-backup-uuid",
			AccountName:     "test-account",
		}

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "test-backup-uuid",
			},
			Name: "test-backup",
			Attributes: &datamodel.BackupAttributes{
				RestoreVolumeCount: 0,
			},
		}

		// Mock storage calls
		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(backup, nil)
		store.On("DeleteBackup", ctx, backup.UUID).Return(&datamodel.Backup{}, nil)

		o := &Orchestrator{storage: store}
		result, err := o.DeleteBackupInternal(ctx, params)

		assert.NoError(t, err)
		assert.Equal(t, "", result)
		store.AssertExpectations(t)
	})

	t.Run("WhenGetBackupFails_ReturnsError", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store := database.NewMockStorage(t)

		// Override hydrationEnabled to false
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = false
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		params := &common.DeleteBackupParams{
			BackupVaultUUID: "test-vault-uuid",
			BackupUUID:      "test-backup-uuid",
			AccountName:     "test-account",
		}

		expectedError := errors.New("backup not found")
		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(nil, expectedError)

		o := &Orchestrator{storage: store}
		result, err := o.DeleteBackupInternal(ctx, params)

		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		assert.Equal(t, "", result)
		store.AssertExpectations(t)
	})

	t.Run("WhenDeleteBackupFails_ReturnsError", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store := database.NewMockStorage(t)

		// Override hydrationEnabled to false
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = false
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		params := &common.DeleteBackupParams{
			BackupVaultUUID: "test-vault-uuid",
			BackupUUID:      "test-backup-uuid",
			AccountName:     "test-account",
		}

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "test-backup-uuid",
			},
			Name: "test-backup",
			Attributes: &datamodel.BackupAttributes{
				RestoreVolumeCount: 0,
			},
		}

		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(backup, nil)
		expectedError := errors.New("failed to delete backup")
		store.On("DeleteBackup", ctx, backup.UUID).Return(nil, expectedError)

		o := &Orchestrator{storage: store}
		result, err := o.DeleteBackupInternal(ctx, params)

		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		assert.Equal(t, "", result)
		store.AssertExpectations(t)
	})

	t.Run("WhenBackupNotFoundError_ReturnsError", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store := database.NewMockStorage(t)

		// Override hydrationEnabled to false
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = false
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		params := &common.DeleteBackupParams{
			BackupVaultUUID: "test-vault-uuid",
			BackupUUID:      "test-backup-uuid",
			AccountName:     "test-account",
		}

		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, params.AccountName).
			Return(nil, vsaerror.NewNotFoundErr("backup", nil))

		o := &Orchestrator{storage: store}
		result, err := o.DeleteBackupInternal(ctx, params)

		assert.NoError(t, err)
		assert.Equal(t, "", result)
		store.AssertExpectations(t)
	})

	t.Run("WhenBackupHasRestoreVolumeCountGreaterThanZero_ReturnsUserInputValidationError", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store := database.NewMockStorage(t)

		// Override hydrationEnabled to false
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = false
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		params := &common.DeleteBackupParams{
			BackupVaultUUID: "test-vault-uuid",
			BackupUUID:      "test-backup-uuid",
			AccountName:     "test-account",
		}

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "test-backup-uuid",
			},
			Name: "test-backup",
			Attributes: &datamodel.BackupAttributes{
				RestoreVolumeCount: 2,
			},
		}

		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(backup, nil)

		o := &Orchestrator{storage: store}
		result, err := o.DeleteBackupInternal(ctx, params)

		assert.Error(t, err)
		assert.True(t, vsaerror.IsUserInputValidationErr(err))
		assert.Contains(t, err.Error(), "Cannot delete the backup as it is being used to restore a volume")
		assert.Equal(t, "", result)
		store.AssertExpectations(t)
	})

	t.Run("WhenHydrationEnabledAndBackupVaultIsNil_ReturnsError", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store := database.NewMockStorage(t)

		params := &common.DeleteBackupParams{
			BackupVaultUUID: "test-vault-uuid",
			BackupUUID:      "test-backup-uuid",
			AccountName:     "test-account",
		}

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "test-backup-uuid",
			},
			Name: "test-backup",
			Attributes: &datamodel.BackupAttributes{
				RestoreVolumeCount: 0,
			},
			BackupVault: nil, // This should trigger the error
		}

		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(backup, nil)
		store.On("DeleteBackup", ctx, backup.UUID).Return(&datamodel.Backup{}, nil)

		o := &Orchestrator{storage: store}
		result, err := o.DeleteBackupInternal(ctx, params)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Could not find the backup vault associated with the backup")
		assert.Equal(t, "", result)
		store.AssertExpectations(t)
	})

	t.Run("WhenHydrationEnabledAndBackupVaultExistsButHydrationFails_ReturnsError", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store := database.NewMockStorage(t)

		// Override hydrateDeletedBackupsToCCFE to return an error
		originalHydrateDeletedBackupsToCCFE := hydrateDeletedBackupsToCCFE
		hydrateDeletedBackupsToCCFE = func(ctx context.Context, params *common.DeleteBackupParams, backup *datamodel.Backup) error {
			return errors.New("failed to hydrate deleted backup to CCFE")
		}
		defer func() { hydrateDeletedBackupsToCCFE = originalHydrateDeletedBackupsToCCFE }()

		params := &common.DeleteBackupParams{
			BackupVaultUUID: "test-vault-uuid",
			BackupUUID:      "test-backup-uuid",
			AccountName:     "test-account",
		}

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "test-backup-uuid",
			},
			Name: "test-backup",
			Attributes: &datamodel.BackupAttributes{
				RestoreVolumeCount: 0,
			},
			BackupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{
					UUID: "test-vault-uuid",
				},
				Name: "test-vault",
			},
		}

		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(backup, nil)
		store.On("DeleteBackup", ctx, backup.UUID).Return(&datamodel.Backup{}, nil)

		o := &Orchestrator{storage: store}
		result, err := o.DeleteBackupInternal(ctx, params)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to hydrate deleted backup to CCFE")
		assert.Equal(t, "", result)
		store.AssertExpectations(t)
	})

	t.Run("WhenHydrationEnabledAndBackupVaultExistsAndHydrationSucceeds_ReturnsNoError", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store := database.NewMockStorage(t)

		// Override hydrationEnabled to true
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = true
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		// Override hydrateDeletedBackupsToCCFE to return success
		originalHydrateDeletedBackupsToCCFE := hydrateDeletedBackupsToCCFE
		hydrateDeletedBackupsToCCFE = func(ctx context.Context, params *common.DeleteBackupParams, backup *datamodel.Backup) error {
			return nil
		}
		defer func() { hydrateDeletedBackupsToCCFE = originalHydrateDeletedBackupsToCCFE }()

		params := &common.DeleteBackupParams{
			BackupVaultUUID: "test-vault-uuid",
			BackupUUID:      "test-backup-uuid",
			AccountName:     "test-account",
		}

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "test-backup-uuid",
			},
			Name: "test-backup",
			Attributes: &datamodel.BackupAttributes{
				RestoreVolumeCount: 0,
			},
			BackupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{
					UUID: "test-vault-uuid",
				},
				Name: "test-vault",
			},
		}

		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(backup, nil)
		store.On("DeleteBackup", ctx, backup.UUID).Return(&datamodel.Backup{}, nil)

		o := &Orchestrator{storage: store}
		result, err := o.DeleteBackupInternal(ctx, params)

		assert.NoError(t, err)
		assert.Equal(t, "", result)
		store.AssertExpectations(t)
	})

	t.Run("WhenHydrationDisabled_SkipsHydration", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		store := database.NewMockStorage(t)

		// Override hydrationEnabled to false
		originalHydrationEnabled := hydrationEnabled
		hydrationEnabled = false
		defer func() { hydrationEnabled = originalHydrationEnabled }()

		params := &common.DeleteBackupParams{
			BackupVaultUUID: "test-vault-uuid",
			BackupUUID:      "test-backup-uuid",
			AccountName:     "test-account",
		}

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "test-backup-uuid",
			},
			Name: "test-backup",
			Attributes: &datamodel.BackupAttributes{
				RestoreVolumeCount: 0,
			},
			BackupVault: nil, // Even with nil BackupVault, should not error when hydration is disabled
		}

		store.On("GetBackup", ctx, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(backup, nil)
		store.On("DeleteBackup", ctx, backup.UUID).Return(&datamodel.Backup{}, nil)

		o := &Orchestrator{storage: store}
		result, err := o.DeleteBackupInternal(ctx, params)

		assert.NoError(t, err)
		assert.Equal(t, "", result)
		store.AssertExpectations(t)
	})
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}

func Test_fetchRemoteBackupFromVCP(t *testing.T) {
	// Common test data
	backupUUID := "test-backup-uuid"
	backupVaultUUID := "test-backup-vault-uuid"
	projectNumber := "123456789"
	region := "us-central1"
	basePath := "https://example.com"
	jwtToken := "test-jwt-token"

	t.Run("Success", func(t *testing.T) {
		// Arrange
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, log.NewLogger())

		// Mock getRemoteRegionConfig
		originalGetRemoteRegionConfig := getRemoteRegionConfig
		defer func() { getRemoteRegionConfig = originalGetRemoteRegionConfig }()
		getRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return basePath, jwtToken, nil
		}

		// Mock googleproxyclient.GetGProxyClient
		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		// Mock V1betaInternalDescribeBackup
		expectedBackup := googleproxyclient.InternalBackupV1beta{
			ResourceId:  googleproxyclient.NewOptString(backupUUID),
			IsRestoring: googleproxyclient.NewOptBool(false),
		}
		mockResponse := &googleproxyclient.V1betaInternalDescribeBackupOK{
			Backups: []googleproxyclient.InternalBackupV1beta{expectedBackup},
		}
		mockInvoker.On("V1betaInternalDescribeBackup", mock.Anything, mock.Anything).Return(mockResponse, nil)

		// Act
		result, err := _fetchRemoteBackupFromVCP(ctx, backupUUID, backupVaultUUID, projectNumber, region)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, expectedBackup.ResourceId, result.ResourceId)
		assert.Equal(t, expectedBackup.IsRestoring, result.IsRestoring)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("GetRemoteRegionConfigError", func(t *testing.T) {
		// Arrange
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, log.NewLogger())
		expectedError := errors.New("failed to get region config")

		// Mock getRemoteRegionConfig to return error
		originalGetRemoteRegionConfig := getRemoteRegionConfig
		defer func() { getRemoteRegionConfig = originalGetRemoteRegionConfig }()
		getRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "", "", expectedError
		}

		// Act
		result, err := _fetchRemoteBackupFromVCP(ctx, backupUUID, backupVaultUUID, projectNumber, region)

		// Assert
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		assert.Equal(t, googleproxyclient.InternalBackupV1beta{}, result)
	})

	t.Run("V1betaInternalDescribeBackupError", func(t *testing.T) {
		// Arrange
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, log.NewLogger())

		// Mock getRemoteRegionConfig
		originalGetRemoteRegionConfig := getRemoteRegionConfig
		defer func() { getRemoteRegionConfig = originalGetRemoteRegionConfig }()
		getRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return basePath, jwtToken, nil
		}

		// Mock googleproxyclient.GetGProxyClient
		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		// Mock V1betaInternalDescribeBackup to return error
		expectedError := errors.New("failed to fetch backup")
		mockInvoker.On("V1betaInternalDescribeBackup", mock.Anything, mock.Anything).Return(nil, expectedError)

		// Act
		result, err := _fetchRemoteBackupFromVCP(ctx, backupUUID, backupVaultUUID, projectNumber, region)

		// Assert
		assert.Error(t, err)
		assert.True(t, vsaerror.IsNotFoundErr(err))
		assert.Contains(t, err.Error(), "remote backup")
		assert.Equal(t, googleproxyclient.InternalBackupV1beta{}, result)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("UnexpectedResponseType", func(t *testing.T) {
		// Arrange
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, log.NewLogger())

		// Mock getRemoteRegionConfig
		originalGetRemoteRegionConfig := getRemoteRegionConfig
		defer func() { getRemoteRegionConfig = originalGetRemoteRegionConfig }()
		getRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return basePath, jwtToken, nil
		}

		// Mock googleproxyclient.GetGProxyClient
		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		// Mock V1betaInternalDescribeBackup to return unexpected response type
		unexpectedResponse := &googleproxyclient.V1betaInternalDescribeBackupBadRequest{}
		mockInvoker.On("V1betaInternalDescribeBackup", mock.Anything, mock.Anything).Return(unexpectedResponse, nil)

		// Act
		result, err := _fetchRemoteBackupFromVCP(ctx, backupUUID, backupVaultUUID, projectNumber, region)

		// Assert
		assert.Error(t, err)
		assert.True(t, vsaerror.IsNotFoundErr(err))
		assert.Contains(t, err.Error(), "remote backup")
		assert.Equal(t, googleproxyclient.InternalBackupV1beta{}, result)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("EmptyBackupsArray", func(t *testing.T) {
		// Arrange
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, log.NewLogger())

		// Mock getRemoteRegionConfig
		originalGetRemoteRegionConfig := getRemoteRegionConfig
		defer func() { getRemoteRegionConfig = originalGetRemoteRegionConfig }()
		getRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return basePath, jwtToken, nil
		}

		// Mock googleproxyclient.GetGProxyClient
		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		// Mock V1betaInternalDescribeBackup to return empty backups array
		mockResponse := &googleproxyclient.V1betaInternalDescribeBackupOK{
			Backups: []googleproxyclient.InternalBackupV1beta{},
		}
		mockInvoker.On("V1betaInternalDescribeBackup", mock.Anything, mock.Anything).Return(mockResponse, nil)

		// Act
		result, err := _fetchRemoteBackupFromVCP(ctx, backupUUID, backupVaultUUID, projectNumber, region)

		// Assert
		assert.Error(t, err)
		assert.True(t, vsaerror.IsNotFoundErr(err))
		assert.Contains(t, err.Error(), "remote backup")
		assert.Equal(t, googleproxyclient.InternalBackupV1beta{}, result)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("MultipleBackups_ReturnsFirst", func(t *testing.T) {
		// Arrange
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, log.NewLogger())

		// Mock getRemoteRegionConfig
		originalGetRemoteRegionConfig := getRemoteRegionConfig
		defer func() { getRemoteRegionConfig = originalGetRemoteRegionConfig }()
		getRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return basePath, jwtToken, nil
		}

		// Mock googleproxyclient.GetGProxyClient
		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		// Mock V1betaInternalDescribeBackup with multiple backups
		firstBackup := googleproxyclient.InternalBackupV1beta{
			ResourceId:  googleproxyclient.NewOptString("first-backup-uuid"),
			IsRestoring: googleproxyclient.NewOptBool(false),
		}
		secondBackup := googleproxyclient.InternalBackupV1beta{
			ResourceId:  googleproxyclient.NewOptString("second-backup-uuid"),
			IsRestoring: googleproxyclient.NewOptBool(true),
		}
		mockResponse := &googleproxyclient.V1betaInternalDescribeBackupOK{
			Backups: []googleproxyclient.InternalBackupV1beta{firstBackup, secondBackup},
		}
		mockInvoker.On("V1betaInternalDescribeBackup", mock.Anything, mock.Anything).Return(mockResponse, nil)

		// Act
		result, err := _fetchRemoteBackupFromVCP(ctx, backupUUID, backupVaultUUID, projectNumber, region)

		// Assert
		assert.NoError(t, err)
		// Should return the first backup
		assert.Equal(t, firstBackup.ResourceId, result.ResourceId)
		assert.Equal(t, firstBackup.IsRestoring, result.IsRestoring)
		mockInvoker.AssertExpectations(t)
	})

	t.Run("NoCorrelationIDInContext", func(t *testing.T) {
		// Arrange
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, log.NewLogger())
		// No correlation ID in context

		// Mock getRemoteRegionConfig
		originalGetRemoteRegionConfig := getRemoteRegionConfig
		defer func() { getRemoteRegionConfig = originalGetRemoteRegionConfig }()
		getRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return basePath, jwtToken, nil
		}

		// Mock googleproxyclient.GetGProxyClient
		mockInvoker := googleproxyclient.NewMockInvoker(t)
		mockClient := &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mockClient
		}

		// Mock V1betaInternalDescribeBackup
		expectedBackup := googleproxyclient.InternalBackupV1beta{
			ResourceId:  googleproxyclient.NewOptString(backupUUID),
			IsRestoring: googleproxyclient.NewOptBool(false),
		}
		mockResponse := &googleproxyclient.V1betaInternalDescribeBackupOK{
			Backups: []googleproxyclient.InternalBackupV1beta{expectedBackup},
		}
		// Should still work even without correlation ID
		mockInvoker.On("V1betaInternalDescribeBackup", mock.Anything, mock.Anything).Return(mockResponse, nil)

		// Act
		result, err := _fetchRemoteBackupFromVCP(ctx, backupUUID, backupVaultUUID, projectNumber, region)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, expectedBackup.ResourceId, result.ResourceId)
		mockInvoker.AssertExpectations(t)
	})
}

func Test_hydrateDeletedBackupsToCCFE(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

	t.Run("WhenSuccessful", func(tt *testing.T) {
		// Arrange
		params := &common.DeleteBackupParams{
			AccountName:     "test-account",
			Region:          "us-central1",
			BackupVaultUUID: "backup-vault-uuid",
			BackupUUID:      "backup-uuid",
		}
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "backup-uuid",
			},
			Name: "test-backup",
			BackupVault: &datamodel.BackupVault{
				Name: "test-backup-vault",
			},
		}

		// Mock auth.GenerateCallbackToken
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		defer func() { auth.GenerateCallbackToken = originalGenerateCallbackToken }()
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mock-token", nil
		}

		// Mock common.HydrateDeletedBackups
		originalHydrateDeletedBackups := common.HydrateDeletedBackups
		defer func() { common.HydrateDeletedBackups = originalHydrateDeletedBackups }()
		common.HydrateDeletedBackups = func(ctx context.Context, logger log.Logger, names []string, backupVaultName string, location string, projectId string, token string) error {
			assert.Equal(tt, "test-backup-vault", backupVaultName)
			assert.Equal(tt, "us-central1", location)
			assert.Equal(tt, "test-account", projectId)
			assert.Equal(tt, "mock-token", token)
			assert.NotEmpty(tt, names)
			return nil
		}

		// Act
		err := _hydrateDeletedBackupsToCCFE(ctx, params, backup)

		// Assert
		assert.NoError(tt, err)
	})

	t.Run("WhenTokenGenerationFails", func(tt *testing.T) {
		// Arrange
		params := &common.DeleteBackupParams{
			AccountName:     "test-account",
			Region:          "us-central1",
			BackupVaultUUID: "backup-vault-uuid",
			BackupUUID:      "backup-uuid",
		}
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "backup-uuid",
			},
			Name: "test-backup",
			BackupVault: &datamodel.BackupVault{
				Name: "test-backup-vault",
			},
		}

		expectedError := errors.New("token generation failed")

		// Mock auth.GenerateCallbackToken to return error
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		defer func() { auth.GenerateCallbackToken = originalGenerateCallbackToken }()
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "", expectedError
		}

		// Act
		err := _hydrateDeletedBackupsToCCFE(ctx, params, backup)

		// Assert
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})

	t.Run("WhenHydrationFails", func(tt *testing.T) {
		// Arrange
		params := &common.DeleteBackupParams{
			AccountName:     "test-account",
			Region:          "us-central1",
			BackupVaultUUID: "backup-vault-uuid",
			BackupUUID:      "backup-uuid",
		}
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				UUID: "backup-uuid",
			},
			Name: "test-backup",
			BackupVault: &datamodel.BackupVault{
				Name: "test-backup-vault",
			},
		}

		expectedError := errors.New("hydration failed")

		// Mock auth.GenerateCallbackToken
		originalGenerateCallbackToken := auth.GenerateCallbackToken
		defer func() { auth.GenerateCallbackToken = originalGenerateCallbackToken }()
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mock-token", nil
		}

		// Mock common.HydrateDeletedBackups to return error
		originalHydrateDeletedBackups := common.HydrateDeletedBackups
		defer func() { common.HydrateDeletedBackups = originalHydrateDeletedBackups }()
		common.HydrateDeletedBackups = func(ctx context.Context, logger log.Logger, names []string, backupVaultName string, location string, projectId string, token string) error {
			return expectedError
		}

		// Act
		err := _hydrateDeletedBackupsToCCFE(ctx, params, backup)

		// Assert
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
}
