package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	vsaerror "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
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

func TestValidateBackupDeleteParams(t *testing.T) {
	t.Run("OnSuccess", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		backup := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "testBackupUUID"},
			VolumeUUID: "volumeUUID1",
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
	t.Run("OnLatestBackupError", func(t *testing.T) {
		ctx := context.Background()
		store := database.NewMockStorage(t)
		backup := &datamodel.Backup{
			BaseModel:  datamodel.BaseModel{UUID: "testBackupUUID"},
			VolumeUUID: "volumeUUID1",
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
}

func TestOrchestrator_GetBackupsUnderBackupVault(t *testing.T) {
	t.Run("WhenGetOrCreateAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		orch := &Orchestrator{storage: mockStorage}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, ownerID string) (*datamodel.Account, error) {
			return nil, errors.New("failed to get or create account")
		}
		defer func() { getOrCreateAccount = _getOrCreateAccount }()
		backups, err := orch.GetBackupsUnderBackupVault(ctx, "backupVaultID", "ownerID", []string{"backupUUID"})
		assert.Nil(tt, backups)
		assert.EqualError(tt, err, "failed to get or create account")
	})

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
