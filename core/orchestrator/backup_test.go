package orchestrator

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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
		assert.EqualError(tt, err, "volume not found")
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
