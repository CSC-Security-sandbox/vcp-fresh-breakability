package orchestrator

import (
	"context"
	"errors"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
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

		getBackups = func(ctx context.Context, se database.Storage, params *common.GetBackupsParams) ([]*datamodel.Backup, error) {
			return []*datamodel.Backup{}, nil
		}

		o := &Orchestrator{storage: store}
		backups, err := o.ListBackups(ctx, params)

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
		assert.EqualError(tt, err, "Already a backup is in creating state for selected volume")
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
		params := &common.GetBackupsParams{}

		store.On("GetBackupsByBackupVault", ctx, params.BackupVaultID).Return([]*datamodel.Backup{}, nil)

		backups, err := _getBackups(ctx, store, params)
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
