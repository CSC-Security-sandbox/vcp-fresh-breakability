package gcp

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	utilerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"go.temporal.io/sdk/mocks"
)

func TestGetBackupPolicyByNameAndOwnerID(tt *testing.T) {
	tt.Run("WhenBackupPolicyExists", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		dbBackupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "backup-policy-uuid",
			},
			Name:    "backup-policy-name",
			Account: account,
		}
		oldGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = oldGetAccountWithName }()
		oldGetBackupPolicyWithName := getBackupPolicyByNameAndOwnerID
		getBackupPolicyByNameAndOwnerID = func(ctx context.Context, se database.Storage, backupPolicy, ownerID string) (*datamodel.BackupPolicy, error) {
			return dbBackupPolicy, nil
		}
		defer func() { getBackupPolicyByNameAndOwnerID = oldGetBackupPolicyWithName }()

		o := &GCPOrchestrator{storage: se}
		result, err := o.GetBackupPolicyByNameAndOwnerID(context.Background(), dbBackupPolicy.Name, account.UUID)
		assert.NoError(tt, err, "Expected no error")
		assert.Equal(tt, dbBackupPolicy.UUID, result.BackupPolicyUUID, "Expected BackupPolicyID to match")
		assert.Equal(tt, dbBackupPolicy.Name, result.ResourceID, "Expected ResourceID to match")
	})
	tt.Run("WhenBackupPolicyDoesNotExist", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		backupPolicyName := "non-existent-backup-policy"
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		oldGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = oldGetAccountWithName }()
		oldGetBackupPolicyWithName := getBackupPolicyByNameAndOwnerID
		getBackupPolicyByNameAndOwnerID = func(ctx context.Context, se database.Storage, backupPolicy, ownerID string) (*datamodel.BackupPolicy, error) {
			return nil, utilerrors.NewNotFoundErr("backup policy", &backupPolicyName)
		}
		defer func() { getBackupPolicyByNameAndOwnerID = oldGetBackupPolicyWithName }()

		o := &GCPOrchestrator{storage: se}
		result, err := o.GetBackupPolicyByNameAndOwnerID(context.Background(), backupPolicyName, account.UUID)
		assert.Error(tt, err, "Expected error")
		assert.Nil(tt, result, "Expected result to be nil")
		assert.Equal(tt, utilerrors.NewNotFoundErr("backup policy", &backupPolicyName).Error(), err.Error(), "Expected error message to match")
	})
	tt.Run("WhenAccountDoesNotExist", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		backupPolicyName := "backup-policy"

		// With getOrCreateAccount, the account will be created and then backup policy search will fail
		o := &GCPOrchestrator{storage: se}
		result, err := o.GetBackupPolicyByNameAndOwnerID(context.Background(), backupPolicyName, "non-existent-owner-uuid")
		assert.Error(tt, err, "Expected error")
		assert.Nil(tt, result, "Expected result to be nil")
		assert.Contains(tt, err.Error(), "backup policy 'backup-policy' not found", "Expected backup policy not found error")
	})
}

func TestListBackupPoliciesAndVolumeCount(tt *testing.T) {
	tt.Run("WhenListBackupPoliciesAndVolumeCountSucceeds", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		dbBackupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "backup-policy-uuid",
			},
			Name:    "backup-policy-name",
			Account: account,
		}
		dbBackupPolicyMap := map[string]int64{"backup-policy-uuid": 5}
		dbBackupPolicies := []*datamodel.BackupPolicy{dbBackupPolicy}

		oldGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = oldGetAccountWithName }()

		oldListBackupPolicyVolumeCount := listBackupPolicyVolumeCount
		listBackupPolicyVolumeCount = func(ctx context.Context, se database.Storage, accountID int64, backupPolicyUUIDs []string) (map[string]int64, error) {
			return dbBackupPolicyMap, nil
		}
		defer func() { listBackupPolicyVolumeCount = oldListBackupPolicyVolumeCount }()

		oldListBackupPolicies := listBackupPolicies
		listBackupPolicies = func(ctx context.Context, se database.Storage, accountID int64, backupPolicyUUIDs []string) ([]*datamodel.BackupPolicy, error) {
			return dbBackupPolicies, nil
		}
		defer func() { listBackupPolicies = oldListBackupPolicies }()

		o := &GCPOrchestrator{storage: se}
		volumeCount, policyMap, err := o.ListBackupPoliciesAndVolumeCount(context.Background(), account.UUID, nil)
		assert.NoError(tt, err)
		assert.Equal(tt, dbBackupPolicyMap, volumeCount)
		assert.Len(tt, policyMap, 1)
		assert.Equal(tt, dbBackupPolicy.UUID, policyMap[dbBackupPolicy.UUID].BackupPolicyUUID)
	})
	tt.Run("WhenListBackupPolicyVolumeCountFails", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}

		oldGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = oldGetAccountWithName }()

		oldListBackupPolicyVolumeCount := listBackupPolicyVolumeCount
		listBackupPolicyVolumeCount = func(ctx context.Context, se database.Storage, accountID int64, backupPolicyUUIDs []string) (map[string]int64, error) {
			return nil, errors.New("volume count error")
		}
		defer func() { listBackupPolicyVolumeCount = oldListBackupPolicyVolumeCount }()

		o := &GCPOrchestrator{storage: se}
		volumeCount, policyMap, err := o.ListBackupPoliciesAndVolumeCount(context.Background(), account.UUID, nil)
		assert.Error(tt, err)
		assert.Nil(tt, volumeCount)
		assert.Nil(tt, policyMap)
		assert.Equal(tt, "volume count error", err.Error())
	})
	tt.Run("WhenListBackupPoliciesFails", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		dbBackupPolicyMap := map[string]int64{"backup-policy-uuid": 5}

		oldGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = oldGetAccountWithName }()

		oldListBackupPolicyVolumeCount := listBackupPolicyVolumeCount
		listBackupPolicyVolumeCount = func(ctx context.Context, se database.Storage, accountID int64, backupPolicyUUIDs []string) (map[string]int64, error) {
			return dbBackupPolicyMap, nil
		}
		defer func() { listBackupPolicyVolumeCount = oldListBackupPolicyVolumeCount }()

		oldListBackupPolicies := listBackupPolicies
		listBackupPolicies = func(ctx context.Context, se database.Storage, accountID int64, backupPolicyUUIDs []string) ([]*datamodel.BackupPolicy, error) {
			return nil, errors.New("list policies error")
		}
		defer func() { listBackupPolicies = oldListBackupPolicies }()

		o := &GCPOrchestrator{storage: se}
		volumeCount, policyMap, err := o.ListBackupPoliciesAndVolumeCount(context.Background(), account.UUID, nil)
		assert.Error(tt, err)
		assert.Nil(tt, volumeCount)
		assert.Nil(tt, policyMap)
		assert.Equal(tt, "list policies error", err.Error())
	})
	tt.Run("WhenNoBackupPoliciesExist", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		oldGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = oldGetAccountWithName }()
		oldListBackupPolicyVolumeCount := listBackupPolicyVolumeCount

		listBackupPolicyVolumeCount = func(ctx context.Context, se database.Storage, accountID int64, backupPolicyUUIDs []string) (map[string]int64, error) {
			return nil, nil
		}
		defer func() { listBackupPolicyVolumeCount = oldListBackupPolicyVolumeCount }()

		oldListBackupPolicies := listBackupPolicies
		listBackupPolicies = func(ctx context.Context, se database.Storage, accountID int64, backupPolicyUUIDs []string) ([]*datamodel.BackupPolicy, error) {
			return nil, nil
		}
		defer func() { listBackupPolicies = oldListBackupPolicies }()

		o := &GCPOrchestrator{storage: se}
		volumeCount, policyMap, err := o.ListBackupPoliciesAndVolumeCount(context.Background(), account.UUID, nil)
		assert.NoError(tt, err)
		assert.Nil(tt, volumeCount)
		assert.Equal(tt, 0, len(policyMap), "Expected no backup policies")
	})
	tt.Run("WhenAccountDoesNotExist", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")

		newAccount := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "non-existent-owner-uuid"}}

		oldGetOrCreateAccount := getOrCreateAccount
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			// Simulate account creation when it doesn't exist
			return newAccount, nil
		}
		defer func() { getOrCreateAccount = oldGetOrCreateAccount }()

		oldListBackupPolicyVolumeCount := listBackupPolicyVolumeCount
		listBackupPolicyVolumeCount = func(ctx context.Context, se database.Storage, accountID int64, backupPolicyUUIDs []string) (map[string]int64, error) {
			return nil, nil
		}
		defer func() { listBackupPolicyVolumeCount = oldListBackupPolicyVolumeCount }()

		oldListBackupPolicies := listBackupPolicies
		listBackupPolicies = func(ctx context.Context, se database.Storage, accountID int64, backupPolicyUUIDs []string) ([]*datamodel.BackupPolicy, error) {
			return nil, nil
		}
		defer func() { listBackupPolicies = oldListBackupPolicies }()

		o := &GCPOrchestrator{storage: se}
		volumeCount, policyMap, err := o.ListBackupPoliciesAndVolumeCount(context.Background(), "non-existent-owner-uuid", nil)
		assert.NoError(tt, err)
		assert.Nil(tt, volumeCount)
		assert.Equal(tt, 0, len(policyMap), "Expected no backup policies")
	})
	tt.Run("WhenAccountDoesNotExistAndIsCreated", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")

		newAccount := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "new-account-uuid"}}
		var accountCreated bool

		oldGetOrCreateAccount := getOrCreateAccount
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			// Mark that account creation was called
			accountCreated = true
			return newAccount, nil
		}
		defer func() { getOrCreateAccount = oldGetOrCreateAccount }()

		oldListBackupPolicyVolumeCount := listBackupPolicyVolumeCount
		listBackupPolicyVolumeCount = func(ctx context.Context, se database.Storage, accountID int64, backupPolicyUUIDs []string) (map[string]int64, error) {
			return nil, nil
		}
		defer func() { listBackupPolicyVolumeCount = oldListBackupPolicyVolumeCount }()

		oldListBackupPolicies := listBackupPolicies
		listBackupPolicies = func(ctx context.Context, se database.Storage, accountID int64, backupPolicyUUIDs []string) ([]*datamodel.BackupPolicy, error) {
			return nil, nil
		}
		defer func() { listBackupPolicies = oldListBackupPolicies }()

		o := &GCPOrchestrator{storage: se}
		volumeCount, policyMap, err := o.ListBackupPoliciesAndVolumeCount(context.Background(), "new-account-uuid", nil)
		assert.NoError(tt, err)
		assert.True(tt, accountCreated, "Expected account creation to be called")
		assert.Nil(tt, volumeCount)
		assert.Equal(tt, 0, len(policyMap), "Expected no backup policies")
	})
}

func TestDeleteBackupPolicy(tt *testing.T) {
	tt.Run("DeleteBackupPolicySucceeds", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(mocks.Client)
		ctx := context.Background()

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupPolicy := datamodel.BackupPolicy{
			BaseModel:             datamodel.BaseModel{ID: 1, UUID: "test-backup-policy-uuid"},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("Test backup policy"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   4,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		updatedBackupPolicy := backupPolicy
		updatedBackupPolicy.LifeCycleState = models.LifeCycleStateDeleting
		updatedBackupPolicy.LifeCycleStateDetails = models.LifeCycleStateDeletingDetails

		job := &datamodel.Job{BaseModel: datamodel.BaseModel{ID: 1, UUID: "job-uuid"}}

		mockStorage.On("GetAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicy.UUID, int64(1)).Return(&backupPolicy, nil)
		mockStorage.On("GetVolumeCountByBackupPolicyID", ctx, backupPolicy.UUID).Return(int64(0), nil)
		mockStorage.On("UpdateBackupPolicy", ctx, backupPolicy.UUID, mock.Anything).Return(&updatedBackupPolicy, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		mockTemporal.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		o := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporal}
		deletingBackupPolicy, _, err := o.DeleteBackupPolicy(ctx, &commonparams.DeleteBackupPolicyParams{
			Name:           backupPolicy.Name,
			OwnerID:        account.UUID,
			BackupPolicyID: backupPolicy.UUID,
			LocationID:     "test-location",
		})

		assert.NoError(tt, err)
		assert.NotNil(tt, deletingBackupPolicy)
		assert.Equal(tt, deletingBackupPolicy.State, models.LifeCycleStateDeleting)
	})
	tt.Run("DeleteBackupPolicyFailsWhenAccountDoesNotExist", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(mocks.Client)
		ctx := context.Background()

		mockStorage.On("GetAccount", ctx, "owner-uuid").Return(nil, errors.New("account not found"))

		o := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporal}
		deletingBackupPolicy, _, err := o.DeleteBackupPolicy(ctx, &commonparams.DeleteBackupPolicyParams{
			Name:           "test-backup-policy",
			OwnerID:        "owner-uuid",
			BackupPolicyID: "test-backup-policy-uuid",
			LocationID:     "test-location",
		})

		assert.Error(tt, err)
		assert.Nil(tt, deletingBackupPolicy)
		assert.Equal(tt, "account not found", err.Error())
	})
	tt.Run("DeleteBackupPolicyFailsWhenBackupPolicyDoesNotExist", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(mocks.Client)
		ctx := context.Background()

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupPolicyID := "test-backup-policy-uuid"

		mockStorage.On("GetAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyID, int64(1)).Return(nil, errors.New("backup policy not found"))

		o := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporal}
		deletingBackupPolicy, _, err := o.DeleteBackupPolicy(ctx, &commonparams.DeleteBackupPolicyParams{
			Name:           "test-backup-policy",
			OwnerID:        account.UUID,
			BackupPolicyID: backupPolicyID,
			LocationID:     "test-location",
		})

		assert.Error(tt, err)
		assert.Nil(tt, deletingBackupPolicy)
		assert.Equal(tt, "backup policy not found", err.Error())
	})
	tt.Run("DeleteBackupPolicyFailsWhenBackupPolicyIsNotInReadyState", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(mocks.Client)
		ctx := context.Background()

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupPolicy := datamodel.BackupPolicy{
			BaseModel:             datamodel.BaseModel{ID: 1, UUID: "test-backup-policy-uuid"},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("Test backup policy"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   4,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateUpdating,
			LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
		}

		// mockStorage.On("GetAccountWithName", ctx, "owner-uuid").Return(account, nil)

		mockStorage.On("GetAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicy.UUID, int64(1)).Return(&backupPolicy, nil)

		o := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporal}
		deletingBackupPolicy, _, err := o.DeleteBackupPolicy(ctx, &commonparams.DeleteBackupPolicyParams{
			Name:           backupPolicy.Name,
			OwnerID:        account.UUID,
			BackupPolicyID: backupPolicy.UUID,
			LocationID:     "test-location",
		})

		assert.Error(tt, err)
		assert.Nil(tt, deletingBackupPolicy)
		assert.Equal(tt, "backup policy is not in ready state, please check the backup policy and try again", err.Error())
	})
	tt.Run("DeleteBackupPolicyFailsWhenGetVolumeCountFails", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(mocks.Client)
		ctx := context.Background()

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupPolicy := datamodel.BackupPolicy{
			BaseModel:             datamodel.BaseModel{ID: 1, UUID: "test-backup-policy-uuid"},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("Test backup policy"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   4,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}

		mockStorage.On("GetAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicy.UUID, int64(1)).Return(&backupPolicy, nil)
		mockStorage.On("GetVolumeCountByBackupPolicyID", ctx, backupPolicy.UUID).Return(int64(0), errors.New("failed to get volume count"))

		o := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporal}
		deletingBackupPolicy, _, err := o.DeleteBackupPolicy(ctx, &commonparams.DeleteBackupPolicyParams{
			Name:           backupPolicy.Name,
			OwnerID:        account.UUID,
			BackupPolicyID: backupPolicy.UUID,
			LocationID:     "test-location",
		})

		assert.Error(tt, err)
		assert.Nil(tt, deletingBackupPolicy)
		assert.Equal(tt, "failed to get volume count", err.Error())
	})
	tt.Run("DeleteBackupPolicyFailsWhenBackupPolicyHasVolumesAttached", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(mocks.Client)
		ctx := context.Background()

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupPolicy := datamodel.BackupPolicy{
			BaseModel:             datamodel.BaseModel{ID: 1, UUID: "test-backup-policy-uuid"},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("Test backup policy"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   4,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}

		mockStorage.On("GetAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicy.UUID, int64(1)).Return(&backupPolicy, nil)
		mockStorage.On("GetVolumeCountByBackupPolicyID", ctx, backupPolicy.UUID).Return(int64(2), nil)

		o := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporal}
		deletingBackupPolicy, _, err := o.DeleteBackupPolicy(ctx, &commonparams.DeleteBackupPolicyParams{
			Name:           backupPolicy.Name,
			OwnerID:        account.UUID,
			BackupPolicyID: backupPolicy.UUID,
			LocationID:     "test-location",
		})

		assert.Error(tt, err)
		assert.Nil(tt, deletingBackupPolicy)
		assert.Equal(tt, "backup policy has volumes attached, please detach backup policy from volumes before deleting backup policy", err.Error())
	})
	tt.Run("DeleteBackupPolicyFailsWhenUpdatingBackupPolicy", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(mocks.Client)
		ctx := context.Background()

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupPolicy := datamodel.BackupPolicy{
			BaseModel:             datamodel.BaseModel{ID: 1, UUID: "test-backup-policy-uuid"},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("Test backup policy"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   4,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}

		mockStorage.On("GetAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicy.UUID, int64(1)).Return(&backupPolicy, nil)
		mockStorage.On("GetVolumeCountByBackupPolicyID", ctx, backupPolicy.UUID).Return(int64(0), nil)
		mockStorage.On("UpdateBackupPolicy", ctx, backupPolicy.UUID, mock.Anything).Return(nil, errors.New("failed to update backup policy"))

		o := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporal}
		deletingBackupPolicy, _, err := o.DeleteBackupPolicy(ctx, &commonparams.DeleteBackupPolicyParams{
			Name:           backupPolicy.Name,
			OwnerID:        account.UUID,
			BackupPolicyID: backupPolicy.UUID,
			LocationID:     "test-location",
		})

		assert.Error(tt, err)
		assert.Nil(tt, deletingBackupPolicy)
		assert.Equal(tt, "failed to update backup policy", err.Error())
	})
	tt.Run("DeleteBackupPolicyFailsWhenCreatingJob", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(mocks.Client)
		ctx := context.Background()

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupPolicy := datamodel.BackupPolicy{
			BaseModel:             datamodel.BaseModel{ID: 1, UUID: "test-backup-policy-uuid"},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("Test backup policy"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   4,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		updatedBackupPolicy := backupPolicy
		updatedBackupPolicy.LifeCycleState = models.LifeCycleStateDeleting
		updatedBackupPolicy.LifeCycleStateDetails = models.LifeCycleStateDeletingDetails

		mockStorage.On("GetAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicy.UUID, int64(1)).Return(&backupPolicy, nil)
		mockStorage.On("GetVolumeCountByBackupPolicyID", ctx, backupPolicy.UUID).Return(int64(0), nil)
		mockStorage.On("UpdateBackupPolicy", ctx, backupPolicy.UUID, mock.Anything).Return(&updatedBackupPolicy, nil).Times(2)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(nil, errors.New("failed to create job"))

		o := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporal}
		deletingBackupPolicy, _, err := o.DeleteBackupPolicy(ctx, &commonparams.DeleteBackupPolicyParams{
			Name:           backupPolicy.Name,
			OwnerID:        account.UUID,
			BackupPolicyID: backupPolicy.UUID,
			LocationID:     "test-location",
		})

		assert.Error(tt, err)
		assert.Nil(tt, deletingBackupPolicy)
		assert.Equal(tt, "failed to create job", err.Error())
	})
	tt.Run("DeleteBackupPolicyFailsWhenExecutingWorkflow", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(mocks.Client)
		ctx := context.Background()

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupPolicy := datamodel.BackupPolicy{
			BaseModel:             datamodel.BaseModel{ID: 1, UUID: "test-backup-policy-uuid"},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("Test backup policy"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   4,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		updatedBackupPolicy := backupPolicy
		updatedBackupPolicy.LifeCycleState = models.LifeCycleStateDeleting
		updatedBackupPolicy.LifeCycleStateDetails = models.LifeCycleStateDeletingDetails

		job := &datamodel.Job{BaseModel: datamodel.BaseModel{ID: 1, UUID: "job-uuid"}}
		temporalError := errors.New("failed to execute workflow")

		mockStorage.On("GetAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicy.UUID, int64(1)).Return(&backupPolicy, nil)
		mockStorage.On("GetVolumeCountByBackupPolicyID", ctx, backupPolicy.UUID).Return(int64(0), nil)
		mockStorage.On("UpdateBackupPolicy", ctx, backupPolicy.UUID, mock.Anything).Return(&updatedBackupPolicy, nil).Times(2)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		mockTemporal.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, temporalError)
		mockStorage.On("UpdateJob", ctx, job.UUID, mock.Anything, job.TrackingID, temporalError.Error()).Return(nil, nil)

		o := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporal}
		deletingBackupPolicy, _, err := o.DeleteBackupPolicy(ctx, &commonparams.DeleteBackupPolicyParams{
			Name:           backupPolicy.Name,
			OwnerID:        account.UUID,
			BackupPolicyID: backupPolicy.UUID,
			LocationID:     "test-location",
		})

		assert.Error(tt, err)
		assert.Nil(tt, deletingBackupPolicy)
		assert.Equal(tt, "failed to execute workflow", err.Error())
	})
}

func Test_convertDatastoreBackupPolicyToModel(t *testing.T) {
	t.Run("WhenBackupPolicyContainsAllFields", func(t *testing.T) {
		createdAt := time.Now()
		description := "backup-policy-description"
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				UUID:      "uuid",
				CreatedAt: createdAt,
				UpdatedAt: createdAt,
				ID:        1,
			},
			AccountID:            123,
			Name:                 "policy",
			DailyBackupsToKeep:   1,
			WeeklyBackupsToKeep:  2,
			MonthlyBackupsToKeep: 3,
			PolicyEnabled:        true,
			Description:          &description,
			LifeCycleState:       "active",
		}
		expectedModel := &models.BackupPolicy{
			ResourceID:         backupPolicy.Name,
			BackupPolicyUUID:   backupPolicy.UUID,
			DailyBackupLimit:   backupPolicy.DailyBackupsToKeep,
			WeeklyBackupLimit:  backupPolicy.WeeklyBackupsToKeep,
			MonthlyBackupLimit: backupPolicy.MonthlyBackupsToKeep,
			Enabled:            backupPolicy.PolicyEnabled,
			Description:        backupPolicy.Description,
			State:              backupPolicy.LifeCycleState,
			CreatedAt:          backupPolicy.CreatedAt,
		}
		model := convertDatastoreBackupPolicyToModel(backupPolicy)
		assert.Equal(t, expectedModel, model)
	})
	t.Run("WhenBackupPolicyHasMissingFields", func(t *testing.T) {
		createdAt := time.Now()
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				UUID:      "uuid",
				CreatedAt: createdAt,
			},
			AccountID:            123,
			Name:                 "policy",
			DailyBackupsToKeep:   1,
			WeeklyBackupsToKeep:  2,
			MonthlyBackupsToKeep: 3,
		}
		expectedModel := &models.BackupPolicy{
			ResourceID:         backupPolicy.Name,
			BackupPolicyUUID:   backupPolicy.UUID,
			DailyBackupLimit:   backupPolicy.DailyBackupsToKeep,
			WeeklyBackupLimit:  backupPolicy.WeeklyBackupsToKeep,
			MonthlyBackupLimit: backupPolicy.MonthlyBackupsToKeep,
			Enabled:            backupPolicy.PolicyEnabled,
			Description:        backupPolicy.Description,
			State:              backupPolicy.LifeCycleState,
			CreatedAt:          backupPolicy.CreatedAt,
		}
		model := convertDatastoreBackupPolicyToModel(backupPolicy)
		assert.Equal(t, expectedModel, model)
	})
}

func TestGetBackupPolicyByUUIDAndOwnerID(t *testing.T) {
	t.Run("WhenBackupPolicyExists", func(tt *testing.T) {
		ctx, se, orchestrator, _ := setup(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test-account-name",
		}
		account, err := se.CreateAccount(ctx, account)
		assert.NoError(tt, err)

		oldGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = oldGetAccountWithName }()
		dbBackupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-backup-policy-uuid",
			},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("Test backup policy"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   4,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		dbBackupPolicy, err = se.CreateBackupPolicyEntryInVCP(ctx, dbBackupPolicy)
		assert.NoError(tt, err)

		result, err := orchestrator.GetBackupPolicyByUUIDAndOwnerID(ctx, dbBackupPolicy.UUID, account.Name)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, dbBackupPolicy.UUID, result.BackupPolicyUUID)
	})

	t.Run("WhenAccountDoesNotExist", func(tt *testing.T) {
		ctx, se, orchestrator, _ := setup(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test-account-name",
		}
		account, err := se.CreateAccount(ctx, account)
		assert.NoError(tt, err)

		dbBackupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-backup-policy-uuid",
			},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("Test backup policy"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   4,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		dbBackupPolicy, err = se.CreateBackupPolicyEntryInVCP(ctx, dbBackupPolicy)
		assert.NoError(tt, err)

		result, err := orchestrator.GetBackupPolicyByUUIDAndOwnerID(ctx, dbBackupPolicy.UUID, "non-existent-account")
		assert.Error(tt, err)
		assert.Nil(tt, result)
	})

	t.Run("WhenBackupPolicyDoesNotExist", func(tt *testing.T) {
		ctx, se, orchestrator, _ := setup(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test-account-name",
		}
		account, err := se.CreateAccount(ctx, account)
		assert.NoError(tt, err)

		dbBackupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-backup-policy-uuid",
			},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("Test backup policy"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   4,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		_, err = se.CreateBackupPolicyEntryInVCP(ctx, dbBackupPolicy)
		assert.NoError(tt, err)

		result, err := orchestrator.GetBackupPolicyByUUIDAndOwnerID(ctx, "non-existent-uuid", account.Name)
		assert.Error(tt, err)
		assert.Nil(tt, result)
	})
}

func TestUpdateBackupPolicy(t *testing.T) {
	t.Run("UpdateBackupPolicySucceeds", func(tt *testing.T) {
		ctx, se, orchestrator, temporal := setup(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test-account",
		}
		account, err := se.CreateAccount(ctx, account)
		assert.NoError(tt, err)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-pool-uuid",
			},
			Name:      "test-pool",
			Account:   account,
			AccountID: account.ID,
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone:  "us-central1-a",
				IsRegionalHA: false,
			},
		}
		pool, err = se.CreatingPool(ctx, pool)
		assert.NoError(tt, err)
		pool, err = se.CreatedPool(ctx, pool)
		assert.NoError(tt, err)

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-backup-vault-uuid",
			},
			Name:      "test-backup-vault",
			AccountID: account.ID,
			Account:   account,
		}
		backupVault, err = se.CreateBackupVaultEntryInVCP(ctx, backupVault)
		assert.NoError(tt, err)

		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-backup-policy-uuid",
			},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("Test backup policy"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   4,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		backupPolicy, err = se.CreateBackupPolicyEntryInVCP(ctx, backupPolicy)
		assert.NoError(tt, err)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-volume-uuid",
			},
			Name:      "test-volume",
			AccountID: account.ID,
			Account:   account,
			PoolID:    pool.ID,
			Pool:      pool,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID:  backupVault.UUID,
				BackupPolicyID: backupPolicy.UUID,
			},
		}
		volume, err = se.CreateVolume(ctx, volume)
		assert.NoError(tt, err)
		_, err = se.UpdateVolumeState(ctx, volume.UUID, models.LifeCycleStateREADY, models.LifeCycleStateReadyDetails)
		assert.NoError(tt, err)

		params := &commonparams.UpdateBackupPolicyParams{
			Name:               "test-backup-policy",
			AccountName:        "test-account",
			BackupPolicyID:     "test-backup-policy-uuid",
			LocationID:         "test-location",
			Description:        nillable.ToPointer("This is a test backup policy"),
			PolicyEnabled:      nillable.ToPointer(false),
			DailyBackupLimit:   nillable.ToPointer(int64(5)),
			WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
			MonthlyBackupLimit: nillable.ToPointer(int64(2)),
		}
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, params, mock.Anything).
			Return(nil, nil)
		updated, _, err := orchestrator.UpdateBackupPolicy(ctx, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, updated)
		assert.Equal(tt, *backupPolicy.Description, *updated.Description)
		assert.Equal(tt, backupPolicy.PolicyEnabled, updated.Enabled)
		assert.Equal(tt, backupPolicy.DailyBackupsToKeep, updated.DailyBackupLimit)
		assert.Equal(tt, backupPolicy.WeeklyBackupsToKeep, updated.WeeklyBackupLimit)
		assert.Equal(tt, backupPolicy.MonthlyBackupsToKeep, updated.MonthlyBackupLimit)
		assert.Equal(tt, models.LifeCycleStateUpdating, updated.State)
	})

	t.Run("SucceedsWhenBackupPolicyIsNotInUseByAnyVolumes", func(tt *testing.T) {
		ctx, se, orchestrator, temporal := setup(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test-account",
		}
		account, err := se.CreateAccount(ctx, account)
		assert.NoError(tt, err)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-pool-uuid",
			},
			Name:      "test-pool",
			Account:   account,
			AccountID: account.ID,
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone:  "us-central1-a",
				IsRegionalHA: false,
			},
		}
		pool, err = se.CreatingPool(ctx, pool)
		assert.NoError(tt, err)
		pool, err = se.CreatedPool(ctx, pool)
		assert.NoError(tt, err)

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-backup-vault-uuid",
			},
			Name:      "test-backup-vault",
			AccountID: account.ID,
			Account:   account,
		}
		backupVault, err = se.CreateBackupVaultEntryInVCP(ctx, backupVault)
		assert.NoError(tt, err)

		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-backup-policy-uuid",
			},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("Test backup policy"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   4,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		backupPolicy, err = se.CreateBackupPolicyEntryInVCP(ctx, backupPolicy)
		assert.NoError(tt, err)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-volume-uuid",
			},
			Name:      "test-volume",
			AccountID: account.ID,
			Account:   account,
			PoolID:    pool.ID,
			Pool:      pool,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: backupVault.UUID,
			},
		}
		volume, err = se.CreateVolume(ctx, volume)
		assert.NoError(tt, err)
		_, err = se.UpdateVolumeState(ctx, volume.UUID, models.LifeCycleStateREADY, models.LifeCycleStateReadyDetails)
		assert.NoError(tt, err)

		params := &commonparams.UpdateBackupPolicyParams{
			Name:               "test-backup-policy",
			AccountName:        "test-account",
			BackupPolicyID:     "test-backup-policy-uuid",
			LocationID:         "test-location",
			Description:        nillable.ToPointer("This is a test backup policy"),
			PolicyEnabled:      nillable.ToPointer(false),
			DailyBackupLimit:   nillable.ToPointer(int64(5)),
			WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
			MonthlyBackupLimit: nillable.ToPointer(int64(2)),
		}
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, params, mock.Anything).
			Return(nil, nil)
		updated, _, err := orchestrator.UpdateBackupPolicy(ctx, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, updated)
		assert.Equal(tt, *backupPolicy.Description, *updated.Description)
		assert.Equal(tt, backupPolicy.PolicyEnabled, updated.Enabled)
		assert.Equal(tt, backupPolicy.DailyBackupsToKeep, updated.DailyBackupLimit)
		assert.Equal(tt, backupPolicy.WeeklyBackupsToKeep, updated.WeeklyBackupLimit)
		assert.Equal(tt, backupPolicy.MonthlyBackupsToKeep, updated.MonthlyBackupLimit)
		assert.Equal(tt, models.LifeCycleStateUpdating, updated.State)
	})

	t.Run("FailsWhenAccountDoesNotExist", func(tt *testing.T) {
		ctx, se, orchestrator, _ := setup(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test-account",
		}
		_, err := se.CreateAccount(ctx, account)
		assert.NoError(tt, err)

		params := &commonparams.UpdateBackupPolicyParams{
			Name:               "test-backup-policy",
			AccountName:        "non-existent-account",
			BackupPolicyID:     "test-backup-policy-uuid",
			LocationID:         "test-location",
			Description:        nillable.ToPointer("This is a test backup policy"),
			PolicyEnabled:      nillable.ToPointer(false),
			DailyBackupLimit:   nillable.ToPointer(int64(5)),
			WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
			MonthlyBackupLimit: nillable.ToPointer(int64(2)),
		}
		updated, _, err := orchestrator.UpdateBackupPolicy(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, updated)
	})

	t.Run("FailsWhenBackupPolicyIsNotInReadyState", func(tt *testing.T) {
		ctx, se, orchestrator, _ := setup(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test-account",
		}
		account, err := se.CreateAccount(ctx, account)
		assert.NoError(tt, err)

		dbBackupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-backup-policy-uuid",
			},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("Test backup policy"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   4,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateUpdating,
			LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
		}
		_, err = se.CreateBackupPolicyEntryInVCP(ctx, dbBackupPolicy)
		assert.NoError(tt, err)

		params := &commonparams.UpdateBackupPolicyParams{
			Name:               "test-backup-policy",
			AccountName:        "test-account",
			BackupPolicyID:     "test-backup-policy-uuid",
			LocationID:         "test-location",
			Description:        nillable.ToPointer("This is a test backup policy"),
			PolicyEnabled:      nillable.ToPointer(false),
			DailyBackupLimit:   nillable.ToPointer(int64(5)),
			WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
			MonthlyBackupLimit: nillable.ToPointer(int64(2)),
		}
		updated, _, err := orchestrator.UpdateBackupPolicy(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, updated)
		assert.IsType(tt, &utilerrors.UserInputValidationErr{}, err)
		assert.Equal(tt, "backup policy is not in a valid state for update", err.Error())
	})

	t.Run("FailsWhenBackupPolicyDoesNotExist", func(tt *testing.T) {
		ctx, se, orchestrator, _ := setup(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test-account",
		}
		account, err := se.CreateAccount(ctx, account)
		assert.NoError(tt, err)

		dbBackupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-backup-policy-uuid",
			},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("Test backup policy"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   4,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateUpdating,
			LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
		}
		_, err = se.CreateBackupPolicyEntryInVCP(ctx, dbBackupPolicy)
		assert.NoError(tt, err)

		params := &commonparams.UpdateBackupPolicyParams{
			Name:               "test-backup-policy",
			AccountName:        "test-account",
			BackupPolicyID:     "non-existent-backup-policy-uuid",
			LocationID:         "test-location",
			Description:        nillable.ToPointer("This is a test backup policy"),
			PolicyEnabled:      nillable.ToPointer(false),
			DailyBackupLimit:   nillable.ToPointer(int64(5)),
			WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
			MonthlyBackupLimit: nillable.ToPointer(int64(2)),
		}
		updated, _, err := orchestrator.UpdateBackupPolicy(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, updated)
	})

	t.Run("FailsWhenBackupPolicyCountExceedsLimits", func(tt *testing.T) {
		ctx, se, orchestrator, _ := setup(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test-account",
		}
		account, err := se.CreateAccount(ctx, account)
		assert.NoError(tt, err)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-pool-uuid",
			},
			Name:      "test-pool",
			Account:   account,
			AccountID: account.ID,
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone:  "us-central1-a",
				IsRegionalHA: false,
			},
		}
		pool, err = se.CreatingPool(ctx, pool)
		assert.NoError(tt, err)
		pool, err = se.CreatedPool(ctx, pool)
		assert.NoError(tt, err)

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-backup-vault-uuid",
			},
			Name:      "test-backup-vault",
			AccountID: account.ID,
			Account:   account,
		}
		backupVault, err = se.CreateBackupVaultEntryInVCP(ctx, backupVault)
		assert.NoError(tt, err)

		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-backup-policy-uuid",
			},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("Test backup policy"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   1,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		backupPolicy, err = se.CreateBackupPolicyEntryInVCP(ctx, backupPolicy)
		assert.NoError(tt, err)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-volume-uuid",
			},
			Name:      "test-volume",
			AccountID: account.ID,
			Account:   account,
			PoolID:    pool.ID,
			Pool:      pool,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID:  backupVault.UUID,
				BackupPolicyID: backupPolicy.UUID,
			},
		}
		volume, err = se.CreateVolume(ctx, volume)
		assert.NoError(tt, err)
		_, err = se.UpdateVolumeState(ctx, volume.UUID, models.LifeCycleStateREADY, models.LifeCycleStateReadyDetails)
		assert.NoError(tt, err)

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-backup-uuid",
			},
			Name:          "test-backup",
			BackupVaultID: backupVault.ID,
			BackupVault:   backupVault,
			VolumeUUID:    volume.UUID,
			Type:          "MANUAL",
			State:         models.LifeCycleStateAvailable,
			StateDetails:  models.LifeCycleStateAvailableDetails,
		}
		_, err = se.CreateBackup(ctx, backup)
		assert.NoError(tt, err)
		_, err = se.FinishBackup(ctx, backup)
		assert.NoError(tt, err)

		params := &commonparams.UpdateBackupPolicyParams{
			Name:               "test-backup-policy",
			AccountName:        "test-account",
			BackupPolicyID:     "test-backup-policy-uuid",
			LocationID:         "test-location",
			Description:        nillable.ToPointer("This is a test backup policy"),
			PolicyEnabled:      nillable.ToPointer(false),
			DailyBackupLimit:   nillable.ToPointer(int64(500)),
			WeeklyBackupLimit:  nillable.ToPointer(int64(300)),
			MonthlyBackupLimit: nillable.ToPointer(int64(200)),
		}
		updated, _, err := orchestrator.UpdateBackupPolicy(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, updated)
		assert.Equal(tt, fmt.Sprintf("the total number of backups exceeds the limit of %d for volume %s", maxBackupsToKeep, volume.UUID), err.Error())
	})

	t.Run("FailsWhenWorkflowExecutionErrors", func(tt *testing.T) {
		ctx, se, orchestrator, temporal := setup(tt)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test-account",
		}
		account, err := se.CreateAccount(ctx, account)
		assert.NoError(tt, err)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-pool-uuid",
			},
			Name:      "test-pool",
			Account:   account,
			AccountID: account.ID,
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone:  "us-central1-a",
				IsRegionalHA: false,
			},
		}
		pool, err = se.CreatingPool(ctx, pool)
		assert.NoError(tt, err)
		pool, err = se.CreatedPool(ctx, pool)
		assert.NoError(tt, err)

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-backup-vault-uuid",
			},
			Name:      "test-backup-vault",
			AccountID: account.ID,
			Account:   account,
		}
		backupVault, err = se.CreateBackupVaultEntryInVCP(ctx, backupVault)
		assert.NoError(tt, err)

		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-backup-policy-uuid",
			},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("Test backup policy"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   4,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		backupPolicy, err = se.CreateBackupPolicyEntryInVCP(ctx, backupPolicy)
		assert.NoError(tt, err)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-volume-uuid",
			},
			Name:      "test-volume",
			AccountID: account.ID,
			Account:   account,
			PoolID:    pool.ID,
			Pool:      pool,
			DataProtection: &datamodel.DataProtection{
				BackupVaultID:  backupVault.UUID,
				BackupPolicyID: backupPolicy.UUID,
			},
		}
		volume, err = se.CreateVolume(ctx, volume)
		assert.NoError(tt, err)
		_, err = se.UpdateVolumeState(ctx, volume.UUID, models.LifeCycleStateREADY, models.LifeCycleStateReadyDetails)
		assert.NoError(tt, err)

		params := &commonparams.UpdateBackupPolicyParams{
			Name:               "test-backup-policy",
			AccountName:        "test-account",
			BackupPolicyID:     "test-backup-policy-uuid",
			LocationID:         "test-location",
			Description:        nillable.ToPointer("This is a test backup policy"),
			PolicyEnabled:      nillable.ToPointer(false),
			DailyBackupLimit:   nillable.ToPointer(int64(5)),
			WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
			MonthlyBackupLimit: nillable.ToPointer(int64(2)),
		}
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, params, mock.Anything).
			Return(nil, errors.New("could not execute workflow"))
		updated, _, err := orchestrator.UpdateBackupPolicy(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, updated)
		assert.Equal(tt, "could not execute workflow", err.Error())

		backupPolicy, err = se.GetBackupPolicyByUUIDAndOwnerID(ctx, backupPolicy.UUID, account.ID)
		assert.NoError(tt, err)
		assert.NotNil(tt, backupPolicy)
		assert.Equal(tt, models.LifeCycleStateREADY, backupPolicy.LifeCycleState)
	})

	t.Run("RollbackBackupPolicyFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockTemporalClient := mocks.NewClient(tt)
		mockWorkflowRun := mocks.NewWorkflowRun(tt)
		orchestrator := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporalClient}

		params := &commonparams.UpdateBackupPolicyParams{
			Name:               "test-backup-policy",
			AccountName:        "test-account",
			BackupPolicyID:     "test-backup-policy-uuid",
			LocationID:         "test-location",
			Description:        nillable.ToPointer("desc"),
			PolicyEnabled:      nillable.ToPointer(false),
			DailyBackupLimit:   nillable.ToPointer(int64(5)),
			WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
			MonthlyBackupLimit: nillable.ToPointer(int64(2)),
		}

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"}, Name: "test-account"}
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel:             datamodel.BaseModel{ID: 1, UUID: "test-backup-policy-uuid"},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("desc"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   4,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		job := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "job-uuid"}, WorkflowID: "job-uuid"}

		mockStorage.On("GetAccount", mock.Anything, params.AccountName).Return(account, nil)
		mockStorage.On("CreateJob", mock.Anything, mock.Anything).Return(job, nil)
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", mock.Anything, params.BackupPolicyID, account.ID).Return(backupPolicy, nil)
		mockStorage.On("UpdateBackupPolicy", mock.Anything, backupPolicy.UUID, mock.Anything).Return(backupPolicy, nil).Once()
		mockStorage.On("GetMultipleVolumes", mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil)
		mockStorage.On("GetMultipleVolumesWithExpertMode", mock.Anything, mock.Anything).Return([]*datamodel.ExpertModeVolumes{}, nil)
		mockStorage.On("GetBackupCountByVolumeUUIDs", mock.Anything, mock.Anything, mock.Anything).Return(map[string]int64{}, nil)
		mockStorage.On("UpdateBackupPolicy", mock.Anything, backupPolicy.UUID, mock.Anything).Return(nil, errors.New("rollback failed")).Once()
		mockStorage.On("UpdateJob", mock.Anything, job.UUID, string(models.JobsStateERROR), mock.Anything, mock.Anything).Return(nil)
		mockTemporalClient.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockWorkflowRun, errors.New("could not execute workflow"))

		_, _, err := orchestrator.UpdateBackupPolicy(ctx, params)
		assert.Error(tt, err)
		assert.Equal(tt, "could not execute workflow", err.Error())
		mockStorage.AssertNumberOfCalls(tt, "UpdateBackupPolicy", 2)
		mockStorage.AssertNumberOfCalls(tt, "UpdateJob", 1)
	})

	t.Run("RollbackJobFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockTemporalClient := mocks.NewClient(tt)
		mockWorkflowRun := mocks.NewWorkflowRun(tt)
		orchestrator := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporalClient}
		params := &commonparams.UpdateBackupPolicyParams{
			Name:               "test-backup-policy",
			AccountName:        "test-account",
			BackupPolicyID:     "test-backup-policy-uuid",
			LocationID:         "test-location",
			Description:        nillable.ToPointer("desc"),
			PolicyEnabled:      nillable.ToPointer(false),
			DailyBackupLimit:   nillable.ToPointer(int64(5)),
			WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
			MonthlyBackupLimit: nillable.ToPointer(int64(2)),
		}

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"}, Name: "test-account"}
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel:             datamodel.BaseModel{ID: 1, UUID: "test-backup-policy-uuid"},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("desc"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   4,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		job := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "job-uuid"}}

		mockStorage.On("GetAccount", mock.Anything, params.AccountName).Return(account, nil)
		mockStorage.On("CreateJob", mock.Anything, mock.Anything).Return(job, nil)
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", mock.Anything, params.BackupPolicyID, account.ID).Return(backupPolicy, nil)
		mockStorage.On("UpdateBackupPolicy", mock.Anything, backupPolicy.UUID, mock.Anything).Return(backupPolicy, nil)
		mockStorage.On("GetMultipleVolumes", mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil)
		mockStorage.On("GetMultipleVolumesWithExpertMode", mock.Anything, mock.Anything).Return([]*datamodel.ExpertModeVolumes{}, nil)
		mockStorage.On("GetBackupCountByVolumeUUIDs", mock.Anything, mock.Anything, mock.Anything).Return(map[string]int64{}, nil)
		mockStorage.On("UpdateJob", mock.Anything, job.UUID, string(models.JobsStateERROR), mock.Anything, mock.Anything).Return(errors.New("job rollback failed")).Once()
		mockTemporalClient.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockWorkflowRun, errors.New("could not execute workflow"))

		_, _, err := orchestrator.UpdateBackupPolicy(ctx, params)
		assert.Error(tt, err)
		assert.Equal(tt, "could not execute workflow", err.Error())
		mockStorage.AssertNumberOfCalls(tt, "UpdateBackupPolicy", 2)
		mockStorage.AssertNumberOfCalls(tt, "UpdateJob", 1)
	})

	t.Run("GetMultipleVolumesWithExpertModeError_ReturnsErr", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockTemporalClient := mocks.NewClient(tt)
		orchestrator := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporalClient}

		params := &commonparams.UpdateBackupPolicyParams{
			Name:               "test-backup-policy",
			AccountName:        "test-account",
			BackupPolicyID:     "test-backup-policy-uuid",
			LocationID:         "test-location",
			DailyBackupLimit:   nillable.ToPointer(int64(5)),
			WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
			MonthlyBackupLimit: nillable.ToPointer(int64(2)),
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"}, Name: "test-account"}
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel:      datamodel.BaseModel{ID: 1, UUID: "test-backup-policy-uuid"},
			Name:           "test-backup-policy",
			Account:        account,
			AccountID:      account.ID,
			LifeCycleState: models.LifeCycleStateREADY,
		}

		// validateBackupLimits runs before CreateJob; error here returns early without touching the job.
		mockStorage.On("GetAccount", mock.Anything, params.AccountName).Return(account, nil)
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", mock.Anything, params.BackupPolicyID, account.ID).Return(backupPolicy, nil)
		mockStorage.On("GetMultipleVolumes", mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil)
		mockStorage.On("GetMultipleVolumesWithExpertMode", mock.Anything, mock.Anything).
			Return(nil, errors.New("expert mode db error"))

		_, _, err := orchestrator.UpdateBackupPolicy(ctx, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "expert mode db error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ExpertModeVolumeUUIDs_AddedToBackupCheck", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockTemporalClient := mocks.NewClient(tt)
		orchestrator := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporalClient}

		dailyLimit := int64(2)
		params := &commonparams.UpdateBackupPolicyParams{
			Name:               "test-backup-policy",
			AccountName:        "test-account",
			BackupPolicyID:     "test-backup-policy-uuid",
			LocationID:         "test-location",
			DailyBackupLimit:   &dailyLimit,
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"}, Name: "test-account"}
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel:      datamodel.BaseModel{ID: 1, UUID: "test-backup-policy-uuid"},
			Name:           "test-backup-policy",
			Account:        account,
			AccountID:      account.ID,
			LifeCycleState: models.LifeCycleStateREADY,
		}
		job := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "job-uuid"}, WorkflowID: "wf-id"}
		expertVols := []*datamodel.ExpertModeVolumes{
			{BaseModel: datamodel.BaseModel{UUID: "emv-uuid-1"}, ExternalUUID: "ext-uuid-1"},
		}

		mockStorage.On("GetAccount", mock.Anything, params.AccountName).Return(account, nil)
		mockStorage.On("CreateJob", mock.Anything, mock.Anything).Return(job, nil)
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", mock.Anything, params.BackupPolicyID, account.ID).Return(backupPolicy, nil)
		mockStorage.On("GetMultipleVolumes", mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil)
		mockStorage.On("GetMultipleVolumesWithExpertMode", mock.Anything, mock.Anything).Return(expertVols, nil)
		// Verify expert mode volume ExternalUUID is passed to GetBackupCountByVolumeUUIDs
		mockStorage.On("GetBackupCountByVolumeUUIDs", mock.Anything, []string{"ext-uuid-1"}, mock.Anything).
			Return(map[string]int64{"ext-uuid-1": 0}, nil)
		mockStorage.On("UpdateBackupPolicy", mock.Anything, backupPolicy.UUID, mock.Anything).Return(backupPolicy, nil)
		mockTemporalClient.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		_, _, err := orchestrator.UpdateBackupPolicy(ctx, params)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("JobUpdateFailsAfterBackupPolicyUpdateError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		mockTemporalClient := mocks.NewClient(tt)
		orchestrator := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporalClient}

		params := &commonparams.UpdateBackupPolicyParams{
			Name:               "test-backup-policy",
			AccountName:        "test-account",
			BackupPolicyID:     "test-backup-policy-uuid",
			LocationID:         "test-location",
			Description:        nillable.ToPointer("desc"),
			PolicyEnabled:      nillable.ToPointer(false),
			DailyBackupLimit:   nillable.ToPointer(int64(5)),
			WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
			MonthlyBackupLimit: nillable.ToPointer(int64(2)),
		}

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"}, Name: "test-account"}
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel:             datamodel.BaseModel{ID: 1, UUID: "test-backup-policy-uuid"},
			Name:                  "test-backup-policy",
			Account:               account,
			AccountID:             account.ID,
			Description:           nillable.ToPointer("desc"),
			PolicyEnabled:         true,
			DailyBackupsToKeep:    7,
			WeeklyBackupsToKeep:   4,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		job := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "job-uuid"}, WorkflowID: "job-uuid"}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-volume-uuid",
			},
			Name: "test-volume",
			DataProtection: &datamodel.DataProtection{
				BackupVaultID:  "test-backup-vault-uuid",
				BackupPolicyID: backupPolicy.UUID,
			},
		}

		mockStorage.On("GetAccount", mock.Anything, params.AccountName).Return(account, nil)
		mockStorage.On("GetMultipleVolumes", mock.Anything, mock.Anything).Return([]*datamodel.Volume{volume}, nil)
		mockStorage.On("GetMultipleVolumesWithExpertMode", mock.Anything, mock.Anything).Return([]*datamodel.ExpertModeVolumes{}, nil)
		mockStorage.On("GetBackupCountByVolumeUUIDs", mock.Anything, mock.Anything, mock.Anything).Return(map[string]int64{volume.UUID: 1}, nil)
		mockStorage.On("CreateJob", mock.Anything, mock.Anything).Return(job, nil)
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", mock.Anything, params.BackupPolicyID, account.ID).Return(backupPolicy, nil)
		// Simulate error updating backup policy (backupPolicyUpdateErr)
		mockStorage.On("UpdateBackupPolicy", mock.Anything, backupPolicy.UUID, mock.Anything).Return(nil, errors.New("update policy failed")).Once()
		// Simulate error updating job state after policy update error
		mockStorage.On("UpdateJob", mock.Anything, job.UUID, string(models.JobsStateERROR), mock.Anything, mock.Anything).Return(errors.New("job update failed")).Once()

		updated, _, err := orchestrator.UpdateBackupPolicy(ctx, params)
		assert.Error(t, err)
		assert.Nil(t, updated)
		assert.Contains(t, err.Error(), "update policy failed")
		mockStorage.AssertNumberOfCalls(t, "UpdateBackupPolicy", 1)
		mockStorage.AssertNumberOfCalls(t, "UpdateJob", 1)
	})
}

func TestCreateBackupPolicy(tt *testing.T) {
	tt.Run("CreateBackupPolicySucceeds", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(mocks.Client)
		ctx := context.Background()

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		createdBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:             datamodel.BaseModel{ID: 1, UUID: "backup-policy-uuid"},
			Name:                  "test-backup-policy",
			AccountID:             account.ID,
			DailyBackupsToKeep:    5,
			WeeklyBackupsToKeep:   3,
			MonthlyBackupsToKeep:  2,
			PolicyEnabled:         true,
			Description:           nillable.ToPointer("Test description"),
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateReadyDetails,
		}

		oldGetOrCreateAccount := getOrCreateAccount
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = oldGetOrCreateAccount }()

		mockStorage.On("GetBackupPolicyByNameAndOwnerID", ctx, "test-backup-policy", account.ID).Return(nil, utilerrors.NewNotFoundErr("backup policy", nil))
		mockStorage.On("CreateBackupPolicyEntryInVCP", ctx, mock.Anything).Return(createdBackupPolicy, nil)
		mockTemporal.On("ScheduleClient").Return(nil)

		originalCreateBackupPolicySchedule := activities.CreateBackupPolicySchedule
		defer func() { activities.CreateBackupPolicySchedule = originalCreateBackupPolicySchedule }()
		activities.CreateBackupPolicySchedule = func(ctx context.Context, temporalScheduler *scheduler.TemporalScheduler, vcpBackupPolicy *datamodel.BackupPolicy, customSchedule string) error {
			return nil
		}

		o := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporal}
		params := &commonparams.CreateBackupPolicyParams{
			Name:               "test-backup-policy",
			AccountName:        "owner-uuid",
			LocationID:         "test-location",
			Description:        nillable.ToPointer("Test description"),
			DailyBackupLimit:   nillable.ToPointer(int64(5)),
			WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
			MonthlyBackupLimit: nillable.ToPointer(int64(2)),
			PolicyEnabled:      nillable.ToPointer(true),
		}

		result, err := o.CreateBackupPolicy(ctx, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, createdBackupPolicy.UUID, result.BackupPolicyUUID)
		assert.Equal(tt, models.LifeCycleStateREADY, result.State)
		mockStorage.AssertExpectations(tt)
	})
	tt.Run("CreateBackupPolicyFailsWhenAccountDoesNotExist", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		ctx := context.Background()

		oldGetOrCreateAccount := getOrCreateAccount
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		defer func() { getOrCreateAccount = oldGetOrCreateAccount }()

		o := &GCPOrchestrator{storage: mockStorage}
		params := &commonparams.CreateBackupPolicyParams{
			Name:        "test-backup-policy",
			AccountName: "owner-uuid",
			LocationID:  "test-location",
		}

		result, err := o.CreateBackupPolicy(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "account not found", err.Error())
	})
	tt.Run("CreateBackupPolicyFailsWhenBackupPolicyAlreadyExists", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		ctx := context.Background()

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		existingBackupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "existing-backup-policy-uuid"},
			Name:      "test-backup-policy",
		}

		oldGetOrCreateAccount := getOrCreateAccount
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = oldGetOrCreateAccount }()

		mockStorage.On("GetBackupPolicyByNameAndOwnerID", ctx, "test-backup-policy", account.ID).Return(existingBackupPolicy, nil)

		o := &GCPOrchestrator{storage: mockStorage}
		params := &commonparams.CreateBackupPolicyParams{
			Name:        "test-backup-policy",
			AccountName: "owner-uuid",
			LocationID:  "test-location",
		}

		result, err := o.CreateBackupPolicy(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "backup policy with name test-backup-policy already exists")
		mockStorage.AssertExpectations(tt)
	})
	tt.Run("CreateBackupPolicyFailsWhenGetBackupPolicyReturnsNonNotFoundError", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		ctx := context.Background()

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}

		oldGetOrCreateAccount := getOrCreateAccount
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = oldGetOrCreateAccount }()

		mockStorage.On("GetBackupPolicyByNameAndOwnerID", ctx, "test-backup-policy", account.ID).Return(nil, errors.New("database error"))

		o := &GCPOrchestrator{storage: mockStorage}
		params := &commonparams.CreateBackupPolicyParams{
			Name:        "test-backup-policy",
			AccountName: "owner-uuid",
			LocationID:  "test-location",
		}

		result, err := o.CreateBackupPolicy(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "database error", err.Error())
		mockStorage.AssertExpectations(tt)
	})
	tt.Run("CreateBackupPolicyFailsWhenBackupPolicyCreationFails", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		ctx := context.Background()

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}

		oldGetOrCreateAccount := getOrCreateAccount
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = oldGetOrCreateAccount }()

		mockStorage.On("GetBackupPolicyByNameAndOwnerID", ctx, "test-backup-policy", account.ID).Return(nil, utilerrors.NewNotFoundErr("backup policy", nil))
		mockStorage.On("CreateBackupPolicyEntryInVCP", ctx, mock.Anything).Return(nil, errors.New("failed to create backup policy"))

		o := &GCPOrchestrator{storage: mockStorage}
		params := &commonparams.CreateBackupPolicyParams{
			Name:        "test-backup-policy",
			AccountName: "owner-uuid",
			LocationID:  "test-location",
		}

		result, err := o.CreateBackupPolicy(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "failed to create backup policy", err.Error())
		mockStorage.AssertExpectations(tt)
	})
	tt.Run("CreateBackupPolicySucceedsWithAllOptionalFields", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(mocks.Client)
		ctx := context.Background()

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		createdBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:             datamodel.BaseModel{ID: 1, UUID: "backup-policy-uuid"},
			Name:                  "test-backup-policy",
			AccountID:             account.ID,
			Description:           nillable.ToPointer("Test description"),
			DailyBackupsToKeep:    5,
			WeeklyBackupsToKeep:   3,
			MonthlyBackupsToKeep:  2,
			PolicyEnabled:         true,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateReadyDetails,
		}

		oldGetOrCreateAccount := getOrCreateAccount
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = oldGetOrCreateAccount }()

		mockStorage.On("GetBackupPolicyByNameAndOwnerID", ctx, "test-backup-policy", account.ID).Return(nil, utilerrors.NewNotFoundErr("backup policy", nil))
		mockStorage.On("CreateBackupPolicyEntryInVCP", ctx, mock.Anything).Return(createdBackupPolicy, nil)
		mockTemporal.On("ScheduleClient").Return(nil)

		originalCreateBackupPolicySchedule := activities.CreateBackupPolicySchedule
		defer func() { activities.CreateBackupPolicySchedule = originalCreateBackupPolicySchedule }()
		activities.CreateBackupPolicySchedule = func(ctx context.Context, temporalScheduler *scheduler.TemporalScheduler, vcpBackupPolicy *datamodel.BackupPolicy, customSchedule string) error {
			return nil
		}

		o := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporal}
		params := &commonparams.CreateBackupPolicyParams{
			Name:               "test-backup-policy",
			AccountName:        "owner-uuid",
			LocationID:         "test-location",
			Description:        nillable.ToPointer("Test description"),
			DailyBackupLimit:   nillable.ToPointer(int64(5)),
			WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
			MonthlyBackupLimit: nillable.ToPointer(int64(2)),
			PolicyEnabled:      nillable.ToPointer(true),
		}

		result, err := o.CreateBackupPolicy(ctx, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, createdBackupPolicy.UUID, result.BackupPolicyUUID)
		assert.Equal(tt, *createdBackupPolicy.Description, *result.Description)
		assert.Equal(tt, createdBackupPolicy.DailyBackupsToKeep, result.DailyBackupLimit)
		assert.Equal(tt, createdBackupPolicy.WeeklyBackupsToKeep, result.WeeklyBackupLimit)
		assert.Equal(tt, createdBackupPolicy.MonthlyBackupsToKeep, result.MonthlyBackupLimit)
		assert.Equal(tt, createdBackupPolicy.PolicyEnabled, result.Enabled)
		assert.Equal(tt, models.LifeCycleStateREADY, result.State)
		mockStorage.AssertExpectations(tt)
	})
	tt.Run("CreateBackupPolicyFailsWhenScheduleCreationFailsAndRollsBack", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(mocks.Client)
		ctx := context.Background()

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		createdBackupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "backup-policy-uuid"},
			Name:      "test-backup-policy",
			AccountID: account.ID,
		}

		oldGetOrCreateAccount := getOrCreateAccount
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = oldGetOrCreateAccount }()

		mockStorage.On("GetBackupPolicyByNameAndOwnerID", ctx, "test-backup-policy", account.ID).Return(nil, utilerrors.NewNotFoundErr("backup policy", nil))
		mockStorage.On("CreateBackupPolicyEntryInVCP", ctx, mock.Anything).Return(createdBackupPolicy, nil)
		mockStorage.On("DeleteBackupPolicy", ctx, createdBackupPolicy.UUID).Return(createdBackupPolicy, nil)
		mockTemporal.On("ScheduleClient").Return(nil)

		originalCreateBackupPolicySchedule := activities.CreateBackupPolicySchedule
		defer func() { activities.CreateBackupPolicySchedule = originalCreateBackupPolicySchedule }()
		activities.CreateBackupPolicySchedule = func(ctx context.Context, temporalScheduler *scheduler.TemporalScheduler, vcpBackupPolicy *datamodel.BackupPolicy, customSchedule string) error {
			return errors.New("failed to create backup policy schedule")
		}

		o := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporal}
		params := &commonparams.CreateBackupPolicyParams{
			Name:        "test-backup-policy",
			AccountName: "owner-uuid",
			LocationID:  "test-location",
		}

		result, err := o.CreateBackupPolicy(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "failed to create backup policy schedule", err.Error())
		mockStorage.AssertExpectations(tt)
	})
	tt.Run("CreateBackupPolicyFailsWhenScheduleCreationFailsAndRollbackFails", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(mocks.Client)
		ctx := context.Background()

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		createdBackupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "backup-policy-uuid"},
			Name:      "test-backup-policy",
			AccountID: account.ID,
		}

		oldGetOrCreateAccount := getOrCreateAccount
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = oldGetOrCreateAccount }()

		mockStorage.On("GetBackupPolicyByNameAndOwnerID", ctx, "test-backup-policy", account.ID).Return(nil, utilerrors.NewNotFoundErr("backup policy", nil))
		mockStorage.On("CreateBackupPolicyEntryInVCP", ctx, mock.Anything).Return(createdBackupPolicy, nil)
		mockStorage.On("DeleteBackupPolicy", ctx, createdBackupPolicy.UUID).Return(nil, errors.New("rollback delete failed"))
		mockTemporal.On("ScheduleClient").Return(nil)

		originalCreateBackupPolicySchedule := activities.CreateBackupPolicySchedule
		defer func() { activities.CreateBackupPolicySchedule = originalCreateBackupPolicySchedule }()
		activities.CreateBackupPolicySchedule = func(ctx context.Context, temporalScheduler *scheduler.TemporalScheduler, vcpBackupPolicy *datamodel.BackupPolicy, customSchedule string) error {
			return errors.New("failed to create backup policy schedule")
		}

		o := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporal}
		params := &commonparams.CreateBackupPolicyParams{
			Name:        "test-backup-policy",
			AccountName: "owner-uuid",
			LocationID:  "test-location",
		}

		result, err := o.CreateBackupPolicy(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "failed to create backup policy schedule", err.Error())
		mockStorage.AssertExpectations(tt)
	})
	tt.Run("CreateBackupPolicyFailsWhenSchedulePauseFailsAndRollsBackScheduleAndDB", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockTemporal := new(mocks.Client)
		mockScheduleClient := new(mocks.ScheduleClient)
		mockScheduleHandle := new(mocks.ScheduleHandle)
		ctx := context.Background()

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		createdBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:     datamodel.BaseModel{ID: 1, UUID: "backup-policy-uuid"},
			Name:          "test-backup-policy",
			AccountID:     account.ID,
			PolicyEnabled: false,
		}

		oldGetOrCreateAccount := getOrCreateAccount
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getOrCreateAccount = oldGetOrCreateAccount }()

		mockStorage.On("GetBackupPolicyByNameAndOwnerID", ctx, "test-backup-policy", account.ID).Return(nil, utilerrors.NewNotFoundErr("backup policy", nil))
		mockStorage.On("CreateBackupPolicyEntryInVCP", ctx, mock.Anything).Return(createdBackupPolicy, nil)
		mockStorage.On("DeleteBackupPolicy", ctx, createdBackupPolicy.UUID).Return(createdBackupPolicy, nil)

		mockTemporal.On("ScheduleClient").Return(mockScheduleClient)
		mockScheduleClient.On("GetHandle", mock.Anything, createdBackupPolicy.UUID).Return(mockScheduleHandle)
		mockScheduleHandle.On("Pause", mock.Anything, mock.Anything).Return(errors.New("pause failed"))
		mockScheduleHandle.On("Delete", mock.Anything).Return(nil)

		originalCreateBackupPolicySchedule := activities.CreateBackupPolicySchedule
		defer func() { activities.CreateBackupPolicySchedule = originalCreateBackupPolicySchedule }()
		activities.CreateBackupPolicySchedule = func(ctx context.Context, temporalScheduler *scheduler.TemporalScheduler, vcpBackupPolicy *datamodel.BackupPolicy, customSchedule string) error {
			return nil
		}

		o := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporal}
		params := &commonparams.CreateBackupPolicyParams{
			Name:        "test-backup-policy",
			AccountName: "owner-uuid",
			LocationID:  "test-location",
		}

		result, err := o.CreateBackupPolicy(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "pause failed", err.Error())
		mockStorage.AssertExpectations(tt)
		mockTemporal.AssertExpectations(tt)
		mockScheduleClient.AssertExpectations(tt)
		mockScheduleHandle.AssertExpectations(tt)
	})
}

