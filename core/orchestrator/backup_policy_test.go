package orchestrator

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
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	utilerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
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

		o := &Orchestrator{storage: se}
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

		o := &Orchestrator{storage: se}
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
		oldGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		defer func() { getAccountWithName = oldGetAccountWithName }()

		o := &Orchestrator{storage: se}
		result, err := o.GetBackupPolicyByNameAndOwnerID(context.Background(), backupPolicyName, "non-existent-owner-uuid")
		assert.Error(tt, err, "Expected error")
		assert.Nil(tt, result, "Expected result to be nil")
		assert.Equal(tt, "account not found", err.Error(), "Expected error message to match")
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

		o := &Orchestrator{storage: se}
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

		o := &Orchestrator{storage: se}
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

		o := &Orchestrator{storage: se}
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

		o := &Orchestrator{storage: se}
		volumeCount, policyMap, err := o.ListBackupPoliciesAndVolumeCount(context.Background(), account.UUID, nil)
		assert.NoError(tt, err)
		assert.Nil(tt, volumeCount)
		assert.Equal(tt, 0, len(policyMap), "Expected no backup policies")
	})
	tt.Run("WhenAccountDoesNotExist", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")

		oldGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		defer func() { getAccountWithName = oldGetAccountWithName }()

		o := &Orchestrator{storage: se}
		volumeCount, policyMap, err := o.ListBackupPoliciesAndVolumeCount(context.Background(), "non-existent-owner-uuid", nil)
		assert.Error(tt, err)
		assert.Nil(tt, volumeCount)
		assert.Nil(tt, policyMap)
		assert.Equal(tt, "account not found", err.Error())
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
	})
}
