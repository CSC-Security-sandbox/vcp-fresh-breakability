package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
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
			return nil, utilErrors.NewNotFoundErr("backup policy", &backupPolicyName)
		}
		defer func() { getBackupPolicyByNameAndOwnerID = oldGetBackupPolicyWithName }()

		o := &Orchestrator{storage: se}
		result, err := o.GetBackupPolicyByNameAndOwnerID(context.Background(), backupPolicyName, account.UUID)
		assert.Error(tt, err, "Expected error")
		assert.Nil(tt, result, "Expected result to be nil")
		assert.Equal(tt, utilErrors.NewNotFoundErr("backup policy", &backupPolicyName).Error(), err.Error(), "Expected error message to match")
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

func TestListBackupPolicyVolumeCount(tt *testing.T) {
	tt.Run("WhenBackupPoliciesExist", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		dbBackupPolicy := make(map[string]int64)
		dbBackupPolicy["backup-policy-uuid"] = 5
		oldGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() { getAccountWithName = oldGetAccountWithName }()
		oldListBackupPolicyVolumeCount := listBackupPolicyVolumeCount
		listBackupPolicyVolumeCount = func(ctx context.Context, se database.Storage, ownerID string, backupPolicyUUIDs []string) (map[string]int64, error) {
			return dbBackupPolicy, nil
		}
		defer func() { listBackupPolicyVolumeCount = oldListBackupPolicyVolumeCount }()

		o := &Orchestrator{storage: se}
		result, err := o.ListBackupPolicyVolumeCount(context.Background(), account.UUID, nil)
		assert.NoError(tt, err, "Expected no error")
		assert.Len(tt, result, 1, "Expected one backup policy")
		assert.Equal(tt, int64(5), result["backup-policy-uuid"], "Expected backup policy volume count to match")
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
		listBackupPolicyVolumeCount = func(ctx context.Context, se database.Storage, ownerID string, backupPolicyUUIDs []string) (map[string]int64, error) {
			return nil, nil
		}
		defer func() { listBackupPolicyVolumeCount = oldListBackupPolicyVolumeCount }()

		o := &Orchestrator{storage: se}
		result, err := o.ListBackupPolicyVolumeCount(context.Background(), account.UUID, nil)
		assert.NoError(tt, err, "Expected no error")
		assert.Len(tt, result, 0, "Expected no backup policies")
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
		result, err := o.ListBackupPolicyVolumeCount(context.Background(), "non-existent-owner-uuid", nil)
		assert.Error(tt, err, "Expected error")
		assert.Nil(tt, result, "Expected result to be nil")
		assert.Equal(tt, "account not found", err.Error(), "Expected error message to match")
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
