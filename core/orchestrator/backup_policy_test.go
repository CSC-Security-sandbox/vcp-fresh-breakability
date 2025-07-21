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
