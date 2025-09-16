package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflow_engine_mock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"gorm.io/gorm"
)

func TestCheck(t *testing.T) {
	t.Run("WhenBackupVaultHasAllFieldsPopulated", func(tt *testing.T) {
		desc := "A test backup vault"
		SourceRegionName := "us-east1"
		BackupRegionName := "us-central1"
		crbName := "cross-region-vault"
		minEnforcedRetentionDuration := int64(30)
		bv := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{
				ID:        1,
				UUID:      "backup-vault-uuid",
				CreatedAt: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				UpdatedAt: time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC),
			},
			Account:               &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "owner-uuid"}},
			Name:                  "backup-vault-name",
			Description:           &desc,
			LifeCycleState:        "ACTIVE",
			LifeCycleStateDetails: "Available for use",
			BackupRegionName:      &BackupRegionName,
			SourceRegionName:      &SourceRegionName,
			RegionName:            "us-central1",
			AccountVendorID:       "vendor-id",
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &minEnforcedRetentionDuration,
				IsDailyBackupImmutable:                 true,
				IsMonthlyBackupImmutable:               false,
				IsWeeklyBackupImmutable:                true,
			},
			CrossRegionBackupVaultName: &crbName,
			BackupVaultType:            "STANDARD",
		}

		result := _convertDatastoreBackupVaultToModel(bv)

		if result.ID != bv.ID {
			tt.Errorf("Expected ID %v, got %v", bv.ID, result.ID)
		}
		if result.OwnerID != bv.Account.UUID {
			tt.Errorf("Expected OwnerID %v, got %v", bv.Account.UUID, result.OwnerID)
		}
		if result.BackupVaultID != bv.UUID {
			tt.Errorf("Expected BackupVaultID %v, got %v", bv.UUID, result.BackupVaultID)
		}
		if result.Name != bv.Name {
			tt.Errorf("Expected Name %v, got %v", bv.Name, result.Name)
		}
		if result.Description != bv.Description {
			tt.Errorf("Expected Description %v, got %v", bv.Description, result.Description)
		}
		if result.LifeCycleState != bv.LifeCycleState {
			tt.Errorf("Expected LifeCycleState %v, got %v", bv.LifeCycleState, result.LifeCycleState)
		}
		if result.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDuration != bv.ImmutableAttributes.BackupMinimumEnforcedRetentionDuration {
			tt.Errorf("Expected BackupMinimumEnforcedRetentionDuration %v, got %v", bv.ImmutableAttributes.BackupMinimumEnforcedRetentionDuration, result.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDuration)
		}
		if result.BackupVaultType == nil || *result.BackupVaultType != bv.BackupVaultType {
			tt.Errorf("Expected BackupVaultType %v, got %v", bv.BackupVaultType, result.BackupVaultType)
		}
	})

	t.Run("WhenBackupVaultHasMissingOptionalFields", func(tt *testing.T) {
		minEnforcedRetentionDuration := int64(30)
		bv := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "backup-vault-uuid",
			},
			Account: &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "owner-uuid"}},

			Name:       "backup-vault-name",
			RegionName: "us-central1",
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &minEnforcedRetentionDuration,
				IsDailyBackupImmutable:                 false,
				IsMonthlyBackupImmutable:               false,
				IsWeeklyBackupImmutable:                false,
			},
		}

		result := _convertDatastoreBackupVaultToModel(bv)

		if result.Description != nil {
			tt.Errorf("Expected empty Description, got %v", result.Description)
		}
		if result.LifeCycleState != "" {
			tt.Errorf("Expected empty LifeCycleState, got %v", result.LifeCycleState)
		}
		if result.BackupRetentionPolicy.IsDailyBackupImmutable != bv.ImmutableAttributes.IsDailyBackupImmutable {
			tt.Errorf("Expected IsDailyBackupImmutable %v, got %v", bv.ImmutableAttributes.IsDailyBackupImmutable, result.BackupRetentionPolicy.IsDailyBackupImmutable)
		}
	})
}

func TestGetBackupVaultByNameAndOwnerIDReturnsBackupVault(tt *testing.T) {
	tt.Run("WhenBackupVaultExists", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		minEnforcedRetentionDuration := int64(30)
		bv := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "backup-vault-uuid"},
			Name:      "backup-vault-name",
			Account:   account,
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &minEnforcedRetentionDuration,
			},
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		getBackupVaultByNameAndOwnerID = func(ctx context.Context, se database.Storage, bvName, ownerID string) (*datamodel.BackupVault, error) {
			return bv, nil
		}

		o := &Orchestrator{storage: se}
		result, err := o.GetBackupVaultByNameAndOwnerID(context.Background(), bv.Name, account.UUID)
		assert.NoError(tt, err, "Expected no error")
		assert.Equal(tt, bv.UUID, result.BackupVaultID, "Expected BackupVaultID to match")
		assert.Equal(tt, bv.Name, result.Name, "Expected Name to match")
	})

	tt.Run("WhenBackupVaultHasNilImmutableAttributes", func(tt *testing.T) {
		bv := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{
				ID:   3,
				UUID: "backup-vault-uuid-3",
			},
			Account: &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "owner-uuid"}},
			Name:    "backup-vault-name",
			// ImmutableAttributes is nil
		}

		result := _convertDatastoreBackupVaultToModel(bv)

		// Test that BackupRetentionPolicy is not set when ImmutableAttributes is nil
		if result.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDuration != nil {
			tt.Errorf("Expected BackupMinimumEnforcedRetentionDuration to be nil when ImmutableAttributes is nil, got %v", result.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDuration)
		}
		if result.BackupRetentionPolicy.IsDailyBackupImmutable != false {
			tt.Errorf("Expected IsDailyBackupImmutable to be false when ImmutableAttributes is nil, got %v", result.BackupRetentionPolicy.IsDailyBackupImmutable)
		}
	})
	tt.Run("WhenBackupVaultDoesNotExist", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		getBackupVaultByNameAndOwnerID = func(ctx context.Context, se database.Storage, bvName, ownerID string) (*datamodel.BackupVault, error) {
			return nil, errors.New("backup vault not found")
		}

		o := &Orchestrator{storage: se}
		result, err := o.GetBackupVaultByNameAndOwnerID(context.Background(), "non-existent-backup-vault", account.UUID)
		assert.Error(tt, err, "Expected error")
		assert.Nil(tt, result, "Expected result to be nil")
		assert.Equal(tt, "backup vault not found", err.Error(), "Expected error message to match")
	})
	tt.Run("WhenAccountDoesNotExist", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		getBackupVaultByNameAndOwnerID = func(ctx context.Context, se database.Storage, bvName, ownerID string) (*datamodel.BackupVault, error) {
			return nil, errors.New("account not found")
		}

		o := &Orchestrator{storage: se}
		result, err := o.GetBackupVaultByNameAndOwnerID(context.Background(), "backup-vault-name", "non-existent-owner-uuid")
		assert.Error(tt, err, "Expected error")
		assert.Nil(tt, result, "Expected result to be nil")
		assert.Equal(tt, "account not found", err.Error(), "Expected error message to match")
	})
	tt.Run("WhenBackupVaultNameIsEmpty", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		getBackupVaultByNameAndOwnerID = func(ctx context.Context, se database.Storage, bvName, ownerID string) (*datamodel.BackupVault, error) {
			return nil, errors.New("backup vault not found")
		}

		o := &Orchestrator{storage: se}
		result, err := o.GetBackupVaultByNameAndOwnerID(context.Background(), "non-existent-backup-vault", account.UUID)
		assert.Error(tt, err, "Expected error")
		assert.Nil(tt, result, "Expected result to be nil")
		assert.Equal(tt, "backup vault not found", err.Error(), "Expected error message to match")
	})
}

func TestReturnsErrorWhenAccountNotFound(tt *testing.T) {
	mockLogger := log.NewLogger()
	se, err := database.NewTestStorage(mockLogger)
	assert.NoError(tt, err, "Failed to create test storage")
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return nil, errors.New("account not found")
	}

	result, err := _getBackupVaultByNameAndOwnerID(context.Background(), se, "backup-vault-name", "non-existent-owner-uuid")
	assert.Error(tt, err, "Expected error")
	assert.Nil(tt, result, "Expected result to be nil")
	assert.Equal(tt, "account not found", err.Error(), "Expected error message to match")
}

func TestListBackupVaults(t *testing.T) {
	t.Run("WhenNoBackupVaultsExist", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		o := &Orchestrator{storage: se}
		result, err := o.ListBackupVaults(context.Background(), account.UUID)
		assert.NoError(tt, err)
		assert.Nil(tt, result)
		assert.Empty(tt, result)
	})
	t.Run("WhenAccountNotFoundAndCreated", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")

		newAccount := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "new-account-uuid"}}
		accountCreated := false

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			// Simulate account creation when account doesn't exist
			accountCreated = true
			return newAccount, nil
		}

		o := &Orchestrator{storage: se}
		result, err := o.ListBackupVaults(context.Background(), "non-existent-account")

		assert.NoError(tt, err, "Expected no error when account is created")
		assert.True(tt, accountCreated, "Expected account to be created")
		assert.Nil(tt, result, "Expected result to be nil")
		assert.Empty(tt, result, "Expected empty backup vaults list for new account")
	})
}

func TestListBackupVaultsByOwnerID(t *testing.T) {
	t.Run("WhenListBackupVaultsErrors", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)

		mockStorage.On("ListBackupVaults", ctx, int64(1)).Return(nil, errors.New("failed to list backup vaults"))
		bvs, err := ListBackupVaultsByOwnerID(ctx, mockStorage, 1)
		assert.Error(t, err, "Expected error when listing backup vaults")
		assert.Nil(t, bvs, "Expected backup vaults to be nil")
	})
	t.Run("WhenListBackupVaultsSuccess", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)

		desc := "desc"
		bRegionName := "us-central1"
		sRegionName := "us-west1"
		minEnforcedDuration := int64(10)
		bVaults := []*datamodel.BackupVault{
			{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "uuid1", CreatedAt: time.Now(), UpdatedAt: time.Now()},
				Name:      "vault-1",
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{ID: 1},
				},
				AccountID:             1,
				RegionName:            "us-east1",
				BackupRegionName:      &bRegionName,
				SourceRegionName:      &sRegionName,
				LifeCycleState:        "Available",
				LifeCycleStateDetails: "Available for use",
				BackupVaultType:       "IN_REGION",
				AccountVendorID:       "vendor1",
				Description:           &desc,
				ImmutableAttributes: &datamodel.ImmutableAttributes{
					BackupMinimumEnforcedRetentionDuration: &minEnforcedDuration,
					IsDailyBackupImmutable:                 false,
					IsWeeklyBackupImmutable:                false,
					IsMonthlyBackupImmutable:               false,
					IsAdhocBackupImmutable:                 false,
				},
			},
		}
		mockStorage.On("ListBackupVaults", ctx, int64(1)).Return(bVaults, nil)
		bvs, err := ListBackupVaultsByOwnerID(ctx, mockStorage, 1)
		assert.NoError(t, err)
		assert.NotNil(t, bvs)
	})
}

func TestGetBackupVaultByUUID(tt *testing.T) {
	mockLogger := log.NewLogger()
	_, err := database.NewTestStorage(mockLogger)
	assert.NoError(tt, err, "Failed to create test storage")
	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}

	mockStorage := new(database.MockStorage)
	mockStorage.On("GetBackupVaultByUUIDndOwnerID", context.Background(), "backup-vault-uuid", int64(account.ID)).Return(nil, gorm.ErrRecordNotFound)

	res, err := GetBackupVaultByUUIDAndOwnerID(context.Background(), mockStorage, "backup-vault-uuid", account.ID)

	assert.Error(tt, err, "Expected error when backup vault not found")
	assert.Nil(tt, res, "Expected result to be nil")
}

func TestGetBackupVaultByUUIDAndOwnerIDSuccess(tt *testing.T) {
	mockLogger := log.NewLogger()
	_, err := database.NewTestStorage(mockLogger)
	assert.NoError(tt, err, "Failed to create test storage")
	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}

	desc := "desc"
	bRegionName := "us-central1"
	sRegionName := "us-west1"
	minEnforcedDuration := int64(10)
	bv := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "uuid1", CreatedAt: time.Now(), UpdatedAt: time.Now()},
		Name:      "vault-1",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
		},
		AccountID:             1,
		RegionName:            "us-east1",
		BackupRegionName:      &bRegionName,
		SourceRegionName:      &sRegionName,
		LifeCycleState:        "Available",
		LifeCycleStateDetails: "Available for use",
		BackupVaultType:       "IN_REGION",
		AccountVendorID:       "vendor1",
		Description:           &desc,
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			BackupMinimumEnforcedRetentionDuration: &minEnforcedDuration,
			IsDailyBackupImmutable:                 false,
			IsWeeklyBackupImmutable:                false,
			IsMonthlyBackupImmutable:               false,
			IsAdhocBackupImmutable:                 false,
		},
	}
	mockStorage := new(database.MockStorage)
	mockStorage.On("GetBackupVaultByUUIDndOwnerID", context.Background(), "backup-vault-uuid", int64(account.ID)).Return(bv, nil)

	res, err := GetBackupVaultByUUIDAndOwnerID(context.Background(), mockStorage, "backup-vault-uuid", account.ID)

	assert.NoError(tt, err, "Expected error when backup vault not found")
	assert.NotNil(tt, res, "Expected result to be nil")
}

func TestGetBackupVaultByUUIDError(tt *testing.T) {
	mockLogger := log.NewLogger()
	_, err := database.NewTestStorage(mockLogger)
	assert.NoError(tt, err, "Failed to create test storage")
	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}

	mockStorage := new(database.MockStorage)
	mockStorage.On("GetBackupVaultByUUIDndOwnerID", context.Background(), "backup-vault-uuid", int64(account.ID)).Return(nil, gorm.ErrCheckConstraintViolated)

	res, err := GetBackupVaultByUUIDAndOwnerID(context.Background(), mockStorage, "backup-vault-uuid", account.ID)

	assert.Error(tt, err, "Expected error when backup vault not found")
	assert.Nil(tt, res, "Expected result to be nil")
}

func TestGetBackupVaultByUUIDGetOrCreateError(tt *testing.T) {
	mockLogger := log.NewLogger()
	se, err := database.NewTestStorage(mockLogger)
	assert.NoError(tt, err, "Failed to create test storage")
	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}

	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return nil, errors.New("account not found")
	}

	o := &Orchestrator{storage: se}
	res, err := o.GetBackupVaultByUUID(context.Background(), "backup-vault-uuid", account.UUID)

	assert.Error(tt, err, "Expected error when backup vault not found")
	assert.Nil(tt, res, "Expected result to be nil")
}

func TestUpdateBackupVault(t *testing.T) {
	t.Run("WhenAccountNotFound", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(t, err, "Failed to create test storage")
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
		mrd := int64(30)
		daily := true
		monthly := true
		weekly := false
		manual := false
		params := &commonparams.BackupVaultParams{
			OwnerID: "owner-uuid",
			Name:    "backup-vault-name",
			Region:  "us-central1",
			BackupRetentionPolicy: commonparams.BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: &mrd,
				IsDailyBackupImmutable:                 &daily,
				IsWeeklyBackupImmutable:                &weekly,
				IsMonthlyBackupImmutable:               &monthly,
				IsAdhocBackupImmutable:                 &manual,
			},
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}

		bv, _, err := updateBackupVault(ctx, se, temporal, params)
		assert.Error(t, err, "Expected error when account not found")
		assert.Nil(t, bv, "Expected backup vault to be nil")
	})
	t.Run("WhenCreateJobFails", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(t, err, "Failed to create test storage")
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
		mrd := int64(30)
		daily := true
		monthly := true
		weekly := false
		manual := false
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		params := &commonparams.BackupVaultParams{
			OwnerID:       "owner-uuid",
			Name:          "backup-vault-name",
			Region:        "us-central1",
			BackupVaultID: "backup-vault-uuid",
			BackupRetentionPolicy: commonparams.BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: &mrd,
				IsDailyBackupImmutable:                 &daily,
				IsWeeklyBackupImmutable:                &weekly,
				IsMonthlyBackupImmutable:               &monthly,
				IsAdhocBackupImmutable:                 &manual,
			},
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		mockStorage := new(database.MockStorage)

		mockStorage.On("CreateJob", ctx, mock.Anything).Return(nil, errors.New("failed to create job"))

		bv, _, err := updateBackupVault(ctx, se, temporal, params)
		assert.Error(t, err, "Expected error when validation fails")
		assert.Nil(t, bv, "Expected backup vault to be nil")
	})
	t.Run("WhenGetBackupVaultByUUIDndOwnerIDFails", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(t, err, "Failed to create test storage")
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
		mrd := int64(30)
		daily := true
		monthly := true
		weekly := false
		manual := false
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		params := &commonparams.BackupVaultParams{
			OwnerID:       "owner-uuid",
			Name:          "backup-vault-name",
			Region:        "us-central1",
			BackupVaultID: "backup-vault-uuid",
			BackupRetentionPolicy: commonparams.BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: &mrd,
				IsDailyBackupImmutable:                 &daily,
				IsWeeklyBackupImmutable:                &weekly,
				IsMonthlyBackupImmutable:               &monthly,
				IsAdhocBackupImmutable:                 &manual,
			},
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		mockStorage := new(database.MockStorage)

		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
		}
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, params.BackupVaultID, int64(account.ID)).Return(nil, gorm.ErrRecordNotFound)

		bv, _, err := updateBackupVault(ctx, se, temporal, params)
		assert.Error(t, err, "Expected error when validation fails")
		assert.Nil(t, bv, "Expected backup vault to be nil")
	})
	t.Run("WhenUpdatingBackupVaultStateFails", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(t, err, "Failed to create test storage")
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		temporal := workflow_engine_mock.NewMockTemporalTestClient(t)
		mrd := int64(30)
		daily := true
		monthly := true
		weekly := false
		manual := false
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		params := &commonparams.BackupVaultParams{
			OwnerID:       "owner-uuid",
			Name:          "backup-vault-name",
			Region:        "us-central1",
			BackupVaultID: "backup-vault-uuid",
			BackupRetentionPolicy: commonparams.BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: &mrd,
				IsDailyBackupImmutable:                 &daily,
				IsWeeklyBackupImmutable:                &weekly,
				IsMonthlyBackupImmutable:               &monthly,
				IsAdhocBackupImmutable:                 &manual,
			},
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		mockStorage := new(database.MockStorage)

		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
		}
		bvResp := &datamodel.BackupVault{}
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, params.BackupVaultID, int64(account.ID)).Return(bvResp, nil)
		mockStorage.On("UpdatingBackupVaultState", ctx, mock.Anything).Return(nil, errors.New("failed to update backup vault state"))

		bv, _, err := updateBackupVault(ctx, se, temporal, params)
		assert.Error(t, err, "Expected error when validation fails")
		assert.Nil(t, bv, "Expected backup vault to be nil")
	})
}

func TestGetMultipleBackupVaultsReturnsVaultsForValidUUIDs(tt *testing.T) {
	mockLogger := log.NewLogger()
	mockStorage := new(database.MockStorage)
	ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)

	backupVaultUUIDList := []string{"uuid1", "uuid2"}
	backupVaults := []*datamodel.BackupVault{
		{
			BaseModel: datamodel.BaseModel{UUID: "uuid1"},
			Name:      "vault1",
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "owner-uuid"},
			},
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: nil,
				IsDailyBackupImmutable:                 false,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
				IsAdhocBackupImmutable:                 false,
			},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "uuid2"},
			Name:      "vault2",
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "owner-uuid"},
			},
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: nil,
				IsDailyBackupImmutable:                 false,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
				IsAdhocBackupImmutable:                 false,
			},
		},
	}
	mockStorage.On("GetMultipleBackupVaults", ctx, [][]interface{}{{"uuid in ?", backupVaultUUIDList}}).Return(backupVaults, nil)

	o := &Orchestrator{storage: mockStorage}
	result, err := o.GetMultipleBackupVaults(ctx, backupVaultUUIDList)

	assert.NoError(tt, err)
	assert.Len(tt, result, 2)
	assert.Equal(tt, "uuid1", result[0].BackupVaultID)
	assert.Equal(tt, "uuid2", result[1].BackupVaultID)
}

func TestGetMultipleBackupVaultsReturnsEmptyListForNoMatchingUUIDs(tt *testing.T) {
	mockLogger := log.NewLogger()
	mockStorage := new(database.MockStorage)
	ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)

	backupVaultUUIDList := []string{"non-existent-uuid"}
	mockStorage.On("GetMultipleBackupVaults", ctx, [][]interface{}{{"uuid in ?", backupVaultUUIDList}}).Return([]*datamodel.BackupVault{}, nil)

	o := &Orchestrator{storage: mockStorage}
	result, err := o.GetMultipleBackupVaults(ctx, backupVaultUUIDList)

	assert.NoError(tt, err)
	assert.Empty(tt, result)
}

func TestGetMultipleBackupVaultsReturnsErrorWhenStorageFails(tt *testing.T) {
	mockLogger := log.NewLogger()
	mockStorage := new(database.MockStorage)
	ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)

	backupVaultUUIDList := []string{"uuid1", "uuid2"}
	mockStorage.On("GetMultipleBackupVaults", ctx, [][]interface{}{{"uuid in ?", backupVaultUUIDList}}).Return(nil, errors.New("database error"))

	o := &Orchestrator{storage: mockStorage}
	result, err := o.GetMultipleBackupVaults(ctx, backupVaultUUIDList)

	assert.Error(tt, err)
	assert.Nil(tt, result)
	assert.Equal(tt, "database error", err.Error())
}

func TestReturnsErrorWhenBackupVaultHasBackups(t *testing.T) {
	mockStorage := new(database.MockStorage)
	mockTemporal := new(workflow_engine_mock.MockTemporalTestClient)
	ctx := context.Background()

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
	backupVault := &datamodel.BackupVault{
		BaseModel:      datamodel.BaseModel{UUID: "backup-vault-uuid", ID: 1},
		LifeCycleState: models.LifeCycleStateAvailable,
	}
	job := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "job-uuid"}}

	mockStorage.On("GetOrCreateAccount", ctx, "owner-uuid").Return(account, nil)
	mockStorage.On("GetAccount", ctx, "owner-uuid").Return(account, nil)
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "backup-vault-uuid", int64(1)).Return(backupVault, nil)
	mockStorage.On("GetBackupCountByBackupVaultID", ctx, backupVault.ID).Return(int64(1), nil)

	o := &Orchestrator{storage: mockStorage, temporal: mockTemporal}
	result, jobID, err := o.DeleteBackupVault(ctx, &commonparams.BackupVaultParams{
		OwnerID:       "owner-uuid",
		BackupVaultID: "backup-vault-uuid",
	})

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Empty(t, jobID)
	assert.Equal(t, "backup vault has backups, please delete backups before deleting backup vault", err.Error())
}

func TestReturnsGetBackupVaultByUUIDndOwnerIDErrorWhenBackupVaultHasBackups(t *testing.T) {
	mockStorage := new(database.MockStorage)
	mockTemporal := new(workflow_engine_mock.MockTemporalTestClient)
	ctx := context.Background()

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
	job := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "job-uuid"}}

	mockStorage.On("GetOrCreateAccount", ctx, "owner-uuid").Return(account, nil)
	mockStorage.On("GetAccount", ctx, "owner-uuid").Return(account, nil)
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "backup-vault-uuid", int64(1)).Return(nil, errors.New("backup vault not found"))

	o := &Orchestrator{storage: mockStorage, temporal: mockTemporal}
	result, jobID, err := o.DeleteBackupVault(ctx, &commonparams.BackupVaultParams{
		OwnerID:       "owner-uuid",
		BackupVaultID: "backup-vault-uuid",
	})

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Empty(t, jobID)
	assert.Equal(t, "backup vault not found", err.Error())
}

func TestReturnsJobErrorWhenBackupVaultHasBackups(t *testing.T) {
	mockStorage := new(database.MockStorage)
	mockTemporal := new(workflow_engine_mock.MockTemporalTestClient)
	ctx := context.Background()

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
	backupVault := &datamodel.BackupVault{
		BaseModel:             datamodel.BaseModel{UUID: "backup-vault-uuid", ID: 1},
		LifeCycleState:        models.LifeCycleStateAvailable,
		LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
	}
	mockStorage.On("GetOrCreateAccount", ctx, "owner-uuid").Return(account, nil)
	mockStorage.On("GetAccount", ctx, "owner-uuid").Return(account, nil)
	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "backup-vault-uuid", int64(1)).Return(backupVault, nil)
	mockStorage.On("GetBackupCountByBackupVaultID", ctx, backupVault.ID).Return(int64(1), nil)
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(nil, errors.New("failed to create job"))
	o := &Orchestrator{storage: mockStorage, temporal: mockTemporal}
	result, jobID, err := o.DeleteBackupVault(ctx, &commonparams.BackupVaultParams{
		OwnerID:       "owner-uuid",
		BackupVaultID: "backup-vault-uuid",
	})

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Empty(t, jobID)
}

func TestReturnsUpdatingErrorWhenBackupVaultHasBackups(t *testing.T) {
	mockStorage := new(database.MockStorage)
	mockTemporal := new(workflow_engine_mock.MockTemporalTestClient)
	ctx := context.Background()

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
	backupVault := &datamodel.BackupVault{
		BaseModel:      datamodel.BaseModel{UUID: "backup-vault-uuid"},
		LifeCycleState: models.LifeCycleStateUpdating,
	}
	var backups []*datamodel.Backup
	job := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "job-uuid"}}

	mockStorage.On("GetOrCreateAccount", ctx, "owner-uuid").Return(account, nil)
	mockStorage.On("GetAccount", ctx, "owner-uuid").Return(account, nil)
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "backup-vault-uuid", int64(1)).Return(backupVault, nil)
	mockStorage.On("GetBackupsByBackupVaultOwnerIDAndFilter", ctx, "backup-vault-uuid", int64(1), [][]interface{}(nil)).Return(backups, nil)

	o := &Orchestrator{storage: mockStorage, temporal: mockTemporal}
	result, jobID, err := o.DeleteBackupVault(ctx, &commonparams.BackupVaultParams{
		OwnerID:       "owner-uuid",
		BackupVaultID: "backup-vault-uuid",
	})

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Empty(t, jobID)
	assert.Equal(t, "backup vault is in transition state", err.Error())
}

func TestDeleteBackupVaultRollbackScenarios(t *testing.T) {
	t.Run("WhenWorkflowStartFails_ShouldRollbackBackupVaultState", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine_mock.MockTemporalTestClient)

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupVault := &datamodel.BackupVault{
			BaseModel:             datamodel.BaseModel{UUID: "backup-vault-uuid", ID: 1},
			LifeCycleState:        models.LifeCycleStateAvailable,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
		}

		// Setup mocks
		mockStorage.On("GetOrCreateAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "backup-vault-uuid", int64(1)).Return(backupVault, nil)
		mockStorage.On("GetBackupCountByBackupVaultID", ctx, int64(1)).Return(int64(0), nil)
		mockStorage.On("GetVolumeCountByBackupVaultID", ctx, "backup-vault-uuid").Return(int64(0), nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		mockStorage.On("UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateDeleting, models.LifeCycleStateDeletingDetails).Return(backupVault, nil)
		mockStorage.On("UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, models.LifeCycleStateAvailableDetails).Return(backupVault, nil)
		mockStorage.On("UpdateJob", ctx, "job-uuid", models.LifeCycleStateError, 0, mock.Anything).Return(nil)

		// Mock workflow execution to fail
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow start failed"))

		o := &Orchestrator{storage: mockStorage, temporal: mockTemporal}
		result, jobID, err := o.DeleteBackupVault(ctx, &commonparams.BackupVaultParams{
			OwnerID:       "owner-uuid",
			BackupVaultID: "backup-vault-uuid",
			Name:          "test-vault",
		})

		// Verify results
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Empty(t, jobID)
		assert.Equal(t, "workflow start failed", err.Error())

		// Verify rollback was called
		mockStorage.AssertCalled(t, "UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, models.LifeCycleStateAvailableDetails)
		mockStorage.AssertCalled(t, "UpdateJob", ctx, "job-uuid", models.LifeCycleStateError, 0, "workflow start failed")
	})

	t.Run("WhenWorkflowStartFails_AndRollbackFails_ShouldLogError", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine_mock.MockTemporalTestClient)

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupVault := &datamodel.BackupVault{
			BaseModel:             datamodel.BaseModel{UUID: "backup-vault-uuid", ID: 1},
			LifeCycleState:        models.LifeCycleStateAvailable,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
		}

		// Setup mocks
		mockStorage.On("GetOrCreateAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "backup-vault-uuid", int64(1)).Return(backupVault, nil)
		mockStorage.On("GetBackupCountByBackupVaultID", ctx, int64(1)).Return(int64(0), nil)
		mockStorage.On("GetVolumeCountByBackupVaultID", ctx, "backup-vault-uuid").Return(int64(0), nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		mockStorage.On("UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateDeleting, models.LifeCycleStateDeletingDetails).Return(backupVault, nil)
		// Mock rollback to fail
		mockStorage.On("UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, models.LifeCycleStateAvailableDetails).Return(nil, errors.New("rollback failed"))
		mockStorage.On("UpdateJob", ctx, "job-uuid", models.LifeCycleStateError, 0, mock.Anything).Return(nil)

		// Mock workflow execution to fail
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow start failed"))

		o := &Orchestrator{storage: mockStorage, temporal: mockTemporal}
		result, jobID, err := o.DeleteBackupVault(ctx, &commonparams.BackupVaultParams{
			OwnerID:       "owner-uuid",
			BackupVaultID: "backup-vault-uuid",
			Name:          "test-vault",
		})

		// Verify results
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Empty(t, jobID)
		assert.Equal(t, "workflow start failed", err.Error())

		// Verify rollback was attempted even though it failed
		mockStorage.AssertCalled(t, "UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, models.LifeCycleStateAvailableDetails)
		mockStorage.AssertCalled(t, "UpdateJob", ctx, "job-uuid", models.LifeCycleStateError, 0, "workflow start failed")
	})

	t.Run("WhenWorkflowStartFails_AndJobUpdateFails_ShouldLogError", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine_mock.MockTemporalTestClient)

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupVault := &datamodel.BackupVault{
			BaseModel:             datamodel.BaseModel{UUID: "backup-vault-uuid", ID: 1},
			LifeCycleState:        models.LifeCycleStateAvailable,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
		}

		// Setup mocks
		mockStorage.On("GetOrCreateAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "backup-vault-uuid", int64(1)).Return(backupVault, nil)
		mockStorage.On("GetBackupCountByBackupVaultID", ctx, int64(1)).Return(int64(0), nil)
		mockStorage.On("GetVolumeCountByBackupVaultID", ctx, "backup-vault-uuid").Return(int64(0), nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		mockStorage.On("UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateDeleting, models.LifeCycleStateDeletingDetails).Return(backupVault, nil)
		mockStorage.On("UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, models.LifeCycleStateAvailableDetails).Return(backupVault, nil)
		// Mock job update to fail
		mockStorage.On("UpdateJob", ctx, "job-uuid", models.LifeCycleStateError, 0, mock.Anything).Return(errors.New("job update failed"))

		// Mock workflow execution to fail
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow start failed"))

		o := &Orchestrator{storage: mockStorage, temporal: mockTemporal}
		result, jobID, err := o.DeleteBackupVault(ctx, &commonparams.BackupVaultParams{
			OwnerID:       "owner-uuid",
			BackupVaultID: "backup-vault-uuid",
			Name:          "test-vault",
		})

		// Verify results
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Empty(t, jobID)
		assert.Equal(t, "workflow start failed", err.Error())

		// Verify both rollback operations were attempted
		mockStorage.AssertCalled(t, "UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, models.LifeCycleStateAvailableDetails)
		mockStorage.AssertCalled(t, "UpdateJob", ctx, "job-uuid", models.LifeCycleStateError, 0, "workflow start failed")
	})

	t.Run("WhenWorkflowStartSucceeds_ShouldNotRollback", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine_mock.MockTemporalTestClient)

		enforcedDuration := int64(30)
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupVault := &datamodel.BackupVault{
			BaseModel:             datamodel.BaseModel{UUID: "backup-vault-uuid", ID: 1},
			LifeCycleState:        models.LifeCycleStateAvailable,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
			Account:               account,
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &enforcedDuration,
				IsDailyBackupImmutable:                 false,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
				IsAdhocBackupImmutable:                 false,
			},
		}
		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
		}

		// Setup mocks
		mockStorage.On("GetOrCreateAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "backup-vault-uuid", int64(1)).Return(backupVault, nil)
		mockStorage.On("GetBackupCountByBackupVaultID", ctx, int64(1)).Return(int64(0), nil)
		mockStorage.On("GetVolumeCountByBackupVaultID", ctx, "backup-vault-uuid").Return(int64(0), nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		mockStorage.On("UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateDeleting, models.LifeCycleStateDeletingDetails).Return(backupVault, nil)

		// Mock workflow execution to succeed
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		o := &Orchestrator{storage: mockStorage, temporal: mockTemporal}
		result, jobID, err := o.DeleteBackupVault(ctx, &commonparams.BackupVaultParams{
			OwnerID:       "owner-uuid",
			BackupVaultID: "backup-vault-uuid",
			Name:          "test-vault",
		})

		// Verify results
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "job-uuid", jobID)

		// Verify rollback was NOT called
		mockStorage.AssertNotCalled(t, "UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, models.LifeCycleStateAvailableDetails)
		mockStorage.AssertNotCalled(t, "UpdateJob", ctx, "job-uuid", models.LifeCycleStateError, 0, mock.Anything)
	})
	t.Run("WhenCreateJobFails_ShouldNotRollback", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine_mock.MockTemporalTestClient)

		enforcedDuration := int64(30)
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupVault := &datamodel.BackupVault{
			BaseModel:             datamodel.BaseModel{UUID: "backup-vault-uuid", ID: 1},
			LifeCycleState:        models.LifeCycleStateAvailable,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
			Account:               account,
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &enforcedDuration,
				IsDailyBackupImmutable:                 false,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
				IsAdhocBackupImmutable:                 false,
			},
		}

		// Setup mocks
		mockStorage.On("GetOrCreateAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "backup-vault-uuid", int64(1)).Return(backupVault, nil)
		mockStorage.On("GetBackupCountByBackupVaultID", ctx, int64(1)).Return(int64(0), nil)
		mockStorage.On("GetVolumeCountByBackupVaultID", ctx, "backup-vault-uuid").Return(int64(0), nil)
		// Mock CreateJob to fail
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(nil, errors.New("create job failed"))

		o := &Orchestrator{storage: mockStorage, temporal: mockTemporal}
		result, jobID, err := o.DeleteBackupVault(ctx, &commonparams.BackupVaultParams{
			OwnerID:       "owner-uuid",
			BackupVaultID: "backup-vault-uuid",
			Name:          "test-vault",
		})

		// Verify results
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Empty(t, jobID)
		assert.Equal(t, "create job failed", err.Error())

		// Verify rollback was NOT called since no job was created
		mockStorage.AssertNotCalled(t, "UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, models.LifeCycleStateAvailableDetails)
		mockStorage.AssertNotCalled(t, "UpdateJob", ctx, mock.Anything, models.LifeCycleStateError, 0, mock.Anything)
	})

	t.Run("WhenBackupVaultHasDifferentOriginalState_ShouldRollbackToCorrectState", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine_mock.MockTemporalTestClient)

		enforcedDuration := int64(30)
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupVault := &datamodel.BackupVault{
			BaseModel:             datamodel.BaseModel{UUID: "backup-vault-uuid", ID: 1},
			LifeCycleState:        models.LifeCycleStateAvailable,
			LifeCycleStateDetails: "Updating backup vault",
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &enforcedDuration,
				IsDailyBackupImmutable:                 false,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
				IsAdhocBackupImmutable:                 false,
			},
			Account: account,
		}
		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
		}

		// Setup mocks
		mockStorage.On("GetOrCreateAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "backup-vault-uuid", int64(1)).Return(backupVault, nil)
		mockStorage.On("GetBackupCountByBackupVaultID", ctx, int64(1)).Return(int64(0), nil)
		mockStorage.On("GetVolumeCountByBackupVaultID", ctx, "backup-vault-uuid").Return(int64(0), nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		mockStorage.On("UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateDeleting, models.LifeCycleStateDeletingDetails).Return(backupVault, nil)
		// Mock rollback to original state
		mockStorage.On("UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, "Updating backup vault").Return(backupVault, nil)
		mockStorage.On("UpdateJob", ctx, "job-uuid", models.LifeCycleStateError, 0, mock.Anything).Return(nil)

		// Mock workflow execution to fail
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow start failed"))

		o := &Orchestrator{storage: mockStorage, temporal: mockTemporal}
		result, jobID, err := o.DeleteBackupVault(ctx, &commonparams.BackupVaultParams{
			OwnerID:       "owner-uuid",
			BackupVaultID: "backup-vault-uuid",
			Name:          "test-vault",
		})

		// Verify results
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Empty(t, jobID)
		assert.Equal(t, "workflow start failed", err.Error())

		// Verify rollback was called with the original state
		mockStorage.AssertCalled(t, "UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, "Updating backup vault")
		mockStorage.AssertCalled(t, "UpdateJob", ctx, "job-uuid", models.LifeCycleStateError, 0, "workflow start failed")
	})
}

func TestUpdateBackupVaultDeferFunction(t *testing.T) {
	t.Run("WhenWorkflowStartFails_ShouldRollbackBackupVaultState", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine_mock.MockTemporalTestClient)

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupVault := &datamodel.BackupVault{
			BaseModel:             datamodel.BaseModel{UUID: "backup-vault-uuid", ID: 1},
			LifeCycleState:        models.LifeCycleStateAvailable,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
		}

		// Setup mocks
		mockStorage.On("GetOrCreateAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "backup-vault-uuid", int64(1)).Return(backupVault, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		mockStorage.On("UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails).Return(backupVault, nil)
		// Mock rollback
		mockStorage.On("UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, models.LifeCycleStateAvailableDetails).Return(backupVault, nil)
		mockStorage.On("UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, mock.Anything).Return(nil)

		// Mock workflow execution to fail
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow start failed"))

		params := &commonparams.BackupVaultParams{
			OwnerID:       "owner-uuid",
			BackupVaultID: "backup-vault-uuid",
			Name:          "test-vault",
		}

		result, jobID, err := updateBackupVault(ctx, mockStorage, mockTemporal, params)

		// Verify results
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Empty(t, jobID)
		assert.Equal(t, "workflow start failed", err.Error())

		// Verify rollback was called
		mockStorage.AssertCalled(t, "UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, models.LifeCycleStateAvailableDetails)
		mockStorage.AssertCalled(t, "UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, "workflow start failed")
	})

	t.Run("WhenWorkflowStartFails_AndRollbackFails_ShouldLogError", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine_mock.MockTemporalTestClient)

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupVault := &datamodel.BackupVault{
			BaseModel:             datamodel.BaseModel{UUID: "backup-vault-uuid", ID: 1},
			LifeCycleState:        models.LifeCycleStateAvailable,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
		}

		// Setup mocks
		mockStorage.On("GetOrCreateAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "backup-vault-uuid", int64(1)).Return(backupVault, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		mockStorage.On("UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails).Return(backupVault, nil)
		// Mock rollback to fail
		mockStorage.On("UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, models.LifeCycleStateAvailableDetails).Return(nil, errors.New("rollback failed"))
		mockStorage.On("UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, mock.Anything).Return(nil)

		// Mock workflow execution to fail
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow start failed"))

		params := &commonparams.BackupVaultParams{
			OwnerID:       "owner-uuid",
			BackupVaultID: "backup-vault-uuid",
			Name:          "test-vault",
		}

		result, jobID, err := updateBackupVault(ctx, mockStorage, mockTemporal, params)

		// Verify results
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Empty(t, jobID)
		assert.Equal(t, "workflow start failed", err.Error())

		// Verify rollback was attempted even though it failed
		mockStorage.AssertCalled(t, "UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, models.LifeCycleStateAvailableDetails)
		mockStorage.AssertCalled(t, "UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, "workflow start failed")
	})

	t.Run("WhenWorkflowStartFails_AndJobUpdateFails_ShouldLogError", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine_mock.MockTemporalTestClient)

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupVault := &datamodel.BackupVault{
			BaseModel:             datamodel.BaseModel{UUID: "backup-vault-uuid", ID: 1},
			LifeCycleState:        models.LifeCycleStateAvailable,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
		}

		// Setup mocks
		mockStorage.On("GetOrCreateAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "backup-vault-uuid", int64(1)).Return(backupVault, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		mockStorage.On("UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails).Return(backupVault, nil)
		mockStorage.On("UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, models.LifeCycleStateAvailableDetails).Return(backupVault, nil)
		// Mock job update to fail
		mockStorage.On("UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, mock.Anything).Return(errors.New("job update failed"))

		// Mock workflow execution to fail
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow start failed"))

		params := &commonparams.BackupVaultParams{
			OwnerID:       "owner-uuid",
			BackupVaultID: "backup-vault-uuid",
			Name:          "test-vault",
		}

		result, jobID, err := updateBackupVault(ctx, mockStorage, mockTemporal, params)

		// Verify results
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Empty(t, jobID)
		assert.Equal(t, "workflow start failed", err.Error())

		// Verify both rollback operations were attempted
		mockStorage.AssertCalled(t, "UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, models.LifeCycleStateAvailableDetails)
		mockStorage.AssertCalled(t, "UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, "workflow start failed")
	})

	t.Run("WhenWorkflowStartSucceeds_ShouldNotRollback", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine_mock.MockTemporalTestClient)

		enforcedDuration := int64(30)
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupVault := &datamodel.BackupVault{
			BaseModel:             datamodel.BaseModel{UUID: "backup-vault-uuid", ID: 1},
			LifeCycleState:        models.LifeCycleStateAvailable,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
			Account:               account,
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &enforcedDuration,
				IsDailyBackupImmutable:                 false,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
				IsAdhocBackupImmutable:                 false,
			},
		}
		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
		}

		// Setup mocks
		mockStorage.On("GetOrCreateAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "backup-vault-uuid", int64(1)).Return(backupVault, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		mockStorage.On("UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails).Return(backupVault, nil)

		// Mock workflow execution to succeed
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		params := &commonparams.BackupVaultParams{
			OwnerID:       "owner-uuid",
			BackupVaultID: "backup-vault-uuid",
			Name:          "test-vault",
		}

		result, jobID, err := updateBackupVault(ctx, mockStorage, mockTemporal, params)

		// Verify results
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "job-uuid", jobID)

		// Verify rollback was NOT called
		mockStorage.AssertNotCalled(t, "UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, models.LifeCycleStateAvailableDetails)
		mockStorage.AssertNotCalled(t, "UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, mock.Anything)
	})

	t.Run("WhenCreateJobFails_ShouldNotRollback", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine_mock.MockTemporalTestClient)

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupVault := &datamodel.BackupVault{
			BaseModel:             datamodel.BaseModel{UUID: "backup-vault-uuid", ID: 1},
			LifeCycleState:        models.LifeCycleStateAvailable,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
			Account:               account,
		}

		// Setup mocks
		mockStorage.On("GetOrCreateAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "backup-vault-uuid", int64(1)).Return(backupVault, nil)
		// Mock CreateJob to fail
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(nil, errors.New("create job failed"))

		params := &commonparams.BackupVaultParams{
			OwnerID:       "owner-uuid",
			BackupVaultID: "backup-vault-uuid",
			Name:          "test-vault",
		}

		result, jobID, err := updateBackupVault(ctx, mockStorage, mockTemporal, params)

		// Verify results
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Empty(t, jobID)
		assert.Equal(t, "create job failed", err.Error())

		// Verify rollback was NOT called since no job was created
		mockStorage.AssertNotCalled(t, "UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, models.LifeCycleStateAvailableDetails)
		mockStorage.AssertNotCalled(t, "UpdateJob", ctx, mock.Anything, string(models.JobsStateERROR), 0, mock.Anything)
	})

	t.Run("WhenBackupVaultHasDifferentOriginalState_ShouldRollbackToCorrectState", func(t *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := new(workflow_engine_mock.MockTemporalTestClient)

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		backupVault := &datamodel.BackupVault{
			BaseModel:             datamodel.BaseModel{UUID: "backup-vault-uuid", ID: 1},
			LifeCycleState:        models.LifeCycleStateAvailable,
			LifeCycleStateDetails: "Updating backup vault",
			Account:               account,
		}
		job := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
		}

		// Setup mocks
		mockStorage.On("GetOrCreateAccount", ctx, "owner-uuid").Return(account, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "backup-vault-uuid", int64(1)).Return(backupVault, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		mockStorage.On("UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails).Return(backupVault, nil)
		// Mock rollback to original state
		mockStorage.On("UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, "Updating backup vault").Return(backupVault, nil)
		mockStorage.On("UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, mock.Anything).Return(nil)

		// Mock workflow execution to fail
		mockTemporal.On("ExecuteWorkflow", ctx, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow start failed"))

		params := &commonparams.BackupVaultParams{
			OwnerID:       "owner-uuid",
			BackupVaultID: "backup-vault-uuid",
			Name:          "test-vault",
		}

		result, jobID, err := updateBackupVault(ctx, mockStorage, mockTemporal, params)

		// Verify results
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Empty(t, jobID)
		assert.Equal(t, "workflow start failed", err.Error())

		// Verify rollback was called with the original state
		mockStorage.AssertCalled(t, "UpdateBackupVaultState", ctx, backupVault, models.LifeCycleStateAvailable, "Updating backup vault")
		mockStorage.AssertCalled(t, "UpdateJob", ctx, "job-uuid", string(models.JobsStateERROR), 0, "workflow start failed")
	})
}
