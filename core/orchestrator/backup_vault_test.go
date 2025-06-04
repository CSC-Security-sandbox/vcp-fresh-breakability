package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflow_engine_mock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
)

func TestCheck(t *testing.T) {
	t.Run("WhenBackupVaultHasAllFieldsPopulated", func(tt *testing.T) {
		desc := "A test backup vault"
		SourceRegionName := "us-east1"
		BackupRegionName := "us-central1"
		crbName := "cross-region-vault"
		mrd := int64(30)
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
				BackupMinimumEnforcedRetentionDuration: &mrd,
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
		mrd := int64(30)
		bv := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "backup-vault-uuid",
			},
			Account: &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "owner-uuid"}},

			Name:       "backup-vault-name",
			RegionName: "us-central1",
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &mrd,
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

func TestValidateBackupVaultParams(t *testing.T) {
	t.Run("WhenNoAccountNotFound", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(t, err, "Failed to create test storage")
		params := &commonparams.BackupVaultParams{
			OwnerID: "non-existent-owner-uuid",
			Name:    "backup-vault",
			Region:  "us-central1",
		}
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}

		err = _validateBackupVaultParams(se, params)
		if err == nil || err.Error() != "account not found" {
			tt.Errorf("Expected error 'account not found', got %v", err)
		}
	})
	t.Run("WhenBackupVaultWithSameNameExists", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(t, err, "Failed to create test storage")
		params := &commonparams.BackupVaultParams{
			OwnerID: "owner-uuid",
			Name:    "existing-backup-vault",
			Region:  "us-central1",
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}}
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		getBackupVaultByNameAndOwnerID = func(ctx context.Context, se database.Storage, bvName, ownerID string) (*datamodel.BackupVault, error) {
			return &datamodel.BackupVault{Name: params.Name}, nil
		}

		err = _validateBackupVaultParams(se, params)
		if err == nil || err.Error() != "backup vault with the same name already exists" {
			tt.Errorf("Expected error 'backup vault with the same name already exists', got %v", err)
		}
	})
	t.Run("WhenBackupVaultNameIsNill", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(t, err, "Failed to create test storage")
		params := &commonparams.BackupVaultParams{
			OwnerID: "owner-uuid",
			Name:    "",
			Region:  "us-central1",
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}}
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		getBackupVaultByNameAndOwnerID = func(ctx context.Context, se database.Storage, bvName, ownerID string) (*datamodel.BackupVault, error) {
			return &datamodel.BackupVault{Name: "abc"}, nil
		}
		err = _validateBackupVaultParams(se, params)
		if err == nil || err.Error() != "backup vault name is required" {
			tt.Errorf("Expected error 'backup vault name is required', got %v", err)
		}
	})
	t.Run("WhenBackupVaultRegionIsNill", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(t, err, "Failed to create test storage")
		params := &commonparams.BackupVaultParams{
			OwnerID: "owner-uuid",
			Name:    "abc1",
			Region:  "",
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}}
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		getBackupVaultByNameAndOwnerID = func(ctx context.Context, se database.Storage, bvName, ownerID string) (*datamodel.BackupVault, error) {
			return nil, nil
		}
		err = _validateBackupVaultParams(se, params)
		if err == nil || err.Error() != "region is required" {
			tt.Errorf("Expected error 'backup vault name is required', got %v", err)
		}
	})
}

func TestGetBackupVaultByNameAndOwnerIDReturnsBackupVault(tt *testing.T) {
	tt.Run("WhenBackupVaultExists", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		mrd := int64(30)
		bv := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "backup-vault-uuid"},
			Name:      "backup-vault-name",
			Account:   account,
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &mrd,
			},
		}
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
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
	tt.Run("WhenBackupVaultDoesNotExist", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		se, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1, UUID: "owner-uuid"}}
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
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
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
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

func TestCreateBackupVault(t *testing.T) {
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
		paramz := gcpserver.V1betaCreateBackupVaultParams{}
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}

		bv, _, err := createBackupVault(ctx, se, temporal, params, paramz)
		assert.Error(t, err, "Expected error when account not found")
		assert.Nil(t, bv, "Expected backup vault to be nil")
	})
	t.Run("WhenValidateParamsFails", func(t *testing.T) {
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
		paramz := gcpserver.V1betaCreateBackupVaultParams{}
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateBackupVaultParams = func(se database.Storage, params *commonparams.BackupVaultParams) error {
			return errors.New("validation failed")
		}

		bv, _, err := createBackupVault(ctx, se, temporal, params, paramz)
		assert.Error(t, err, "Expected error when validation fails")
		assert.Nil(t, bv, "Expected backup vault to be nil")
	})
}

func TestReturnsErrorWhenAccountNotFound(tt *testing.T) {
	mockLogger := log.NewLogger()
	se, err := database.NewTestStorage(mockLogger)
	assert.NoError(tt, err, "Failed to create test storage")
	getAccountWithName = func(ctx context.Context, se database.Storage, ownerID string) (*datamodel.Account, error) {
		return nil, errors.New("account not found")
	}

	result, err := _getBackupVaultByNameAndOwnerID(context.Background(), se, "backup-vault-name", "non-existent-owner-uuid")
	assert.Error(tt, err, "Expected error")
	assert.Nil(tt, result, "Expected result to be nil")
	assert.Equal(tt, "account not found", err.Error(), "Expected error message to match")
}
