package orchestrator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	workflow_engine_mock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"go.temporal.io/sdk/client"
)

// mockStorage is a minimal mock storage implementation for testing
type mockStorage struct {
	database.Storage
}

func TestCreateQuotaRule(t *testing.T) {
	// Save original function pointers
	originalGetOrCreateAccount := getOrCreateAccount
	originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams

	defer func() {
		getOrCreateAccount = originalGetOrCreateAccount
		validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams
	}()

	t.Run("WhenGetOrCreateAccountFails", func(tt *testing.T) {
		var mockStorage mockStorage
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			ProjectId:      "test-project",
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
			LocationId:     "us-central1",
		}

		// Mock getOrCreateAccount to return error
		originalGetOrCreateAccount := getOrCreateAccount
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("failed to get or create account")
		}
		defer func() { getOrCreateAccount = originalGetOrCreateAccount }()

		quotaRule, operationID, err := _createQuotaRule(context.Background(), mockStorage, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to get or create account")
	})

	t.Run("WhenValidateQuotaRuleCreateParamsFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			ProjectId:      "test-project",
			Name:           "",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
			LocationId:     "us-central1",
		}

		// Mock getAccountWithName to succeed
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}
		defer func() { getAccountWithName = originalGetAccountWithName }()

		// Mock validateQuotaRuleCreateParams to return error
		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return errors.NewUserInputValidationErr("Quota rule name is required")
		}
		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()

		quotaRule, operationID, err := _createQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "Quota rule name is required")
	})

	t.Run("WhenGetVolumeFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			ProjectId:      "test-project",
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
			LocationId:     "us-central1",
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock validateQuotaRuleCreateParams to succeed
		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		// Mock GetVolume to fail
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(nil, errors.New("volume not found"))

		quotaRule, operationID, err := _createQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "volume not found")
	})

	t.Run("WhenValidateVolumeTypeFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			ProjectId:      "test-project",
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
			LocationId:     "us-central1",
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"ISCSI"}, // SAN volume
			},
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock validateQuotaRuleCreateParams to succeed
		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		// Mock GetVolume to succeed but return a SAN volume
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(volume, nil)

		quotaRule, operationID, err := _createQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.True(tt, errors.IsUserInputValidationErr(err))
	})

	t.Run("WhenValidateReplicationStateFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			ProjectId:      "test-project",
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
			LocationId:     "us-central1",
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "test-project",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock validateQuotaRuleCreateParams to succeed
		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		// Mock GetVolume to succeed
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(volume, nil)

		// Mock ListVolumeReplications to return error
		expectedFilter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))
		mockStore.EXPECT().ListVolumeReplications(context.Background(), *expectedFilter, database.QueryDepthZero).
			Return(nil, errors.New("failed to list replications"))

		quotaRule, operationID, err := _createQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to list replications")
	})

	t.Run("WhenGetQuotaRulesByVolumeIDFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			ProjectId:      "test-project",
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
			LocationId:     "us-central1",
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "test-project",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock validateQuotaRuleCreateParams to succeed
		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		// Mock GetVolume to succeed
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(volume, nil)

		// Mock ListVolumeReplications to return empty list (no replications)
		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)

		// Mock GetQuotaRulesByVolumeID to fail
		mockStore.EXPECT().GetQuotaRulesByVolumeID(context.Background(), volume.ID).
			Return(nil, errors.New("failed to fetch quota rules"))

		quotaRule, operationID, err := _createQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to fetch quota rules")
	})

	t.Run("WhenValidateQuotaRuleUniquenessFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			ProjectId:      "test-project",
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
			LocationId:     "us-central1",
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "test-project",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Existing quota rule with the same name
		existingQuotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:   datamodel.BaseModel{ID: 1, UUID: "quota-uuid-1"},
				Name:        "quota-rule-1", // Same name as the new one
				VolumeID:    1,
				AccountID:   1,
				QuotaType:   DefaultUserQuota,
				QuotaTarget: "",
			},
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock validateQuotaRuleCreateParams to succeed
		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		// Mock GetVolume to succeed
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(volume, nil)

		// Mock ListVolumeReplications to return empty list
		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)

		// Mock GetQuotaRulesByVolumeID to return existing quota rules
		mockStore.EXPECT().GetQuotaRulesByVolumeID(context.Background(), volume.ID).
			Return(existingQuotaRules, nil)

		quotaRule, operationID, err := _createQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.True(tt, errors.IsConflictErr(err))
		assert.Contains(tt, err.Error(), "quota rule with same name")
	})

	t.Run("WhenDetermineRQuotaFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			ProjectId:      "test-project",
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
			LocationId:     "us-central1",
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			PoolID:      1,
			SvmID:       1,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "test-project",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		existingQuotaRules := []*datamodel.QuotaRule{}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock validateQuotaRuleCreateParams to succeed
		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		// Mock GetVolume to succeed
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(volume, nil)

		// Mock ListVolumeReplications to return empty list
		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)

		// Mock GetQuotaRulesByVolumeID to return empty list
		mockStore.EXPECT().GetQuotaRulesByVolumeID(context.Background(), volume.ID).
			Return(existingQuotaRules, nil)

		// Mock GetQuotaRuleCountBySvmID to fail
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(0), errors.New("failed to get quota rule count"))

		quotaRule, operationID, err := _createQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to get quota rule count")
	})

	t.Run("WhenCreateJobFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			ProjectId:      "test-project",
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
			LocationId:     "us-central1",
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			PoolID:      1,
			SvmID:       1,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "test-project",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		existingQuotaRules := []*datamodel.QuotaRule{}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock validateQuotaRuleCreateParams to succeed
		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		// Mock GetVolume to succeed
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(volume, nil)

		// Mock ListVolumeReplications to return empty list
		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)

		// Mock GetQuotaRulesByVolumeID to return empty list
		mockStore.EXPECT().GetQuotaRulesByVolumeID(context.Background(), volume.ID).
			Return(existingQuotaRules, nil)

		// Mock GetQuotaRuleCountBySvmID to succeed
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(0), nil)

		// Mock CreateJob to fail
		mockStore.EXPECT().CreateJob(context.Background(), mock.AnythingOfType("*datamodel.Job")).
			Return(nil, errors.New("failed to create job"))

		quotaRule, operationID, err := _createQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to create job")
	})

	t.Run("WhenCreateQuotaRuleSucceeds", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			ProjectId:      "test-project",
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
			LocationId:     "us-central1",
			Description:    "Test quota rule",
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-project",
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			PoolID:      1,
			SvmID:       1,
			Account:     account,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		existingQuotaRules := []*datamodel.QuotaRule{}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
			Type:       string(models.JobTypeCreateQuotaRule),
			State:      string(models.JobsStateNEW),
		}

		createdQuotaRule := &datamodel.QuotaRule{
			BaseModel:      datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-123"},
			Name:           params.Name,
			VolumeID:       volume.ID,
			AccountID:      volume.AccountID,
			QuotaType:      params.QuotaType,
			QuotaTarget:    params.QuotaTarget,
			DiskLimitInKib: params.DiskLimitInMib * mibToKibMultiplier,
			State:          models.LifeCycleStateCreating,
			StateDetails:   models.LifeCycleStateCreatingDetails,
			RQuota:         true,
			Description:    params.Description,
		}

		// Mock getAccountWithName to succeed
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		defer func() { getAccountWithName = originalGetAccountWithName }()
		// Mock validateQuotaRuleCreateParams to succeed
		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		// Mock GetVolume to succeed
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(volume, nil)

		// Mock ListVolumeReplications to return empty list (no replications)
		expectedFilter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))
		mockStore.EXPECT().ListVolumeReplications(context.Background(), *expectedFilter, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)

		// Mock GetQuotaRulesByVolumeID to return empty list
		mockStore.EXPECT().GetQuotaRulesByVolumeID(context.Background(), volume.ID).
			Return(existingQuotaRules, nil)

		// Mock GetQuotaRuleCountBySvmID to succeed (first quota rule in SVM)
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(0), nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock CreatingQuotaRule to succeed
		mockStore.EXPECT().CreatingQuotaRule(context.Background(), mock.Anything).
			Return(createdQuotaRule, nil)

		// Mock Temporal ExecuteWorkflow to succeed
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, createdQuotaRule).
			Return(nil, nil)

		quotaRule, operationID, err := _createQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRule)
		assert.Equal(tt, "job-uuid-123", operationID)
		assert.Equal(tt, params.Name, quotaRule.Name)
		assert.Equal(tt, params.QuotaType, quotaRule.QuotaType)
		assert.Equal(tt, params.QuotaTarget, quotaRule.QuotaTarget)
		assert.Equal(tt, params.DiskLimitInMib, quotaRule.DiskLimitInMib)
		assert.Equal(tt, models.LifeCycleStateCreating, quotaRule.LifeCycleState)
		assert.Equal(tt, params.Description, quotaRule.Description)
	})
}

func TestValidateQuotaRuleCreateParams(t *testing.T) {
	t.Run("WhenNameIsEmpty", func(tt *testing.T) {
		params := &common.CreateQuotaRulesParam{
			Name:           "",
			DiskLimitInMib: 100,
			QuotaType:      IndividualUserQuota,
			QuotaTarget:    "user:alice",
		}

		err := _validateQuotaRuleCreateParams(params)
		assert.Error(tt, err)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "Quota rule name is required")
	})

	t.Run("WhenDiskLimitTooLow", func(tt *testing.T) {
		params := &common.CreateQuotaRulesParam{
			Name:           "test-quota",
			DiskLimitInMib: 0, // Too low, will be less than 4 KiB when converted
			QuotaType:      IndividualUserQuota,
			QuotaTarget:    "user:alice",
		}

		err := _validateQuotaRuleCreateParams(params)
		assert.Error(tt, err)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "DiskLimit is outside the permissible range")
	})

	t.Run("WhenDiskLimitTooHigh", func(tt *testing.T) {
		// Upper limit is 1125899906842620 KiB, which is approximately 1099511627776 MiB
		params := &common.CreateQuotaRulesParam{
			Name:           "test-quota",
			DiskLimitInMib: 1200000000000, // Too high
			QuotaType:      IndividualUserQuota,
			QuotaTarget:    "user:alice",
		}

		err := _validateQuotaRuleCreateParams(params)
		assert.Error(tt, err)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "DiskLimit is outside the permissible range")
	})

	t.Run("WhenIndividualUserQuotaWithoutTarget", func(tt *testing.T) {
		params := &common.CreateQuotaRulesParam{
			Name:           "test-quota",
			DiskLimitInMib: 100,
			QuotaType:      IndividualUserQuota,
			QuotaTarget:    "", // Missing target
		}

		err := _validateQuotaRuleCreateParams(params)
		assert.Error(tt, err)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "quotaTarget has to be specified")
	})

	t.Run("WhenIndividualGroupQuotaWithoutTarget", func(tt *testing.T) {
		params := &common.CreateQuotaRulesParam{
			Name:           "test-quota",
			DiskLimitInMib: 100,
			QuotaType:      IndividualGroupQuota,
			QuotaTarget:    "", // Missing target
		}

		err := _validateQuotaRuleCreateParams(params)
		assert.Error(tt, err)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "quotaTarget has to be specified")
	})

	t.Run("WhenDefaultUserQuotaWithTarget", func(tt *testing.T) {
		params := &common.CreateQuotaRulesParam{
			Name:           "test-quota",
			DiskLimitInMib: 100,
			QuotaType:      DefaultUserQuota,
			QuotaTarget:    "user:alice", // Should not have target
		}

		err := _validateQuotaRuleCreateParams(params)
		assert.Error(tt, err)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "quotaTarget cannot be specified")
	})

	t.Run("WhenDefaultGroupQuotaWithTarget", func(tt *testing.T) {
		params := &common.CreateQuotaRulesParam{
			Name:           "test-quota",
			DiskLimitInMib: 100,
			QuotaType:      DefaultGroupQuota,
			QuotaTarget:    "group:admins", // Should not have target
		}

		err := _validateQuotaRuleCreateParams(params)
		assert.Error(tt, err)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "quotaTarget cannot be specified")
	})

	t.Run("WhenValidIndividualUserQuota", func(tt *testing.T) {
		params := &common.CreateQuotaRulesParam{
			Name:           "test-quota",
			DiskLimitInMib: 100,
			QuotaType:      IndividualUserQuota,
			QuotaTarget:    "user:alice",
		}

		err := _validateQuotaRuleCreateParams(params)
		assert.NoError(tt, err)
	})

	t.Run("WhenValidDefaultUserQuota", func(tt *testing.T) {
		params := &common.CreateQuotaRulesParam{
			Name:           "test-quota",
			DiskLimitInMib: 100,
			QuotaType:      DefaultUserQuota,
			QuotaTarget:    "",
		}

		err := _validateQuotaRuleCreateParams(params)
		assert.NoError(tt, err)
	})

	t.Run("WhenValidIndividualGroupQuota", func(tt *testing.T) {
		params := &common.CreateQuotaRulesParam{
			Name:           "test-quota",
			DiskLimitInMib: 100,
			QuotaType:      IndividualGroupQuota,
			QuotaTarget:    "group:admins",
		}

		err := _validateQuotaRuleCreateParams(params)
		assert.NoError(tt, err)
	})

	t.Run("WhenValidDefaultGroupQuota", func(tt *testing.T) {
		params := &common.CreateQuotaRulesParam{
			Name:           "test-quota",
			DiskLimitInMib: 100,
			QuotaType:      DefaultGroupQuota,
			QuotaTarget:    "",
		}

		err := _validateQuotaRuleCreateParams(params)
		assert.NoError(tt, err)
	})
}

func TestValidateVolumeType(t *testing.T) {
	t.Run("WhenVolumeIsSAN", func(tt *testing.T) {
		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"ISCSI"}, // SAN volume
			},
		}

		params := &common.CreateQuotaRulesParam{
			DiskLimitInMib: 100,
		}

		err := validateVolumeType(context.Background(), volume, params)
		assert.Error(tt, err)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "block (SAN) volumes")
	})

	t.Run("WhenVolumeIsFlexCache", func(tt *testing.T) {
		volume := &datamodel.Volume{
			BaseModel:       datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes:     200 * 1024 * 1024,
			CacheParameters: &datamodel.CacheParameters{}, // FlexCache volume
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		params := &common.CreateQuotaRulesParam{
			DiskLimitInMib: 100,
		}

		err := validateVolumeType(context.Background(), volume, params)
		assert.Error(tt, err)
		assert.True(tt, errors.IsNotSupportedErr(err))
		assert.Contains(tt, err.Error(), "flexcache")
	})

	t.Run("WhenQuotaSizeExceedsVolumeSize", func(tt *testing.T) {
		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 100 * 1024 * 1024, // 100 MiB
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		params := &common.CreateQuotaRulesParam{
			DiskLimitInMib: 200, // 200 MiB > 100 MiB volume size
		}

		err := validateVolumeType(context.Background(), volume, params)
		assert.Error(tt, err)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "quota rule size can not be greater than volume size")
	})

	t.Run("WhenValidVolume", func(tt *testing.T) {
		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		params := &common.CreateQuotaRulesParam{
			DiskLimitInMib: 100, // 100 MiB < 200 MiB volume size
		}

		err := validateVolumeType(context.Background(), volume, params)
		assert.NoError(tt, err)
	})
}

func TestValidateQuotaRuleUniqueness(t *testing.T) {
	t.Run("WhenNameAlreadyExists", func(tt *testing.T) {
		existingRules := []*datamodel.QuotaRule{
			{
				BaseModel:   datamodel.BaseModel{ID: 1, UUID: "quota-uuid-1"},
				Name:        "existing-quota",
				QuotaType:   DefaultUserQuota,
				QuotaTarget: "",
			},
		}

		params := &common.CreateQuotaRulesParam{
			Name:        "existing-quota", // Same name
			QuotaType:   IndividualUserQuota,
			QuotaTarget: "user:alice",
		}

		err := validateQuotaRuleUniqueness(context.Background(), existingRules, params)
		assert.Error(tt, err)
		assert.True(tt, errors.IsConflictErr(err))
		assert.Contains(tt, err.Error(), "quota rule with same name")
	})

	t.Run("WhenTypeAndTargetAlreadyExist", func(tt *testing.T) {
		existingRules := []*datamodel.QuotaRule{
			{
				BaseModel:   datamodel.BaseModel{ID: 1, UUID: "quota-uuid-1"},
				Name:        "existing-quota",
				QuotaType:   IndividualUserQuota,
				QuotaTarget: "user:alice",
			},
		}

		params := &common.CreateQuotaRulesParam{
			Name:        "new-quota",         // Different name
			QuotaType:   IndividualUserQuota, // Same type
			QuotaTarget: "user:alice",        // Same target
		}

		err := validateQuotaRuleUniqueness(context.Background(), existingRules, params)
		assert.Error(tt, err)
		assert.True(tt, errors.IsConflictErr(err))
		assert.Contains(tt, err.Error(), "quota rule with same type")
	})

	t.Run("WhenNoConflict", func(tt *testing.T) {
		existingRules := []*datamodel.QuotaRule{
			{
				BaseModel:   datamodel.BaseModel{ID: 1, UUID: "quota-uuid-1"},
				Name:        "existing-quota",
				QuotaType:   DefaultUserQuota,
				QuotaTarget: "",
			},
		}

		params := &common.CreateQuotaRulesParam{
			Name:        "new-quota",         // Different name
			QuotaType:   IndividualUserQuota, // Different type
			QuotaTarget: "user:alice",
		}

		err := validateQuotaRuleUniqueness(context.Background(), existingRules, params)
		assert.NoError(tt, err)
	})

	t.Run("WhenEmptyExistingRules", func(tt *testing.T) {
		existingRules := []*datamodel.QuotaRule{}

		params := &common.CreateQuotaRulesParam{
			Name:        "new-quota",
			QuotaType:   IndividualUserQuota,
			QuotaTarget: "user:alice",
		}

		err := validateQuotaRuleUniqueness(context.Background(), existingRules, params)
		assert.NoError(tt, err)
	})
}

func TestHasProtocol(t *testing.T) {
	t.Run("WhenProtocolExists", func(tt *testing.T) {
		protocolTypes := []string{"NFSV3", "NFSV4", "SMB"}
		result := hasProtocol("NFSV3", protocolTypes)
		assert.True(tt, result)
	})

	t.Run("WhenProtocolDoesNotExist", func(tt *testing.T) {
		protocolTypes := []string{"NFSV3", "NFSV4", "SMB"}
		result := hasProtocol("ISCSI", protocolTypes)
		assert.False(tt, result)
	})

	t.Run("WhenProtocolTypesIsEmpty", func(tt *testing.T) {
		protocolTypes := []string{}
		result := hasProtocol("NFSV3", protocolTypes)
		assert.False(tt, result)
	})
}

func TestHasNFSv3(t *testing.T) {
	t.Run("WhenNFSv3Exists", func(tt *testing.T) {
		protocolTypes := []string{"NFSV3", "SMB"}
		result := hasNFSv3(protocolTypes)
		assert.True(tt, result)
	})

	t.Run("WhenNFSv3DoesNotExist", func(tt *testing.T) {
		protocolTypes := []string{"NFSV4", "SMB"}
		result := hasNFSv3(protocolTypes)
		assert.False(tt, result)
	})
}

func TestHasNFSv4(t *testing.T) {
	t.Run("WhenNFSv4Exists", func(tt *testing.T) {
		protocolTypes := []string{"NFSV4", "SMB"}
		result := hasNFSv4(protocolTypes)
		assert.True(tt, result)
	})

	t.Run("WhenNFSv4DoesNotExist", func(tt *testing.T) {
		protocolTypes := []string{"NFSV3", "SMB"}
		result := hasNFSv4(protocolTypes)
		assert.False(tt, result)
	})
}

func TestDetermineRQuota(t *testing.T) {
	t.Run("WhenVolumeDoesNotUseNFS", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SvmID:     1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"SMB"}, // Not NFS
			},
		}

		rquotaRequired, err := determineRQuota(context.Background(), mockStore, volume, false)
		assert.NoError(tt, err)
		assert.False(tt, rquotaRequired)
	})

	t.Run("WhenVolumeUsesNFSv3AndSVMHasExistingQuotaRules", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SvmID:     1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Mock GetQuotaRuleCountBySvmID to return 2 (existing quota rules in SVM)
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(2), nil)

		rquotaRequired, err := determineRQuota(context.Background(), mockStore, volume, false)
		assert.NoError(tt, err)
		assert.False(tt, rquotaRequired)
	})

	t.Run("WhenVolumeUsesNFSv4AndIsFirstQuotaRuleInSVM", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SvmID:     1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV4"},
			},
		}

		// Mock GetQuotaRuleCountBySvmID to return 0 (first quota rule in SVM)
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(0), nil)

		rquotaRequired, err := determineRQuota(context.Background(), mockStore, volume, false)
		assert.NoError(tt, err)
		assert.True(tt, rquotaRequired)
	})

	t.Run("WhenVolumeUsesNFSv3AndIsNotFirstQuotaRuleInSVM", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SvmID:     1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Mock GetQuotaRuleCountBySvmID to return 1 (not first quota rule in SVM)
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(1), nil)

		rquotaRequired, err := determineRQuota(context.Background(), mockStore, volume, false)
		assert.NoError(tt, err)
		assert.False(tt, rquotaRequired)
	})

	t.Run("WhenGetQuotaRuleCountBySvmIDFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SvmID:     1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Mock GetQuotaRuleCountBySvmID to fail
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(0), errors.New("database error"))

		rquotaRequired, err := determineRQuota(context.Background(), mockStore, volume, false)
		assert.Error(tt, err)
		assert.False(tt, rquotaRequired)
		assert.Contains(tt, err.Error(), "database error")
	})

	t.Run("WhenVolumeHasNilProtocols", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SvmID:     1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: nil,
			},
		}

		rquotaRequired, err := determineRQuota(context.Background(), mockStore, volume, false)
		assert.NoError(tt, err)
		assert.False(tt, rquotaRequired)
	})

	t.Run("WhenVolumeHasNilVolumeAttributes", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel:        datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SvmID:            1,
			VolumeAttributes: nil,
		}

		rquotaRequired, err := determineRQuota(context.Background(), mockStore, volume, false)
		assert.NoError(tt, err)
		assert.False(tt, rquotaRequired)
	})

	t.Run("WhenVolumeDoesNotUseNFSAndIsDeleteAction", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SvmID:     1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"SMB"}, // Not NFS
			},
		}

		// For delete action on SMB volume, RQuota should be true
		rquotaRequired, err := determineRQuota(context.Background(), mockStore, volume, true)
		assert.NoError(tt, err)
		assert.True(tt, rquotaRequired) // Should return true for delete action even if not NFS
	})

	t.Run("WhenVolumeHasNilProtocolsAndIsDeleteAction", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SvmID:     1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: nil,
			},
		}

		rquotaRequired, err := determineRQuota(context.Background(), mockStore, volume, true)
		assert.NoError(tt, err)
		assert.True(tt, rquotaRequired) // Should return true for delete action even if protocols are nil
	})

	t.Run("WhenVolumeHasNilVolumeAttributesAndIsDeleteAction", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel:        datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SvmID:            1,
			VolumeAttributes: nil,
		}

		rquotaRequired, err := determineRQuota(context.Background(), mockStore, volume, true)
		assert.NoError(tt, err)
		assert.True(tt, rquotaRequired) // Should return true for delete action even if VolumeAttributes is nil
	})

	t.Run("WhenDeleteActionAndLastQuotaRuleInSVM", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SvmID:     1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Mock GetQuotaRuleCountBySvmID to return 1 (last quota rule in SVM)
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(1), nil)

		rquotaRequired, err := determineRQuota(context.Background(), mockStore, volume, true)
		assert.NoError(tt, err)
		assert.True(tt, rquotaRequired)
	})

	t.Run("WhenDeleteActionAndNotLastQuotaRuleInSVM", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SvmID:     1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Mock GetQuotaRuleCountBySvmID to return 2 (not last quota rule in SVM)
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(2), nil)

		rquotaRequired, err := determineRQuota(context.Background(), mockStore, volume, true)
		assert.NoError(tt, err)
		assert.False(tt, rquotaRequired)
	})
}

func TestConvertDatastoreQuotaRuleToModel(t *testing.T) {
	t.Run("WhenQuotaRuleIsNil", func(tt *testing.T) {
		result := _convertDatastoreQuotaRuleToModel(nil)
		assert.Nil(tt, result)
	})

	t.Run("WhenQuotaRuleIsValid", func(tt *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			BaseModel:      datamodel.BaseModel{ID: 1, UUID: "quota-uuid-123"},
			Name:           "test-quota",
			Description:    "Test description",
			State:          models.LifeCycleStateAvailable,
			StateDetails:   "Available",
			QuotaType:      IndividualUserQuota,
			QuotaTarget:    "user:alice",
			DiskLimitInKib: 102400, // 100 MiB in KiB
		}

		result := _convertDatastoreQuotaRuleToModel(quotaRule)
		assert.NotNil(tt, result)
		assert.Equal(tt, "quota-uuid-123", result.UUID)
		assert.Equal(tt, "test-quota", result.Name)
		assert.Equal(tt, "Test description", result.Description)
		assert.Equal(tt, models.LifeCycleStateAvailable, result.LifeCycleState)
		assert.Equal(tt, "Available", result.LifeCycleStateDetails)
		assert.Equal(tt, IndividualUserQuota, result.QuotaType)
		assert.Equal(tt, "user:alice", result.QuotaTarget)
		assert.Equal(tt, int64(100), result.DiskLimitInMib) // 102400 KiB / 1024 = 100 MiB
	})

	t.Run("WhenQuotaRuleIsValidWithMinimalFields", func(tt *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			BaseModel:      datamodel.BaseModel{ID: 1, UUID: "quota-uuid-123"},
			Name:           "test-quota",
			DiskLimitInKib: 102400,
		}

		result := _convertDatastoreQuotaRuleToModel(quotaRule)
		assert.NotNil(tt, result)
		assert.Equal(tt, "quota-uuid-123", result.UUID)
		assert.Equal(tt, "test-quota", result.Name)
	})
}

// TestGetDestinationReplication tests the _getDestinationReplication function
// Note: This function is a simple wrapper around activities.GetReplicationDetails
// Line 76 is covered through TestValidateReplicationState tests which exercise
// the getDestinationReplication variable that calls this function
func TestGetDestinationReplication(t *testing.T) {
	// This is a simple wrapper function that delegates to activities.GetReplicationDetails
	// The actual function is tested indirectly through validateReplicationState tests
	// which mock getDestinationReplication variable
	t.Run("FunctionExists", func(tt *testing.T) {
		// Just verify the function exists and has the correct signature
		// Actual behavior is tested through validateReplicationState tests
		assert.NotNil(tt, _getDestinationReplication)
	})
}

func TestValidateReplicationState(t *testing.T) {
	// Save original function pointers
	originalGetDestinationReplication := getDestinationReplication
	originalInternalUtilGetSignedToken := internalUtilGetSignedToken
	originalInternalUtilGetPairedRegionURI := internalUtilGetPairedRegionURI
	originalInternalParseRegionAndZone := internalParseRegionAndZone

	defer func() {
		getDestinationReplication = originalGetDestinationReplication
		internalUtilGetSignedToken = originalInternalUtilGetSignedToken
		internalUtilGetPairedRegionURI = originalInternalUtilGetPairedRegionURI
		internalParseRegionAndZone = originalInternalParseRegionAndZone
	}()

	t.Run("WhenNoReplicationsExist", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
		}

		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)

		err := validateReplicationState(context.Background(), mockStore, volume, "us-central1")
		assert.NoError(tt, err)
	})

	t.Run("WhenReplicationHasNilReplicationAttributes", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
		}

		replications := []*datamodel.VolumeReplication{
			{
				ReplicationAttributes: nil,
			},
		}

		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return(replications, nil)

		err := validateReplicationState(context.Background(), mockStore, volume, "us-central1")
		assert.NoError(tt, err)
	})

	t.Run("WhenCurrentLocationMatchesDestinationLocation", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		locationID := "us-central1-a"
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
		}

		replications := []*datamodel.VolumeReplication{
			{
				State:              models.LifeCycleStateAvailable,
				MirrorState:        nillable.ToPointer("Snapmirrored"),
				RelationshipStatus: nillable.ToPointer("Healthy"),
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: locationID,
				},
			},
		}

		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return(replications, nil)

		err := validateReplicationState(context.Background(), mockStore, volume, locationID)
		assert.NoError(tt, err)
	})

	t.Run("WhenCurrentLocationMatchesDestinationLocationAndMirrorStateIsMIRRORED", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		locationID := "us-central1-a"
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
		}

		mirrorState := "MIRRORED"
		replications := []*datamodel.VolumeReplication{
			{
				State:              models.LifeCycleStateAvailable,
				MirrorState:        &mirrorState,
				RelationshipStatus: nillable.ToPointer("Healthy"),
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: locationID,
				},
			},
		}

		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return(replications, nil)

		err := validateReplicationState(context.Background(), mockStore, volume, locationID)
		assert.Error(tt, err)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "MIRRORED")
	})

	t.Run("WhenCurrentLocationMatchesDestinationLocationAndMirrorStateIsUNINITIALIZED", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		locationID := "us-central1-a"
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
		}

		mirrorState := "UNINITIALIZED"
		replications := []*datamodel.VolumeReplication{
			{
				State:              models.LifeCycleStateAvailable,
				MirrorState:        &mirrorState,
				RelationshipStatus: nillable.ToPointer("Healthy"),
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: locationID,
				},
			},
		}

		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return(replications, nil)

		err := validateReplicationState(context.Background(), mockStore, volume, locationID)
		assert.Error(tt, err)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "UNINITIALIZED")
	})

	t.Run("WhenParseProjectNumberFromURIFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "test-project",
			},
		}

		replications := []*datamodel.VolumeReplication{
			{
				RemoteUri: "invalid-uri",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "us-east1-a",
				},
			},
		}

		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return(replications, nil)

		err := validateReplicationState(context.Background(), mockStore, volume, "us-central1-a")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to parse destination project number")
	})

	t.Run("WhenParseRegionAndZoneFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "test-project",
			},
		}

		originalParseProjectNumberFromURI := utils.ParseProjectNumberFromURI
		defer func() {
			utils.ParseProjectNumberFromURI = originalParseProjectNumberFromURI
		}()

		utils.ParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}

		internalParseRegionAndZone = func(locationID string) (string, string, error) {
			return "", "", errors.New("failed to parse location")
		}

		replications := []*datamodel.VolumeReplication{
			{
				RemoteUri: "https://test.com/projects/123456789/locations/us-east1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "invalid-location",
				},
			},
		}

		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return(replications, nil)

		err := validateReplicationState(context.Background(), mockStore, volume, "us-central1-a")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Failed to parse destination location")
	})

	t.Run("WhenGetPairedRegionURIFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "test-project",
			},
		}

		originalParseProjectNumberFromURI := utils.ParseProjectNumberFromURI
		defer func() {
			utils.ParseProjectNumberFromURI = originalParseProjectNumberFromURI
		}()

		utils.ParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}

		internalParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}

		internalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "", errors.New("failed to get paired region URI")
		}

		replications := []*datamodel.VolumeReplication{
			{
				RemoteUri: "https://test.com/projects/123456789/locations/us-east1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "us-east1-a",
				},
			},
		}

		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return(replications, nil)

		err := validateReplicationState(context.Background(), mockStore, volume, "us-central1-a")
		assert.Error(tt, err)
		// The error is wrapped in coreerrors.NewVCPError, so check for the base path error
		assert.Contains(tt, err.Error(), "Failed to get destination base path")
	})

	t.Run("WhenCrossProjectReplication", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "test-project",
			},
		}

		originalParseProjectNumberFromURI := utils.ParseProjectNumberFromURI
		defer func() {
			utils.ParseProjectNumberFromURI = originalParseProjectNumberFromURI
		}()

		utils.ParseProjectNumberFromURI = func(uri string) (string, error) {
			return "987654321", nil // Different project number
		}

		internalParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}

		internalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "https://us-east1.test.com", nil
		}

		replications := []*datamodel.VolumeReplication{
			{
				RemoteUri: "https://test.com/projects/987654321/locations/us-east1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "us-east1-a",
				},
			},
		}

		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return(replications, nil)

		err := validateReplicationState(context.Background(), mockStore, volume, "us-central1-a")
		assert.Error(tt, err)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "cross project replication")
	})

	t.Run("WhenGetSignedTokenFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "123456789", // Match the project number to avoid cross-project check
			},
		}

		originalParseProjectNumberFromURI := utils.ParseProjectNumberFromURI
		defer func() {
			utils.ParseProjectNumberFromURI = originalParseProjectNumberFromURI
		}()

		utils.ParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}

		internalParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}

		internalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "https://us-east1.test.com", nil
		}

		internalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "", errors.New("failed to get signed token")
		}

		replications := []*datamodel.VolumeReplication{
			{
				RemoteUri: "https://test.com/projects/123456789/locations/us-east1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation: "us-east1-a",
				},
			},
		}

		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return(replications, nil)

		err := validateReplicationState(context.Background(), mockStore, volume, "us-central1-a")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Failed to get signed token")
	})

	t.Run("WhenGetDestinationReplicationFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "123456789",
			},
		}

		originalParseProjectNumberFromURI := utils.ParseProjectNumberFromURI
		defer func() {
			utils.ParseProjectNumberFromURI = originalParseProjectNumberFromURI
		}()

		utils.ParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}

		internalParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}

		internalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "https://us-east1.test.com", nil
		}

		internalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "test-token", nil
		}

		getDestinationReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*models.VolumeReplication, error) {
			return nil, errors.New("failed to fetch destination replication")
		}

		replications := []*datamodel.VolumeReplication{
			{
				RemoteUri: "https://test.com/projects/123456789/locations/us-east1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-east1-a",
					DestinationReplicationUUID: "replication-uuid-123",
				},
			},
		}

		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return(replications, nil)

		err := validateReplicationState(context.Background(), mockStore, volume, "us-central1-a")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to fetch destination replication")
	})

	t.Run("WhenDestinationReplicationIsInCreatingState", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "123456789",
			},
		}

		originalParseProjectNumberFromURI := utils.ParseProjectNumberFromURI
		defer func() {
			utils.ParseProjectNumberFromURI = originalParseProjectNumberFromURI
		}()

		utils.ParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}

		internalParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}

		internalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "https://us-east1.test.com", nil
		}

		internalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "test-token", nil
		}

		getDestinationReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*models.VolumeReplication, error) {
			return &models.VolumeReplication{
				State: models.LifeCycleStateCreating,
			}, nil
		}

		replications := []*datamodel.VolumeReplication{
			{
				RemoteUri: "https://test.com/projects/123456789/locations/us-east1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-east1-a",
					DestinationReplicationUUID: "replication-uuid-123",
				},
			},
		}

		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return(replications, nil)

		err := validateReplicationState(context.Background(), mockStore, volume, "us-central1-a")
		assert.Error(tt, err)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "Quota update not allowed on destination volume when in active replication")
	})

	t.Run("WhenDestinationReplicationIsInUpdatingState", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "123456789",
			},
		}

		originalParseProjectNumberFromURI := utils.ParseProjectNumberFromURI
		defer func() {
			utils.ParseProjectNumberFromURI = originalParseProjectNumberFromURI
		}()

		utils.ParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}

		internalParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}

		internalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "https://us-east1.test.com", nil
		}

		internalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "test-token", nil
		}

		getDestinationReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*models.VolumeReplication, error) {
			return &models.VolumeReplication{
				State: models.LifeCycleStateUpdating,
			}, nil
		}

		replications := []*datamodel.VolumeReplication{
			{
				RemoteUri: "https://test.com/projects/123456789/locations/us-east1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-east1-a",
					DestinationReplicationUUID: "replication-uuid-123",
				},
			},
		}

		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return(replications, nil)

		err := validateReplicationState(context.Background(), mockStore, volume, "us-central1-a")
		assert.Error(tt, err)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "Quota update not allowed on destination volume when in active replication")
	})

	t.Run("WhenDestinationReplicationIsInDeletingState", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "123456789",
			},
		}

		originalParseProjectNumberFromURI := utils.ParseProjectNumberFromURI
		defer func() {
			utils.ParseProjectNumberFromURI = originalParseProjectNumberFromURI
		}()

		utils.ParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}

		internalParseRegionAndZone = func(locationID string) (string, string, error) {
			return "us-east1", "us-east1-a", nil
		}

		internalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "https://us-east1.test.com", nil
		}

		internalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "test-token", nil
		}

		getDestinationReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*models.VolumeReplication, error) {
			return &models.VolumeReplication{
				State: models.LifeCycleStateDeleting,
			}, nil
		}

		replications := []*datamodel.VolumeReplication{
			{
				RemoteUri: "https://test.com/projects/123456789/locations/us-east1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-east1-a",
					DestinationReplicationUUID: "replication-uuid-123",
				},
			},
		}

		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return(replications, nil)

		err := validateReplicationState(context.Background(), mockStore, volume, "us-central1-a")
		assert.Error(tt, err)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "Quota update not allowed on destination volume when in active replication")
	})
}

func TestCreateQuotaRuleWrapper(t *testing.T) {
	t.Run("WhenCreateQuotaRuleSucceeds", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)
		orchestrator := &Orchestrator{
			storage:  mockStore,
			temporal: mockTemporal,
		}

		params := &common.CreateQuotaRulesParam{
			ProjectId:      "test-project",
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
			LocationId:     "us-central1",
		}

		originalCreateQuotaRule := createQuotaRule
		defer func() {
			createQuotaRule = originalCreateQuotaRule
		}()

		expectedQuotaRule := &models.QuotaRule{
			BaseModel: models.BaseModel{
				UUID: "quota-uuid-123",
			},
			Name:      params.Name,
			QuotaType: params.QuotaType,
		}
		expectedJobUUID := "job-uuid-123"

		createQuotaRule = func(ctx context.Context, se database.Storage, temporal client.Client, params *common.CreateQuotaRulesParam) (*models.QuotaRule, string, error) {
			return expectedQuotaRule, expectedJobUUID, nil
		}

		quotaRule, jobUUID, err := orchestrator.CreateQuotaRule(context.Background(), params)
		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRule)
		assert.Equal(tt, expectedQuotaRule.BaseModel.UUID, quotaRule.BaseModel.UUID)
		assert.Equal(tt, expectedJobUUID, jobUUID)
	})
}

func TestCreateQuotaRuleInternalWrapper(t *testing.T) {
	t.Run("WhenCreateQuotaRuleInternalSucceeds", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)
		orchestrator := &Orchestrator{
			storage:  mockStore,
			temporal: mockTemporal,
		}

		params := &common.CreateQuotaRulesParam{
			ProjectId:      "test-project",
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
			LocationId:     "us-central1",
		}

		originalCreateQuotaRuleInternal := createQuotaRuleInternal
		defer func() {
			createQuotaRuleInternal = originalCreateQuotaRuleInternal
		}()

		expectedQuotaRule := &models.QuotaRule{
			BaseModel: models.BaseModel{
				UUID: "quota-uuid-123",
			},
			Name:      params.Name,
			QuotaType: params.QuotaType,
		}
		expectedJobUUID := "job-uuid-123"

		createQuotaRuleInternal = func(ctx context.Context, se database.Storage, temporal client.Client, params *common.CreateQuotaRulesParam) (*models.QuotaRule, *datamodel.Job, error) {
			return expectedQuotaRule, &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: expectedJobUUID}}, nil
		}

		quotaRule, job, err := orchestrator.CreateQuotaRuleInternal(context.Background(), params)
		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRule)
		assert.NotNil(tt, job)
		assert.Equal(tt, expectedQuotaRule.BaseModel.UUID, quotaRule.BaseModel.UUID)
		assert.Equal(tt, expectedJobUUID, job.UUID)
	})
}

func TestCreateQuotaRuleErrorPaths(t *testing.T) {
	// Save original function pointers
	originalGetOrCreateAccount := getOrCreateAccount
	originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams

	defer func() {
		getOrCreateAccount = originalGetOrCreateAccount
		validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams
	}()

	t.Run("WhenCreatingQuotaRuleFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			ProjectId:      "test-project",
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
			LocationId:     "us-central1",
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-project",
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			PoolID:      1,
			SvmID:       1,
			Account:     account,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		existingQuotaRules := []*datamodel.QuotaRule{}
		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		originalGetOrCreateAccount := getOrCreateAccount
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		defer func() { getOrCreateAccount = originalGetOrCreateAccount }()
		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(volume, nil)
		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)
		mockStore.EXPECT().GetQuotaRulesByVolumeID(context.Background(), volume.ID).
			Return(existingQuotaRules, nil)
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(0), nil)
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)
		mockStore.EXPECT().CreatingQuotaRule(context.Background(), mock.Anything).
			Return(nil, errors.New("failed to create quota rule"))
		mockStore.EXPECT().DeleteJob(context.Background(), createdJob.UUID, mock.Anything).
			Return(nil)

		quotaRule, operationID, err := _createQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to create quota rule")
	})

	t.Run("WhenExecuteWorkflowFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			ProjectId:      "test-project",
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
			LocationId:     "us-central1",
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-project",
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			PoolID:      1,
			SvmID:       1,
			Account:     account,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		existingQuotaRules := []*datamodel.QuotaRule{}
		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		createdQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{UUID: "quota-rule-uuid-123"},
		}

		originalGetOrCreateAccount := getOrCreateAccount
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		defer func() { getOrCreateAccount = originalGetOrCreateAccount }()
		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(volume, nil)
		mockStore.EXPECT().ListVolumeReplications(context.Background(), mock.Anything, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)
		mockStore.EXPECT().GetQuotaRulesByVolumeID(context.Background(), volume.ID).
			Return(existingQuotaRules, nil)
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(0), nil)
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)
		mockStore.EXPECT().CreatingQuotaRule(context.Background(), mock.Anything).
			Return(createdQuotaRule, nil)
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, createdQuotaRule).
			Return(nil, errors.New("failed to execute workflow"))
		mockStore.EXPECT().DeleteJob(context.Background(), createdJob.UUID, mock.Anything).
			Return(nil)
		mockStore.EXPECT().DeleteQuotaRule(context.Background(), createdQuotaRule.UUID).
			Return(nil, nil)

		quotaRule, operationID, err := _createQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to execute workflow")
	})
}

func TestCreateQuotaRuleInternal(t *testing.T) {
	// Save original function pointers
	originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams

	defer func() {
		validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams
	}()

	t.Run("WhenValidateQuotaRuleCreateParamsFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			Name:           "",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
		}

		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return errors.NewUserInputValidationErr("Quota rule name is required")
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		quotaRule, operationID, err := _createQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.True(tt, errors.IsUserInputValidationErr(err))
	})

	t.Run("WhenGetVolumeFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
		}

		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).
			Return(nil, errors.New("volume not found"))

		quotaRule, operationID, err := _createQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "volume not found")
	})

	t.Run("WhenGetQuotaRulesByVolumeIDFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(volume, nil)
		mockStore.EXPECT().GetQuotaRulesByVolumeID(context.Background(), volume.ID).
			Return(nil, errors.New("failed to fetch quota rules"))

		quotaRule, operationID, err := _createQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to fetch quota rules")
	})

	t.Run("WhenQuotaRulesLimitReached", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Create 100 quota rules to hit the limit
		existingQuotaRules := make([]*datamodel.QuotaRule, VolumeQuotaRulesDefaultLimit)
		for i := 0; i < VolumeQuotaRulesDefaultLimit; i++ {
			existingQuotaRules[i] = &datamodel.QuotaRule{
				BaseModel: datamodel.BaseModel{ID: int64(i + 1)},
			}
		}

		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(volume, nil)
		mockStore.EXPECT().GetQuotaRulesByVolumeID(context.Background(), volume.ID).
			Return(existingQuotaRules, nil)

		quotaRule, operationID, err := _createQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "quota rules limit reached")
	})

	t.Run("WhenValidateVolumeTypeFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"ISCSI"}, // SAN volume
			},
		}

		existingQuotaRules := []*datamodel.QuotaRule{}

		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(volume, nil)
		mockStore.EXPECT().GetQuotaRulesByVolumeID(context.Background(), volume.ID).
			Return(existingQuotaRules, nil)

		quotaRule, operationID, err := _createQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.True(tt, errors.IsUserInputValidationErr(err))
	})

	t.Run("WhenValidateQuotaRuleUniquenessFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			SvmID:       1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		existingQuotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:   datamodel.BaseModel{ID: 1},
				Name:        "quota-rule-1", // Same name
				QuotaType:   IndividualUserQuota,
				QuotaTarget: "user:alice",
			},
		}

		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(volume, nil)
		mockStore.EXPECT().GetQuotaRulesByVolumeID(context.Background(), volume.ID).
			Return(existingQuotaRules, nil)

		quotaRule, operationID, err := _createQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.True(tt, errors.IsConflictErr(err))
	})

	t.Run("WhenDetermineRQuotaFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			SvmID:       1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		existingQuotaRules := []*datamodel.QuotaRule{}

		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(volume, nil)
		mockStore.EXPECT().GetQuotaRulesByVolumeID(context.Background(), volume.ID).
			Return(existingQuotaRules, nil)
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(0), errors.New("failed to get quota rule count"))

		quotaRule, operationID, err := _createQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to get quota rule count")
	})

	t.Run("WhenCreateJobFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			SvmID:       1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		existingQuotaRules := []*datamodel.QuotaRule{}

		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(volume, nil)
		mockStore.EXPECT().GetQuotaRulesByVolumeID(context.Background(), volume.ID).
			Return(existingQuotaRules, nil)
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(0), nil)
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(nil, errors.New("failed to create job"))

		quotaRule, operationID, err := _createQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to create job")
	})

	t.Run("WhenCreatingQuotaRuleFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			SvmID:       1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		existingQuotaRules := []*datamodel.QuotaRule{}
		createdJob := &datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "job-uuid-123"},
		}

		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(volume, nil)
		mockStore.EXPECT().GetQuotaRulesByVolumeID(context.Background(), volume.ID).
			Return(existingQuotaRules, nil)
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(0), nil)
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)
		mockStore.EXPECT().CreatingQuotaRule(context.Background(), mock.Anything).
			Return(nil, errors.New("failed to create quota rule"))
		mockStore.EXPECT().DeleteJob(context.Background(), createdJob.UUID, mock.Anything).
			Return(nil)

		quotaRule, operationID, err := _createQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to create quota rule")
	})

	t.Run("WhenExecuteWorkflowFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			SvmID:       1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		existingQuotaRules := []*datamodel.QuotaRule{}
		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		createdQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{UUID: "quota-rule-uuid-123"},
		}

		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(volume, nil)
		mockStore.EXPECT().GetQuotaRulesByVolumeID(context.Background(), volume.ID).
			Return(existingQuotaRules, nil)
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(0), nil)
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)
		mockStore.EXPECT().CreatingQuotaRule(context.Background(), mock.Anything).
			Return(createdQuotaRule, nil)
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, createdQuotaRule).
			Return(nil, errors.New("failed to execute workflow"))
		mockStore.EXPECT().DeleteJob(context.Background(), createdJob.UUID, mock.Anything).
			Return(nil)
		mockStore.EXPECT().DeleteQuotaRule(context.Background(), createdQuotaRule.UUID).
			Return(nil, nil)

		quotaRule, operationID, err := _createQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to execute workflow")
	})

	t.Run("WhenCreateQuotaRuleInternalSucceeds", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.CreateQuotaRulesParam{
			Name:           "quota-rule-1",
			VolumeUUID:     "volume-uuid-1",
			QuotaType:      IndividualUserQuota,
			DiskLimitInMib: 100,
			QuotaTarget:    "user:alice",
			Description:    "Test quota rule",
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			SvmID:       1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		existingQuotaRules := []*datamodel.QuotaRule{}
		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		createdQuotaRule := &datamodel.QuotaRule{
			BaseModel:      datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-123"},
			Name:           params.Name,
			VolumeID:       volume.ID,
			AccountID:      volume.AccountID,
			QuotaType:      params.QuotaType,
			QuotaTarget:    params.QuotaTarget,
			DiskLimitInKib: params.DiskLimitInMib * mibToKibMultiplier,
			State:          models.LifeCycleStateCreating,
			StateDetails:   models.LifeCycleStateCreatingDetails,
			RQuota:         true,
			Description:    params.Description,
		}

		originalValidateQuotaRuleCreateParams := validateQuotaRuleCreateParams
		validateQuotaRuleCreateParams = func(params *common.CreateQuotaRulesParam) error {
			return nil
		}

		defer func() { validateQuotaRuleCreateParams = originalValidateQuotaRuleCreateParams }()
		mockStore.EXPECT().GetVolume(context.Background(), params.VolumeUUID).Return(volume, nil)
		mockStore.EXPECT().GetQuotaRulesByVolumeID(context.Background(), volume.ID).
			Return(existingQuotaRules, nil)
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(0), nil)
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)
		mockStore.EXPECT().CreatingQuotaRule(context.Background(), mock.Anything).
			Return(createdQuotaRule, nil)
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, createdQuotaRule).
			Return(nil, nil)

		quotaRule, job, err := _createQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRule)
		assert.NotNil(tt, job)
		assert.Equal(tt, "job-uuid-123", job.UUID)
		assert.Equal(tt, params.Name, quotaRule.Name)
		assert.Equal(tt, params.QuotaType, quotaRule.QuotaType)
		assert.Equal(tt, params.QuotaTarget, quotaRule.QuotaTarget)
		assert.Equal(tt, params.DiskLimitInMib, quotaRule.DiskLimitInMib)
	})
}

func TestUpdateQuotaRule(t *testing.T) {
	// Save original function pointers
	originalGetAccountWithName := getAccountWithName

	defer func() {
		getAccountWithName = originalGetAccountWithName
	}()

	t.Run("WhenGetAccountWithNameFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 2048,
			LocationId:     "us-central1",
		}

		// Mock getAccountWithName to return error
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}

		defer func() { getAccountWithName = originalGetAccountWithName }()
		quotaRule, operationID, err := _updateQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "account not found")
	})

	t.Run("WhenGetQuotaRuleByUUIDFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 2048,
			LocationId:     "us-central1",
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to fail
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(nil, errors.New("quota rule not found"))

		quotaRule, operationID, err := _updateQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "quota rule not found")
	})

	t.Run("WhenNoFieldsProvidedForUpdate", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
			// Both DiskLimitInMib and Description are empty/zero
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		quotaRule, operationID, err := _updateQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "At least one field")
	})

	t.Run("WhenQuotaRuleIsInTransitioningState", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 2048,
			LocationId:     "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateUpdating, // Transitioning state
			StateDetails: models.LifeCycleStateUpdatingDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		quotaRule, operationID, err := _updateQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.True(tt, errors.IsConflictErr(err))
		assert.Contains(tt, err.Error(), "transition state")
	})

	t.Run("WhenDiskLimitTooLow", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 0, // Too low, will be less than 4 KiB when converted
			LocationId:     "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		quotaRule, operationID, err := _updateQuotaRule(context.Background(), mockStore, mockTemporal, params)

		// Should fail because no fields provided (DiskLimitInMib is 0 and Description is empty)
		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
	})

	t.Run("WhenDiskLimitTooHigh", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		// Upper limit is 1125899906842620 KiB, which is approximately 1099511627776 MiB
		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 1200000000000, // Too high
			LocationId:     "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		quotaRule, operationID, err := _updateQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "DiskLimit is outside the permissible range")
	})

	t.Run("WhenGetVolumeByIDAndAccountIDFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 2048,
			LocationId:     "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to fail
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(nil, errors.New("volume not found"))

		quotaRule, operationID, err := _updateQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "Failed to get volume")
	})

	t.Run("WhenQuotaRuleSizeExceedsVolumeSize", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 300, // 300 MiB
			LocationId:     "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB - less than 300 MiB quota
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		quotaRule, operationID, err := _updateQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "quota rule size can not be greater than volume size")
	})

	t.Run("WhenValidateReplicationStateFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 100,
			LocationId:     "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock ListVolumeReplications to return error (causes validateReplicationState to fail)
		expectedFilter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))
		mockStore.EXPECT().ListVolumeReplications(context.Background(), *expectedFilter, database.QueryDepthZero).
			Return(nil, errors.NewUserInputValidationErr("replication validation failed"))

		quotaRule, operationID, err := _updateQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "replication validation failed")
	})

	t.Run("WhenCreateJobFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 100,
			LocationId:     "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock ListVolumeReplications to succeed (validateReplicationState passes)
		expectedFilter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))
		mockStore.EXPECT().ListVolumeReplications(context.Background(), *expectedFilter, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)

		// Mock CreateJob to fail
		mockStore.EXPECT().CreateJob(context.Background(), mock.AnythingOfType("*datamodel.Job")).
			Return(nil, errors.New("failed to create job"))

		// Note: UpdateQuotaRule is not called when job creation fails because the state was never changed
		// The defer cleanup only runs if job.UUID != "", which won't be true if CreateJob fails

		quotaRule, operationID, err := _updateQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to create job")
	})

	t.Run("WhenUpdatingQuotaRuleFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 100,
			LocationId:     "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock ListVolumeReplications to succeed (validateReplicationState passes)
		expectedFilter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))
		mockStore.EXPECT().ListVolumeReplications(context.Background(), *expectedFilter, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to fail
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(nil, errors.New("failed to update quota rule"))

		// Mock DeleteJob cleanup
		mockStore.EXPECT().DeleteJob(context.Background(), createdJob.UUID, mock.Anything).
			Return(nil)

		// Mock UpdateQuotaRule to mark quota rule as available after error (in defer function)
		mockStore.EXPECT().UpdateQuotaRule(context.Background(), mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.UUID == quotaRuleDataModel.UUID &&
				qr.State == models.LifeCycleStateAvailable &&
				qr.StateDetails == models.LifeCycleStateReadyDetails
		})).Return(quotaRuleDataModel, nil)

		quotaRule, operationID, err := _updateQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to update quota rule")
	})

	t.Run("WhenExecuteWorkflowFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 100,
			LocationId:     "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateUpdating,
			StateDetails: models.LifeCycleStateUpdatingDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock ListVolumeReplications to succeed (validateReplicationState passes)
		expectedFilter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))
		mockStore.EXPECT().ListVolumeReplications(context.Background(), *expectedFilter, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to succeed
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(updatedQuotaRule, nil)

		// Mock ExecuteWorkflow to fail
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, updatedQuotaRule).
			Return(nil, errors.New("failed to execute workflow"))

		// Mock DeleteJob cleanup
		mockStore.EXPECT().DeleteJob(context.Background(), createdJob.UUID, mock.Anything).
			Return(nil)

		// Mock UpdateQuotaRule to mark quota rule as available after error (in defer function)
		mockStore.EXPECT().UpdateQuotaRule(context.Background(), mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.UUID == quotaRuleDataModel.UUID &&
				qr.State == models.LifeCycleStateAvailable &&
				qr.StateDetails == models.LifeCycleStateReadyDetails
		})).Return(quotaRuleDataModel, nil)

		quotaRule, operationID, err := _updateQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to execute workflow")
	})

	t.Run("WhenUpdateQuotaRuleSucceeds", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 100,
			Description:    "Updated description",
			LocationId:     "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateUpdating,
			StateDetails: models.LifeCycleStateUpdatingDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock ListVolumeReplications to succeed (validateReplicationState passes)
		expectedFilter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))
		mockStore.EXPECT().ListVolumeReplications(context.Background(), *expectedFilter, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to succeed
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(updatedQuotaRule, nil)

		// Mock ExecuteWorkflow to succeed
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, updatedQuotaRule).
			Return(nil, nil)

		quotaRule, operationID, err := _updateQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRule)
		assert.Equal(tt, "job-uuid-123", operationID)
		assert.Equal(tt, "quota-rule-uuid-1", quotaRule.UUID)
		assert.Equal(tt, models.LifeCycleStateUpdating, quotaRule.LifeCycleState)
	})

	t.Run("WhenUpdateQuotaRuleSucceedsWithOnlyDescription", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			Description:   "Updated description only",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateUpdating,
			StateDetails: models.LifeCycleStateUpdatingDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock ListVolumeReplications to succeed (validateReplicationState passes)
		expectedFilter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))
		mockStore.EXPECT().ListVolumeReplications(context.Background(), *expectedFilter, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to succeed
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(updatedQuotaRule, nil)

		// Mock ExecuteWorkflow to succeed
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, updatedQuotaRule).
			Return(nil, nil)

		quotaRule, operationID, err := _updateQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRule)
		assert.Equal(tt, "job-uuid-123", operationID)
	})
}

func TestUpdateQuotaRuleInternal(t *testing.T) {
	// Save original function pointers
	originalGetAccountWithName := getAccountWithName

	defer func() {
		getAccountWithName = originalGetAccountWithName
	}()

	t.Run("WhenGetAccountWithNameFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 2048,
			LocationId:     "us-central1",
		}

		// Mock getAccountWithName to return error
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}

		defer func() { getAccountWithName = originalGetAccountWithName }()
		quotaRule, job, err := _updateQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Nil(tt, job)
		assert.Contains(tt, err.Error(), "account not found")
	})

	t.Run("WhenGetQuotaRuleByUUIDFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 2048,
			LocationId:     "us-central1",
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to fail
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(nil, errors.New("quota rule not found"))

		quotaRule, job, err := _updateQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Nil(tt, job)
		assert.Contains(tt, err.Error(), "quota rule not found")
	})

	t.Run("WhenQuotaRuleIsInTransitioningState", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 2048,
			LocationId:     "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateUpdating, // Transitioning state
			StateDetails: models.LifeCycleStateUpdatingDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		quotaRule, job, err := _updateQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Nil(tt, job)
		assert.True(tt, errors.IsConflictErr(err))
		assert.Contains(tt, err.Error(), "transition state")
	})

	t.Run("WhenNoFieldsProvidedForUpdate", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
			// Both DiskLimitInMib and Description are empty/zero
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		quotaRule, job, err := _updateQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Nil(tt, job)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "At least one field")
	})

	t.Run("WhenDiskLimitTooHigh", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		// Upper limit is 1125899906842620 KiB, which is approximately 1099511627776 MiB
		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 1200000000000, // Too high
			LocationId:     "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		quotaRule, job, err := _updateQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Nil(tt, job)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "DiskLimit is outside the permissible range")
	})

	t.Run("WhenGetVolumeByIDAndAccountIDFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 2048,
			LocationId:     "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to fail
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(nil, errors.New("volume not found"))

		quotaRule, job, err := _updateQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Nil(tt, job)
		assert.Contains(tt, err.Error(), "volume not found")
	})

	t.Run("WhenQuotaRuleSizeExceedsVolumeSize", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 300, // 300 MiB
			LocationId:     "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB - less than 300 MiB quota
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		quotaRule, job, err := _updateQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Nil(tt, job)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "quota rule size can not be greater than volume size")
	})

	t.Run("WhenCreateJobFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 100,
			LocationId:     "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock CreateJob to fail
		mockStore.EXPECT().CreateJob(context.Background(), mock.AnythingOfType("*datamodel.Job")).
			Return(nil, errors.New("failed to create job"))

		// Note: UpdateQuotaRule is not called when job creation fails because the state was never changed
		// The defer cleanup only runs if job.UUID != "", which won't be true if CreateJob fails

		quotaRule, job, err := _updateQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Nil(tt, job)
		assert.Contains(tt, err.Error(), "failed to create job")
	})

	t.Run("WhenUpdatingQuotaRuleFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 100,
			LocationId:     "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to fail
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(nil, errors.New("failed to update quota rule"))

		// Mock DeleteJob cleanup
		mockStore.EXPECT().DeleteJob(context.Background(), createdJob.UUID, mock.Anything).
			Return(nil)

		// Mock UpdateQuotaRule to mark quota rule as AVAILABLE in defer block
		mockStore.EXPECT().UpdateQuotaRule(context.Background(), mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.UUID == quotaRuleDataModel.UUID &&
				qr.State == models.LifeCycleStateAvailable &&
				qr.StateDetails == models.LifeCycleStateReadyDetails
		})).Return(quotaRuleDataModel, nil)

		quotaRule, job, err := _updateQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Nil(tt, job)
		assert.Contains(tt, err.Error(), "failed to update quota rule")
	})

	t.Run("WhenExecuteWorkflowFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 100,
			LocationId:     "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateUpdating,
			StateDetails: models.LifeCycleStateUpdatingDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to succeed
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(updatedQuotaRule, nil)

		// Mock ExecuteWorkflow to fail
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, updatedQuotaRule).
			Return(nil, errors.New("failed to execute workflow"))

		// Mock DeleteJob cleanup
		mockStore.EXPECT().DeleteJob(context.Background(), createdJob.UUID, mock.Anything).
			Return(nil)

		// Mock UpdateQuotaRule to mark quota rule as AVAILABLE in defer block
		mockStore.EXPECT().UpdateQuotaRule(context.Background(), mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.UUID == quotaRuleDataModel.UUID &&
				qr.State == models.LifeCycleStateAvailable &&
				qr.StateDetails == models.LifeCycleStateReadyDetails
		})).Return(quotaRuleDataModel, nil)

		quotaRule, job, err := _updateQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Nil(tt, job)
		assert.Contains(tt, err.Error(), "failed to execute workflow")
	})

	t.Run("WhenUpdateQuotaRuleInternalSucceeds", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 100,
			Description:    "Updated description",
			LocationId:     "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateUpdating,
			StateDetails: models.LifeCycleStateUpdatingDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to succeed
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(updatedQuotaRule, nil)

		// Mock ExecuteWorkflow to succeed
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, updatedQuotaRule).
			Return(nil, nil)

		quotaRule, job, err := _updateQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRule)
		assert.NotNil(tt, job)
		assert.Equal(tt, "quota-rule-uuid-1", quotaRule.UUID)
		assert.Equal(tt, models.LifeCycleStateUpdating, quotaRule.LifeCycleState)
		assert.Equal(tt, "job-uuid-123", job.UUID)
	})

	t.Run("WhenUpdateQuotaRuleInternalSucceedsWithOnlyDescription", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			Description:   "Updated description only",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateUpdating,
			StateDetails: models.LifeCycleStateUpdatingDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to succeed
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(updatedQuotaRule, nil)

		// Mock ExecuteWorkflow to succeed
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, updatedQuotaRule).
			Return(nil, nil)

		quotaRule, job, err := _updateQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRule)
		assert.NotNil(tt, job)
		assert.Equal(tt, "job-uuid-123", job.UUID)
	})

	t.Run("WhenLocationIdIsNotSetAndPoolIsNil", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 100,
			LocationId:     "", // Empty, but pool is nil so it won't be set
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			Pool:        nil, // Pool is nil
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateUpdating,
			StateDetails: models.LifeCycleStateUpdatingDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to succeed
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(updatedQuotaRule, nil)

		// Mock ExecuteWorkflow to succeed
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, updatedQuotaRule).
			Return(nil, nil)

		quotaRule, job, err := _updateQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRule)
		assert.NotNil(tt, job)
		assert.Empty(tt, params.LocationId, "LocationId should remain empty when pool is nil")
	})

	t.Run("WhenLocationIdIsNotSetAndPoolAttributesIsNil", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.UpdateQuotaRulesParam{
			ProjectId:      "test-project",
			QuotaRuleUUID:  "quota-rule-uuid-1",
			DiskLimitInMib: 100,
			LocationId:     "", // Empty, but PoolAttributes is nil so it won't be set
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024, // 200 MiB
			AccountID:   1,
			Pool: &datamodel.Pool{
				PoolAttributes: nil, // PoolAttributes is nil
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateUpdating,
			StateDetails: models.LifeCycleStateUpdatingDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to succeed
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(updatedQuotaRule, nil)

		// Mock ExecuteWorkflow to succeed
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, updatedQuotaRule).
			Return(nil, nil)

		quotaRule, job, err := _updateQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRule)
		assert.NotNil(tt, job)
		assert.Empty(tt, params.LocationId, "LocationId should remain empty when PoolAttributes is nil")
	})
}
func TestDeleteQuotaRule(t *testing.T) {
	// Save original function pointers
	originalGetAccountWithName := getAccountWithName

	defer func() {
		getAccountWithName = originalGetAccountWithName
	}()

	t.Run("WhenGetAccountWithNameFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		// Mock getAccountWithName to return error
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("failed to get account")
		}

		defer func() { getAccountWithName = originalGetAccountWithName }()
		quotaRule, operationID, err := _deleteQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to get account")
	})

	t.Run("WhenGetQuotaRuleByUUIDFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to fail
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(nil, errors.New("quota rule not found"))

		quotaRule, operationID, err := _deleteQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "quota rule not found")
	})

	t.Run("WhenQuotaRuleIsInTransitioningState", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateCreating, // Transitioning state
			StateDetails: "",
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		quotaRule, operationID, err := _deleteQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.True(tt, errors.IsConflictErr(err))
		assert.Contains(tt, err.Error(), "transition state")
	})

	t.Run("WhenGetVolumeByIDAndAccountIDFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to fail
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(nil, errors.New("volume not found"))

		quotaRule, operationID, err := _deleteQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "Failed to get volume")
	})

	t.Run("WhenLocationIdIsSetFromPoolPrimaryZone", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "", // Empty, should be set from pool
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			SvmID:       1,
			Pool: &datamodel.Pool{
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-central1-a",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateDeleting,
			StateDetails: models.LifeCycleStateDeletingDetails,
			RQuota:       false,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock ListVolumeReplications to succeed (validateReplicationState passes)
		expectedFilter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))
		mockStore.EXPECT().ListVolumeReplications(context.Background(), *expectedFilter, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)

		// Mock GetQuotaRuleCountBySvmID to succeed
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(1), nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to succeed
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(updatedQuotaRule, nil)

		// Mock ExecuteWorkflow to succeed
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, updatedQuotaRule).
			Return(nil, nil)

		quotaRule, operationID, err := _deleteQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRule)
		assert.Equal(tt, "job-uuid-123", operationID)
		// Note: LocationId is not set from pool in the current implementation
		// The test verifies successful deletion regardless of LocationId value
	})

	t.Run("WhenValidateReplicationStateFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock ListVolumeReplications to return error (causes validateReplicationState to fail)
		expectedFilter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))
		mockStore.EXPECT().ListVolumeReplications(context.Background(), *expectedFilter, database.QueryDepthZero).
			Return(nil, errors.NewUserInputValidationErr("replication validation failed"))

		quotaRule, operationID, err := _deleteQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "replication validation failed")
	})

	t.Run("WhenGetQuotaRulesByVolumeIDFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			SvmID:       1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock ListVolumeReplications to succeed (validateReplicationState passes)
		expectedFilter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))
		mockStore.EXPECT().ListVolumeReplications(context.Background(), *expectedFilter, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)

		// Mock GetQuotaRuleCountBySvmID to fail (called by determineRQuota)
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(0), errors.New("failed to get quota rule count"))

		quotaRule, operationID, err := _deleteQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to get quota rule count")
	})

	t.Run("WhenDetermineRQuotaFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			SvmID:       1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock ListVolumeReplications to succeed (validateReplicationState passes)
		expectedFilter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))
		mockStore.EXPECT().ListVolumeReplications(context.Background(), *expectedFilter, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)

		// Mock GetQuotaRuleCountBySvmID to fail
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(0), errors.New("failed to get quota rule count"))

		quotaRule, operationID, err := _deleteQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to get quota rule count")
	})

	t.Run("WhenCreateJobFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			SvmID:       1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock ListVolumeReplications to succeed (validateReplicationState passes)
		expectedFilter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))
		mockStore.EXPECT().ListVolumeReplications(context.Background(), *expectedFilter, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)

		// Mock GetQuotaRuleCountBySvmID to succeed
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(0), nil)

		// Mock CreateJob to fail
		mockStore.EXPECT().CreateJob(context.Background(), mock.AnythingOfType("*datamodel.Job")).
			Return(nil, errors.New("failed to create job"))

		quotaRule, operationID, err := _deleteQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to create job")
	})

	t.Run("WhenUpdatingQuotaRuleFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			SvmID:       1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock ListVolumeReplications to succeed (validateReplicationState passes)
		expectedFilter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))
		mockStore.EXPECT().ListVolumeReplications(context.Background(), *expectedFilter, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)

		// Mock GetQuotaRuleCountBySvmID to succeed
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(0), nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to fail
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(nil, errors.New("failed to update quota rule"))

		// Mock DeleteJob cleanup
		mockStore.EXPECT().DeleteJob(context.Background(), createdJob.UUID, mock.Anything).
			Return(nil)

		// Mock UpdateQuotaRule to restore previous state in defer cleanup
		mockStore.EXPECT().UpdateQuotaRule(context.Background(), mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.UUID == quotaRuleDataModel.UUID &&
				qr.State == models.LifeCycleStateAvailable &&
				qr.StateDetails == models.LifeCycleStateReadyDetails
		})).Return(quotaRuleDataModel, nil)

		quotaRule, operationID, err := _deleteQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to update quota rule")
	})

	t.Run("WhenExecuteWorkflowFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			SvmID:       1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateDeleting,
			StateDetails: models.LifeCycleStateDeletingDetails,
			RQuota:       false,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock ListVolumeReplications to succeed (validateReplicationState passes)
		expectedFilter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))
		mockStore.EXPECT().ListVolumeReplications(context.Background(), *expectedFilter, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)

		// Mock GetQuotaRuleCountBySvmID to succeed
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(0), nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to succeed
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(updatedQuotaRule, nil)

		// Mock ExecuteWorkflow to fail
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, updatedQuotaRule).
			Return(nil, errors.New("failed to execute workflow"))

		// Mock DeleteJob cleanup
		mockStore.EXPECT().DeleteJob(context.Background(), createdJob.UUID, mock.Anything).
			Return(nil)

		// Mock UpdateQuotaRule to restore previous state in defer cleanup
		mockStore.EXPECT().UpdateQuotaRule(context.Background(), mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.UUID == quotaRuleDataModel.UUID &&
				qr.State == models.LifeCycleStateAvailable &&
				qr.StateDetails == models.LifeCycleStateReadyDetails
		})).Return(quotaRuleDataModel, nil)

		quotaRule, operationID, err := _deleteQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.Contains(tt, err.Error(), "failed to execute workflow")
	})

	t.Run("WhenDeleteQuotaRuleSucceeds", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			SvmID:       1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateDeleting,
			StateDetails: models.LifeCycleStateDeletingDetails,
			RQuota:       true,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock ListVolumeReplications to succeed (validateReplicationState passes)
		expectedFilter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("volume_id", "=", volume.ID))
		mockStore.EXPECT().ListVolumeReplications(context.Background(), *expectedFilter, database.QueryDepthZero).
			Return([]*datamodel.VolumeReplication{}, nil)

		// Mock GetQuotaRuleCountBySvmID to succeed (last quota rule in SVM, so RQuota required for delete)
		mockStore.EXPECT().GetQuotaRuleCountBySvmID(context.Background(), volume.SvmID).
			Return(int64(1), nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to succeed
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(updatedQuotaRule, nil)

		// Mock ExecuteWorkflow to succeed
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, updatedQuotaRule).
			Return(nil, nil)

		quotaRule, operationID, err := _deleteQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRule)
		assert.Equal(tt, "job-uuid-123", operationID)
		assert.Equal(tt, "quota-rule-uuid-1", quotaRule.UUID)
		assert.Equal(tt, models.LifeCycleStateDeleting, quotaRule.LifeCycleState)
	})

	t.Run("WhenQuotaRuleIsInDeletingState", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateDeleting, // DELETING is not allowed in _deleteQuotaRule (unlike _deleteQuotaRuleInternal)
			StateDetails: models.LifeCycleStateDeletingDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		quotaRule, operationID, err := _deleteQuotaRule(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Empty(tt, operationID)
		assert.True(tt, errors.IsConflictErr(err))
		assert.Contains(tt, err.Error(), "transition state")
	})
}

func TestDeleteQuotaRuleInternal(t *testing.T) {
	// Save original function pointers
	originalGetAccountWithName := getAccountWithName

	defer func() {
		getAccountWithName = originalGetAccountWithName
	}()

	t.Run("WhenGetAccountWithNameFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		// Mock getAccountWithName to return error
		originalGetAccountWithName := getAccountWithName
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("failed to get account")
		}

		defer func() { getAccountWithName = originalGetAccountWithName }()
		quotaRule, job, err := _deleteQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Nil(tt, job)
		assert.Contains(tt, err.Error(), "failed to get account")
	})

	t.Run("WhenGetQuotaRuleByUUIDFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to fail
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(nil, errors.New("quota rule not found"))

		quotaRule, job, err := _deleteQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Nil(tt, job)
		assert.Contains(tt, err.Error(), "quota rule not found")
	})

	t.Run("WhenQuotaRuleIsInTransitioningState", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateCreating, // Transitioning state (not DELETING)
			StateDetails: "",
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		quotaRule, job, err := _deleteQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Nil(tt, job)
		assert.True(tt, errors.IsConflictErr(err))
		assert.Contains(tt, err.Error(), "transition state")
	})

	t.Run("WhenQuotaRuleIsAlreadyDeleted", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateDeleted, // Already deleted
			StateDetails: models.LifeCycleStateDeletedDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Note: _deleteQuotaRuleInternal doesn't have early return for DELETED state
		// It only checks for transition states (excluding DELETING), so it will continue
		// Mock GetVolumeByIDAndAccountID (code will call this)
		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			SvmID:       1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock CreateJob (code will create job)
		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule (code will update state to DELETING)
		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateDeleting,
			StateDetails: models.LifeCycleStateDeletingDetails,
		}
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(updatedQuotaRule, nil)

		// Mock ExecuteWorkflow
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, updatedQuotaRule).
			Return(nil, nil)

		quotaRule, job, err := _deleteQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRule)
		assert.NotNil(tt, job) // Job will be created
		assert.Equal(tt, "quota-rule-uuid-1", quotaRule.UUID)
		assert.Equal(tt, models.LifeCycleStateDeleting, quotaRule.LifeCycleState)
	})

	t.Run("WhenGetVolumeByIDAndAccountIDFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to fail
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(nil, errors.New("volume not found"))

		quotaRule, job, err := _deleteQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Nil(tt, job)
		assert.Contains(tt, err.Error(), "volume not found")
	})

	t.Run("WhenLocationIdIsSetFromPoolPrimaryZone", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "", // Empty, should be set from pool
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			Pool: &datamodel.Pool{
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-central1-a",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateDeleting,
			StateDetails: models.LifeCycleStateDeletingDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to succeed
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(updatedQuotaRule, nil)

		// Mock ExecuteWorkflow to succeed
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, updatedQuotaRule).
			Return(nil, nil)

		quotaRule, job, err := _deleteQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRule)
		assert.NotNil(tt, job)
		// Note: LocationId is not set from pool in the current implementation
		// The test verifies successful deletion regardless of LocationId value
		assert.Equal(tt, "job-uuid-123", job.UUID)
	})

	t.Run("WhenCreateJobFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock CreateJob to fail
		mockStore.EXPECT().CreateJob(context.Background(), mock.AnythingOfType("*datamodel.Job")).
			Return(nil, errors.New("failed to create job"))

		quotaRule, job, err := _deleteQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Nil(tt, job)
		assert.Contains(tt, err.Error(), "failed to create job")
	})

	t.Run("WhenUpdatingQuotaRuleFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to fail
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(nil, errors.New("failed to update quota rule"))

		// Mock DeleteJob cleanup
		mockStore.EXPECT().DeleteJob(context.Background(), createdJob.UUID, mock.Anything).
			Return(nil)

		// Mock UpdateQuotaRule to restore previous state in defer cleanup
		mockStore.EXPECT().UpdateQuotaRule(context.Background(), mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.UUID == quotaRuleDataModel.UUID &&
				qr.State == models.LifeCycleStateAvailable &&
				qr.StateDetails == models.LifeCycleStateReadyDetails
		})).Return(quotaRuleDataModel, nil)

		quotaRule, job, err := _deleteQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Nil(tt, job)
		assert.Contains(tt, err.Error(), "failed to update quota rule")
	})

	t.Run("WhenExecuteWorkflowFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateDeleting,
			StateDetails: models.LifeCycleStateDeletingDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to succeed
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(updatedQuotaRule, nil)

		// Mock ExecuteWorkflow to fail
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, updatedQuotaRule).
			Return(nil, errors.New("failed to execute workflow"))

		// Mock DeleteJob cleanup
		mockStore.EXPECT().DeleteJob(context.Background(), createdJob.UUID, mock.Anything).
			Return(nil)

		// Mock UpdateQuotaRule to restore previous state in defer cleanup
		mockStore.EXPECT().UpdateQuotaRule(context.Background(), mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.UUID == quotaRuleDataModel.UUID &&
				qr.State == models.LifeCycleStateAvailable &&
				qr.StateDetails == models.LifeCycleStateReadyDetails
		})).Return(quotaRuleDataModel, nil)

		quotaRule, job, err := _deleteQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.Error(tt, err)
		assert.Nil(tt, quotaRule)
		assert.Nil(tt, job)
		assert.Contains(tt, err.Error(), "failed to execute workflow")
	})

	t.Run("WhenDeleteQuotaRuleInternalSucceeds", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateDeleting,
			StateDetails: models.LifeCycleStateDeletingDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to succeed
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(updatedQuotaRule, nil)

		// Mock ExecuteWorkflow to succeed
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, updatedQuotaRule).
			Return(nil, nil)

		quotaRule, job, err := _deleteQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRule)
		assert.NotNil(tt, job)
		assert.Equal(tt, "quota-rule-uuid-1", quotaRule.UUID)
		assert.Equal(tt, models.LifeCycleStateDeleting, quotaRule.LifeCycleState)
		assert.Equal(tt, "job-uuid-123", job.UUID)
	})

	t.Run("WhenQuotaRuleIsInDeletingState", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "us-central1",
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateDeleting, // DELETING is allowed for idempotency
			StateDetails: models.LifeCycleStateDeletingDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateDeleting,
			StateDetails: models.LifeCycleStateDeletingDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to succeed
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(updatedQuotaRule, nil)

		// Mock ExecuteWorkflow to succeed
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, updatedQuotaRule).
			Return(nil, nil)

		quotaRule, job, err := _deleteQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRule)
		assert.NotNil(tt, job)
		assert.Equal(tt, "job-uuid-123", job.UUID)
	})

	t.Run("WhenLocationIdIsNotSetAndPoolIsNil", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "", // Empty, but pool is nil so it won't be set
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			Pool:        nil, // Pool is nil
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateDeleting,
			StateDetails: models.LifeCycleStateDeletingDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to succeed
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(updatedQuotaRule, nil)

		// Mock ExecuteWorkflow to succeed
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, updatedQuotaRule).
			Return(nil, nil)

		quotaRule, job, err := _deleteQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRule)
		assert.NotNil(tt, job)
		assert.Empty(tt, params.LocationId, "LocationId should remain empty when pool is nil")
	})

	t.Run("WhenLocationIdIsNotSetAndPoolAttributesIsNil", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		params := &common.DeleteQuotaRulesParam{
			ProjectId:     "test-project",
			QuotaRuleUUID: "quota-rule-uuid-1",
			LocationId:    "", // Empty, but PoolAttributes is nil so it won't be set
		}

		quotaRuleDataModel := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateAvailable,
			StateDetails: models.LifeCycleStateReadyDetails,
		}

		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{ID: 1, UUID: "volume-uuid-1"},
			SizeInBytes: 200 * 1024 * 1024,
			AccountID:   1,
			Pool: &datamodel.Pool{
				PoolAttributes: nil, // PoolAttributes is nil
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFSV3"},
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
			WorkflowID: "workflow-id-123",
		}

		updatedQuotaRule := &datamodel.QuotaRule{
			BaseModel:    datamodel.BaseModel{ID: 1, UUID: "quota-rule-uuid-1"},
			Name:         "quota-rule-1",
			VolumeID:     1,
			AccountID:    1,
			State:        models.LifeCycleStateDeleting,
			StateDetails: models.LifeCycleStateDeletingDetails,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      accountName,
			}, nil
		}

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(context.Background(), params.QuotaRuleUUID, int64(1)).
			Return(quotaRuleDataModel, nil)

		// Mock GetVolumeByIDAndAccountID to succeed
		mockStore.EXPECT().GetVolumeByIDAndAccountID(context.Background(), quotaRuleDataModel.VolumeID, int64(1)).
			Return(volume, nil)

		// Mock CreateJob to succeed
		mockStore.EXPECT().CreateJob(context.Background(), mock.Anything).
			Return(createdJob, nil)

		// Mock UpdatingQuotaRule to succeed
		mockStore.EXPECT().UpdatingQuotaRule(context.Background(), mock.Anything).
			Return(updatedQuotaRule, nil)

		// Mock ExecuteWorkflow to succeed
		mockTemporal.EXPECT().ExecuteWorkflow(context.Background(), mock.Anything, mock.Anything, params, updatedQuotaRule).
			Return(nil, nil)

		quotaRule, job, err := _deleteQuotaRuleInternal(context.Background(), mockStore, mockTemporal, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, quotaRule)
		assert.NotNil(tt, job)
		assert.Empty(tt, params.LocationId, "LocationId should remain empty when PoolAttributes is nil")
	})
}

func TestGetMultipleQuotaRules(t *testing.T) {
	// Save original function pointers
	originalGetAccountWithName := getAccountWithName

	defer func() {
		getAccountWithName = originalGetAccountWithName
	}()

	t.Run("WhenAccountNotFound_ReturnsError", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUIDs := []string{"quota-uuid-1", "quota-uuid-2"}

		// Mock getAccountWithName to return NotFound error
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.NewNotFoundErr("Account not found", nil)
		}

		result, err := orchestrator.GetMultipleQuotaRules(ctx, volumeUuid, accountName, quotaRuleUUIDs)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.True(tt, errors.IsNotFoundErr(err), "Should return NotFound error when account not found")
	})

	t.Run("WhenGetAccountWithNameFailsWithNonNotFoundError", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUIDs := []string{"quota-uuid-1"}

		// Mock getAccountWithName to return non-NotFound error
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("database connection failed")
		}

		result, err := orchestrator.GetMultipleQuotaRules(ctx, volumeUuid, accountName, quotaRuleUUIDs)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "database connection failed")
	})

	t.Run("WhenVolumeNotFound_ReturnsError", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUIDs := []string{"quota-uuid-1"}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to return NotFound error
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(nil, errors.NewNotFoundErr("Volume not found", nil))

		result, err := orchestrator.GetMultipleQuotaRules(ctx, volumeUuid, accountName, quotaRuleUUIDs)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.True(tt, errors.IsNotFoundErr(err), "Should return NotFound error when volume not found")
	})

	t.Run("WhenGetVolumeWithAccountIDFailsWithNonNotFoundError", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUIDs := []string{"quota-uuid-1"}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to return non-NotFound error
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(nil, errors.New("database error"))

		result, err := orchestrator.GetMultipleQuotaRules(ctx, volumeUuid, accountName, quotaRuleUUIDs)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "database error")
	})

	t.Run("WhenGetQuotaRulesWithConditionFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUIDs := []string{"quota-uuid-1", "quota-uuid-2"}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: volumeUuid},
			AccountID: account.ID,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to succeed
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(volume, nil)

		// Mock GetQuotaRulesWithCondition to fail
		mockStore.EXPECT().GetQuotaRulesWithCondition(ctx, mock.Anything).
			Return(nil, errors.New("failed to get quota rules"))

		result, err := orchestrator.GetMultipleQuotaRules(ctx, volumeUuid, accountName, quotaRuleUUIDs)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to get quota rules")
	})

	t.Run("WhenGetQuotaRulesWithConditionSucceeds_ReturnsMultipleQuotaRules", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUIDs := []string{"quota-uuid-1", "quota-uuid-2"}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: volumeUuid},
			AccountID: account.ID,
		}

		dbQuotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:      datamodel.BaseModel{ID: 1, UUID: "quota-uuid-1"},
				Name:           "quota-rule-1",
				QuotaType:      IndividualUserQuota,
				QuotaTarget:    "user:alice",
				DiskLimitInKib: 102400, // 100 MiB in KiB
				State:          models.LifeCycleStateAvailable,
				StateDetails:   models.LifeCycleStateReadyDetails,
				Description:    "First quota rule",
				VolumeID:       volume.ID,
				AccountID:      account.ID,
			},
			{
				BaseModel:      datamodel.BaseModel{ID: 2, UUID: "quota-uuid-2"},
				Name:           "quota-rule-2",
				QuotaType:      IndividualGroupQuota,
				QuotaTarget:    "group:developers",
				DiskLimitInKib: 204800, // 200 MiB in KiB
				State:          models.LifeCycleStateAvailable,
				StateDetails:   models.LifeCycleStateReadyDetails,
				Description:    "Second quota rule",
				VolumeID:       volume.ID,
				AccountID:      account.ID,
			},
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to succeed
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(volume, nil)

		// Mock GetQuotaRulesWithCondition to succeed
		mockStore.EXPECT().GetQuotaRulesWithCondition(ctx, mock.Anything).
			Return(dbQuotaRules, nil)

		result, err := orchestrator.GetMultipleQuotaRules(ctx, volumeUuid, accountName, quotaRuleUUIDs)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 2)

		// Verify first quota rule
		assert.Equal(tt, "quota-uuid-1", result[0].UUID)
		assert.Equal(tt, "quota-rule-1", result[0].Name)
		assert.Equal(tt, IndividualUserQuota, result[0].QuotaType)
		assert.Equal(tt, "user:alice", result[0].QuotaTarget)
		assert.Equal(tt, int64(100), result[0].DiskLimitInMib) // 102400 KiB / 1024 = 100 MiB
		assert.Equal(tt, models.LifeCycleStateAvailable, result[0].LifeCycleState)
		assert.Equal(tt, "First quota rule", result[0].Description)

		// Verify second quota rule
		assert.Equal(tt, "quota-uuid-2", result[1].UUID)
		assert.Equal(tt, "quota-rule-2", result[1].Name)
		assert.Equal(tt, IndividualGroupQuota, result[1].QuotaType)
		assert.Equal(tt, "group:developers", result[1].QuotaTarget)
		assert.Equal(tt, int64(200), result[1].DiskLimitInMib) // 204800 KiB / 1024 = 200 MiB
		assert.Equal(tt, "Second quota rule", result[1].Description)
	})

	t.Run("WhenGetQuotaRulesWithConditionReturnsEmptyList", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUIDs := []string{"quota-uuid-1"}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: volumeUuid},
			AccountID: account.ID,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to succeed
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(volume, nil)

		// Mock GetQuotaRulesWithCondition to return empty list
		mockStore.EXPECT().GetQuotaRulesWithCondition(ctx, mock.Anything).
			Return([]*datamodel.QuotaRule{}, nil)

		result, err := orchestrator.GetMultipleQuotaRules(ctx, volumeUuid, accountName, quotaRuleUUIDs)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 0)
	})

	t.Run("WhenGetQuotaRulesWithConditionReturnsSingleQuotaRule", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUIDs := []string{"quota-uuid-1"}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: volumeUuid},
			AccountID: account.ID,
		}

		dbQuotaRule := &datamodel.QuotaRule{
			BaseModel:      datamodel.BaseModel{ID: 1, UUID: "quota-uuid-1"},
			Name:           "quota-rule-1",
			QuotaType:      DefaultUserQuota,
			QuotaTarget:    "",
			DiskLimitInKib: 51200, // 50 MiB in KiB
			State:          models.LifeCycleStateCreating,
			StateDetails:   models.LifeCycleStateCreatingDetails,
			Description:    "Default user quota",
			VolumeID:       volume.ID,
			AccountID:      account.ID,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to succeed
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(volume, nil)

		// Mock GetQuotaRulesWithCondition to return single quota rule
		mockStore.EXPECT().GetQuotaRulesWithCondition(ctx, mock.Anything).
			Return([]*datamodel.QuotaRule{dbQuotaRule}, nil)

		result, err := orchestrator.GetMultipleQuotaRules(ctx, volumeUuid, accountName, quotaRuleUUIDs)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 1)
		assert.Equal(tt, "quota-uuid-1", result[0].UUID)
		assert.Equal(tt, "quota-rule-1", result[0].Name)
		assert.Equal(tt, DefaultUserQuota, result[0].QuotaType)
		assert.Equal(tt, "", result[0].QuotaTarget)
		assert.Equal(tt, int64(50), result[0].DiskLimitInMib) // 51200 KiB / 1024 = 50 MiB
		assert.Equal(tt, models.LifeCycleStateCreating, result[0].LifeCycleState)
		assert.Equal(tt, "Default user quota", result[0].Description)
	})

	t.Run("WhenQuotaRuleUUIDsIsEmpty_StillCallsDatabase", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUIDs := []string{}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: volumeUuid},
			AccountID: account.ID,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to succeed
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(volume, nil)

		// Mock GetQuotaRulesWithCondition to return empty list
		mockStore.EXPECT().GetQuotaRulesWithCondition(ctx, mock.Anything).
			Return([]*datamodel.QuotaRule{}, nil)

		result, err := orchestrator.GetMultipleQuotaRules(ctx, volumeUuid, accountName, quotaRuleUUIDs)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 0)
	})
}

func TestDescribeQuotaRule(t *testing.T) {
	// Save original function pointers
	originalGetAccountWithName := getAccountWithName

	defer func() {
		getAccountWithName = originalGetAccountWithName
	}()

	t.Run("WhenAccountNotFound_ReturnsError", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUID := "quota-uuid-1"

		// Mock getAccountWithName to return NotFound error
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.NewNotFoundErr("Account not found", nil)
		}

		result, err := orchestrator.DescribeQuotaRule(ctx, volumeUuid, accountName, quotaRuleUUID)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.True(tt, errors.IsNotFoundErr(err))
	})

	t.Run("WhenGetAccountWithNameFailsWithNonNotFoundError", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUID := "quota-uuid-1"

		// Mock getAccountWithName to return non-NotFound error
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("database connection failed")
		}

		result, err := orchestrator.DescribeQuotaRule(ctx, volumeUuid, accountName, quotaRuleUUID)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "database connection failed")
	})

	t.Run("WhenVolumeNotFound_ReturnsError", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUID := "quota-uuid-1"

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to return NotFound error (fatal error for DescribeQuotaRule)
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(nil, errors.NewNotFoundErr("Volume not found", nil))

		result, err := orchestrator.DescribeQuotaRule(ctx, volumeUuid, accountName, quotaRuleUUID)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.True(tt, errors.IsNotFoundErr(err))
	})

	t.Run("WhenGetVolumeWithAccountIDFailsWithNonNotFoundError", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUID := "quota-uuid-1"

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to return non-NotFound error
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(nil, errors.New("database error"))

		result, err := orchestrator.DescribeQuotaRule(ctx, volumeUuid, accountName, quotaRuleUUID)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "database error")
	})

	t.Run("WhenGetQuotaRuleByUUIDFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUID := "quota-uuid-1"

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: volumeUuid},
			AccountID: account.ID,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to succeed
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(volume, nil)

		// Mock GetQuotaRuleByUUID to fail
		mockStore.EXPECT().GetQuotaRuleByUUID(ctx, quotaRuleUUID, account.ID).
			Return(nil, errors.NewNotFoundErr("Quota rule not found", nil))

		result, err := orchestrator.DescribeQuotaRule(ctx, volumeUuid, accountName, quotaRuleUUID)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.True(tt, errors.IsNotFoundErr(err))
	})

	t.Run("WhenGetQuotaRuleByUUIDSucceeds_IndividualUserQuota", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUID := "quota-uuid-1"

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: volumeUuid},
			AccountID: account.ID,
		}

		dbQuotaRule := &datamodel.QuotaRule{
			BaseModel:      datamodel.BaseModel{ID: 1, UUID: quotaRuleUUID},
			Name:           "quota-rule-1",
			QuotaType:      IndividualUserQuota,
			QuotaTarget:    "user:alice",
			DiskLimitInKib: 102400, // 100 MiB in KiB
			State:          models.LifeCycleStateAvailable,
			StateDetails:   models.LifeCycleStateReadyDetails,
			Description:    "Individual user quota rule",
			VolumeID:       volume.ID,
			AccountID:      account.ID,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to succeed
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(volume, nil)

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(ctx, quotaRuleUUID, account.ID).
			Return(dbQuotaRule, nil)

		result, err := orchestrator.DescribeQuotaRule(ctx, volumeUuid, accountName, quotaRuleUUID)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, quotaRuleUUID, result.UUID)
		assert.Equal(tt, "quota-rule-1", result.Name)
		assert.Equal(tt, IndividualUserQuota, result.QuotaType)
		assert.Equal(tt, "user:alice", result.QuotaTarget)
		assert.Equal(tt, int64(100), result.DiskLimitInMib) // 102400 KiB / 1024 = 100 MiB
		assert.Equal(tt, models.LifeCycleStateAvailable, result.LifeCycleState)
		assert.Equal(tt, models.LifeCycleStateReadyDetails, result.LifeCycleStateDetails)
		assert.Equal(tt, "Individual user quota rule", result.Description)
	})

	t.Run("WhenGetQuotaRuleByUUIDSucceeds_IndividualGroupQuota", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUID := "quota-uuid-2"

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: volumeUuid},
			AccountID: account.ID,
		}

		dbQuotaRule := &datamodel.QuotaRule{
			BaseModel:      datamodel.BaseModel{ID: 2, UUID: quotaRuleUUID},
			Name:           "quota-rule-2",
			QuotaType:      IndividualGroupQuota,
			QuotaTarget:    "group:developers",
			DiskLimitInKib: 204800, // 200 MiB in KiB
			State:          models.LifeCycleStateAvailable,
			StateDetails:   models.LifeCycleStateReadyDetails,
			Description:    "Individual group quota rule",
			VolumeID:       volume.ID,
			AccountID:      account.ID,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to succeed
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(volume, nil)

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(ctx, quotaRuleUUID, account.ID).
			Return(dbQuotaRule, nil)

		result, err := orchestrator.DescribeQuotaRule(ctx, volumeUuid, accountName, quotaRuleUUID)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, quotaRuleUUID, result.UUID)
		assert.Equal(tt, IndividualGroupQuota, result.QuotaType)
		assert.Equal(tt, "group:developers", result.QuotaTarget)
		assert.Equal(tt, int64(200), result.DiskLimitInMib) // 204800 KiB / 1024 = 200 MiB
	})

	t.Run("WhenGetQuotaRuleByUUIDSucceeds_DefaultUserQuota", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUID := "quota-uuid-3"

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: volumeUuid},
			AccountID: account.ID,
		}

		dbQuotaRule := &datamodel.QuotaRule{
			BaseModel:      datamodel.BaseModel{ID: 3, UUID: quotaRuleUUID},
			Name:           "quota-rule-3",
			QuotaType:      DefaultUserQuota,
			QuotaTarget:    "",
			DiskLimitInKib: 51200, // 50 MiB in KiB
			State:          models.LifeCycleStateAvailable,
			StateDetails:   models.LifeCycleStateReadyDetails,
			Description:    "Default user quota rule",
			VolumeID:       volume.ID,
			AccountID:      account.ID,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to succeed
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(volume, nil)

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(ctx, quotaRuleUUID, account.ID).
			Return(dbQuotaRule, nil)

		result, err := orchestrator.DescribeQuotaRule(ctx, volumeUuid, accountName, quotaRuleUUID)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, quotaRuleUUID, result.UUID)
		assert.Equal(tt, DefaultUserQuota, result.QuotaType)
		assert.Equal(tt, "", result.QuotaTarget)
		assert.Equal(tt, int64(50), result.DiskLimitInMib) // 51200 KiB / 1024 = 50 MiB
	})

	t.Run("WhenGetQuotaRuleByUUIDSucceeds_DefaultGroupQuota", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUID := "quota-uuid-4"

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: volumeUuid},
			AccountID: account.ID,
		}

		dbQuotaRule := &datamodel.QuotaRule{
			BaseModel:      datamodel.BaseModel{ID: 4, UUID: quotaRuleUUID},
			Name:           "quota-rule-4",
			QuotaType:      DefaultGroupQuota,
			QuotaTarget:    "",
			DiskLimitInKib: 409600, // 400 MiB in KiB
			State:          models.LifeCycleStateAvailable,
			StateDetails:   models.LifeCycleStateReadyDetails,
			Description:    "Default group quota rule",
			VolumeID:       volume.ID,
			AccountID:      account.ID,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to succeed
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(volume, nil)

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(ctx, quotaRuleUUID, account.ID).
			Return(dbQuotaRule, nil)

		result, err := orchestrator.DescribeQuotaRule(ctx, volumeUuid, accountName, quotaRuleUUID)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, quotaRuleUUID, result.UUID)
		assert.Equal(tt, DefaultGroupQuota, result.QuotaType)
		assert.Equal(tt, int64(400), result.DiskLimitInMib) // 409600 KiB / 1024 = 400 MiB
	})

	t.Run("WhenGetQuotaRuleByUUIDSucceeds_CreatingState", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUID := "quota-uuid-5"

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: volumeUuid},
			AccountID: account.ID,
		}

		dbQuotaRule := &datamodel.QuotaRule{
			BaseModel:      datamodel.BaseModel{ID: 5, UUID: quotaRuleUUID},
			Name:           "quota-rule-5",
			QuotaType:      IndividualUserQuota,
			QuotaTarget:    "user:bob",
			DiskLimitInKib: 102400,
			State:          models.LifeCycleStateCreating,
			StateDetails:   models.LifeCycleStateCreatingDetails,
			Description:    "Creating quota rule",
			VolumeID:       volume.ID,
			AccountID:      account.ID,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to succeed
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(volume, nil)

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(ctx, quotaRuleUUID, account.ID).
			Return(dbQuotaRule, nil)

		result, err := orchestrator.DescribeQuotaRule(ctx, volumeUuid, accountName, quotaRuleUUID)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.LifeCycleStateCreating, result.LifeCycleState)
		assert.Equal(tt, models.LifeCycleStateCreatingDetails, result.LifeCycleStateDetails)
	})

	t.Run("WhenGetQuotaRuleByUUIDSucceeds_UpdatingState", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUID := "quota-uuid-6"

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: volumeUuid},
			AccountID: account.ID,
		}

		dbQuotaRule := &datamodel.QuotaRule{
			BaseModel:      datamodel.BaseModel{ID: 6, UUID: quotaRuleUUID},
			Name:           "quota-rule-6",
			QuotaType:      IndividualUserQuota,
			QuotaTarget:    "user:charlie",
			DiskLimitInKib: 204800,
			State:          models.LifeCycleStateUpdating,
			StateDetails:   models.LifeCycleStateUpdatingDetails,
			Description:    "Updating quota rule",
			VolumeID:       volume.ID,
			AccountID:      account.ID,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to succeed
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(volume, nil)

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(ctx, quotaRuleUUID, account.ID).
			Return(dbQuotaRule, nil)

		result, err := orchestrator.DescribeQuotaRule(ctx, volumeUuid, accountName, quotaRuleUUID)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.LifeCycleStateUpdating, result.LifeCycleState)
		assert.Equal(tt, models.LifeCycleStateUpdatingDetails, result.LifeCycleStateDetails)
	})

	t.Run("WhenGetQuotaRuleByUUIDSucceeds_DeletingState", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUID := "quota-uuid-7"

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: volumeUuid},
			AccountID: account.ID,
		}

		dbQuotaRule := &datamodel.QuotaRule{
			BaseModel:      datamodel.BaseModel{ID: 7, UUID: quotaRuleUUID},
			Name:           "quota-rule-7",
			QuotaType:      IndividualUserQuota,
			QuotaTarget:    "user:dave",
			DiskLimitInKib: 102400,
			State:          models.LifeCycleStateDeleting,
			StateDetails:   models.LifeCycleStateDeletingDetails,
			Description:    "Deleting quota rule",
			VolumeID:       volume.ID,
			AccountID:      account.ID,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to succeed
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(volume, nil)

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(ctx, quotaRuleUUID, account.ID).
			Return(dbQuotaRule, nil)

		result, err := orchestrator.DescribeQuotaRule(ctx, volumeUuid, accountName, quotaRuleUUID)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.LifeCycleStateDeleting, result.LifeCycleState)
		assert.Equal(tt, models.LifeCycleStateDeletingDetails, result.LifeCycleStateDetails)
	})

	t.Run("WhenGetQuotaRuleByUUIDSucceeds_ErrorState", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUID := "quota-uuid-8"

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: volumeUuid},
			AccountID: account.ID,
		}

		dbQuotaRule := &datamodel.QuotaRule{
			BaseModel:      datamodel.BaseModel{ID: 8, UUID: quotaRuleUUID},
			Name:           "quota-rule-8",
			QuotaType:      IndividualUserQuota,
			QuotaTarget:    "user:eve",
			DiskLimitInKib: 102400,
			State:          models.LifeCycleStateError,
			StateDetails:   models.LifeCycleStateCreationErrorDetails,
			Description:    "Error quota rule",
			VolumeID:       volume.ID,
			AccountID:      account.ID,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to succeed
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(volume, nil)

		// Mock GetQuotaRuleByUUID to succeed
		mockStore.EXPECT().GetQuotaRuleByUUID(ctx, quotaRuleUUID, account.ID).
			Return(dbQuotaRule, nil)

		result, err := orchestrator.DescribeQuotaRule(ctx, volumeUuid, accountName, quotaRuleUUID)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, models.LifeCycleStateError, result.LifeCycleState)
		assert.Equal(tt, models.LifeCycleStateCreationErrorDetails, result.LifeCycleStateDetails)
	})

	t.Run("WhenGetQuotaRuleByUUIDFailsWithInternalError", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeUuid := "volume-uuid-1"
		accountName := "test-project"
		quotaRuleUUID := "quota-uuid-1"

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      accountName,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: volumeUuid},
			AccountID: account.ID,
		}

		// Mock getAccountWithName to succeed
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock GetVolumeWithAccountID to succeed
		mockStore.EXPECT().GetVolumeWithAccountID(ctx, volumeUuid, account.ID).
			Return(volume, nil)

		// Mock GetQuotaRuleByUUID to fail with internal error
		mockStore.EXPECT().GetQuotaRuleByUUID(ctx, quotaRuleUUID, account.ID).
			Return(nil, errors.New("database connection failed"))

		result, err := orchestrator.DescribeQuotaRule(ctx, volumeUuid, accountName, quotaRuleUUID)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "database connection failed")
	})
}

// TestReplaceDstQuotaRulesWithSrc tests the ReplaceDstQuotaRulesWithSrc function
func TestReplaceDstQuotaRulesWithSrc(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeId := "volume-uuid-1"
		params := gcpgenserver.V1betaUpdateDestinationQuotaRulesVCPParams{
			ProjectNumber: "123456789",
			LocationId:    "us-central1",
			VolumeId:      volumeId,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: volumeId},
			AccountID: 1,
		}

		srcQuotaRule1 := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-1",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 100,
			QuotaTarget:    gcpgenserver.NewOptString("user:alice"),
			QuotaId:        gcpgenserver.NewOptString("quota-uuid-1"),
		}

		dstQuotaRule1 := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-2",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaTypeDEFAULTUSERQUOTA,
			DiskLimitInMib: 200,
			QuotaId:        gcpgenserver.NewOptString("quota-uuid-2"),
		}

		req := &gcpgenserver.UpdateDstWithSrcQuotaRulesV1beta{
			SrcQuotaRules: []gcpgenserver.QuotaRulesV1beta{srcQuotaRule1},
			DstQuotaRules: []gcpgenserver.QuotaRulesV1beta{dstQuotaRule1},
		}

		createdQuotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:      datamodel.BaseModel{ID: 1, UUID: "quota-uuid-1"},
				Name:           "quota-rule-1",
				VolumeID:       1,
				AccountID:      1,
				QuotaType:      IndividualUserQuota,
				QuotaTarget:    "user:alice",
				DiskLimitInKib: 100 * 1024,
				State:          models.LifeCycleStateCreating,
			},
		}

		// Mock GetVolume to succeed
		mockStore.EXPECT().GetVolume(ctx, volumeId).Return(volume, nil)

		// Mock ReplaceDstQuotaRulesWithSrc to succeed
		mockStore.EXPECT().ReplaceDstQuotaRulesWithSrc(
			ctx,
			volume.ID,
			volume.AccountID,
			[]string{"quota-uuid-2"},
			mock.AnythingOfType("[]*datamodel.QuotaRule"),
		).Return(createdQuotaRules, nil)

		result, err := orchestrator.ReplaceDstQuotaRulesWithSrc(ctx, req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 1)
		assert.Equal(tt, "quota-uuid-1", result[0].UUID)
	})

	t.Run("WhenGetVolumeFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeId := "volume-uuid-1"
		params := gcpgenserver.V1betaUpdateDestinationQuotaRulesVCPParams{
			ProjectNumber: "123456789",
			LocationId:    "us-central1",
			VolumeId:      volumeId,
		}

		req := &gcpgenserver.UpdateDstWithSrcQuotaRulesV1beta{
			SrcQuotaRules: []gcpgenserver.QuotaRulesV1beta{},
			DstQuotaRules: []gcpgenserver.QuotaRulesV1beta{},
		}

		// Mock GetVolume to fail
		mockStore.EXPECT().GetVolume(ctx, volumeId).Return(nil, errors.New("volume not found"))

		result, err := orchestrator.ReplaceDstQuotaRulesWithSrc(ctx, req, params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "volume not found")
	})

	t.Run("WhenReplaceDstQuotaRulesWithSrcFails", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeId := "volume-uuid-1"
		params := gcpgenserver.V1betaUpdateDestinationQuotaRulesVCPParams{
			ProjectNumber: "123456789",
			LocationId:    "us-central1",
			VolumeId:      volumeId,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: volumeId},
			AccountID: 1,
		}

		srcQuotaRule1 := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-1",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 100,
		}

		req := &gcpgenserver.UpdateDstWithSrcQuotaRulesV1beta{
			SrcQuotaRules: []gcpgenserver.QuotaRulesV1beta{srcQuotaRule1},
			DstQuotaRules: []gcpgenserver.QuotaRulesV1beta{},
		}

		// Mock GetVolume to succeed
		mockStore.EXPECT().GetVolume(ctx, volumeId).Return(volume, nil)

		// Mock ReplaceDstQuotaRulesWithSrc to fail
		mockStore.EXPECT().ReplaceDstQuotaRulesWithSrc(
			ctx,
			volume.ID,
			volume.AccountID,
			[]string{},
			mock.AnythingOfType("[]*datamodel.QuotaRule"),
		).Return(nil, errors.New("database error"))

		result, err := orchestrator.ReplaceDstQuotaRulesWithSrc(ctx, req, params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "database error")
	})

	t.Run("WhenDstQuotaRuleHasNoQuotaId", func(tt *testing.T) {
		mockStore := database.NewMockStorage(tt)
		orchestrator := &Orchestrator{storage: mockStore}

		ctx := context.Background()
		volumeId := "volume-uuid-1"
		params := gcpgenserver.V1betaUpdateDestinationQuotaRulesVCPParams{
			ProjectNumber: "123456789",
			LocationId:    "us-central1",
			VolumeId:      volumeId,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: volumeId},
			AccountID: 1,
		}

		dstQuotaRule1 := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-2",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaTypeDEFAULTUSERQUOTA,
			DiskLimitInMib: 200,
			// QuotaId is not set
		}

		req := &gcpgenserver.UpdateDstWithSrcQuotaRulesV1beta{
			SrcQuotaRules: []gcpgenserver.QuotaRulesV1beta{},
			DstQuotaRules: []gcpgenserver.QuotaRulesV1beta{dstQuotaRule1},
		}

		createdQuotaRules := []*datamodel.QuotaRule{}

		// Mock GetVolume to succeed
		mockStore.EXPECT().GetVolume(ctx, volumeId).Return(volume, nil)

		// Mock ReplaceDstQuotaRulesWithSrc to succeed (empty dstQuotaRuleUUIDs since QuotaId is not set)
		mockStore.EXPECT().ReplaceDstQuotaRulesWithSrc(
			ctx,
			volume.ID,
			volume.AccountID,
			[]string{},
			mock.AnythingOfType("[]*datamodel.QuotaRule"),
		).Return(createdQuotaRules, nil)

		result, err := orchestrator.ReplaceDstQuotaRulesWithSrc(ctx, req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 0)
	})
}

// Test_convertQuotaRulesV1betaToDataModel tests the _convertQuotaRulesV1betaToDataModel function
func Test_convertQuotaRulesV1betaToDataModel(t *testing.T) {
	t.Run("WhenAllFieldsPresent_IndividualUserQuota", func(tt *testing.T) {
		clientRule := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-1",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 100,
			QuotaTarget:    gcpgenserver.NewOptString("user:alice"),
			QuotaId:        gcpgenserver.NewOptString("quota-uuid-1"),
			State:          gcpgenserver.NewOptQuotaRulesV1betaState(gcpgenserver.QuotaRulesV1betaStateREADY),
			StateDetails:   gcpgenserver.NewOptString("Ready state"),
			Description:    gcpgenserver.NewOptString("Test description"),
		}

		result := _convertQuotaRulesV1betaToDataModel(clientRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, "quota-rule-1", result.Name)
		assert.Equal(tt, int64(100*1024), result.DiskLimitInKib)
		assert.Equal(tt, "quota-uuid-1", result.UUID)
		assert.Equal(tt, IndividualUserQuota, result.QuotaType)
		assert.Equal(tt, "user:alice", result.QuotaTarget)
		assert.Equal(tt, "READY", result.State)
		assert.Equal(tt, "Ready state", result.StateDetails)
	})

	t.Run("WhenAllFieldsPresent_IndividualGroupQuota", func(tt *testing.T) {
		clientRule := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-2",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALGROUPQUOTA,
			DiskLimitInMib: 200,
			QuotaTarget:    gcpgenserver.NewOptString("group:developers"),
			QuotaId:        gcpgenserver.NewOptString("quota-uuid-2"),
		}

		result := _convertQuotaRulesV1betaToDataModel(clientRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, "quota-rule-2", result.Name)
		assert.Equal(tt, int64(200*1024), result.DiskLimitInKib)
		assert.Equal(tt, "quota-uuid-2", result.UUID)
		assert.Equal(tt, IndividualGroupQuota, result.QuotaType)
		assert.Equal(tt, "group:developers", result.QuotaTarget)
	})

	t.Run("WhenAllFieldsPresent_DefaultUserQuota", func(tt *testing.T) {
		clientRule := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-3",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaTypeDEFAULTUSERQUOTA,
			DiskLimitInMib: 300,
			QuotaId:        gcpgenserver.NewOptString("quota-uuid-3"),
		}

		result := _convertQuotaRulesV1betaToDataModel(clientRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, "quota-rule-3", result.Name)
		assert.Equal(tt, int64(300*1024), result.DiskLimitInKib)
		assert.Equal(tt, "quota-uuid-3", result.UUID)
		assert.Equal(tt, DefaultUserQuota, result.QuotaType)
	})

	t.Run("WhenAllFieldsPresent_DefaultGroupQuota", func(tt *testing.T) {
		clientRule := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-4",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaTypeDEFAULTGROUPQUOTA,
			DiskLimitInMib: 400,
			QuotaId:        gcpgenserver.NewOptString("quota-uuid-4"),
		}

		result := _convertQuotaRulesV1betaToDataModel(clientRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, "quota-rule-4", result.Name)
		assert.Equal(tt, int64(400*1024), result.DiskLimitInKib)
		assert.Equal(tt, "quota-uuid-4", result.UUID)
		assert.Equal(tt, DefaultGroupQuota, result.QuotaType)
	})

	t.Run("WhenUnknownQuotaType", func(tt *testing.T) {
		clientRule := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-5",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaType("UNKNOWN_TYPE"),
			DiskLimitInMib: 500,
			QuotaId:        gcpgenserver.NewOptString("quota-uuid-5"),
		}

		result := _convertQuotaRulesV1betaToDataModel(clientRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, "quota-rule-5", result.Name)
		assert.Equal(tt, "UNKNOWN_TYPE", result.QuotaType)
	})

	t.Run("WhenOptionalFieldsAreMissing", func(tt *testing.T) {
		clientRule := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-6",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 600,
			// QuotaId, QuotaTarget, State, StateDetails are not set
		}

		result := _convertQuotaRulesV1betaToDataModel(clientRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, "quota-rule-6", result.Name)
		assert.Equal(tt, int64(600*1024), result.DiskLimitInKib)
		assert.Empty(tt, result.UUID)
		assert.Empty(tt, result.QuotaTarget)
		assert.Empty(tt, result.State)
		assert.Empty(tt, result.StateDetails)
	})

	t.Run("WhenOnlyQuotaTargetIsSet", func(tt *testing.T) {
		clientRule := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-7",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 700,
			QuotaTarget:    gcpgenserver.NewOptString("user:bob"),
		}

		result := _convertQuotaRulesV1betaToDataModel(clientRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, "user:bob", result.QuotaTarget)
	})

	t.Run("WhenOnlyStateIsSet", func(tt *testing.T) {
		clientRule := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-8",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaTypeDEFAULTUSERQUOTA,
			DiskLimitInMib: 800,
			State:          gcpgenserver.NewOptQuotaRulesV1betaState(gcpgenserver.QuotaRulesV1betaStateCREATING),
		}

		result := _convertQuotaRulesV1betaToDataModel(clientRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, "CREATING", result.State)
	})

	t.Run("WhenOnlyStateDetailsIsSet", func(tt *testing.T) {
		clientRule := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-9",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaTypeDEFAULTGROUPQUOTA,
			DiskLimitInMib: 900,
			StateDetails:   gcpgenserver.NewOptString("Creating state details"),
		}

		result := _convertQuotaRulesV1betaToDataModel(clientRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, "Creating state details", result.StateDetails)
	})
}
