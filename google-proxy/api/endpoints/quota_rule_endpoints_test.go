package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestV1betaCreateQuotaRule(t *testing.T) {
	t.Run("WhenCreateQuotaRuleSucceeds", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaCreateQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleCreateV1beta{
			ResourceId:     "quota-name",
			QuotaType:      gcpgenserver.QuotaRuleCreateV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 1024,
			QuotaTarget:    gcpgenserver.NewOptString("user:alice"),
			Description:    gcpgenserver.NewOptString("desc"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		expQuota := &models.QuotaRule{
			BaseModel:      models.BaseModel{UUID: "uuid-1"},
			Name:           "quota-name",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInMib: 1024,
			QuotaTarget:    "user:alice",
		}
		mockOrch.On("CreateQuotaRule", mock.Anything, mock.Anything).Return(expQuota, "op-1", nil)

		res, err := handler.V1betaCreateQuotaRule(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		op, ok := res.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok, "expected OperationV1beta, got %T", res)
		assert.NotNil(tt, op.Name)
		assert.Equal(tt, "/v1beta/projects/project-1/locations/us-central1/operations/op-1", op.Name.Value)
	})

	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}

		params := gcpgenserver.V1betaCreateQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "invalid-location",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleCreateV1beta{
			ResourceId:     "quota-name",
			QuotaType:      gcpgenserver.QuotaRuleCreateV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 1024,
			QuotaTarget:    gcpgenserver.NewOptString("user:alice"),
			Description:    gcpgenserver.NewOptString("desc"),
		}

		res, err := handler.V1betaCreateQuotaRule(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		bad, ok := res.(*gcpgenserver.V1betaCreateQuotaRuleBadRequest)
		assert.True(tt, ok, "expected V1betaCreateQuotaRuleBadRequest, got %T", res)
		assert.Equal(tt, float64(http.StatusBadRequest), bad.Code)
		assert.NotEmpty(tt, bad.Message)
	})

	t.Run("WhenCreateQuotaRuleFailsWithBadRequest", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaCreateQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleCreateV1beta{
			ResourceId:     "quota-name",
			QuotaType:      gcpgenserver.QuotaRuleCreateV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 1024,
			QuotaTarget:    gcpgenserver.NewOptString("user:alice"),
			Description:    gcpgenserver.NewOptString("desc"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("CreateQuotaRule", mock.Anything, mock.Anything).Return(nil, "", errors.NewUserInputValidationErr("invalid request"))

		res, err := handler.V1betaCreateQuotaRule(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		bad, ok := res.(*gcpgenserver.V1betaCreateQuotaRuleBadRequest)
		assert.True(tt, ok, "expected V1betaCreateQuotaRuleBadRequest, got %T", res)
		assert.Equal(tt, float64(http.StatusBadRequest), bad.Code)
		assert.NotEmpty(tt, bad.Message)
	})

	t.Run("WhenCreateQuotaRuleFailsWithInternalError", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaCreateQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleCreateV1beta{
			ResourceId:     "quota-name",
			QuotaType:      gcpgenserver.QuotaRuleCreateV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 1024,
			QuotaTarget:    gcpgenserver.NewOptString("user:alice"),
			Description:    gcpgenserver.NewOptString("desc"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("CreateQuotaRule", mock.Anything, mock.Anything).Return(nil, "", errors.New("something bad"))

		res, err := handler.V1betaCreateQuotaRule(context.Background(), req, params)

		assert.Error(tt, err)
		assert.NotNil(tt, res)

		internal, ok := res.(*gcpgenserver.V1betaCreateQuotaRuleInternalServerError)
		assert.True(tt, ok, "expected V1betaCreateQuotaRuleInternalServerError, got %T", res)
		assert.Equal(tt, float64(http.StatusInternalServerError), internal.Code)
	})
}

func TestV1betaCreateQuotaRuleVCP(t *testing.T) {
	t.Run("WhenCreateQuotaRuleInternalSucceeds", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaCreateQuotaRuleVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleCreateV1beta{
			ResourceId:     "quota-name",
			QuotaType:      gcpgenserver.QuotaRuleCreateV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 1024,
			QuotaTarget:    gcpgenserver.NewOptString("user:alice"),
			Description:    gcpgenserver.NewOptString("desc"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		expQuota := &models.QuotaRule{
			BaseModel:             models.BaseModel{UUID: "uuid-1"},
			Name:                  "quota-name",
			QuotaType:             "INDIVIDUAL_USER_QUOTA",
			DiskLimitInMib:        1024,
			QuotaTarget:           "user:alice",
			LifeCycleState:        models.LifeCycleStateCreating,
			LifeCycleStateDetails: models.LifeCycleStateCreatingDetails,
			Description:           "desc",
		}

		expJob := &models.Job{
			BaseModel:  models.BaseModel{UUID: "job-uuid-1"},
			WorkflowID: "workflow-id-1",
			State:      models.JobsStateNEW,
		}

		mockOrch.On("CreateQuotaRuleInternal", mock.Anything, mock.Anything).Return(expQuota, "", nil)
		mockOrch.On("GetJobByResourceUUID", mock.Anything, "uuid-1", string(models.JobTypeCreateQuotaRule)).Return(expJob, nil)

		res, err := handler.V1betaCreateQuotaRuleVCP(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		quotaRuleVCP, ok := res.(*gcpgenserver.QuotaRulesVCPV1beta)
		assert.True(tt, ok, "expected QuotaRulesVCPV1beta, got %T", res)
		assert.Equal(tt, "uuid-1", quotaRuleVCP.QuotaId.Value)
		assert.Equal(tt, "quota-name", quotaRuleVCP.ResourceId)
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaQuotaTypeINDIVIDUALUSERQUOTA, quotaRuleVCP.QuotaType)
		assert.Equal(tt, int64(1024), quotaRuleVCP.DiskLimitInMib)
		assert.Equal(tt, "user:alice", quotaRuleVCP.QuotaTarget.Value)
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaStateCREATING, quotaRuleVCP.State.Value)
		assert.Equal(tt, models.LifeCycleStateCreatingDetails, quotaRuleVCP.StateDetails.Value)
		assert.Equal(tt, "desc", quotaRuleVCP.Description.Value)
		assert.Len(tt, quotaRuleVCP.Jobs, 1, "Expected 1 job in response")
		assert.Equal(tt, "job-uuid-1", quotaRuleVCP.Jobs[0].JobId.Value)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenCreateQuotaRuleInternalSucceedsWithoutJob", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaCreateQuotaRuleVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleCreateV1beta{
			ResourceId:     "quota-name",
			QuotaType:      gcpgenserver.QuotaRuleCreateV1betaQuotaTypeDEFAULTUSERQUOTA,
			DiskLimitInMib: 2048,
			QuotaTarget:    gcpgenserver.NewOptString(""),
			Description:    gcpgenserver.NewOptString("default quota"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		expQuota := &models.QuotaRule{
			BaseModel:             models.BaseModel{UUID: "uuid-2"},
			Name:                  "quota-name",
			QuotaType:             "DEFAULT_USER_QUOTA",
			DiskLimitInMib:        2048,
			QuotaTarget:           "",
			LifeCycleState:        models.LifeCycleStateAvailable,
			LifeCycleStateDetails: models.LifeCycleStateReadyDetails,
			Description:           "default quota",
		}

		mockOrch.On("CreateQuotaRuleInternal", mock.Anything, mock.Anything).Return(expQuota, "", nil)
		mockOrch.On("GetJobByResourceUUID", mock.Anything, "uuid-2", string(models.JobTypeCreateQuotaRule)).Return(nil, errors.New("job not found"))

		res, err := handler.V1betaCreateQuotaRuleVCP(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		quotaRuleVCP, ok := res.(*gcpgenserver.QuotaRulesVCPV1beta)
		assert.True(tt, ok, "expected QuotaRulesVCPV1beta, got %T", res)
		assert.Equal(tt, "uuid-2", quotaRuleVCP.QuotaId.Value)
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaQuotaTypeDEFAULTUSERQUOTA, quotaRuleVCP.QuotaType)
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaStateREADY, quotaRuleVCP.State.Value)
		assert.Empty(tt, quotaRuleVCP.Jobs, "Expected empty jobs array when job not found")
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}

		params := gcpgenserver.V1betaCreateQuotaRuleVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "invalid-location",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleCreateV1beta{
			ResourceId:     "quota-name",
			QuotaType:      gcpgenserver.QuotaRuleCreateV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 1024,
			QuotaTarget:    gcpgenserver.NewOptString("user:alice"),
			Description:    gcpgenserver.NewOptString("desc"),
		}

		res, err := handler.V1betaCreateQuotaRuleVCP(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		bad, ok := res.(*gcpgenserver.V1betaCreateQuotaRuleVCPBadRequest)
		assert.True(tt, ok, "expected V1betaCreateQuotaRuleVCPBadRequest, got %T", res)
		assert.Equal(tt, float64(http.StatusBadRequest), bad.Code)
		assert.NotEmpty(tt, bad.Message)
	})

	t.Run("WhenCreateQuotaRuleInternalFailsWithBadRequest", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaCreateQuotaRuleVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleCreateV1beta{
			ResourceId:     "quota-name",
			QuotaType:      gcpgenserver.QuotaRuleCreateV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 1024,
			QuotaTarget:    gcpgenserver.NewOptString("user:alice"),
			Description:    gcpgenserver.NewOptString("desc"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("CreateQuotaRuleInternal", mock.Anything, mock.Anything).Return(nil, "", errors.NewUserInputValidationErr("invalid request"))

		res, err := handler.V1betaCreateQuotaRuleVCP(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		bad, ok := res.(*gcpgenserver.V1betaCreateQuotaRuleVCPBadRequest)
		assert.True(tt, ok, "expected V1betaCreateQuotaRuleVCPBadRequest, got %T", res)
		assert.Equal(tt, float64(http.StatusBadRequest), bad.Code)
		assert.NotEmpty(tt, bad.Message)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenCreateQuotaRuleInternalFailsWithConflict", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaCreateQuotaRuleVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleCreateV1beta{
			ResourceId:     "quota-name",
			QuotaType:      gcpgenserver.QuotaRuleCreateV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 1024,
			QuotaTarget:    gcpgenserver.NewOptString("user:alice"),
			Description:    gcpgenserver.NewOptString("desc"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("CreateQuotaRuleInternal", mock.Anything, mock.Anything).Return(nil, "", errors.NewConflictErr("quota rule already exists"))

		res, err := handler.V1betaCreateQuotaRuleVCP(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		conflict, ok := res.(*gcpgenserver.V1betaCreateQuotaRuleVCPConflict)
		assert.True(tt, ok, "expected V1betaCreateQuotaRuleVCPConflict, got %T", res)
		assert.Equal(tt, float64(http.StatusConflict), conflict.Code)
		assert.NotEmpty(tt, conflict.Message)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenCreateQuotaRuleInternalFailsWithInternalError", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaCreateQuotaRuleVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleCreateV1beta{
			ResourceId:     "quota-name",
			QuotaType:      gcpgenserver.QuotaRuleCreateV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 1024,
			QuotaTarget:    gcpgenserver.NewOptString("user:alice"),
			Description:    gcpgenserver.NewOptString("desc"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("CreateQuotaRuleInternal", mock.Anything, mock.Anything).Return(nil, "", errors.New("something bad"))

		res, err := handler.V1betaCreateQuotaRuleVCP(context.Background(), req, params)

		assert.Error(tt, err)
		assert.NotNil(tt, res)

		internal, ok := res.(*gcpgenserver.V1betaCreateQuotaRuleVCPInternalServerError)
		assert.True(tt, ok, "expected V1betaCreateQuotaRuleVCPInternalServerError, got %T", res)
		assert.Equal(tt, float64(http.StatusInternalServerError), internal.Code)
		mockOrch.AssertExpectations(tt)
	})
}

func TestConvertQuotaRuleToV1beta(t *testing.T) {
	t.Run("WhenConvertingIndividualUserQuota", func(tt *testing.T) {
		quotaRule := &models.QuotaRule{
			BaseModel:             models.BaseModel{UUID: "uuid-1"},
			Name:                  "quota-rule-1",
			QuotaType:             "INDIVIDUAL_USER_QUOTA",
			DiskLimitInMib:        1024,
			QuotaTarget:           "user:alice",
			LifeCycleState:        models.LifeCycleStateAvailable,
			LifeCycleStateDetails: models.LifeCycleStateReadyDetails,
			Description:           "Test quota rule",
		}

		result := convertQuotaRuleToV1beta(quotaRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, "uuid-1", result.QuotaId.Value)
		assert.Equal(tt, "quota-rule-1", result.ResourceId)
		assert.Equal(tt, gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA, result.QuotaType)
		assert.Equal(tt, int64(1024), result.DiskLimitInMib)
		assert.Equal(tt, "user:alice", result.QuotaTarget.Value)
		assert.Equal(tt, gcpgenserver.QuotaRulesV1betaStateREADY, result.State.Value)
		assert.Equal(tt, models.LifeCycleStateReadyDetails, result.StateDetails.Value)
		assert.Equal(tt, "Test quota rule", result.Description.Value)
	})

	t.Run("WhenConvertingIndividualGroupQuota", func(tt *testing.T) {
		quotaRule := &models.QuotaRule{
			BaseModel:             models.BaseModel{UUID: "uuid-2"},
			Name:                  "quota-rule-2",
			QuotaType:             "INDIVIDUAL_GROUP_QUOTA",
			DiskLimitInMib:        2048,
			QuotaTarget:           "group:developers",
			LifeCycleState:        models.LifeCycleStateCreating,
			LifeCycleStateDetails: models.LifeCycleStateCreatingDetails,
			Description:           "Group quota rule",
		}

		result := convertQuotaRuleToV1beta(quotaRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALGROUPQUOTA, result.QuotaType)
		assert.Equal(tt, gcpgenserver.QuotaRulesV1betaStateCREATING, result.State.Value)
		assert.Equal(tt, "group:developers", result.QuotaTarget.Value)
	})

	t.Run("WhenConvertingDefaultUserQuota", func(tt *testing.T) {
		quotaRule := &models.QuotaRule{
			BaseModel:             models.BaseModel{UUID: "uuid-3"},
			Name:                  "quota-rule-3",
			QuotaType:             "DEFAULT_USER_QUOTA",
			DiskLimitInMib:        512,
			QuotaTarget:           "",
			LifeCycleState:        models.LifeCycleStateUpdating,
			LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
			Description:           "Default user quota",
		}

		result := convertQuotaRuleToV1beta(quotaRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, gcpgenserver.QuotaRulesV1betaQuotaTypeDEFAULTUSERQUOTA, result.QuotaType)
		assert.Equal(tt, gcpgenserver.QuotaRulesV1betaStateUPDATING, result.State.Value)
		assert.Equal(tt, "", result.QuotaTarget.Value)
	})

	t.Run("WhenConvertingDefaultGroupQuota", func(tt *testing.T) {
		quotaRule := &models.QuotaRule{
			BaseModel:             models.BaseModel{UUID: "uuid-4"},
			Name:                  "quota-rule-4",
			QuotaType:             "DEFAULT_GROUP_QUOTA",
			DiskLimitInMib:        4096,
			QuotaTarget:           "",
			LifeCycleState:        models.LifeCycleStateDeleting,
			LifeCycleStateDetails: models.LifeCycleStateDeletingDetails,
			Description:           "Default group quota",
		}

		result := convertQuotaRuleToV1beta(quotaRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, gcpgenserver.QuotaRulesV1betaQuotaTypeDEFAULTGROUPQUOTA, result.QuotaType)
		assert.Equal(tt, gcpgenserver.QuotaRulesV1betaStateDELETING, result.State.Value)
	})

	t.Run("WhenConvertingErrorState", func(tt *testing.T) {
		quotaRule := &models.QuotaRule{
			BaseModel:             models.BaseModel{UUID: "uuid-5"},
			Name:                  "quota-rule-5",
			QuotaType:             "INDIVIDUAL_USER_QUOTA",
			DiskLimitInMib:        1024,
			QuotaTarget:           "user:bob",
			LifeCycleState:        models.LifeCycleStateError,
			LifeCycleStateDetails: models.LifeCycleStateCreationErrorDetails,
			Description:           "Failed quota rule",
		}

		result := convertQuotaRuleToV1beta(quotaRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, gcpgenserver.QuotaRulesV1betaStateERROR, result.State.Value)
		assert.Equal(tt, models.LifeCycleStateCreationErrorDetails, result.StateDetails.Value)
	})

	t.Run("WhenConvertingUnknownState", func(tt *testing.T) {
		quotaRule := &models.QuotaRule{
			BaseModel:             models.BaseModel{UUID: "uuid-6"},
			Name:                  "quota-rule-6",
			QuotaType:             "INDIVIDUAL_USER_QUOTA",
			DiskLimitInMib:        1024,
			QuotaTarget:           "user:charlie",
			LifeCycleState:        "UNKNOWN_STATE",
			LifeCycleStateDetails: "Unknown state details",
			Description:           "Unknown state quota rule",
		}

		result := convertQuotaRuleToV1beta(quotaRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, gcpgenserver.QuotaRulesV1betaStateSTATEUNSPECIFIED, result.State.Value)
	})

	t.Run("WhenConvertingUnknownQuotaType", func(tt *testing.T) {
		quotaRule := &models.QuotaRule{
			BaseModel:             models.BaseModel{UUID: "uuid-7"},
			Name:                  "quota-rule-7",
			QuotaType:             "UNKNOWN_QUOTA_TYPE",
			DiskLimitInMib:        1024,
			QuotaTarget:           "user:dave",
			LifeCycleState:        models.LifeCycleStateAvailable,
			LifeCycleStateDetails: models.LifeCycleStateReadyDetails,
			Description:           "Unknown quota type",
		}

		result := convertQuotaRuleToV1beta(quotaRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, gcpgenserver.QuotaRulesV1betaQuotaType("UNKNOWN_QUOTA_TYPE"), result.QuotaType)
	})
}
