package api

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	orchestratorcommon "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
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

func TestV1betaUpdateQuotaRule(t *testing.T) {
	t.Run("WhenUpdateQuotaRuleSucceeds", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			DiskLimitInMib: gcpgenserver.NewOptInt64(2048),
			Description:    gcpgenserver.NewOptString("updated description"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		expQuota := &models.QuotaRule{
			BaseModel:      models.BaseModel{UUID: "quota-rule-uuid-1"},
			Name:           "quota-name",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInMib: 2048,
			QuotaTarget:    "user:alice",
			Description:    "updated description",
		}
		mockOrch.On("UpdateQuotaRule", mock.Anything, mock.Anything).Return(expQuota, "op-1", nil)

		res, err := handler.V1betaUpdateQuotaRule(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		op, ok := res.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok, "expected OperationV1beta, got %T", res)
		assert.NotNil(tt, op.Name)
		assert.Equal(tt, "/v1beta/projects/project-1/locations/us-central1/operations/op-1", op.Name.Value)
		assert.NotNil(tt, op.Done)
		assert.False(tt, op.Done.Value)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQuotaRuleSucceedsWithoutOperationID", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			DiskLimitInMib: gcpgenserver.NewOptInt64(2048),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		expQuota := &models.QuotaRule{
			BaseModel:      models.BaseModel{UUID: "quota-rule-uuid-1"},
			Name:           "quota-name",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInMib: 2048,
		}
		mockOrch.On("UpdateQuotaRule", mock.Anything, mock.Anything).Return(expQuota, "", nil)

		res, err := handler.V1betaUpdateQuotaRule(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		op, ok := res.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok, "expected OperationV1beta, got %T", res)
		assert.NotNil(tt, op.Name)
		assert.Equal(tt, "", op.Name.Value, "operation name should be empty when operationID is empty")
		assert.NotNil(tt, op.Done)
		assert.False(tt, op.Done.Value)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQuotaRuleSucceedsWithOnlyDiskLimit", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			DiskLimitInMib: gcpgenserver.NewOptInt64(4096),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		expQuota := &models.QuotaRule{
			BaseModel:      models.BaseModel{UUID: "quota-rule-uuid-1"},
			Name:           "quota-name",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInMib: 4096,
		}
		mockOrch.On("UpdateQuotaRule", mock.Anything, mock.MatchedBy(func(p *orchestratorcommon.UpdateQuotaRulesParam) bool {
			return p.DiskLimitInMib == 4096 && p.Description == ""
		})).Return(expQuota, "op-2", nil)

		res, err := handler.V1betaUpdateQuotaRule(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQuotaRuleSucceedsWithOnlyDescription", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			Description: gcpgenserver.NewOptString("new description only"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		expQuota := &models.QuotaRule{
			BaseModel:   models.BaseModel{UUID: "quota-rule-uuid-1"},
			Name:        "quota-name",
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			Description: "new description only",
		}
		mockOrch.On("UpdateQuotaRule", mock.Anything, mock.MatchedBy(func(p *orchestratorcommon.UpdateQuotaRulesParam) bool {
			return p.Description == "new description only" && p.DiskLimitInMib == 0
		})).Return(expQuota, "op-3", nil)

		res, err := handler.V1betaUpdateQuotaRule(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQuotaRuleSucceedsWithBothFields", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			DiskLimitInMib: gcpgenserver.NewOptInt64(8192),
			Description:    gcpgenserver.NewOptString("updated both fields"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		expQuota := &models.QuotaRule{
			BaseModel:      models.BaseModel{UUID: "quota-rule-uuid-1"},
			Name:           "quota-name",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInMib: 8192,
			Description:    "updated both fields",
		}
		mockOrch.On("UpdateQuotaRule", mock.Anything, mock.MatchedBy(func(p *orchestratorcommon.UpdateQuotaRulesParam) bool {
			return p.DiskLimitInMib == 8192 && p.Description == "updated both fields"
		})).Return(expQuota, "op-4", nil)

		res, err := handler.V1betaUpdateQuotaRule(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}

		params := gcpgenserver.V1betaUpdateQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "invalid-location",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			DiskLimitInMib: gcpgenserver.NewOptInt64(2048),
		}

		res, err := handler.V1betaUpdateQuotaRule(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		bad, ok := res.(*gcpgenserver.V1betaUpdateQuotaRuleBadRequest)
		assert.True(tt, ok, "expected V1betaUpdateQuotaRuleBadRequest, got %T", res)
		assert.Equal(tt, float64(http.StatusBadRequest), bad.Code)
		assert.NotEmpty(tt, bad.Message)
	})

	t.Run("WhenUpdateQuotaRuleFailsWithBadRequest", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			DiskLimitInMib: gcpgenserver.NewOptInt64(2048),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("UpdateQuotaRule", mock.Anything, mock.Anything).Return(nil, "", errors.NewUserInputValidationErr("invalid request"))

		res, err := handler.V1betaUpdateQuotaRule(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		bad, ok := res.(*gcpgenserver.V1betaUpdateQuotaRuleBadRequest)
		assert.True(tt, ok, "expected V1betaUpdateQuotaRuleBadRequest, got %T", res)
		assert.Equal(tt, float64(http.StatusBadRequest), bad.Code)
		assert.NotEmpty(tt, bad.Message)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQuotaRuleFailsWithNotFound", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			DiskLimitInMib: gcpgenserver.NewOptInt64(2048),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("UpdateQuotaRule", mock.Anything, mock.Anything).Return(nil, "", errors.NewNotFoundErr("quota rule not found", nil))

		res, err := handler.V1betaUpdateQuotaRule(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		bad, ok := res.(*gcpgenserver.V1betaUpdateQuotaRuleBadRequest)
		assert.True(tt, ok, "expected V1betaUpdateQuotaRuleBadRequest, got %T", res)
		assert.Equal(tt, float64(http.StatusBadRequest), bad.Code)
		assert.NotEmpty(tt, bad.Message)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQuotaRuleFailsWithConflict", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			DiskLimitInMib: gcpgenserver.NewOptInt64(2048),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("UpdateQuotaRule", mock.Anything, mock.Anything).Return(nil, "", errors.NewConflictErr("quota rule is in transition state"))

		res, err := handler.V1betaUpdateQuotaRule(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		conflict, ok := res.(*gcpgenserver.V1betaUpdateQuotaRuleConflict)
		assert.True(tt, ok, "expected V1betaUpdateQuotaRuleConflict, got %T", res)
		assert.Equal(tt, float64(http.StatusConflict), conflict.Code)
		assert.NotEmpty(tt, conflict.Message)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQuotaRuleFailsWithInternalError", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			DiskLimitInMib: gcpgenserver.NewOptInt64(2048),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("UpdateQuotaRule", mock.Anything, mock.Anything).Return(nil, "", errors.New("something bad"))

		res, err := handler.V1betaUpdateQuotaRule(context.Background(), req, params)

		assert.Error(tt, err)
		assert.NotNil(tt, res)

		internal, ok := res.(*gcpgenserver.V1betaUpdateQuotaRuleInternalServerError)
		assert.True(tt, ok, "expected V1betaUpdateQuotaRuleInternalServerError, got %T", res)
		assert.Equal(tt, float64(http.StatusInternalServerError), internal.Code)
		mockOrch.AssertExpectations(tt)
	})
}

func TestV1betaUpdateQuotaRuleVCP(t *testing.T) {
	t.Run("WhenUpdateQuotaRuleInternalSucceeds", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateQuotaRuleVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			DiskLimitInMib: gcpgenserver.NewOptInt64(2048),
			Description:    gcpgenserver.NewOptString("updated description"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		expQuota := &models.QuotaRule{
			BaseModel:             models.BaseModel{UUID: "quota-rule-uuid-1"},
			Name:                  "quota-name",
			QuotaType:             "INDIVIDUAL_USER_QUOTA",
			DiskLimitInMib:        2048,
			QuotaTarget:           "user:alice",
			Description:           "updated description",
			LifeCycleState:        models.LifeCycleStateUpdating,
			LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
		}

		expJob := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID:      "job-uuid-1",
				CreatedAt: time.Now(),
			},
			WorkflowID: "workflow-id-1",
			State:      string(models.JobsStateNEW),
		}

		mockOrch.On("UpdateQuotaRuleInternal", mock.Anything, mock.Anything).Return(expQuota, expJob, nil)

		res, err := handler.V1betaUpdateQuotaRuleVCP(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		quotaRuleVCP, ok := res.(*gcpgenserver.QuotaRulesVCPV1beta)
		assert.True(tt, ok, "expected QuotaRulesVCPV1beta, got %T", res)
		assert.Equal(tt, "quota-rule-uuid-1", quotaRuleVCP.QuotaId.Value)
		assert.Equal(tt, "quota-name", quotaRuleVCP.ResourceId)
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaQuotaTypeINDIVIDUALUSERQUOTA, quotaRuleVCP.QuotaType)
		assert.Equal(tt, int64(2048), quotaRuleVCP.DiskLimitInMib)
		assert.Equal(tt, "user:alice", quotaRuleVCP.QuotaTarget.Value)
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaStateUPDATING, quotaRuleVCP.State.Value)
		assert.Equal(tt, models.LifeCycleStateUpdatingDetails, quotaRuleVCP.StateDetails.Value)
		assert.Equal(tt, "updated description", quotaRuleVCP.Description.Value)
		assert.Len(tt, quotaRuleVCP.Jobs, 1, "Expected 1 job in response")
		assert.Equal(tt, "job-uuid-1", quotaRuleVCP.Jobs[0].JobId.Value)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQuotaRuleInternalSucceedsWithoutJob", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateQuotaRuleVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			DiskLimitInMib: gcpgenserver.NewOptInt64(2048),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		expQuota := &models.QuotaRule{
			BaseModel:             models.BaseModel{UUID: "quota-rule-uuid-1"},
			Name:                  "quota-name",
			QuotaType:             "INDIVIDUAL_USER_QUOTA",
			DiskLimitInMib:        2048,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateReadyDetails,
		}

		mockOrch.On("UpdateQuotaRuleInternal", mock.Anything, mock.Anything).Return(expQuota, nil, nil)

		res, err := handler.V1betaUpdateQuotaRuleVCP(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		quotaRuleVCP, ok := res.(*gcpgenserver.QuotaRulesVCPV1beta)
		assert.True(tt, ok, "expected QuotaRulesVCPV1beta, got %T", res)
		assert.Equal(tt, "quota-rule-uuid-1", quotaRuleVCP.QuotaId.Value)
		assert.Empty(tt, quotaRuleVCP.Jobs, "Expected empty jobs array when job is nil")
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQuotaRuleInternalSucceedsWithOnlyDiskLimit", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateQuotaRuleVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			DiskLimitInMib: gcpgenserver.NewOptInt64(4096),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		expQuota := &models.QuotaRule{
			BaseModel:      models.BaseModel{UUID: "quota-rule-uuid-1"},
			Name:           "quota-name",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInMib: 4096,
		}

		mockOrch.On("UpdateQuotaRuleInternal", mock.Anything, mock.MatchedBy(func(p *orchestratorcommon.UpdateQuotaRulesParam) bool {
			return p.DiskLimitInMib == 4096 && p.Description == ""
		})).Return(expQuota, nil, nil)

		res, err := handler.V1betaUpdateQuotaRuleVCP(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQuotaRuleInternalSucceedsWithOnlyDescription", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateQuotaRuleVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			Description: gcpgenserver.NewOptString("new description only"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		expQuota := &models.QuotaRule{
			BaseModel:   models.BaseModel{UUID: "quota-rule-uuid-1"},
			Name:        "quota-name",
			QuotaType:   "INDIVIDUAL_USER_QUOTA",
			Description: "new description only",
		}

		mockOrch.On("UpdateQuotaRuleInternal", mock.Anything, mock.MatchedBy(func(p *orchestratorcommon.UpdateQuotaRulesParam) bool {
			return p.Description == "new description only" && p.DiskLimitInMib == 0
		})).Return(expQuota, nil, nil)

		res, err := handler.V1betaUpdateQuotaRuleVCP(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQuotaRuleInternalSucceedsWithBothFields", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateQuotaRuleVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			DiskLimitInMib: gcpgenserver.NewOptInt64(8192),
			Description:    gcpgenserver.NewOptString("updated both fields"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		expQuota := &models.QuotaRule{
			BaseModel:      models.BaseModel{UUID: "quota-rule-uuid-1"},
			Name:           "quota-name",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInMib: 8192,
			Description:    "updated both fields",
		}

		mockOrch.On("UpdateQuotaRuleInternal", mock.Anything, mock.MatchedBy(func(p *orchestratorcommon.UpdateQuotaRulesParam) bool {
			return p.DiskLimitInMib == 8192 && p.Description == "updated both fields"
		})).Return(expQuota, nil, nil)

		res, err := handler.V1betaUpdateQuotaRuleVCP(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}

		params := gcpgenserver.V1betaUpdateQuotaRuleVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "invalid-location",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			DiskLimitInMib: gcpgenserver.NewOptInt64(2048),
		}

		res, err := handler.V1betaUpdateQuotaRuleVCP(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		bad, ok := res.(*gcpgenserver.V1betaUpdateQuotaRuleVCPBadRequest)
		assert.True(tt, ok, "expected V1betaUpdateQuotaRuleVCPBadRequest, got %T", res)
		assert.Equal(tt, float64(http.StatusBadRequest), bad.Code)
		assert.NotEmpty(tt, bad.Message)
	})

	t.Run("WhenUpdateQuotaRuleInternalFailsWithBadRequest", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateQuotaRuleVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			DiskLimitInMib: gcpgenserver.NewOptInt64(2048),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("UpdateQuotaRuleInternal", mock.Anything, mock.Anything).Return(nil, nil, errors.NewUserInputValidationErr("invalid request"))

		res, err := handler.V1betaUpdateQuotaRuleVCP(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		bad, ok := res.(*gcpgenserver.V1betaUpdateQuotaRuleVCPBadRequest)
		assert.True(tt, ok, "expected V1betaUpdateQuotaRuleVCPBadRequest, got %T", res)
		assert.Equal(tt, float64(http.StatusBadRequest), bad.Code)
		assert.NotEmpty(tt, bad.Message)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQuotaRuleInternalFailsWithNotFound", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateQuotaRuleVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			DiskLimitInMib: gcpgenserver.NewOptInt64(2048),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("UpdateQuotaRuleInternal", mock.Anything, mock.Anything).Return(nil, nil, errors.NewNotFoundErr("quota rule not found", nil))

		res, err := handler.V1betaUpdateQuotaRuleVCP(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		bad, ok := res.(*gcpgenserver.V1betaUpdateQuotaRuleVCPBadRequest)
		assert.True(tt, ok, "expected V1betaUpdateQuotaRuleVCPBadRequest, got %T", res)
		assert.Equal(tt, float64(http.StatusBadRequest), bad.Code)
		assert.NotEmpty(tt, bad.Message)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQuotaRuleInternalFailsWithConflict", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateQuotaRuleVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			DiskLimitInMib: gcpgenserver.NewOptInt64(2048),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("UpdateQuotaRuleInternal", mock.Anything, mock.Anything).Return(nil, nil, errors.NewConflictErr("quota rule is in transition state"))

		res, err := handler.V1betaUpdateQuotaRuleVCP(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		conflict, ok := res.(*gcpgenserver.V1betaUpdateQuotaRuleVCPConflict)
		assert.True(tt, ok, "expected V1betaUpdateQuotaRuleVCPConflict, got %T", res)
		assert.Equal(tt, float64(http.StatusConflict), conflict.Code)
		assert.NotEmpty(tt, conflict.Message)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQuotaRuleInternalFailsWithInternalError", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateQuotaRuleVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		req := &gcpgenserver.QuotaRulesUpdateV1beta{
			DiskLimitInMib: gcpgenserver.NewOptInt64(2048),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("UpdateQuotaRuleInternal", mock.Anything, mock.Anything).Return(nil, nil, errors.New("something bad"))

		res, err := handler.V1betaUpdateQuotaRuleVCP(context.Background(), req, params)

		assert.Error(tt, err)
		assert.NotNil(tt, res)

		internal, ok := res.(*gcpgenserver.V1betaUpdateQuotaRuleVCPInternalServerError)
		assert.True(tt, ok, "expected V1betaUpdateQuotaRuleVCPInternalServerError, got %T", res)
		assert.Equal(tt, float64(http.StatusInternalServerError), internal.Code)
		mockOrch.AssertExpectations(tt)
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

		expJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-1"},
			WorkflowID: "workflow-id-1",
			State:      string(models.JobsStateNEW),
		}

		mockOrch.On("CreateQuotaRuleInternal", mock.Anything, mock.Anything).Return(expQuota, expJob, nil)

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
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateReadyDetails,
			Description:           "default quota",
		}

		mockOrch.On("CreateQuotaRuleInternal", mock.Anything, mock.Anything).Return(expQuota, nil, nil)

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

		mockOrch.On("CreateQuotaRuleInternal", mock.Anything, mock.Anything).Return(nil, nil, errors.NewUserInputValidationErr("invalid request"))

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

		mockOrch.On("CreateQuotaRuleInternal", mock.Anything, mock.Anything).Return(nil, nil, errors.NewConflictErr("quota rule already exists"))

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

		mockOrch.On("CreateQuotaRuleInternal", mock.Anything, mock.Anything).Return(nil, nil, errors.New("something bad"))

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
			LifeCycleState:        models.LifeCycleStateREADY,
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
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateReadyDetails,
			Description:           "Unknown quota type",
		}

		result := convertQuotaRuleToV1beta(quotaRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, gcpgenserver.QuotaRulesV1betaQuotaType("UNKNOWN_QUOTA_TYPE"), result.QuotaType)
	})
}

func TestConvertToVCPQuotaRulesV1Beta(t *testing.T) {
	t.Run("WhenConvertingMultipleQuotaRules", func(tt *testing.T) {
		quotaRules := []*models.QuotaRule{
			{
				BaseModel:             models.BaseModel{UUID: "uuid-1"},
				Name:                  "quota-rule-1",
				QuotaType:             "INDIVIDUAL_USER_QUOTA",
				DiskLimitInMib:        1024,
				QuotaTarget:           "user:alice",
				LifeCycleState:        models.LifeCycleStateAvailable,
				LifeCycleStateDetails: models.LifeCycleStateReadyDetails,
			},
			{
				BaseModel:             models.BaseModel{UUID: "uuid-2"},
				Name:                  "quota-rule-2",
				QuotaType:             "INDIVIDUAL_GROUP_QUOTA",
				DiskLimitInMib:        2048,
				QuotaTarget:           "group:developers",
				LifeCycleState:        models.LifeCycleStateCreating,
				LifeCycleStateDetails: models.LifeCycleStateCreatingDetails,
			},
		}

		result := convertToVCPQuotaRulesV1Beta(quotaRules)

		assert.NotNil(tt, result)
		assert.Len(tt, result, 2)
		assert.Equal(tt, "uuid-1", result[0].QuotaId.Value)
		assert.Equal(tt, "quota-rule-1", result[0].ResourceId)
		assert.Equal(tt, "uuid-2", result[1].QuotaId.Value)
		assert.Equal(tt, "quota-rule-2", result[1].ResourceId)
	})

	t.Run("WhenConvertingEmptyQuotaRulesList", func(tt *testing.T) {
		quotaRules := []*models.QuotaRule{}

		result := convertToVCPQuotaRulesV1Beta(quotaRules)

		assert.NotNil(tt, result)
		assert.Len(tt, result, 0)
	})
}

func TestQuotaRuleQuotaTypeVCPV1Beta(t *testing.T) {
	t.Run("WhenConvertingIndividualUserQuota", func(tt *testing.T) {
		result := QuotaRuleQuotaTypeVCPV1Beta("INDIVIDUAL_USER_QUOTA")
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaQuotaTypeINDIVIDUALUSERQUOTA, result)
	})

	t.Run("WhenConvertingIndividualGroupQuota", func(tt *testing.T) {
		result := QuotaRuleQuotaTypeVCPV1Beta("INDIVIDUAL_GROUP_QUOTA")
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaQuotaTypeINDIVIDUALGROUPQUOTA, result)
	})

	t.Run("WhenConvertingDefaultUserQuota", func(tt *testing.T) {
		result := QuotaRuleQuotaTypeVCPV1Beta("DEFAULT_USER_QUOTA")
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaQuotaTypeDEFAULTUSERQUOTA, result)
	})

	t.Run("WhenConvertingDefaultGroupQuota", func(tt *testing.T) {
		result := QuotaRuleQuotaTypeVCPV1Beta("DEFAULT_GROUP_QUOTA")
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaQuotaTypeDEFAULTGROUPQUOTA, result)
	})

	t.Run("WhenConvertingUnknownQuotaType", func(tt *testing.T) {
		result := QuotaRuleQuotaTypeVCPV1Beta("UNKNOWN_QUOTA_TYPE")
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaQuotaType("UNKNOWN_QUOTA_TYPE"), result)
	})
}

func TestQuotaRuleLifeCycleVCPV1Beta(t *testing.T) {
	t.Run("WhenConvertingCreatingState", func(tt *testing.T) {
		result := QuotaRuleLifeCycleVCPV1Beta(models.LifeCycleStateCreating)
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaStateCREATING, result.Value)
	})

	t.Run("WhenConvertingReadyState", func(tt *testing.T) {
		result := QuotaRuleLifeCycleVCPV1Beta(models.LifeCycleStateREADY)
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaStateREADY, result.Value)
	})

	t.Run("WhenConvertingUpdatingState", func(tt *testing.T) {
		result := QuotaRuleLifeCycleVCPV1Beta(models.LifeCycleStateUpdating)
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaStateUPDATING, result.Value)
	})

	t.Run("WhenConvertingDeletingState", func(tt *testing.T) {
		result := QuotaRuleLifeCycleVCPV1Beta(models.LifeCycleStateDeleting)
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaStateDELETING, result.Value)
	})

	t.Run("WhenConvertingErrorState", func(tt *testing.T) {
		result := QuotaRuleLifeCycleVCPV1Beta(models.LifeCycleStateError)
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaStateERROR, result.Value)
	})

	t.Run("WhenConvertingUnknownState", func(tt *testing.T) {
		result := QuotaRuleLifeCycleVCPV1Beta("UNKNOWN_STATE")
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaStateSTATEUNSPECIFIED, result.Value)
	})
}

func TestJobStateToVCPV1Beta(t *testing.T) {
	t.Run("WhenConvertingNewJobState", func(tt *testing.T) {
		result := JobStateToVCPV1Beta(models.JobsStateNEW)
		assert.Equal(tt, gcpgenserver.JobV1betaStateOngoing, result.Value)
	})

	t.Run("WhenConvertingProcessingJobState", func(tt *testing.T) {
		result := JobStateToVCPV1Beta(models.JobsStatePROCESSING)
		assert.Equal(tt, gcpgenserver.JobV1betaStateOngoing, result.Value)
	})

	t.Run("WhenConvertingDoneJobState", func(tt *testing.T) {
		result := JobStateToVCPV1Beta(models.JobsStateDONE)
		assert.Equal(tt, gcpgenserver.JobV1betaStateDone, result.Value)
	})

	t.Run("WhenConvertingErrorJobState", func(tt *testing.T) {
		result := JobStateToVCPV1Beta(models.JobsStateERROR)
		assert.Equal(tt, gcpgenserver.JobV1betaStateError, result.Value)
	})

	t.Run("WhenConvertingUnknownJobState", func(tt *testing.T) {
		result := JobStateToVCPV1Beta(models.JobState("UNKNOWN_STATE"))
		assert.Equal(tt, gcpgenserver.JobV1betaStateOngoing, result.Value)
	})
}

func TestV1betaListAllQuotaRules(t *testing.T) {
	t.Run("WhenListAllQuotaRulesSucceeds", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaListAllQuotaRulesParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		quotaRuleList := []*models.QuotaRule{
			{
				BaseModel:      models.BaseModel{UUID: "quota-rule-uuid-1"},
				Name:           "quota-rule-1",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				DiskLimitInMib: 1024,
				QuotaTarget:    "user:alice",
			},
			{
				BaseModel:      models.BaseModel{UUID: "quota-rule-uuid-2"},
				Name:           "quota-rule-2",
				QuotaType:      "INDIVIDUAL_GROUP_QUOTA",
				DiskLimitInMib: 2048,
				QuotaTarget:    "group:developers",
			},
		}

		mockOrch.On("ListQuotaRules", mock.Anything, mock.Anything).Return(quotaRuleList, nil)

		res, err := handler.V1betaListAllQuotaRules(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		ok, okType := res.(*gcpgenserver.V1betaListAllQuotaRulesOK)
		assert.True(tt, okType, "expected V1betaListAllQuotaRulesOK, got %T", res)
		assert.NotNil(tt, ok.QuotaRules)
		assert.Len(tt, ok.QuotaRules, 2)
		assert.Equal(tt, "quota-rule-uuid-1", ok.QuotaRules[0].QuotaId.Value)
		assert.Equal(tt, "quota-rule-1", ok.QuotaRules[0].ResourceId)
		assert.Equal(tt, "quota-rule-uuid-2", ok.QuotaRules[1].QuotaId.Value)
		assert.Equal(tt, "quota-rule-2", ok.QuotaRules[1].ResourceId)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}

		params := gcpgenserver.V1betaListAllQuotaRulesParams{
			ProjectNumber: "project-1",
			LocationId:    "invalid-location",
			VolumeId:      "vol-1",
		}

		res, err := handler.V1betaListAllQuotaRules(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		bad, ok := res.(*gcpgenserver.V1betaListAllQuotaRulesBadRequest)
		assert.True(tt, ok, "expected V1betaListAllQuotaRulesBadRequest, got %T", res)
		assert.Equal(tt, float64(http.StatusBadRequest), bad.Code)
		assert.NotEmpty(tt, bad.Message)
	})

	t.Run("WhenListQuotaRulesFailsWithNotFound", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaListAllQuotaRulesParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("ListQuotaRules", mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("volume not found", nil))

		res, err := handler.V1betaListAllQuotaRules(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		notFound, ok := res.(*gcpgenserver.V1betaListAllQuotaRulesNotFound)
		assert.True(tt, ok, "expected V1betaListAllQuotaRulesNotFound, got %T", res)
		assert.Equal(tt, float64(http.StatusNotFound), notFound.Code)
		assert.NotEmpty(tt, notFound.Message)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenListQuotaRulesFailsWithInternalError", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaListAllQuotaRulesParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("ListQuotaRules", mock.Anything, mock.Anything).Return(nil, errors.New("internal server error"))

		res, err := handler.V1betaListAllQuotaRules(context.Background(), params)

		assert.Error(tt, err)
		assert.NotNil(tt, res)

		internal, ok := res.(*gcpgenserver.V1betaListAllQuotaRulesInternalServerError)
		assert.True(tt, ok, "expected V1betaListAllQuotaRulesInternalServerError, got %T", res)
		assert.Equal(tt, float64(http.StatusInternalServerError), internal.Code)
		assert.Equal(tt, "Internal server error", internal.Message)
		mockOrch.AssertExpectations(tt)
	})
}
func TestV1betaDeleteQuotaRule(t *testing.T) {
	t.Run("WhenDeleteQuotaRuleSucceeds", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaDeleteQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		expQuota := &models.QuotaRule{
			BaseModel:      models.BaseModel{UUID: "quota-rule-uuid-1"},
			Name:           "quota-name",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInMib: 1024,
			QuotaTarget:    "user:alice",
		}
		mockOrch.On("DeleteQuotaRule", mock.Anything, mock.Anything).Return(expQuota, "op-1", nil)

		res, err := handler.V1betaDeleteQuotaRule(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		op, ok := res.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok, "expected OperationV1beta, got %T", res)
		assert.NotNil(tt, op.Name)
		assert.Equal(tt, "/v1beta/projects/project-1/locations/us-central1/operations/op-1", op.Name.Value)
		assert.NotNil(tt, op.Done)
		assert.False(tt, op.Done.Value)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenDeleteQuotaRuleSucceedsWithoutOperationID", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaDeleteQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		expQuota := &models.QuotaRule{
			BaseModel:      models.BaseModel{UUID: "quota-rule-uuid-1"},
			Name:           "quota-name",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInMib: 1024,
		}
		mockOrch.On("DeleteQuotaRule", mock.Anything, mock.Anything).Return(expQuota, "", nil)

		res, err := handler.V1betaDeleteQuotaRule(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		op, ok := res.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok, "expected OperationV1beta, got %T", res)
		assert.NotNil(tt, op.Name)
		assert.Equal(tt, "", op.Name.Value, "operation name should be empty when operationID is empty")
		assert.NotNil(tt, op.Done)
		assert.False(tt, op.Done.Value)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}

		params := gcpgenserver.V1betaDeleteQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "invalid-location",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		res, err := handler.V1betaDeleteQuotaRule(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		bad, ok := res.(*gcpgenserver.V1betaDeleteQuotaRuleBadRequest)
		assert.True(tt, ok, "expected V1betaDeleteQuotaRuleBadRequest, got %T", res)
		assert.Equal(tt, float64(http.StatusBadRequest), bad.Code)
		assert.NotEmpty(tt, bad.Message)
	})

	t.Run("WhenDeleteQuotaRuleFailsWithBadRequest", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaDeleteQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("DeleteQuotaRule", mock.Anything, mock.Anything).Return(nil, "", errors.NewUserInputValidationErr("invalid request"))

		res, err := handler.V1betaDeleteQuotaRule(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		bad, ok := res.(*gcpgenserver.V1betaDeleteQuotaRuleBadRequest)
		assert.True(tt, ok, "expected V1betaDeleteQuotaRuleBadRequest, got %T", res)
		assert.Equal(tt, float64(http.StatusBadRequest), bad.Code)
		assert.NotEmpty(tt, bad.Message)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenDeleteQuotaRuleFailsWithNotFound", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaDeleteQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("DeleteQuotaRule", mock.Anything, mock.Anything).Return(nil, "", errors.NewNotFoundErr("quota rule not found", nil))

		res, err := handler.V1betaDeleteQuotaRule(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		bad, ok := res.(*gcpgenserver.V1betaDeleteQuotaRuleBadRequest)
		assert.True(tt, ok, "expected V1betaDeleteQuotaRuleBadRequest, got %T", res)
		assert.Equal(tt, float64(http.StatusBadRequest), bad.Code)
		assert.NotEmpty(tt, bad.Message)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenDeleteQuotaRuleFailsWithConflict", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaDeleteQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("DeleteQuotaRule", mock.Anything, mock.Anything).Return(nil, "", errors.NewConflictErr("quota rule is in transition state"))

		res, err := handler.V1betaDeleteQuotaRule(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		conflict, ok := res.(*gcpgenserver.V1betaDeleteQuotaRuleConflict)
		assert.True(tt, ok, "expected V1betaDeleteQuotaRuleConflict, got %T", res)
		assert.Equal(tt, float64(http.StatusConflict), conflict.Code)
		assert.NotEmpty(tt, conflict.Message)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenDeleteQuotaRuleFailsWithInternalError", func(tt *testing.T) {
		mockOrch := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaDeleteQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("DeleteQuotaRule", mock.Anything, mock.Anything).Return(nil, "", errors.New("something bad"))

		res, err := handler.V1betaDeleteQuotaRule(context.Background(), params)

		assert.Error(tt, err)
		assert.NotNil(tt, res)

		internal, ok := res.(*gcpgenserver.V1betaDeleteQuotaRuleInternalServerError)
		assert.True(tt, ok, "expected V1betaDeleteQuotaRuleInternalServerError, got %T", res)
		assert.Equal(tt, float64(http.StatusInternalServerError), internal.Code)
		mockOrch.AssertExpectations(tt)
	})
}
