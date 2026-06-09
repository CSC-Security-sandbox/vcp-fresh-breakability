package api

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/quota_rules"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	orchestratorcommon "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"gorm.io/gorm"
)

func TestMain(m *testing.M) {
	// Enable quota rule API for tests that expect success; individual tests may override for disabled behavior
	_ = os.Setenv("ENABLE_QUOTA_RULE", "true")
	os.Exit(m.Run())
}

func TestV1betaCreateQuotaRule(t *testing.T) {
	t.Run("WhenCreateQuotaRuleSucceeds", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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

	t.Run("WhenQuotaRuleAPIDisabled", func(tt *testing.T) {
		// enableQuotaRule is a package-level var set at init; override it for this test
		orig := enableQuotaRule
		defer func() { enableQuotaRule = orig }()
		enableQuotaRule = false

		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
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

		res, err := handler.V1betaCreateQuotaRule(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		bad, ok := res.(*gcpgenserver.V1betaCreateQuotaRuleBadRequest)
		assert.True(tt, ok, "expected V1betaCreateQuotaRuleBadRequest when API disabled, got %T", res)
		assert.Equal(tt, float64(http.StatusBadRequest), bad.Code)
		assert.Equal(tt, "quota rule API is disabled", bad.Message)
	})

	t.Run("WhenCreateQuotaRuleFailsWithBadRequest", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
			LifeCycleState:        datamodel.LifeCycleStateUpdating,
			LifeCycleStateDetails: datamodel.LifeCycleStateUpdatingDetails,
		}

		expJob := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID:      "job-uuid-1",
				CreatedAt: time.Now(),
			},
			WorkflowID: "workflow-id-1",
			State:      string(datamodel.JobsStateNEW),
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
		assert.Equal(tt, datamodel.LifeCycleStateUpdatingDetails, quotaRuleVCP.StateDetails.Value)
		assert.Equal(tt, "updated description", quotaRuleVCP.Description.Value)
		assert.Len(tt, quotaRuleVCP.Jobs, 1, "Expected 1 job in response")
		assert.Equal(tt, "job-uuid-1", quotaRuleVCP.Jobs[0].JobId.Value)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQuotaRuleInternalSucceedsWithoutJob", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
			LifeCycleState:        datamodel.LifeCycleStateREADY,
			LifeCycleStateDetails: datamodel.LifeCycleStateReadyDetails,
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
			LifeCycleState:        datamodel.LifeCycleStateCreating,
			LifeCycleStateDetails: datamodel.LifeCycleStateCreatingDetails,
			Description:           "desc",
		}

		expJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid-1"},
			WorkflowID: "workflow-id-1",
			State:      string(datamodel.JobsStateNEW),
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
		assert.Equal(tt, datamodel.LifeCycleStateCreatingDetails, quotaRuleVCP.StateDetails.Value)
		assert.Equal(tt, "desc", quotaRuleVCP.Description.Value)
		assert.Len(tt, quotaRuleVCP.Jobs, 1, "Expected 1 job in response")
		assert.Equal(tt, "job-uuid-1", quotaRuleVCP.Jobs[0].JobId.Value)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenCreateQuotaRuleInternalSucceedsWithoutJob", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
			LifeCycleState:        datamodel.LifeCycleStateREADY,
			LifeCycleStateDetails: datamodel.LifeCycleStateReadyDetails,
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
			LifeCycleState:        datamodel.LifeCycleStateREADY,
			LifeCycleStateDetails: datamodel.LifeCycleStateReadyDetails,
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
		assert.Equal(tt, datamodel.LifeCycleStateReadyDetails, result.StateDetails.Value)
		assert.Equal(tt, "Test quota rule", result.Description.Value)
	})

	t.Run("WhenConvertingIndividualGroupQuota", func(tt *testing.T) {
		quotaRule := &models.QuotaRule{
			BaseModel:             models.BaseModel{UUID: "uuid-2"},
			Name:                  "quota-rule-2",
			QuotaType:             "INDIVIDUAL_GROUP_QUOTA",
			DiskLimitInMib:        2048,
			QuotaTarget:           "group:developers",
			LifeCycleState:        datamodel.LifeCycleStateCreating,
			LifeCycleStateDetails: datamodel.LifeCycleStateCreatingDetails,
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
			LifeCycleState:        datamodel.LifeCycleStateUpdating,
			LifeCycleStateDetails: datamodel.LifeCycleStateUpdatingDetails,
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
			LifeCycleState:        datamodel.LifeCycleStateDeleting,
			LifeCycleStateDetails: datamodel.LifeCycleStateDeletingDetails,
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
			LifeCycleState:        datamodel.LifeCycleStateError,
			LifeCycleStateDetails: datamodel.LifeCycleStateCreationErrorDetails,
			Description:           "Failed quota rule",
		}

		result := convertQuotaRuleToV1beta(quotaRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, gcpgenserver.QuotaRulesV1betaStateERROR, result.State.Value)
		assert.Equal(tt, datamodel.LifeCycleStateCreationErrorDetails, result.StateDetails.Value)
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
			LifeCycleState:        datamodel.LifeCycleStateREADY,
			LifeCycleStateDetails: datamodel.LifeCycleStateReadyDetails,
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
				LifeCycleState:        datamodel.LifeCycleStateREADY,
				LifeCycleStateDetails: datamodel.LifeCycleStateReadyDetails,
			},
			{
				BaseModel:             models.BaseModel{UUID: "uuid-2"},
				Name:                  "quota-rule-2",
				QuotaType:             "INDIVIDUAL_GROUP_QUOTA",
				DiskLimitInMib:        2048,
				QuotaTarget:           "group:developers",
				LifeCycleState:        datamodel.LifeCycleStateCreating,
				LifeCycleStateDetails: datamodel.LifeCycleStateCreatingDetails,
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
		result := QuotaRuleLifeCycleVCPV1Beta(datamodel.LifeCycleStateCreating)
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaStateCREATING, result.Value)
	})

	t.Run("WhenConvertingReadyState", func(tt *testing.T) {
		result := QuotaRuleLifeCycleVCPV1Beta(datamodel.LifeCycleStateREADY)
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaStateREADY, result.Value)
	})

	t.Run("WhenConvertingUpdatingState", func(tt *testing.T) {
		result := QuotaRuleLifeCycleVCPV1Beta(datamodel.LifeCycleStateUpdating)
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaStateUPDATING, result.Value)
	})

	t.Run("WhenConvertingDeletingState", func(tt *testing.T) {
		result := QuotaRuleLifeCycleVCPV1Beta(datamodel.LifeCycleStateDeleting)
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaStateDELETING, result.Value)
	})

	t.Run("WhenConvertingErrorState", func(tt *testing.T) {
		result := QuotaRuleLifeCycleVCPV1Beta(datamodel.LifeCycleStateError)
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaStateERROR, result.Value)
	})

	t.Run("WhenConvertingUnknownState", func(tt *testing.T) {
		result := QuotaRuleLifeCycleVCPV1Beta("UNKNOWN_STATE")
		assert.Equal(tt, gcpgenserver.QuotaRulesVCPV1betaStateSTATEUNSPECIFIED, result.Value)
	})
}

func TestJobStateToVCPV1Beta(t *testing.T) {
	t.Run("WhenConvertingNewJobState", func(tt *testing.T) {
		result := JobStateToVCPV1Beta(datamodel.JobsStateNEW)
		assert.Equal(tt, gcpgenserver.JobV1betaStateOngoing, result.Value)
	})

	t.Run("WhenConvertingProcessingJobState", func(tt *testing.T) {
		result := JobStateToVCPV1Beta(datamodel.JobsStatePROCESSING)
		assert.Equal(tt, gcpgenserver.JobV1betaStateOngoing, result.Value)
	})

	t.Run("WhenConvertingDoneJobState", func(tt *testing.T) {
		result := JobStateToVCPV1Beta(datamodel.JobsStateDONE)
		assert.Equal(tt, gcpgenserver.JobV1betaStateDone, result.Value)
	})

	t.Run("WhenConvertingErrorJobState", func(tt *testing.T) {
		result := JobStateToVCPV1Beta(datamodel.JobsStateERROR)
		assert.Equal(tt, gcpgenserver.JobV1betaStateError, result.Value)
	})

	t.Run("WhenConvertingUnknownJobState", func(tt *testing.T) {
		result := JobStateToVCPV1Beta(datamodel.JobState("UNKNOWN_STATE"))
		assert.Equal(tt, gcpgenserver.JobV1betaStateOngoing, result.Value)
	})
}

func TestV1betaListAllQuotaRules(t *testing.T) {
	t.Run("WhenListAllQuotaRulesSucceeds", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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
		mockOrch := factory.NewMockOrchestratorFactory(tt)
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

func TestV1betaGetMultipleQuotaRules(t *testing.T) {
	t.Run("WhenGetMultipleQuotaRulesSucceeds", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaGetMultipleQuotaRulesParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleIdListV1beta{
			QuotaRuleUuids: []string{"quota-rule-uuid-1", "quota-rule-uuid-2"},
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

		mockOrch.On("GetMultipleQuotaRules", mock.Anything, "vol-1", "project-1", []string{"quota-rule-uuid-1", "quota-rule-uuid-2"}).Return(quotaRuleList, nil)

		res, err := handler.V1betaGetMultipleQuotaRules(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		ok, okType := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesOK)
		assert.True(tt, okType, "expected V1betaGetMultipleQuotaRulesOK, got %T", res)
		assert.NotNil(tt, ok.QuotaRules)
		assert.Len(tt, ok.QuotaRules, 2)
		assert.Equal(tt, "quota-rule-uuid-1", ok.QuotaRules[0].QuotaId.Value)
		assert.Equal(tt, "quota-rule-1", ok.QuotaRules[0].ResourceId)
		assert.Equal(tt, "quota-rule-uuid-2", ok.QuotaRules[1].QuotaId.Value)
		assert.Equal(tt, "quota-rule-2", ok.QuotaRules[1].ResourceId)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}

		params := gcpgenserver.V1betaGetMultipleQuotaRulesParams{
			ProjectNumber: "project-1",
			LocationId:    "invalid-location",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleIdListV1beta{
			QuotaRuleUuids: []string{"quota-rule-uuid-1"},
		}

		res, err := handler.V1betaGetMultipleQuotaRules(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		bad, ok := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesBadRequest)
		assert.True(tt, ok, "expected V1betaGetMultipleQuotaRulesBadRequest, got %T", res)
		assert.NotZero(tt, bad.Code)
		assert.NotEmpty(tt, bad.Message)
	})

	t.Run("WhenGetMultipleQuotaRulesFailsWithError", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaGetMultipleQuotaRulesParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleIdListV1beta{
			QuotaRuleUuids: []string{"quota-rule-uuid-1"},
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("GetMultipleQuotaRules", mock.Anything, "vol-1", "project-1", []string{"quota-rule-uuid-1"}).Return(nil, errors.New("internal server error"))

		res, err := handler.V1betaGetMultipleQuotaRules(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		internal, ok := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesInternalServerError)
		assert.True(tt, ok, "expected V1betaGetMultipleQuotaRulesInternalServerError, got %T", res)
		assert.Equal(tt, float64(http.StatusInternalServerError), internal.Code)
		assert.Equal(tt, "Internal server error", internal.Message)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenGetMultipleQuotaRulesReturnsEmptyList_DoesNotCallCVP_ReturnsEmptyList", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
			getMultipleQuotaRulesFromCVP = _getMultipleQuotaRulesFromCVP
		}()

		params := gcpgenserver.V1betaGetMultipleQuotaRulesParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleIdListV1beta{
			QuotaRuleUuids: []string{"quota-rule-uuid-1"},
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("GetMultipleQuotaRules", mock.Anything, "vol-1", "project-1", []string{"quota-rule-uuid-1"}).Return([]*models.QuotaRule{}, nil)

		cvpCalled := false
		originalGetMultipleQuotaRulesFromCVP := getMultipleQuotaRulesFromCVP
		getMultipleQuotaRulesFromCVP = func(ctx context.Context, req *gcpgenserver.QuotaRuleIdListV1beta, params gcpgenserver.V1betaGetMultipleQuotaRulesParams, vcpQuotaRules []gcpgenserver.QuotaRulesV1beta) (gcpgenserver.V1betaGetMultipleQuotaRulesRes, error) {
			cvpCalled = true
			return &gcpgenserver.V1betaGetMultipleQuotaRulesOK{QuotaRules: []gcpgenserver.QuotaRulesV1beta{}}, nil
		}

		res, err := handler.V1betaGetMultipleQuotaRules(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.False(tt, cvpCalled, "CVP should not be called when VCP returns success with empty list")
		ok, okType := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesOK)
		assert.True(tt, okType, "expected V1betaGetMultipleQuotaRulesOK, got %T", res)
		assert.NotNil(tt, ok.QuotaRules)
		assert.Len(tt, ok.QuotaRules, 0)
		mockOrch.AssertExpectations(tt)
		getMultipleQuotaRulesFromCVP = originalGetMultipleQuotaRulesFromCVP
	})

	t.Run("WhenVCPReturnsNotFoundErr_TriggersCVPFallback", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
			getMultipleQuotaRulesFromCVP = _getMultipleQuotaRulesFromCVP
		}()

		params := gcpgenserver.V1betaGetMultipleQuotaRulesParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleIdListV1beta{
			QuotaRuleUuids: []string{"quota-rule-uuid-1"},
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		// Mock VCP to return volume/account not found in VCP (triggers CVP fallback)
		mockOrch.On("GetMultipleQuotaRules", mock.Anything, "vol-1", "project-1", []string{"quota-rule-uuid-1"}).Return(nil, vsaerrors.NewVCPError(vsaerrors.ErrVolumeOrAccountNotFoundInVCP, nil))

		// Mock CVP client to return quota rules
		mockQuotaRulesClient := quota_rules.NewMockClientService(tt)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockQuotaRulesClient}
		originalGetMultipleQuotaRulesFromCVP := getMultipleQuotaRulesFromCVP
		getMultipleQuotaRulesFromCVP = func(ctx context.Context, req *gcpgenserver.QuotaRuleIdListV1beta, params gcpgenserver.V1betaGetMultipleQuotaRulesParams, vcpQuotaRules []gcpgenserver.QuotaRulesV1beta) (gcpgenserver.V1betaGetMultipleQuotaRulesRes, error) {
			return &gcpgenserver.V1betaGetMultipleQuotaRulesOK{
				QuotaRules: []gcpgenserver.QuotaRulesV1beta{
					{
						QuotaId:    gcpgenserver.NewOptString("cvp-quota-rule-uuid-1"),
						ResourceId: "cvp-quota-rule-1",
					},
				},
			}, nil
		}

		res, err := handler.V1betaGetMultipleQuotaRules(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		ok, okType := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesOK)
		assert.True(tt, okType, "expected V1betaGetMultipleQuotaRulesOK, got %T", res)
		assert.NotNil(tt, ok.QuotaRules)
		assert.Len(tt, ok.QuotaRules, 1)
		assert.Equal(tt, "cvp-quota-rule-uuid-1", ok.QuotaRules[0].QuotaId.Value)
		mockOrch.AssertExpectations(tt)
		_ = cvpClient // avoid unused variable
		getMultipleQuotaRulesFromCVP = originalGetMultipleQuotaRulesFromCVP
	})

	t.Run("WhenCVPReturnsNotFound_ReturnsNotFound", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
			getMultipleQuotaRulesFromCVP = _getMultipleQuotaRulesFromCVP
		}()

		params := gcpgenserver.V1betaGetMultipleQuotaRulesParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleIdListV1beta{
			QuotaRuleUuids: []string{"quota-rule-uuid-1"},
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		// Mock VCP to return volume/account not found in VCP (triggers CVP fallback)
		mockOrch.On("GetMultipleQuotaRules", mock.Anything, "vol-1", "project-1", []string{"quota-rule-uuid-1"}).Return(nil, vsaerrors.NewVCPError(vsaerrors.ErrVolumeOrAccountNotFoundInVCP, nil))

		// Mock CVP to return NotFound
		mockQuotaRulesClient := quota_rules.NewMockClientService(tt)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockQuotaRulesClient}
		originalGetMultipleQuotaRulesFromCVP := getMultipleQuotaRulesFromCVP
		getMultipleQuotaRulesFromCVP = func(ctx context.Context, req *gcpgenserver.QuotaRuleIdListV1beta, params gcpgenserver.V1betaGetMultipleQuotaRulesParams, vcpQuotaRules []gcpgenserver.QuotaRulesV1beta) (gcpgenserver.V1betaGetMultipleQuotaRulesRes, error) {
			return &gcpgenserver.V1betaGetMultipleQuotaRulesNotFound{
				Code:    404,
				Message: "Quota rules not found",
			}, nil
		}

		res, err := handler.V1betaGetMultipleQuotaRules(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		notFound, ok := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesNotFound)
		assert.True(tt, ok, "expected V1betaGetMultipleQuotaRulesNotFound, got %T", res)
		assert.Equal(tt, float64(404), notFound.Code)
		assert.Equal(tt, "Quota rules not found", notFound.Message)
		mockOrch.AssertExpectations(tt)
		_ = cvpClient // avoid unused variable
		getMultipleQuotaRulesFromCVP = originalGetMultipleQuotaRulesFromCVP
	})

	t.Run("WhenCVPReturnsBadRequest_ReturnsBadRequest", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
			getMultipleQuotaRulesFromCVP = _getMultipleQuotaRulesFromCVP
		}()

		params := gcpgenserver.V1betaGetMultipleQuotaRulesParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleIdListV1beta{
			QuotaRuleUuids: []string{"quota-rule-uuid-1"},
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		// Mock VCP to return volume/account not found in VCP (triggers CVP fallback)
		mockOrch.On("GetMultipleQuotaRules", mock.Anything, "vol-1", "project-1", []string{"quota-rule-uuid-1"}).Return(nil, vsaerrors.NewVCPError(vsaerrors.ErrVolumeOrAccountNotFoundInVCP, nil))

		// Mock CVP client to return BadRequest
		mockQuotaRulesClient := quota_rules.NewMockClientService(tt)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockQuotaRulesClient}
		originalGetMultipleQuotaRulesFromCVP := getMultipleQuotaRulesFromCVP
		getMultipleQuotaRulesFromCVP = func(ctx context.Context, req *gcpgenserver.QuotaRuleIdListV1beta, params gcpgenserver.V1betaGetMultipleQuotaRulesParams, vcpQuotaRules []gcpgenserver.QuotaRulesV1beta) (gcpgenserver.V1betaGetMultipleQuotaRulesRes, error) {
			return &gcpgenserver.V1betaGetMultipleQuotaRulesBadRequest{
				Code:    400,
				Message: "Invalid request",
			}, nil
		}

		res, err := handler.V1betaGetMultipleQuotaRules(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		badRequest, ok := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesBadRequest)
		assert.True(tt, ok, "expected V1betaGetMultipleQuotaRulesBadRequest, got %T", res)
		assert.Equal(tt, float64(400), badRequest.Code)
		assert.Equal(tt, "Invalid request", badRequest.Message)
		mockOrch.AssertExpectations(tt)
		_ = cvpClient // avoid unused variable
		getMultipleQuotaRulesFromCVP = originalGetMultipleQuotaRulesFromCVP
	})

	t.Run("WhenCVPReturnsUnauthorized_ReturnsUnauthorized", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
			getMultipleQuotaRulesFromCVP = _getMultipleQuotaRulesFromCVP
		}()

		params := gcpgenserver.V1betaGetMultipleQuotaRulesParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleIdListV1beta{
			QuotaRuleUuids: []string{"quota-rule-uuid-1"},
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		// Mock VCP to return volume/account not found in VCP (triggers CVP fallback)
		mockOrch.On("GetMultipleQuotaRules", mock.Anything, "vol-1", "project-1", []string{"quota-rule-uuid-1"}).Return(nil, vsaerrors.NewVCPError(vsaerrors.ErrVolumeOrAccountNotFoundInVCP, nil))

		// Mock CVP client to return Unauthorized
		mockQuotaRulesClient := quota_rules.NewMockClientService(tt)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockQuotaRulesClient}
		originalGetMultipleQuotaRulesFromCVP := getMultipleQuotaRulesFromCVP
		getMultipleQuotaRulesFromCVP = func(ctx context.Context, req *gcpgenserver.QuotaRuleIdListV1beta, params gcpgenserver.V1betaGetMultipleQuotaRulesParams, vcpQuotaRules []gcpgenserver.QuotaRulesV1beta) (gcpgenserver.V1betaGetMultipleQuotaRulesRes, error) {
			return &gcpgenserver.V1betaGetMultipleQuotaRulesUnauthorized{
				Code:    401,
				Message: "Unauthorized",
			}, nil
		}

		res, err := handler.V1betaGetMultipleQuotaRules(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		unauthorized, ok := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesUnauthorized)
		assert.True(tt, ok, "expected V1betaGetMultipleQuotaRulesUnauthorized, got %T", res)
		assert.Equal(tt, float64(401), unauthorized.Code)
		assert.Equal(tt, "Unauthorized", unauthorized.Message)
		mockOrch.AssertExpectations(tt)
		_ = cvpClient // avoid unused variable
		getMultipleQuotaRulesFromCVP = originalGetMultipleQuotaRulesFromCVP
	})

	t.Run("WhenCVPReturnsForbidden_ReturnsForbidden", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
			getMultipleQuotaRulesFromCVP = _getMultipleQuotaRulesFromCVP
		}()

		params := gcpgenserver.V1betaGetMultipleQuotaRulesParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleIdListV1beta{
			QuotaRuleUuids: []string{"quota-rule-uuid-1"},
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		// Mock VCP to return volume/account not found in VCP (triggers CVP fallback)
		mockOrch.On("GetMultipleQuotaRules", mock.Anything, "vol-1", "project-1", []string{"quota-rule-uuid-1"}).Return(nil, vsaerrors.NewVCPError(vsaerrors.ErrVolumeOrAccountNotFoundInVCP, nil))

		// Mock CVP client to return Forbidden
		mockQuotaRulesClient := quota_rules.NewMockClientService(tt)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockQuotaRulesClient}
		originalGetMultipleQuotaRulesFromCVP := getMultipleQuotaRulesFromCVP
		getMultipleQuotaRulesFromCVP = func(ctx context.Context, req *gcpgenserver.QuotaRuleIdListV1beta, params gcpgenserver.V1betaGetMultipleQuotaRulesParams, vcpQuotaRules []gcpgenserver.QuotaRulesV1beta) (gcpgenserver.V1betaGetMultipleQuotaRulesRes, error) {
			return &gcpgenserver.V1betaGetMultipleQuotaRulesForbidden{
				Code:    403,
				Message: "Forbidden",
			}, nil
		}

		res, err := handler.V1betaGetMultipleQuotaRules(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		forbidden, ok := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesForbidden)
		assert.True(tt, ok, "expected V1betaGetMultipleQuotaRulesForbidden, got %T", res)
		assert.Equal(tt, float64(403), forbidden.Code)
		assert.Equal(tt, "Forbidden", forbidden.Message)
		mockOrch.AssertExpectations(tt)
		_ = cvpClient // avoid unused variable
		getMultipleQuotaRulesFromCVP = originalGetMultipleQuotaRulesFromCVP
	})

	t.Run("WhenCVPReturnsTooManyRequests_ReturnsTooManyRequests", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
			getMultipleQuotaRulesFromCVP = _getMultipleQuotaRulesFromCVP
		}()

		params := gcpgenserver.V1betaGetMultipleQuotaRulesParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleIdListV1beta{
			QuotaRuleUuids: []string{"quota-rule-uuid-1"},
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		// Mock VCP to return volume/account not found in VCP (triggers CVP fallback)
		mockOrch.On("GetMultipleQuotaRules", mock.Anything, "vol-1", "project-1", []string{"quota-rule-uuid-1"}).Return(nil, vsaerrors.NewVCPError(vsaerrors.ErrVolumeOrAccountNotFoundInVCP, nil))

		// Mock CVP client to return TooManyRequests
		mockQuotaRulesClient := quota_rules.NewMockClientService(tt)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockQuotaRulesClient}
		originalGetMultipleQuotaRulesFromCVP := getMultipleQuotaRulesFromCVP
		getMultipleQuotaRulesFromCVP = func(ctx context.Context, req *gcpgenserver.QuotaRuleIdListV1beta, params gcpgenserver.V1betaGetMultipleQuotaRulesParams, vcpQuotaRules []gcpgenserver.QuotaRulesV1beta) (gcpgenserver.V1betaGetMultipleQuotaRulesRes, error) {
			return &gcpgenserver.V1betaGetMultipleQuotaRulesTooManyRequests{
				Code:    429,
				Message: "Too many requests",
			}, nil
		}

		res, err := handler.V1betaGetMultipleQuotaRules(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		tooManyRequests, ok := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesTooManyRequests)
		assert.True(tt, ok, "expected V1betaGetMultipleQuotaRulesTooManyRequests, got %T", res)
		assert.Equal(tt, float64(429), tooManyRequests.Code)
		assert.Equal(tt, "Too many requests", tooManyRequests.Message)
		mockOrch.AssertExpectations(tt)
		_ = cvpClient // avoid unused variable
		getMultipleQuotaRulesFromCVP = originalGetMultipleQuotaRulesFromCVP
	})

	t.Run("WhenCVPReturnsQuotaRules_ReturnsCVPQuotaRules", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
			getMultipleQuotaRulesFromCVP = _getMultipleQuotaRulesFromCVP
		}()

		params := gcpgenserver.V1betaGetMultipleQuotaRulesParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.QuotaRuleIdListV1beta{
			QuotaRuleUuids: []string{"quota-rule-uuid-1"},
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		// Mock VCP to return volume/account not found in VCP (triggers CVP fallback)
		mockOrch.On("GetMultipleQuotaRules", mock.Anything, "vol-1", "project-1", []string{"quota-rule-uuid-1"}).Return(nil, vsaerrors.NewVCPError(vsaerrors.ErrVolumeOrAccountNotFoundInVCP, nil))

		// Mock CVP client to return quota rules
		mockQuotaRulesClient := quota_rules.NewMockClientService(tt)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockQuotaRulesClient}
		originalGetMultipleQuotaRulesFromCVP := getMultipleQuotaRulesFromCVP
		getMultipleQuotaRulesFromCVP = func(ctx context.Context, req *gcpgenserver.QuotaRuleIdListV1beta, params gcpgenserver.V1betaGetMultipleQuotaRulesParams, vcpQuotaRules []gcpgenserver.QuotaRulesV1beta) (gcpgenserver.V1betaGetMultipleQuotaRulesRes, error) {
			return &gcpgenserver.V1betaGetMultipleQuotaRulesOK{
				QuotaRules: []gcpgenserver.QuotaRulesV1beta{
					{
						QuotaId:    gcpgenserver.NewOptString("cvp-quota-rule-uuid-1"),
						ResourceId: "cvp-quota-rule-1",
						QuotaType:  gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA,
					},
				},
			}, nil
		}

		res, err := handler.V1betaGetMultipleQuotaRules(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		ok, okType := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesOK)
		assert.True(tt, okType, "expected V1betaGetMultipleQuotaRulesOK, got %T", res)
		assert.NotNil(tt, ok.QuotaRules)
		assert.Len(tt, ok.QuotaRules, 1)
		assert.Equal(tt, "cvp-quota-rule-uuid-1", ok.QuotaRules[0].QuotaId.Value)
		assert.Equal(tt, "cvp-quota-rule-1", ok.QuotaRules[0].ResourceId)
		mockOrch.AssertExpectations(tt)
		_ = cvpClient // avoid unused variable
		getMultipleQuotaRulesFromCVP = originalGetMultipleQuotaRulesFromCVP
	})
}

func TestV1betaDescribeQuotaRule(t *testing.T) {
	t.Run("WhenDescribeQuotaRuleSucceeds", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaDescribeQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		expQuota := &models.QuotaRule{
			BaseModel:      models.BaseModel{UUID: "quota-rule-uuid-1"},
			Name:           "quota-rule-1",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInMib: 1024,
			QuotaTarget:    "user:alice",
		}

		mockOrch.On("DescribeQuotaRule", mock.Anything, "vol-1", "project-1", "quota-rule-uuid-1").Return(expQuota, nil)

		res, err := handler.V1betaDescribeQuotaRule(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		quotaRule, ok := res.(*gcpgenserver.QuotaRulesV1beta)
		assert.True(tt, ok, "expected QuotaRulesV1beta, got %T", res)
		assert.Equal(tt, "quota-rule-uuid-1", quotaRule.QuotaId.Value)
		assert.Equal(tt, "quota-rule-1", quotaRule.ResourceId)
		assert.Equal(tt, gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA, quotaRule.QuotaType)
		assert.Equal(tt, int64(1024), quotaRule.DiskLimitInMib)
		assert.Equal(tt, "user:alice", quotaRule.QuotaTarget.Value)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}

		params := gcpgenserver.V1betaDescribeQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "invalid-location",
			VolumeId:      "vol-1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		res, err := handler.V1betaDescribeQuotaRule(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		bad, ok := res.(*gcpgenserver.V1betaDescribeQuotaRuleBadRequest)
		assert.True(tt, ok, "expected V1betaDescribeQuotaRuleBadRequest, got %T", res)
		assert.NotZero(tt, bad.Code)
		assert.NotEmpty(tt, bad.Message)
	})

	t.Run("WhenDescribeQuotaRuleFailsWithNotFound", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaDescribeQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("DescribeQuotaRule", mock.Anything, "vol-1", "project-1", "quota-rule-uuid-1").Return(nil, errors.NewNotFoundErr("quota rule not found", nil))

		res, err := handler.V1betaDescribeQuotaRule(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		notFound, ok := res.(*gcpgenserver.V1betaDescribeQuotaRuleNotFound)
		assert.True(tt, ok, "expected V1betaDescribeQuotaRuleNotFound, got %T", res)
		assert.Equal(tt, float64(http.StatusNotFound), notFound.Code)
		assert.Equal(tt, "Quota rule not found", notFound.Message)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenDescribeQuotaRuleFailsWithInternalError", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaDescribeQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("DescribeQuotaRule", mock.Anything, "vol-1", "project-1", "quota-rule-uuid-1").Return(nil, errors.New("internal server error"))

		res, err := handler.V1betaDescribeQuotaRule(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		internal, ok := res.(*gcpgenserver.V1betaDescribeQuotaRuleInternalServerError)
		assert.True(tt, ok, "expected V1betaDescribeQuotaRuleInternalServerError, got %T", res)
		assert.Equal(tt, float64(http.StatusInternalServerError), internal.Code)
		assert.Equal(tt, "Internal server error", internal.Message)
		mockOrch.AssertExpectations(tt)
	})

	t.Run("WhenDescribeQuotaRuleReturnsNil", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaDescribeQuotaRuleParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
			QuotaRuleId:   "quota-rule-uuid-1",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		mockOrch.On("DescribeQuotaRule", mock.Anything, "vol-1", "project-1", "quota-rule-uuid-1").Return(nil, nil)

		res, err := handler.V1betaDescribeQuotaRule(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		notFound, ok := res.(*gcpgenserver.V1betaDescribeQuotaRuleNotFound)
		assert.True(tt, ok, "expected V1betaDescribeQuotaRuleNotFound, got %T", res)
		assert.Equal(tt, float64(http.StatusNotFound), notFound.Code)
		assert.Equal(tt, "Quota rule not found", notFound.Message)
		mockOrch.AssertExpectations(tt)
	})
}

func TestConvertCVPQuotaRuleToV1beta(t *testing.T) {
	t.Run("WhenConvertingCVPQuotaRuleWithAllFields", func(tt *testing.T) {
		quotaType := "INDIVIDUAL_USER_QUOTA"
		createdAt := strfmt.DateTime(time.Now())
		updatedAt := strfmt.DateTime(time.Now())
		cvpRule := &cvpmodels.QuotaRulesV1beta{
			QuotaID:        "cvp-quota-id",
			ResourceID:     nillable.ToPointer("cvp-res-id"),
			QuotaType:      &quotaType,
			DiskLimitInMib: nillable.ToPointer(int64(1024)),
			QuotaTarget:    nillable.ToPointer("user:alice"),
			State:          "READY",
			StateDetails:   "Ready for use",
			Description:    nillable.ToPointer("Test description"),
			CreatedAt:      createdAt,
			UpdatedAt:      updatedAt,
		}

		result := convertCVPQuotaRuleToV1beta(cvpRule)

		assert.Equal(tt, "cvp-quota-id", result.QuotaId.Value)
		assert.Equal(tt, "cvp-res-id", result.ResourceId)
		assert.Equal(tt, gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA, result.QuotaType)
		assert.Equal(tt, int64(1024), result.DiskLimitInMib)
		assert.Equal(tt, "user:alice", result.QuotaTarget.Value)
		assert.Equal(tt, gcpgenserver.QuotaRulesV1betaStateREADY, result.State.Value)
		assert.Equal(tt, "Ready for use", result.StateDetails.Value)
		assert.Equal(tt, "Test description", result.Description.Value)
		assert.True(tt, result.CreatedAt.IsSet())
		assert.True(tt, result.UpdatedAt.IsSet())
	})

	t.Run("WhenConvertingCVPQuotaRuleWithNilQuotaType", func(tt *testing.T) {
		cvpRule := &cvpmodels.QuotaRulesV1beta{
			QuotaID:    "cvp-quota-id",
			ResourceID: nillable.ToPointer("cvp-res-id"),
			QuotaType:  nil, // nil QuotaType
			State:      "READY",
		}

		result := convertCVPQuotaRuleToV1beta(cvpRule)

		assert.Equal(tt, "cvp-quota-id", result.QuotaId.Value)
		// QuotaType should be zero value when nil
		assert.Equal(tt, gcpgenserver.QuotaRulesV1betaQuotaType(""), result.QuotaType)
	})

	t.Run("WhenConvertingCVPQuotaRuleWithEmptyState", func(tt *testing.T) {
		quotaType := "INDIVIDUAL_USER_QUOTA"
		cvpRule := &cvpmodels.QuotaRulesV1beta{
			QuotaID:    "cvp-quota-id",
			ResourceID: nillable.ToPointer("cvp-res-id"),
			QuotaType:  &quotaType,
			State:      "", // Empty state
		}

		result := convertCVPQuotaRuleToV1beta(cvpRule)

		assert.Equal(tt, gcpgenserver.QuotaRulesV1betaStateSTATEUNSPECIFIED, result.State.Value)
	})

	t.Run("WhenConvertingCVPQuotaRuleWithZeroTimestamps", func(tt *testing.T) {
		quotaType := "INDIVIDUAL_USER_QUOTA"
		cvpRule := &cvpmodels.QuotaRulesV1beta{
			QuotaID:    "cvp-quota-id",
			ResourceID: nillable.ToPointer("cvp-res-id"),
			QuotaType:  &quotaType,
			State:      "READY",
			CreatedAt:  strfmt.DateTime{}, // Zero timestamp
			UpdatedAt:  strfmt.DateTime{}, // Zero timestamp
		}

		result := convertCVPQuotaRuleToV1beta(cvpRule)

		assert.False(tt, result.CreatedAt.IsSet(), "CreatedAt should not be set for zero timestamp")
		assert.False(tt, result.UpdatedAt.IsSet(), "UpdatedAt should not be set for zero timestamp")
	})

	t.Run("WhenConvertingCVPQuotaRuleWithNilOptionalFields", func(tt *testing.T) {
		quotaType := "DEFAULT_USER_QUOTA"
		cvpRule := &cvpmodels.QuotaRulesV1beta{
			QuotaID:        "cvp-quota-id",
			ResourceID:     nil,
			QuotaType:      &quotaType,
			DiskLimitInMib: nil,
			QuotaTarget:    nil,
			Description:    nil,
			State:          "CREATING",
		}

		result := convertCVPQuotaRuleToV1beta(cvpRule)

		assert.Equal(tt, "", result.ResourceId)
		assert.Equal(tt, int64(0), result.DiskLimitInMib)
		// nillable.GetString returns empty string for nil, which is then wrapped in OptString, so it's set
		assert.True(tt, result.QuotaTarget.IsSet())
		assert.Equal(tt, "", result.QuotaTarget.Value)
		assert.True(tt, result.Description.IsSet())
		assert.Equal(tt, "", result.Description.Value)
	})

	t.Run("WhenConvertingCVPQuotaRuleWithDifferentStates", func(tt *testing.T) {
		quotaType := "INDIVIDUAL_GROUP_QUOTA"
		testCases := []struct {
			state    string
			expected gcpgenserver.QuotaRulesV1betaState
		}{
			{"CREATING", gcpgenserver.QuotaRulesV1betaStateCREATING},
			{"READY", gcpgenserver.QuotaRulesV1betaStateREADY},
			{"UPDATING", gcpgenserver.QuotaRulesV1betaStateUPDATING},
			{"DELETING", gcpgenserver.QuotaRulesV1betaStateDELETING},
			{"ERROR", gcpgenserver.QuotaRulesV1betaStateERROR},
		}

		for _, tc := range testCases {
			tt.Run("State_"+tc.state, func(t *testing.T) {
				cvpRule := &cvpmodels.QuotaRulesV1beta{
					QuotaID:   "cvp-quota-id",
					QuotaType: &quotaType,
					State:     tc.state,
				}

				result := convertCVPQuotaRuleToV1beta(cvpRule)

				assert.Equal(t, tc.expected, result.State.Value)
			})
		}
	})
}

func Test_getMultipleQuotaRulesFromCVP(t *testing.T) {
	ctx := context.Background()
	params := gcpgenserver.V1betaGetMultipleQuotaRulesParams{
		LocationId:     "location-id",
		ProjectNumber:  "project-number",
		VolumeId:       "volume-id",
		XCorrelationID: gcpgenserver.NewOptString("corr-id"),
	}
	req := &gcpgenserver.QuotaRuleIdListV1beta{
		QuotaRuleUuids: []string{"quota-rule-uuid-1"},
	}

	t.Run("WhenCVPReturnsSuccessWithQuotaRules", func(tt *testing.T) {
		mockClient := quota_rules.NewMockClientService(tt)
		quotaType := "INDIVIDUAL_USER_QUOTA"
		quotaRule := &cvpmodels.QuotaRulesV1beta{
			QuotaID:        "cvp-quota-id",
			ResourceID:     nillable.ToPointer("cvp-res-id"),
			QuotaType:      &quotaType,
			DiskLimitInMib: nillable.ToPointer(int64(1024)),
			QuotaTarget:    nillable.ToPointer("user:alice"),
			State:          "READY",
			StateDetails:   "Ready for use",
			Description:    nillable.ToPointer("desc"),
			CreatedAt:      strfmt.DateTime(time.Now()),
			UpdatedAt:      strfmt.DateTime(time.Now()),
		}
		resp := &quota_rules.V1betaGetMultipleQuotaRulesOK{
			Payload: &quota_rules.V1betaGetMultipleQuotaRulesOKBody{
				QuotaRules: []*cvpmodels.QuotaRulesV1beta{quotaRule},
			},
		}
		mockClient.EXPECT().V1betaGetMultipleQuotaRules(mock.Anything).Return(resp, nil)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockClient}
		// Use the createCVPClient from volume_endpoint.go (same package)
		originalCreateCVPClient := createCVPClient
		defer func() {
			createCVPClient = originalCreateCVPClient
		}()
		createCVPClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		res, err := _getMultipleQuotaRulesFromCVP(ctx, req, params, nil)

		assert.NoError(tt, err)
		ok, okType := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesOK)
		assert.True(tt, okType)
		assert.Len(tt, ok.QuotaRules, 1)
		assert.Equal(tt, "cvp-quota-id", ok.QuotaRules[0].QuotaId.Value)
		assert.Equal(tt, "cvp-res-id", ok.QuotaRules[0].ResourceId)
	})

	t.Run("WhenCVPReturnsNotFoundError", func(tt *testing.T) {
		mockClient := quota_rules.NewMockClientService(tt)
		mockErr := &quota_rules.V1betaGetMultipleQuotaRulesNotFound{
			Payload: &cvpmodels.Error{Code: 404, Message: "not found"},
		}
		mockClient.EXPECT().V1betaGetMultipleQuotaRules(mock.Anything).Return(nil, mockErr)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockClient}
		// Use the createCVPClient from volume_endpoint.go (same package)
		originalCreateCVPClient := createCVPClient
		defer func() {
			createCVPClient = originalCreateCVPClient
		}()
		createCVPClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		res, err := _getMultipleQuotaRulesFromCVP(ctx, req, params, nil)

		assert.NoError(tt, err)
		notFound, ok := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesNotFound)
		assert.True(tt, ok)
		assert.Equal(tt, float64(404), notFound.Code)
		assert.Equal(tt, "not found", notFound.Message)
	})

	t.Run("WhenCVPReturnsBadRequestError", func(tt *testing.T) {
		mockClient := quota_rules.NewMockClientService(tt)
		mockErr := &quota_rules.V1betaGetMultipleQuotaRulesBadRequest{
			Payload: &cvpmodels.Error{Code: 400, Message: "bad request"},
		}
		mockClient.EXPECT().V1betaGetMultipleQuotaRules(mock.Anything).Return(nil, mockErr)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockClient}
		// Use the createCVPClient from volume_endpoint.go (same package)
		originalCreateCVPClient := createCVPClient
		defer func() {
			createCVPClient = originalCreateCVPClient
		}()
		createCVPClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		res, err := _getMultipleQuotaRulesFromCVP(ctx, req, params, nil)

		assert.NoError(tt, err)
		badReq, ok := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), badReq.Code)
		assert.Equal(tt, "bad request", badReq.Message)
	})

	t.Run("WhenCVPReturnsUnauthorizedError", func(tt *testing.T) {
		mockClient := quota_rules.NewMockClientService(tt)
		mockErr := &quota_rules.V1betaGetMultipleQuotaRulesUnauthorized{
			Payload: &cvpmodels.Error{Code: 401, Message: "unauthorized"},
		}
		mockClient.EXPECT().V1betaGetMultipleQuotaRules(mock.Anything).Return(nil, mockErr)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockClient}
		// Use the createCVPClient from volume_endpoint.go (same package)
		originalCreateCVPClient := createCVPClient
		defer func() {
			createCVPClient = originalCreateCVPClient
		}()
		createCVPClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		res, err := _getMultipleQuotaRulesFromCVP(ctx, req, params, nil)

		assert.NoError(tt, err)
		unauth, ok := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesUnauthorized)
		assert.True(tt, ok)
		assert.Equal(tt, float64(401), unauth.Code)
		assert.Equal(tt, "unauthorized", unauth.Message)
	})

	t.Run("WhenCVPReturnsForbiddenError", func(tt *testing.T) {
		mockClient := quota_rules.NewMockClientService(tt)
		mockErr := &quota_rules.V1betaGetMultipleQuotaRulesForbidden{
			Payload: &cvpmodels.Error{Code: 403, Message: "forbidden"},
		}
		mockClient.EXPECT().V1betaGetMultipleQuotaRules(mock.Anything).Return(nil, mockErr)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockClient}
		// Use the createCVPClient from volume_endpoint.go (same package)
		originalCreateCVPClient := createCVPClient
		defer func() {
			createCVPClient = originalCreateCVPClient
		}()
		createCVPClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		res, err := _getMultipleQuotaRulesFromCVP(ctx, req, params, nil)

		assert.NoError(tt, err)
		forbidden, ok := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesForbidden)
		assert.True(tt, ok)
		assert.Equal(tt, float64(403), forbidden.Code)
		assert.Equal(tt, "forbidden", forbidden.Message)
	})

	t.Run("WhenCVPReturnsTooManyRequestsError", func(tt *testing.T) {
		mockClient := quota_rules.NewMockClientService(tt)
		mockErr := &quota_rules.V1betaGetMultipleQuotaRulesTooManyRequests{
			Payload: &cvpmodels.Error{Code: 429, Message: "too many requests"},
		}
		mockClient.EXPECT().V1betaGetMultipleQuotaRules(mock.Anything).Return(nil, mockErr)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockClient}
		// Use the createCVPClient from volume_endpoint.go (same package)
		originalCreateCVPClient := createCVPClient
		defer func() {
			createCVPClient = originalCreateCVPClient
		}()
		createCVPClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		res, err := _getMultipleQuotaRulesFromCVP(ctx, req, params, nil)

		assert.NoError(tt, err)
		tooMany, ok := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesTooManyRequests)
		assert.True(tt, ok)
		assert.Equal(tt, float64(429), tooMany.Code)
		assert.Equal(tt, "too many requests", tooMany.Message)
	})

	t.Run("WhenCVPReturnsDefaultError", func(tt *testing.T) {
		mockClient := quota_rules.NewMockClientService(tt)
		mockErr := &quota_rules.V1betaGetMultipleQuotaRulesDefault{}
		mockClient.EXPECT().V1betaGetMultipleQuotaRules(mock.Anything).Return(nil, mockErr)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockClient}
		// Use the createCVPClient from volume_endpoint.go (same package)
		originalCreateCVPClient := createCVPClient
		defer func() {
			createCVPClient = originalCreateCVPClient
		}()
		createCVPClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		res, err := _getMultipleQuotaRulesFromCVP(ctx, req, params, nil)

		assert.NoError(tt, err)
		internal, ok := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internal.Code)
	})

	t.Run("WhenCVPReturnsEmptyQuotaRulesList", func(tt *testing.T) {
		mockClient := quota_rules.NewMockClientService(tt)
		resp := &quota_rules.V1betaGetMultipleQuotaRulesOK{
			Payload: &quota_rules.V1betaGetMultipleQuotaRulesOKBody{
				QuotaRules: []*cvpmodels.QuotaRulesV1beta{},
			},
		}
		mockClient.EXPECT().V1betaGetMultipleQuotaRules(mock.Anything).Return(resp, nil)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockClient}
		// Use the createCVPClient from volume_endpoint.go (same package)
		originalCreateCVPClient := createCVPClient
		defer func() {
			createCVPClient = originalCreateCVPClient
		}()
		createCVPClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		res, err := _getMultipleQuotaRulesFromCVP(ctx, req, params, nil)

		assert.NoError(tt, err)
		ok, okType := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesOK)
		assert.True(tt, okType)
		assert.Len(tt, ok.QuotaRules, 0)
	})

	t.Run("WhenCVPReturnsNilResponse", func(tt *testing.T) {
		mockClient := quota_rules.NewMockClientService(tt)
		mockClient.EXPECT().V1betaGetMultipleQuotaRules(mock.Anything).Return(nil, nil)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockClient}
		// Use the createCVPClient from volume_endpoint.go (same package)
		originalCreateCVPClient := createCVPClient
		defer func() {
			createCVPClient = originalCreateCVPClient
		}()
		createCVPClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		res, err := _getMultipleQuotaRulesFromCVP(ctx, req, params, nil)

		assert.NoError(tt, err)
		ok, okType := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesOK)
		assert.True(tt, okType)
		assert.Len(tt, ok.QuotaRules, 0)
	})

	t.Run("WhenAppendingVCPQuotaRules", func(tt *testing.T) {
		mockClient := quota_rules.NewMockClientService(tt)
		quotaType := "INDIVIDUAL_USER_QUOTA"
		quotaRule := &cvpmodels.QuotaRulesV1beta{
			QuotaID:    "cvp-quota-id",
			ResourceID: nillable.ToPointer("cvp-res-id"),
			QuotaType:  &quotaType,
		}
		resp := &quota_rules.V1betaGetMultipleQuotaRulesOK{
			Payload: &quota_rules.V1betaGetMultipleQuotaRulesOKBody{
				QuotaRules: []*cvpmodels.QuotaRulesV1beta{quotaRule},
			},
		}
		mockClient.EXPECT().V1betaGetMultipleQuotaRules(mock.Anything).Return(resp, nil)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockClient}
		// Use the createCVPClient from volume_endpoint.go (same package)
		originalCreateCVPClient := createCVPClient
		defer func() {
			createCVPClient = originalCreateCVPClient
		}()
		createCVPClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		vcpQuotaRules := []gcpgenserver.QuotaRulesV1beta{
			{
				QuotaId:    gcpgenserver.NewOptString("vcp-quota-id"),
				ResourceId: "vcp-res-id",
			},
		}

		res, err := _getMultipleQuotaRulesFromCVP(ctx, req, params, vcpQuotaRules)

		assert.NoError(tt, err)
		ok, okType := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesOK)
		assert.True(tt, okType)
		assert.Len(tt, ok.QuotaRules, 2)
		assert.Equal(tt, "cvp-quota-id", ok.QuotaRules[0].QuotaId.Value)
		assert.Equal(tt, "vcp-quota-id", ok.QuotaRules[1].QuotaId.Value)
	})

	t.Run("WhenXCorrelationIDIsNotSet", func(tt *testing.T) {
		mockClient := quota_rules.NewMockClientService(tt)
		paramsNoCorr := gcpgenserver.V1betaGetMultipleQuotaRulesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "volume-id",
			// XCorrelationID not set
		}
		resp := &quota_rules.V1betaGetMultipleQuotaRulesOK{
			Payload: &quota_rules.V1betaGetMultipleQuotaRulesOKBody{
				QuotaRules: []*cvpmodels.QuotaRulesV1beta{},
			},
		}
		mockClient.EXPECT().V1betaGetMultipleQuotaRules(mock.Anything).Return(resp, nil)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockClient}
		// Use the createCVPClient from volume_endpoint.go (same package)
		originalCreateCVPClient := createCVPClient
		defer func() {
			createCVPClient = originalCreateCVPClient
		}()
		createCVPClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		res, err := _getMultipleQuotaRulesFromCVP(ctx, req, paramsNoCorr, nil)

		assert.NoError(tt, err)
		ok, okType := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesOK)
		assert.True(tt, okType)
		assert.Len(tt, ok.QuotaRules, 0)
	})

	t.Run("WhenCVPReturnsMultipleQuotaRules", func(tt *testing.T) {
		mockClient := quota_rules.NewMockClientService(tt)
		quotaType1 := "INDIVIDUAL_USER_QUOTA"
		quotaType2 := "INDIVIDUAL_GROUP_QUOTA"
		quotaRule1 := &cvpmodels.QuotaRulesV1beta{
			QuotaID:    "cvp-quota-id-1",
			ResourceID: nillable.ToPointer("cvp-res-id-1"),
			QuotaType:  &quotaType1,
		}
		quotaRule2 := &cvpmodels.QuotaRulesV1beta{
			QuotaID:    "cvp-quota-id-2",
			ResourceID: nillable.ToPointer("cvp-res-id-2"),
			QuotaType:  &quotaType2,
		}
		resp := &quota_rules.V1betaGetMultipleQuotaRulesOK{
			Payload: &quota_rules.V1betaGetMultipleQuotaRulesOKBody{
				QuotaRules: []*cvpmodels.QuotaRulesV1beta{quotaRule1, quotaRule2},
			},
		}
		mockClient.EXPECT().V1betaGetMultipleQuotaRules(mock.Anything).Return(resp, nil)
		cvpClient := &cvpapi.Cvp{QuotaRules: mockClient}
		// Use the createCVPClient from volume_endpoint.go (same package)
		originalCreateCVPClient := createCVPClient
		defer func() {
			createCVPClient = originalCreateCVPClient
		}()
		createCVPClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		res, err := _getMultipleQuotaRulesFromCVP(ctx, req, params, nil)

		assert.NoError(tt, err)
		ok, okType := res.(*gcpgenserver.V1betaGetMultipleQuotaRulesOK)
		assert.True(tt, okType)
		assert.Len(tt, ok.QuotaRules, 2)
		assert.Equal(tt, "cvp-quota-id-1", ok.QuotaRules[0].QuotaId.Value)
		assert.Equal(tt, "cvp-quota-id-2", ok.QuotaRules[1].QuotaId.Value)
	})
}

// TestConvertDatastoreQuotaRuleToModel tests the convertDatastoreQuotaRuleToModel function
func TestConvertDatastoreQuotaRuleToModel(t *testing.T) {
	t.Run("WhenQuotaRuleIsNil", func(tt *testing.T) {
		result := convertDatastoreQuotaRuleToModel(nil)
		assert.Nil(tt, result)
	})

	t.Run("WhenQuotaRuleHasAllFields", func(tt *testing.T) {
		deletedAtTime := time.Now()
		deletedAt := gorm.DeletedAt{
			Time:  deletedAtTime,
			Valid: true,
		}
		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				ID:        1,
				UUID:      "quota-uuid-1",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
				DeletedAt: &deletedAt,
			},
			Name:           "quota-rule-1",
			Description:    "Test description",
			State:          datamodel.LifeCycleStateREADY,
			StateDetails:   "Ready state",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			QuotaTarget:    "user:alice",
			DiskLimitInKib: 1024 * 100, // 100 MiB in KiB
		}

		result := convertDatastoreQuotaRuleToModel(quotaRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, "quota-uuid-1", result.UUID)
		assert.Equal(tt, "quota-rule-1", result.Name)
		assert.Equal(tt, "Test description", result.Description)
		assert.Equal(tt, datamodel.LifeCycleStateREADY, result.LifeCycleState)
		assert.Equal(tt, "Ready state", result.LifeCycleStateDetails)
		assert.Equal(tt, "INDIVIDUAL_USER_QUOTA", result.QuotaType)
		assert.Equal(tt, "user:alice", result.QuotaTarget)
		assert.Equal(tt, int64(100), result.DiskLimitInMib)
		assert.NotNil(tt, result.DeletedAt)
		assert.Equal(tt, deletedAtTime, *result.DeletedAt)
	})

	t.Run("WhenQuotaRuleHasNoDeletedAt", func(tt *testing.T) {
		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				ID:        1,
				UUID:      "quota-uuid-2",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
				DeletedAt: nil,
			},
			Name:           "quota-rule-2",
			State:          datamodel.LifeCycleStateCreating,
			QuotaType:      "DEFAULT_USER_QUOTA",
			DiskLimitInKib: 200 * 1024, // 200 MiB in KiB
		}

		result := convertDatastoreQuotaRuleToModel(quotaRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, "quota-uuid-2", result.UUID)
		assert.Equal(tt, "quota-rule-2", result.Name)
		assert.Equal(tt, int64(200), result.DiskLimitInMib)
		assert.Nil(tt, result.DeletedAt)
	})

	t.Run("WhenQuotaRuleDeletedAtIsNotValid", func(tt *testing.T) {
		deletedAt := gorm.DeletedAt{
			Time:  time.Now(),
			Valid: false, // Not valid
		}
		quotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				ID:        1,
				UUID:      "quota-uuid-3",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
				DeletedAt: &deletedAt,
			},
			Name:           "quota-rule-3",
			State:          datamodel.LifeCycleStateAvailable,
			QuotaType:      "DEFAULT_GROUP_QUOTA",
			DiskLimitInKib: 300 * 1024, // 300 MiB in KiB
		}

		result := convertDatastoreQuotaRuleToModel(quotaRule)

		assert.NotNil(tt, result)
		assert.Equal(tt, "quota-uuid-3", result.UUID)
		assert.Equal(tt, int64(300), result.DiskLimitInMib)
		assert.Nil(tt, result.DeletedAt) // Should be nil when Valid is false
	})
}

// TestV1betaUpdateDestinationQuotaRulesVCP tests the V1betaUpdateDestinationQuotaRulesVCP handler
func TestV1betaUpdateDestinationQuotaRulesVCP(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateDestinationQuotaRulesVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		srcQuotaRule := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-1",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 100,
			QuotaTarget:    gcpgenserver.NewOptString("user:alice"),
			QuotaId:        gcpgenserver.NewOptString("quota-uuid-1"),
		}

		dstQuotaRule := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-2",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaTypeDEFAULTUSERQUOTA,
			DiskLimitInMib: 200,
			QuotaId:        gcpgenserver.NewOptString("quota-uuid-2"),
		}

		req := &gcpgenserver.UpdateDstWithSrcQuotaRulesV1beta{
			SrcQuotaRules: []gcpgenserver.QuotaRulesV1beta{srcQuotaRule},
			DstQuotaRules: []gcpgenserver.QuotaRulesV1beta{dstQuotaRule},
		}

		createdQuotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{
					ID:        1,
					UUID:      "quota-uuid-1",
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				},
				Name:           "quota-rule-1",
				Description:    "Test description",
				State:          datamodel.LifeCycleStateREADY,
				StateDetails:   "Ready state",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "user:alice",
				DiskLimitInKib: 100 * 1024,
			},
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		// Convert to commonparams types for the mock expectation
		commonReq := convertGcpUpdateDstWithSrcQuotaRulesToCommon(req)
		commonParams := convertGcpV1betaUpdateDestinationQuotaRulesVCPParamsToCommon(params)

		mockOrch.EXPECT().ReplaceDstQuotaRulesWithSrc(mock.Anything, commonReq, commonParams).
			Return(createdQuotaRules, nil)

		ctx := context.Background()
		res, err := handler.V1betaUpdateDestinationQuotaRulesVCP(ctx, req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		ok, okType := res.(*gcpgenserver.UpdateDestinationQuotaRulesResponseV1beta)
		assert.True(tt, okType)
		assert.True(tt, ok.State.IsSet())
		assert.Equal(tt, "SUCCESS", ok.State.Value)
		assert.Len(tt, ok.QuotaRules, 1)
		assert.Equal(tt, "quota-uuid-1", ok.QuotaRules[0].QuotaId.Value)
	})

	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateDestinationQuotaRulesVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "invalid-location",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.UpdateDstWithSrcQuotaRulesV1beta{
			SrcQuotaRules: []gcpgenserver.QuotaRulesV1beta{},
			DstQuotaRules: []gcpgenserver.QuotaRulesV1beta{},
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{
				Code:    400,
				Message: "Invalid location",
			}
		}

		ctx := context.Background()
		res, err := handler.V1betaUpdateDestinationQuotaRulesVCP(ctx, req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		badRequest, okType := res.(*gcpgenserver.V1betaUpdateDestinationQuotaRulesVCPBadRequest)
		assert.True(tt, okType)
		assert.Equal(tt, float64(400), badRequest.Code)
		assert.Equal(tt, "Invalid location", badRequest.Message)
	})

	t.Run("WhenOrchestratorReturnsNotFoundError", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateDestinationQuotaRulesVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.UpdateDstWithSrcQuotaRulesV1beta{
			SrcQuotaRules: []gcpgenserver.QuotaRulesV1beta{},
			DstQuotaRules: []gcpgenserver.QuotaRulesV1beta{},
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		// Convert to commonparams types for the mock expectation
		commonReq := convertGcpUpdateDstWithSrcQuotaRulesToCommon(req)
		commonParams := convertGcpV1betaUpdateDestinationQuotaRulesVCPParamsToCommon(params)

		mockOrch.EXPECT().ReplaceDstQuotaRulesWithSrc(mock.Anything, commonReq, commonParams).
			Return(nil, errors.NewNotFoundErr("Volume", nillable.ToPointer("vol-1")))

		ctx := context.Background()
		res, err := handler.V1betaUpdateDestinationQuotaRulesVCP(ctx, req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		notFound, okType := res.(*gcpgenserver.V1betaUpdateDestinationQuotaRulesVCPNotFound)
		assert.True(tt, okType)
		assert.Equal(tt, float64(404), notFound.Code)
		assert.Contains(tt, notFound.Message, "Volume")
	})

	t.Run("WhenOrchestratorReturnsConflictError", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateDestinationQuotaRulesVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.UpdateDstWithSrcQuotaRulesV1beta{
			SrcQuotaRules: []gcpgenserver.QuotaRulesV1beta{},
			DstQuotaRules: []gcpgenserver.QuotaRulesV1beta{},
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		// Convert to commonparams types for the mock expectation
		commonReq := convertGcpUpdateDstWithSrcQuotaRulesToCommon(req)
		commonParams := convertGcpV1betaUpdateDestinationQuotaRulesVCPParamsToCommon(params)

		mockOrch.EXPECT().ReplaceDstQuotaRulesWithSrc(mock.Anything, commonReq, commonParams).
			Return(nil, errors.NewConflictErr("Quota rule conflict"))

		ctx := context.Background()
		res, err := handler.V1betaUpdateDestinationQuotaRulesVCP(ctx, req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		unprocessable, okType := res.(*gcpgenserver.V1betaUpdateDestinationQuotaRulesVCPUnprocessableEntity)
		assert.True(tt, okType)
		assert.Equal(tt, float64(422), unprocessable.Code)
		assert.Contains(tt, unprocessable.Message, "Quota rule conflict")
	})

	t.Run("WhenOrchestratorReturnsInternalError", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateDestinationQuotaRulesVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		req := &gcpgenserver.UpdateDstWithSrcQuotaRulesV1beta{
			SrcQuotaRules: []gcpgenserver.QuotaRulesV1beta{},
			DstQuotaRules: []gcpgenserver.QuotaRulesV1beta{},
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		// Convert to commonparams types for the mock expectation
		commonReq := convertGcpUpdateDstWithSrcQuotaRulesToCommon(req)
		commonParams := convertGcpV1betaUpdateDestinationQuotaRulesVCPParamsToCommon(params)

		mockOrch.EXPECT().ReplaceDstQuotaRulesWithSrc(mock.Anything, commonReq, commonParams).
			Return(nil, errors.New("database connection failed"))

		ctx := context.Background()
		res, err := handler.V1betaUpdateDestinationQuotaRulesVCP(ctx, req, params)

		assert.Error(tt, err)
		assert.NotNil(tt, res)
		internalError, okType := res.(*gcpgenserver.V1betaUpdateDestinationQuotaRulesVCPInternalServerError)
		assert.True(tt, okType)
		assert.Equal(tt, float64(500), internalError.Code)
		assert.Equal(tt, "Internal server error", internalError.Message)
	})

	t.Run("WhenMultipleQuotaRulesAreCreated", func(tt *testing.T) {
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrch}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaUpdateDestinationQuotaRulesVCPParams{
			ProjectNumber: "project-1",
			LocationId:    "us-central1",
			VolumeId:      "vol-1",
		}

		srcQuotaRule1 := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-1",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 100,
			QuotaId:        gcpgenserver.NewOptString("quota-uuid-1"),
		}

		srcQuotaRule2 := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-2",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaTypeDEFAULTUSERQUOTA,
			DiskLimitInMib: 200,
			QuotaId:        gcpgenserver.NewOptString("quota-uuid-2"),
		}

		req := &gcpgenserver.UpdateDstWithSrcQuotaRulesV1beta{
			SrcQuotaRules: []gcpgenserver.QuotaRulesV1beta{srcQuotaRule1, srcQuotaRule2},
			DstQuotaRules: []gcpgenserver.QuotaRulesV1beta{},
		}

		createdQuotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{
					ID:        1,
					UUID:      "quota-uuid-1",
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				},
				Name:           "quota-rule-1",
				State:          datamodel.LifeCycleStateREADY,
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				DiskLimitInKib: 100 * 1024,
			},
			{
				BaseModel: datamodel.BaseModel{
					ID:        2,
					UUID:      "quota-uuid-2",
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				},
				Name:           "quota-rule-2",
				State:          datamodel.LifeCycleStateREADY,
				QuotaType:      "DEFAULT_USER_QUOTA",
				DiskLimitInKib: 200 * 1024,
			},
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-central1", "us-central1", nil
		}

		// Convert to commonparams types for the mock expectation
		commonReq := convertGcpUpdateDstWithSrcQuotaRulesToCommon(req)
		commonParams := convertGcpV1betaUpdateDestinationQuotaRulesVCPParamsToCommon(params)

		mockOrch.EXPECT().ReplaceDstQuotaRulesWithSrc(mock.Anything, commonReq, commonParams).
			Return(createdQuotaRules, nil)

		ctx := context.Background()
		res, err := handler.V1betaUpdateDestinationQuotaRulesVCP(ctx, req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		ok, okType := res.(*gcpgenserver.UpdateDestinationQuotaRulesResponseV1beta)
		assert.True(tt, okType)
		assert.Equal(tt, "SUCCESS", ok.State.Value)
		assert.Len(tt, ok.QuotaRules, 2)
		assert.Equal(tt, "quota-uuid-1", ok.QuotaRules[0].QuotaId.Value)
		assert.Equal(tt, "quota-uuid-2", ok.QuotaRules[1].QuotaId.Value)
	})
}

func TestConvertGcpQuotaRulesV1betaToCommon(t *testing.T) {
	t.Run("WhenAllOptionalFieldsAreSet", func(tt *testing.T) {
		state := gcpgenserver.QuotaRulesV1betaState("Active")
		stateDetails := "Quota rule is active"
		description := "Test quota rule"
		createdAt := time.Now()
		updatedAt := time.Now()

		rule := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-1",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA,
			DiskLimitInMib: 100,
			QuotaId:        gcpgenserver.NewOptString("quota-id-1"),
			QuotaTarget:    gcpgenserver.NewOptString("user:alice"),
			State:          gcpgenserver.NewOptQuotaRulesV1betaState(state),
			StateDetails:   gcpgenserver.NewOptString(stateDetails),
			Description:    gcpgenserver.NewOptString(description),
			CreatedAt:      gcpgenserver.NewOptDateTime(createdAt),
			UpdatedAt:      gcpgenserver.NewOptDateTime(updatedAt),
		}

		result := convertGcpQuotaRulesV1betaToCommon(rule)

		assert.Equal(tt, "quota-rule-1", result.ResourceId)
		assert.Equal(tt, "INDIVIDUAL_USER_QUOTA", result.QuotaType)
		assert.Equal(tt, int64(100), result.DiskLimitInMib)
		assert.NotNil(tt, result.QuotaId)
		assert.Equal(tt, "quota-id-1", *result.QuotaId)
		assert.NotNil(tt, result.QuotaTarget)
		assert.Equal(tt, "user:alice", *result.QuotaTarget)
		assert.NotNil(tt, result.State)
		assert.Equal(tt, "Active", *result.State)
		assert.NotNil(tt, result.StateDetails)
		assert.Equal(tt, "Quota rule is active", *result.StateDetails)
		assert.NotNil(tt, result.Description)
		assert.Equal(tt, "Test quota rule", *result.Description)
		assert.NotNil(tt, result.CreatedAt)
		assert.Equal(tt, createdAt, *result.CreatedAt)
		assert.NotNil(tt, result.UpdatedAt)
		assert.Equal(tt, updatedAt, *result.UpdatedAt)
	})

	t.Run("WhenOptionalFieldsAreNotSet", func(tt *testing.T) {
		rule := gcpgenserver.QuotaRulesV1beta{
			ResourceId:     "quota-rule-2",
			QuotaType:      gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALGROUPQUOTA,
			DiskLimitInMib: 200,
		}

		result := convertGcpQuotaRulesV1betaToCommon(rule)

		assert.Equal(tt, "quota-rule-2", result.ResourceId)
		assert.Equal(tt, "INDIVIDUAL_GROUP_QUOTA", result.QuotaType)
		assert.Equal(tt, int64(200), result.DiskLimitInMib)
		assert.Nil(tt, result.State)
		assert.Nil(tt, result.StateDetails)
		assert.Nil(tt, result.Description)
		assert.Nil(tt, result.CreatedAt)
		assert.Nil(tt, result.UpdatedAt)
	})
}

func TestConvertGcpV1betaUpdateDestinationQuotaRulesVCPParamsToCommon(t *testing.T) {
	t.Run("WhenXCorrelationIDIsSet", func(tt *testing.T) {
		params := gcpgenserver.V1betaUpdateDestinationQuotaRulesVCPParams{
			ProjectNumber:  "test-project",
			LocationId:     "us-central1",
			VolumeId:       "volume-123",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		result := convertGcpV1betaUpdateDestinationQuotaRulesVCPParamsToCommon(params)

		assert.Equal(tt, "test-project", result.ProjectNumber)
		assert.Equal(tt, "us-central1", result.LocationId)
		assert.Equal(tt, "volume-123", result.VolumeId)
		assert.NotNil(tt, result.XCorrelationID)
		assert.Equal(tt, "test-correlation-id", *result.XCorrelationID)
	})

	t.Run("WhenXCorrelationIDIsNotSet", func(tt *testing.T) {
		params := gcpgenserver.V1betaUpdateDestinationQuotaRulesVCPParams{
			ProjectNumber:  "test-project",
			LocationId:     "us-central1",
			VolumeId:       "volume-123",
			XCorrelationID: gcpgenserver.OptString{},
		}

		result := convertGcpV1betaUpdateDestinationQuotaRulesVCPParamsToCommon(params)

		assert.Equal(tt, "test-project", result.ProjectNumber)
		assert.Equal(tt, "us-central1", result.LocationId)
		assert.Equal(tt, "volume-123", result.VolumeId)
		assert.Nil(tt, result.XCorrelationID)
	})
}
