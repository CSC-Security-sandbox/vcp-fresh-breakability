package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-faster/jx"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	orchestratorcommon "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// convertQuotaRuleToV1beta converts models.QuotaRule to gcpgenserver.QuotaRulesV1beta
func convertQuotaRuleToV1beta(quotaRule *models.QuotaRule) *gcpgenserver.QuotaRulesV1beta {
	// Convert quota type
	var quotaType gcpgenserver.QuotaRulesV1betaQuotaType
	switch quotaRule.QuotaType {
	case "INDIVIDUAL_USER_QUOTA":
		quotaType = gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA
	case "INDIVIDUAL_GROUP_QUOTA":
		quotaType = gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALGROUPQUOTA
	case "DEFAULT_USER_QUOTA":
		quotaType = gcpgenserver.QuotaRulesV1betaQuotaTypeDEFAULTUSERQUOTA
	case "DEFAULT_GROUP_QUOTA":
		quotaType = gcpgenserver.QuotaRulesV1betaQuotaTypeDEFAULTGROUPQUOTA
	default:
		quotaType = gcpgenserver.QuotaRulesV1betaQuotaType(quotaRule.QuotaType)
	}

	// Convert lifecycle state
	var state gcpgenserver.OptQuotaRulesV1betaState
	switch quotaRule.LifeCycleState {
	case models.LifeCycleStateCreating:
		state = gcpgenserver.NewOptQuotaRulesV1betaState(gcpgenserver.QuotaRulesV1betaStateCREATING)
	case models.LifeCycleStateAvailable:
		state = gcpgenserver.NewOptQuotaRulesV1betaState(gcpgenserver.QuotaRulesV1betaStateREADY)
	case models.LifeCycleStateUpdating:
		state = gcpgenserver.NewOptQuotaRulesV1betaState(gcpgenserver.QuotaRulesV1betaStateUPDATING)
	case models.LifeCycleStateDeleting:
		state = gcpgenserver.NewOptQuotaRulesV1betaState(gcpgenserver.QuotaRulesV1betaStateDELETING)
	case models.LifeCycleStateError:
		state = gcpgenserver.NewOptQuotaRulesV1betaState(gcpgenserver.QuotaRulesV1betaStateERROR)
	default:
		state = gcpgenserver.NewOptQuotaRulesV1betaState(gcpgenserver.QuotaRulesV1betaStateSTATEUNSPECIFIED)
	}

	return &gcpgenserver.QuotaRulesV1beta{
		QuotaId:        gcpgenserver.NewOptString(quotaRule.UUID),
		ResourceId:     quotaRule.Name,
		QuotaType:      quotaType,
		DiskLimitInMib: quotaRule.DiskLimitInMib,
		QuotaTarget:    gcpgenserver.NewOptString(quotaRule.QuotaTarget),
		State:          state,
		StateDetails:   gcpgenserver.NewOptString(quotaRule.LifeCycleStateDetails),
		Description:    gcpgenserver.NewOptString(quotaRule.Description),
		CreatedAt:      gcpgenserver.NewOptDateTime(quotaRule.CreatedAt),
		UpdatedAt:      gcpgenserver.NewOptDateTime(quotaRule.UpdatedAt),
	}
}

// encodeQuotaRuleV1 encodes a QuotaRulesV1beta struct to JSON.
func encodeQuotaRuleV1(quotaRuleV1beta *gcpgenserver.QuotaRulesV1beta) (jx.Raw, error) {
	data, err := json.Marshal(quotaRuleV1beta)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// V1betaCreateQuotaRule is a handler for creating a quota rule
func (h Handler) V1betaCreateQuotaRule(ctx context.Context, req *gcpgenserver.QuotaRuleCreateV1beta, params gcpgenserver.V1betaCreateQuotaRuleParams) (gcpgenserver.V1betaCreateQuotaRuleRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaCreateQuotaRuleBadRequest{
			Code:    400,
			Message: parsingErr.GetMessage(),
		}, nil
	}

	// Create a parameter struct for orchestrator
	quotaParam := &orchestratorcommon.CreateQuotaRulesParam{
		Name:           req.ResourceId,
		VolumeUUID:     params.VolumeId,
		QuotaType:      string(req.QuotaType),
		DiskLimitInMib: req.DiskLimitInMib,
		QuotaTarget:    req.QuotaTarget.Value,
		ProjectId:      params.ProjectNumber,
		Description:    req.Description.Value,
		LocationId:     params.LocationId,
	}

	// Call an orchestrator to create the quota rule
	quotaRule, operationID, err := h.Orchestrator.CreateQuotaRule(ctx, quotaParam)

	if err != nil {
		// Handle validation and not found errors
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaCreateQuotaRuleBadRequest{
				Code:    http.StatusBadRequest,
				Message: err.Error(),
			}, nil
		} else if errors.IsConflictErr(err) {
			return &gcpgenserver.V1betaCreateQuotaRuleConflict{
				Code:    http.StatusConflict,
				Message: err.Error(),
			}, nil
		}

		logger.Errorf("Failed to create quota rule: %v", err)
		return &gcpgenserver.V1betaCreateQuotaRuleInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		}, err
	}

	quotaRuleRes, err := encodeQuotaRuleV1(convertQuotaRuleToV1beta(quotaRule))
	if err != nil {
		return &gcpgenserver.V1betaCreateQuotaRuleInternalServerError{Code: 500, Message: err.Error()}, nil
	}
	// Build operation ID
	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, operationID)),
		Response: quotaRuleRes,
		Done:     gcpgenserver.NewOptBool(false),
	}, nil
}

// V1betaCreateQuotaRuleVCP is a handler for creating a quota rule via internal VCP API
func (h Handler) V1betaCreateQuotaRuleVCP(ctx context.Context, req *gcpgenserver.QuotaRuleCreateV1beta, params gcpgenserver.V1betaCreateQuotaRuleVCPParams) (gcpgenserver.V1betaCreateQuotaRuleVCPRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaCreateQuotaRuleVCPBadRequest{
			Code:    400,
			Message: parsingErr.GetMessage(),
		}, nil
	}

	// Create a parameter struct for orchestrator
	quotaParam := &orchestratorcommon.CreateQuotaRulesParam{
		Name:           req.ResourceId,
		VolumeUUID:     params.VolumeId,
		QuotaType:      string(req.QuotaType),
		DiskLimitInMib: req.DiskLimitInMib,
		QuotaTarget:    req.QuotaTarget.Value,
		ProjectId:      params.ProjectNumber,
		Description:    req.Description.Value,
		LocationId:     params.LocationId,
	}

	// Call internal orchestrator function (skips replication validation)
	quotaRule, _, err := h.Orchestrator.CreateQuotaRuleInternal(ctx, quotaParam)

	if err != nil {
		// Handle validation and not found errors
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaCreateQuotaRuleVCPBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		} else if errors.IsConflictErr(err) {
			return &gcpgenserver.V1betaCreateQuotaRuleVCPConflict{
				Code:    409,
				Message: err.Error(),
			}, nil
		}

		logger.Errorf("Failed to create quota rule via internal API: %v", err)
		return &gcpgenserver.V1betaCreateQuotaRuleVCPInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, err
	}

	// Get job for the quota rule
	job, jobErr := h.Orchestrator.GetJobByResourceUUID(ctx, quotaRule.UUID, string(models.JobTypeCreateQuotaRule))
	if jobErr != nil {
		logger.Warnf("Failed to find job for quota rule: %v", jobErr)
		// Continue without job - jobs array will be empty
		job = nil
	}

	// Convert quota type
	var quotaType gcpgenserver.QuotaRulesVCPV1betaQuotaType
	switch quotaRule.QuotaType {
	case "INDIVIDUAL_USER_QUOTA":
		quotaType = gcpgenserver.QuotaRulesVCPV1betaQuotaTypeINDIVIDUALUSERQUOTA
	case "INDIVIDUAL_GROUP_QUOTA":
		quotaType = gcpgenserver.QuotaRulesVCPV1betaQuotaTypeINDIVIDUALGROUPQUOTA
	case "DEFAULT_USER_QUOTA":
		quotaType = gcpgenserver.QuotaRulesVCPV1betaQuotaTypeDEFAULTUSERQUOTA
	case "DEFAULT_GROUP_QUOTA":
		quotaType = gcpgenserver.QuotaRulesVCPV1betaQuotaTypeDEFAULTGROUPQUOTA
	default:
		quotaType = gcpgenserver.QuotaRulesVCPV1betaQuotaType(quotaRule.QuotaType)
	}

	// Convert lifecycle state
	var state gcpgenserver.OptQuotaRulesVCPV1betaState
	switch quotaRule.LifeCycleState {
	case models.LifeCycleStateCreating:
		state = gcpgenserver.NewOptQuotaRulesVCPV1betaState(gcpgenserver.QuotaRulesVCPV1betaStateCREATING)
	case models.LifeCycleStateAvailable:
		state = gcpgenserver.NewOptQuotaRulesVCPV1betaState(gcpgenserver.QuotaRulesVCPV1betaStateREADY)
	case models.LifeCycleStateUpdating:
		state = gcpgenserver.NewOptQuotaRulesVCPV1betaState(gcpgenserver.QuotaRulesVCPV1betaStateUPDATING)
	case models.LifeCycleStateDeleting:
		state = gcpgenserver.NewOptQuotaRulesVCPV1betaState(gcpgenserver.QuotaRulesVCPV1betaStateDELETING)
	case models.LifeCycleStateError:
		state = gcpgenserver.NewOptQuotaRulesVCPV1betaState(gcpgenserver.QuotaRulesVCPV1betaStateERROR)
	default:
		state = gcpgenserver.NewOptQuotaRulesVCPV1betaState(gcpgenserver.QuotaRulesVCPV1betaStateSTATEUNSPECIFIED)
	}

	// Build jobs array
	jobsList := make([]gcpgenserver.JobV1beta, 0)
	if job != nil {
		// Convert job state to JobV1betaState
		var jobState gcpgenserver.OptJobV1betaState
		switch job.State {
		case models.JobsStateNEW, models.JobsStatePROCESSING:
			jobState = gcpgenserver.NewOptJobV1betaState(gcpgenserver.JobV1betaStateOngoing)
		case models.JobsStateDONE:
			jobState = gcpgenserver.NewOptJobV1betaState(gcpgenserver.JobV1betaStateDone)
		case models.JobsStateERROR:
			jobState = gcpgenserver.NewOptJobV1betaState(gcpgenserver.JobV1betaStateError)
		default:
			jobState = gcpgenserver.NewOptJobV1betaState(gcpgenserver.JobV1betaStateOngoing)
		}

		jobsList = append(jobsList, gcpgenserver.JobV1beta{
			JobId:    gcpgenserver.NewOptString(job.UUID),
			Created:  gcpgenserver.NewOptDateTime(job.CreatedAt),
			WorkerId: gcpgenserver.NewOptString(job.WorkflowID),
			ObjectId: gcpgenserver.NewOptString(quotaRule.UUID),
			State:    jobState,
		})
	}

	// Build response
	response := &gcpgenserver.QuotaRulesVCPV1beta{
		QuotaId:        gcpgenserver.NewOptString(quotaRule.UUID),
		ResourceId:     quotaRule.Name,
		QuotaType:      quotaType,
		DiskLimitInMib: quotaRule.DiskLimitInMib,
		QuotaTarget:    gcpgenserver.NewOptString(quotaRule.QuotaTarget),
		State:          state,
		StateDetails:   gcpgenserver.NewOptString(quotaRule.LifeCycleStateDetails),
		Description:    gcpgenserver.NewOptString(quotaRule.Description),
		CreatedAt:      gcpgenserver.NewOptDateTime(quotaRule.CreatedAt),
		UpdatedAt:      gcpgenserver.NewOptDateTime(quotaRule.UpdatedAt),
		Jobs:           jobsList,
	}

	return response, nil
}
