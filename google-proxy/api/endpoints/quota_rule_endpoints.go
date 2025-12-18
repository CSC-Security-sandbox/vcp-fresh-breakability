package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-faster/jx"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	orchestratorcommon "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// convertQuotaRuleToV1beta converts models.QuotaRule to gcpgenserver.QuotaRulesV1beta
func convertQuotaRuleToV1beta(quotaRule *models.QuotaRule) *gcpgenserver.QuotaRulesV1beta {
	// Convert quota type and lifecycle state
	quotaType := QuotaRuleQuotaTypeV1Beta(quotaRule.QuotaType)
	state := QuotaRuleLifeCycleV1Beta(quotaRule.LifeCycleState)

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

// convertToVCPQuotaRulesV1Beta converts an array of models.QuotaRule to []gcpgenserver.QuotaRulesV1beta
func convertToVCPQuotaRulesV1Beta(quotaRules []*models.QuotaRule) []gcpgenserver.QuotaRulesV1beta {
	quotaRulesV1Beta := make([]gcpgenserver.QuotaRulesV1beta, len(quotaRules))
	for i, quotaRule := range quotaRules {
		quotaRulesV1Beta[i] = *convertQuotaRuleToV1beta(quotaRule)
	}
	return quotaRulesV1Beta
}

// QuotaRuleQuotaTypeV1Beta converts a quota type string to gcpgenserver.QuotaRulesV1betaQuotaType
func QuotaRuleQuotaTypeV1Beta(quotaType string) gcpgenserver.QuotaRulesV1betaQuotaType {
	switch quotaType {
	case "INDIVIDUAL_USER_QUOTA":
		return gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA
	case "INDIVIDUAL_GROUP_QUOTA":
		return gcpgenserver.QuotaRulesV1betaQuotaTypeINDIVIDUALGROUPQUOTA
	case "DEFAULT_USER_QUOTA":
		return gcpgenserver.QuotaRulesV1betaQuotaTypeDEFAULTUSERQUOTA
	case "DEFAULT_GROUP_QUOTA":
		return gcpgenserver.QuotaRulesV1betaQuotaTypeDEFAULTGROUPQUOTA
	default:
		return gcpgenserver.QuotaRulesV1betaQuotaType(quotaType)
	}
}

// QuotaRuleLifeCycleV1Beta converts a lifecycle state to gcpgenserver.OptQuotaRulesV1betaState
func QuotaRuleLifeCycleV1Beta(lifeCycleState string) gcpgenserver.OptQuotaRulesV1betaState {
	switch lifeCycleState {
	case models.LifeCycleStateCreating:
		return gcpgenserver.NewOptQuotaRulesV1betaState(gcpgenserver.QuotaRulesV1betaStateCREATING)
	case models.LifeCycleStateREADY:
		return gcpgenserver.NewOptQuotaRulesV1betaState(gcpgenserver.QuotaRulesV1betaStateREADY)
	case models.LifeCycleStateUpdating:
		return gcpgenserver.NewOptQuotaRulesV1betaState(gcpgenserver.QuotaRulesV1betaStateUPDATING)
	case models.LifeCycleStateDeleting:
		return gcpgenserver.NewOptQuotaRulesV1betaState(gcpgenserver.QuotaRulesV1betaStateDELETING)
	case models.LifeCycleStateError:
		return gcpgenserver.NewOptQuotaRulesV1betaState(gcpgenserver.QuotaRulesV1betaStateERROR)
	default:
		return gcpgenserver.NewOptQuotaRulesV1betaState(gcpgenserver.QuotaRulesV1betaStateSTATEUNSPECIFIED)
	}
}

// QuotaRuleQuotaTypeVCPV1Beta converts a quota type string to gcpgenserver.QuotaRulesVCPV1betaQuotaType
func QuotaRuleQuotaTypeVCPV1Beta(quotaType string) gcpgenserver.QuotaRulesVCPV1betaQuotaType {
	switch quotaType {
	case "INDIVIDUAL_USER_QUOTA":
		return gcpgenserver.QuotaRulesVCPV1betaQuotaTypeINDIVIDUALUSERQUOTA
	case "INDIVIDUAL_GROUP_QUOTA":
		return gcpgenserver.QuotaRulesVCPV1betaQuotaTypeINDIVIDUALGROUPQUOTA
	case "DEFAULT_USER_QUOTA":
		return gcpgenserver.QuotaRulesVCPV1betaQuotaTypeDEFAULTUSERQUOTA
	case "DEFAULT_GROUP_QUOTA":
		return gcpgenserver.QuotaRulesVCPV1betaQuotaTypeDEFAULTGROUPQUOTA
	default:
		return gcpgenserver.QuotaRulesVCPV1betaQuotaType(quotaType)
	}
}

// QuotaRuleLifeCycleVCPV1Beta converts a lifecycle state to gcpgenserver.OptQuotaRulesVCPV1betaState
func QuotaRuleLifeCycleVCPV1Beta(lifeCycleState string) gcpgenserver.OptQuotaRulesVCPV1betaState {
	switch lifeCycleState {
	case models.LifeCycleStateCreating:
		return gcpgenserver.NewOptQuotaRulesVCPV1betaState(gcpgenserver.QuotaRulesVCPV1betaStateCREATING)
	case models.LifeCycleStateREADY:
		return gcpgenserver.NewOptQuotaRulesVCPV1betaState(gcpgenserver.QuotaRulesVCPV1betaStateREADY)
	case models.LifeCycleStateUpdating:
		return gcpgenserver.NewOptQuotaRulesVCPV1betaState(gcpgenserver.QuotaRulesVCPV1betaStateUPDATING)
	case models.LifeCycleStateDeleting:
		return gcpgenserver.NewOptQuotaRulesVCPV1betaState(gcpgenserver.QuotaRulesVCPV1betaStateDELETING)
	case models.LifeCycleStateError:
		return gcpgenserver.NewOptQuotaRulesVCPV1betaState(gcpgenserver.QuotaRulesVCPV1betaStateERROR)
	default:
		return gcpgenserver.NewOptQuotaRulesVCPV1betaState(gcpgenserver.QuotaRulesVCPV1betaStateSTATEUNSPECIFIED)
	}
}

// convertQuotaRuleToVCPResponse converts models.QuotaRule and datamodel.Job to gcpgenserver.QuotaRulesVCPV1beta
func convertQuotaRuleToVCPResponse(quotaRule *models.QuotaRule, job *datamodel.Job) *gcpgenserver.QuotaRulesVCPV1beta {
	// Convert quota type and lifecycle state
	quotaType := QuotaRuleQuotaTypeVCPV1Beta(quotaRule.QuotaType)
	state := QuotaRuleLifeCycleVCPV1Beta(quotaRule.LifeCycleState)

	// Build jobs array
	jobsList := make([]gcpgenserver.JobV1beta, 0)
	if job != nil {
		jobState := JobStateToVCPV1Beta(models.JobState(job.State))
		jobsList = append(jobsList, gcpgenserver.JobV1beta{
			JobId:    gcpgenserver.NewOptString(job.UUID),
			Created:  gcpgenserver.NewOptDateTime(job.CreatedAt),
			WorkerId: gcpgenserver.NewOptString(job.WorkflowID),
			ObjectId: gcpgenserver.NewOptString(quotaRule.UUID),
			State:    jobState,
		})
	}

	// Build response
	return &gcpgenserver.QuotaRulesVCPV1beta{
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
}

// JobStateToVCPV1Beta converts a job state to gcpgenserver.OptJobV1betaState
func JobStateToVCPV1Beta(jobState models.JobState) gcpgenserver.OptJobV1betaState {
	switch jobState {
	case models.JobsStateNEW, models.JobsStatePROCESSING:
		return gcpgenserver.NewOptJobV1betaState(gcpgenserver.JobV1betaStateOngoing)
	case models.JobsStateDONE:
		return gcpgenserver.NewOptJobV1betaState(gcpgenserver.JobV1betaStateDone)
	case models.JobsStateERROR:
		return gcpgenserver.NewOptJobV1betaState(gcpgenserver.JobV1betaStateError)
	default:
		return gcpgenserver.NewOptJobV1betaState(gcpgenserver.JobV1betaStateOngoing)
	}
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
	quotaRule, job, err := h.Orchestrator.CreateQuotaRuleInternal(ctx, quotaParam)

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

	return convertQuotaRuleToVCPResponse(quotaRule, job), nil
}

// V1betaUpdateQuotaRule is a handler for updating a quota rule
func (h Handler) V1betaUpdateQuotaRule(ctx context.Context, req *gcpgenserver.QuotaRulesUpdateV1beta, params gcpgenserver.V1betaUpdateQuotaRuleParams) (gcpgenserver.V1betaUpdateQuotaRuleRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaUpdateQuotaRuleBadRequest{
			Code:    400,
			Message: parsingErr.GetMessage(),
		}, nil
	}

	// Create a parameter struct for orchestrator
	quotaParam := &orchestratorcommon.UpdateQuotaRulesParam{
		QuotaRuleUUID: params.QuotaRuleId,
		ProjectId:     params.ProjectNumber,
		LocationId:    params.LocationId,
	}

	// Set DiskLimitInMib only if provided in request
	if req.DiskLimitInMib.IsSet() {
		quotaParam.DiskLimitInMib = req.DiskLimitInMib.Value
	}

	// Set Description only if provided in request
	if req.Description.IsSet() {
		quotaParam.Description = req.Description.Value
	}

	// Call orchestrator to update the quota rule
	quotaRule, operationID, err := h.Orchestrator.UpdateQuotaRule(ctx, quotaParam)

	if err != nil {
		// Handle validation and not found errors
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaUpdateQuotaRuleBadRequest{
				Code:    http.StatusBadRequest,
				Message: err.Error(),
			}, nil
		} else if errors.IsConflictErr(err) {
			return &gcpgenserver.V1betaUpdateQuotaRuleConflict{
				Code:    http.StatusConflict,
				Message: err.Error(),
			}, nil
		}

		logger.Errorf("Failed to update quota rule: %v", err)
		return &gcpgenserver.V1betaUpdateQuotaRuleInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		}, err
	}

	quotaRuleRes, err := encodeQuotaRuleV1(convertQuotaRuleToV1beta(quotaRule))
	if err != nil {
		return &gcpgenserver.V1betaUpdateQuotaRuleInternalServerError{Code: 500, Message: err.Error()}, nil
	}
	// Build operation ID - use empty string if operationID is empty (synchronous update)
	operationName := ""
	if operationID != "" {
		operationName = fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, operationID)
	}
	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(operationName),
		Response: quotaRuleRes,
		Done:     gcpgenserver.NewOptBool(false),
	}, nil
}

// V1betaDeleteQuotaRule is a handler for deleting a quota rule
func (h Handler) V1betaDeleteQuotaRule(ctx context.Context, params gcpgenserver.V1betaDeleteQuotaRuleParams) (gcpgenserver.V1betaDeleteQuotaRuleRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaDeleteQuotaRuleBadRequest{
			Code:    400,
			Message: parsingErr.GetMessage(),
		}, nil
	}

	// Create a parameter struct for orchestrator
	quotaParam := &orchestratorcommon.DeleteQuotaRulesParam{
		QuotaRuleUUID: params.QuotaRuleId,
		ProjectId:     params.ProjectNumber,
		LocationId:    params.LocationId,
	}

	// Call orchestrator to delete the quota rule
	quotaRule, operationID, err := h.Orchestrator.DeleteQuotaRule(ctx, quotaParam)

	if err != nil {
		// Handle validation and not found errors
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaDeleteQuotaRuleBadRequest{
				Code:    http.StatusBadRequest,
				Message: err.Error(),
			}, nil
		} else if errors.IsConflictErr(err) {
			return &gcpgenserver.V1betaDeleteQuotaRuleConflict{
				Code:    http.StatusConflict,
				Message: err.Error(),
			}, nil
		}

		logger.Errorf("Failed to delete quota rule: %v", err)
		return &gcpgenserver.V1betaDeleteQuotaRuleInternalServerError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		}, err
	}

	quotaRuleRes, err := encodeQuotaRuleV1(convertQuotaRuleToV1beta(quotaRule))
	if err != nil {
		return &gcpgenserver.V1betaDeleteQuotaRuleInternalServerError{Code: 500, Message: err.Error()}, nil
	}
	// Build operation ID - use empty string if operationID is empty (synchronous delete)
	operationName := ""
	if operationID != "" {
		operationName = fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, operationID)
	}
	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(operationName),
		Response: quotaRuleRes,
		Done:     gcpgenserver.NewOptBool(false),
	}, nil
}

// V1betaUpdateQuotaRuleVCP is a handler for updating a quota rule via internal VCP API
func (h Handler) V1betaUpdateQuotaRuleVCP(ctx context.Context, req *gcpgenserver.QuotaRulesUpdateV1beta, params gcpgenserver.V1betaUpdateQuotaRuleVCPParams) (gcpgenserver.V1betaUpdateQuotaRuleVCPRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaUpdateQuotaRuleVCPBadRequest{
			Code:    400,
			Message: parsingErr.GetMessage(),
		}, nil
	}

	// Create a parameter struct for orchestrator
	quotaParam := &orchestratorcommon.UpdateQuotaRulesParam{
		QuotaRuleUUID: params.QuotaRuleId,
		ProjectId:     params.ProjectNumber,
		LocationId:    params.LocationId,
	}

	// Set DiskLimitInMib only if provided in request
	if req.DiskLimitInMib.IsSet() {
		quotaParam.DiskLimitInMib = req.DiskLimitInMib.Value
	}

	// Set Description only if provided in request
	if req.Description.IsSet() {
		quotaParam.Description = req.Description.Value
	}

	// Call internal orchestrator function (skips replication validation)
	quotaRule, job, err := h.Orchestrator.UpdateQuotaRuleInternal(ctx, quotaParam)

	if err != nil {
		// Handle validation and not found errors
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaUpdateQuotaRuleVCPBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		} else if errors.IsConflictErr(err) {
			return &gcpgenserver.V1betaUpdateQuotaRuleVCPConflict{
				Code:    409,
				Message: err.Error(),
			}, nil
		}

		logger.Errorf("Failed to update quota rule via internal API: %v", err)
		return &gcpgenserver.V1betaUpdateQuotaRuleVCPInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, err
	}

	return convertQuotaRuleToVCPResponse(quotaRule, job), nil
}

// V1betaDeleteQuotaRuleVCP is a handler for deleting a quota rule via internal VCP API
func (h Handler) V1betaDeleteQuotaRuleVCP(ctx context.Context, params gcpgenserver.V1betaDeleteQuotaRuleVCPParams) (gcpgenserver.V1betaDeleteQuotaRuleVCPRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaDeleteQuotaRuleVCPBadRequest{
			Code:    400,
			Message: parsingErr.GetMessage(),
		}, nil
	}

	// Create a parameter struct for orchestrator
	quotaParam := &orchestratorcommon.DeleteQuotaRulesParam{
		QuotaRuleUUID: params.QuotaRuleId,
		ProjectId:     params.ProjectNumber,
		LocationId:    params.LocationId,
	}

	// Call internal orchestrator function (skips replication validation)
	quotaRule, job, err := h.Orchestrator.DeleteQuotaRuleInternal(ctx, quotaParam)

	if err != nil {
		// Handle validation and not found errors
		if errors.IsUserInputValidationErr(err) || errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaDeleteQuotaRuleVCPBadRequest{
				Code:    400,
				Message: err.Error(),
			}, nil
		} else if errors.IsConflictErr(err) {
			return &gcpgenserver.V1betaDeleteQuotaRuleVCPConflict{
				Code:    409,
				Message: err.Error(),
			}, nil
		}

		logger.Errorf("Failed to delete quota rule via internal API: %v", err)
		return &gcpgenserver.V1betaDeleteQuotaRuleVCPInternalServerError{
			Code:    500,
			Message: err.Error(),
		}, err
	}

	return convertQuotaRuleToVCPResponse(quotaRule, job), nil
}

// V1betaListAllQuotaRules is a handler for listing all quota rules for a volume
func (h Handler) V1betaListAllQuotaRules(ctx context.Context, params gcpgenserver.V1betaListAllQuotaRulesParams) (gcpgenserver.V1betaListAllQuotaRulesRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)

	// Validate location
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaListAllQuotaRulesBadRequest{
			Code:    400,
			Message: parsingErr.GetMessage(),
		}, nil
	}

	// Create parameter struct for orchestrator
	listParams := &orchestratorcommon.ListQuotaRulesParams{
		AccountName: params.ProjectNumber,
		VolumeID:    params.VolumeId,
	}

	// Call orchestrator to list quota rules
	quotaRuleList, err := h.Orchestrator.ListQuotaRules(ctx, listParams)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return &gcpgenserver.V1betaListAllQuotaRulesNotFound{
				Code:    404,
				Message: err.Error(),
			}, nil
		}
		logger.Errorf("Failed to list quota rules for volume %s with error: %v", params.VolumeId, err.Error())
		return &gcpgenserver.V1betaListAllQuotaRulesInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}, err
	}

	// Convert to API response format
	return &gcpgenserver.V1betaListAllQuotaRulesOK{
		QuotaRules: convertToVCPQuotaRulesV1Beta(quotaRuleList),
	}, nil
}
