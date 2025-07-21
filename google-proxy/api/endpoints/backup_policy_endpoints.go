package api

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_policy"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func (h Handler) V1betaCreateBackupPolicy(ctx context.Context, req *gcpgenserver.BackupPolicyCreateV1beta, params gcpgenserver.V1betaCreateBackupPolicyParams) (gcpgenserver.V1betaCreateBackupPolicyRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaCreateBackupPolicyBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	// Check if the Backup policy already exists in VCP
	existingBackupPolicy, err := h.Orchestrator.GetBackupPolicyByNameAndOwnerID(ctx, req.ResourceId, params.ProjectNumber)
	if err == nil && existingBackupPolicy != nil {
		logger.Infof("backup policy with name: %s already exists ", req.ResourceId)
		backupPolicyJSON, err := json.Marshal(existingBackupPolicy)
		if err != nil {
			logger.Errorf("Failed to marshal backup policy: %v", err)
			return &gcpgenserver.V1betaCreateBackupPolicyInternalServerError{
				Code:    500,
				Message: "Failed to marshal backup policy",
			}, err
		}

		return &gcpgenserver.OperationV1beta{
			Name:     gcpgenserver.OptString{Value: "operation-id"},
			Done:     gcpgenserver.NewOptBool(true),
			Response: backupPolicyJSON,
		}, nil
	} else if err != nil && !errors.IsNotFoundErr(err) {
		logger.Errorf("Failed to check existing backup policy : %v", err)
		return &gcpgenserver.V1betaCreateBackupPolicyInternalServerError{
			Code:    500,
			Message: "Failed to check existing backup policy",
		}, err
	}

	// Call SDE to create backup policy
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	backupPolicyParams := createBackupPolicyParams(req, params)
	res, err := cvpClient.BackupPolicy.V1betaCreateBackupPolicy(backupPolicyParams)
	if err != nil {
		switch e := err.(type) {
		case *backup_policy.V1betaCreateBackupPolicyConflict:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateBackupPolicyConflict{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaCreateBackupPolicyBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateBackupPolicyBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaCreateBackupPolicyUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateBackupPolicyUnauthorized{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaCreateBackupPolicyForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateBackupPolicyForbidden{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaCreateBackupPolicyDefault:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateBackupPolicyInternalServerError{
				Code:    code,
				Message: msg,
			}, nil
		}
	}
	if res == nil || res.Payload == nil {
		return &gcpgenserver.V1betaCreateBackupPolicyInternalServerError{
			Code:    500,
			Message: "unknown error during the create backup policy",
		}, nil
	}
	backupPolicyJSON, err := json.Marshal(res.Payload.Response)
	if err != nil {
		logger.Errorf("Failed to marshal backup policy: %s", err.Error())
		return &gcpgenserver.V1betaCreateBackupPolicyInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("Failed to marshal backup policy: %s", err.Error()),
		}, nil
	}
	return &gcpgenserver.OperationV1beta{
		Name:     gcpgenserver.NewOptString(res.Payload.Name),
		Response: backupPolicyJSON,
		Done:     gcpgenserver.NewOptBool(true),
	}, nil
}

func (h Handler) V1betaDeleteBackupPolicy(ctx context.Context, params gcpgenserver.V1betaDeleteBackupPolicyParams) (gcpgenserver.V1betaDeleteBackupPolicyRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	deleteBackupPolicyParams := &backup_policy.V1betaDeleteBackupPolicyParams{
		BackupPolicyID: params.BackupPolicyId,
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
	}
	res, _, err := cvpClient.BackupPolicy.V1betaDeleteBackupPolicy(deleteBackupPolicyParams)
	if err != nil {
		switch e := err.(type) {
		case *backup_policy.V1betaDeleteBackupPolicyConflict:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteBackupPolicyConflict{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaDeleteBackupPolicyBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteBackupPolicyBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaDeleteBackupPolicyUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteBackupPolicyUnauthorized{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaDeleteBackupPolicyForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteBackupPolicyForbidden{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaDeleteBackupPolicyDefault:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteBackupPolicyInternalServerError{
				Code:    code,
				Message: msg,
			}, nil
		}
	}
	if res == nil || res.Payload == nil {
		return &gcpgenserver.V1betaDeleteBackupPolicyInternalServerError{
			Code:    500,
			Message: "unknown error during the delete backup policy",
		}, nil
	}
	return convertToOperationV1betaBackupPolicy(res.Payload), nil
}

func (h Handler) V1betaDescribeBackupPolicy(ctx context.Context, params gcpgenserver.V1betaDescribeBackupPolicyParams) (gcpgenserver.V1betaDescribeBackupPolicyRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	describeBackupPolicyParams := &backup_policy.V1betaDescribeBackupPolicyParams{
		BackupPolicyID: params.BackupPolicyId,
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
	}
	res, err := cvpClient.BackupPolicy.V1betaDescribeBackupPolicy(describeBackupPolicyParams)
	if err != nil {
		switch e := err.(type) {
		case *backup_policy.V1betaDescribeBackupPolicyBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDescribeBackupPolicyBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaDescribeBackupPolicyUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDescribeBackupPolicyUnauthorized{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaDescribeBackupPolicyForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDescribeBackupPolicyForbidden{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaDescribeBackupPolicyNotFound:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDescribeBackupPolicyNotFound{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaDescribeBackupPolicyDefault:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDescribeBackupPolicyInternalServerError{
				Code:    code,
				Message: msg,
			}, nil
		}
	}
	if res == nil || res.Payload == nil {
		return &gcpgenserver.V1betaDescribeBackupPolicyInternalServerError{
			Code:    500,
			Message: "unknown error during the describe backup policy",
		}, nil
	}
	return convertToBackupPolicyDetailsV1beta(res), nil
}

func (h Handler) V1betaGetMultipleBackupPolicies(ctx context.Context, req *gcpgenserver.BackupPolicyIdListV1beta, params gcpgenserver.V1betaGetMultipleBackupPoliciesParams) (gcpgenserver.V1betaGetMultipleBackupPoliciesRes, error) {
	if !backupEnabled {
		return &gcpgenserver.V1betaGetMultipleBackupPoliciesBadRequest{
			Code:    400,
			Message: "Backup feature is currently not enabled.",
		}, nil
	}
	logger := util.GetLogger(ctx)
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaGetMultipleBackupPoliciesBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)

	body := &models.BackupPolicyIDListV1beta{
		BackupPolicyUUIDs: req.BackupPolicyUuids,
	}
	getMultipleBackupPoliciesParams := &backup_policy.V1betaGetMultipleBackupPoliciesParams{
		Body:           body,
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
	}

	res, err := cvpClient.BackupPolicy.V1betaGetMultipleBackupPolicies(getMultipleBackupPoliciesParams)
	if err != nil {
		switch e := err.(type) {
		case *backup_policy.V1betaGetMultipleBackupPoliciesBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultipleBackupPoliciesBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaGetMultipleBackupPoliciesUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultipleBackupPoliciesUnauthorized{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaGetMultipleBackupPoliciesForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultipleBackupPoliciesForbidden{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaGetMultipleBackupPoliciesNotFound:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultipleBackupPoliciesNotFound{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaGetMultipleBackupPoliciesDefault:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaGetMultipleBackupPoliciesInternalServerError{
				Code:    code,
				Message: msg,
			}, nil
		}
	}
	if res == nil || res.Payload == nil {
		return &gcpgenserver.V1betaGetMultipleBackupPoliciesInternalServerError{
			Code:    500,
			Message: "unknown error during the get multiple backup policies",
		}, nil
	}

	vcpBackupPolicyVolumeCount, vcpBackupPolicies, err := h.Orchestrator.ListBackupPoliciesAndVolumeCount(ctx, params.ProjectNumber, req.BackupPolicyUuids)
	if err != nil {
		logger.Errorf("Failed to get backup policies and volume counts: %v", err)
		return &gcpgenserver.V1betaGetMultipleBackupPoliciesInternalServerError{
			Code:    500,
			Message: "Failed to get backup policies",
		}, nil
	}

	operationResponse := gcpgenserver.V1betaGetMultipleBackupPoliciesOK{
		BackupPolicies: []gcpgenserver.BackupPolicyV1beta{},
	}
	for _, bp := range res.Payload.BackupPolicies {
		if vcpBackupPolicyVolumeCount[bp.BackupPolicyID] > 0 {
			// Update the backup policy's volume count if volumes are assigned to this policy in VCP
			totalVolumesAssigned := vcpBackupPolicyVolumeCount[bp.BackupPolicyID]
			if bp.VolumeCount != nil {
				totalVolumesAssigned += *bp.VolumeCount
			}
			bp.VolumeCount = &totalVolumesAssigned
		}
		if vcpBackupPolicies[bp.BackupPolicyID] != nil {
			// Update the backup policy state if it exists in VCP
			bp.State = vcpBackupPolicies[bp.BackupPolicyID].State
		}
		operationResponse.BackupPolicies = append(operationResponse.BackupPolicies, convertToBackupPolicyV1beta(bp))
	}
	return &operationResponse, nil
}

func (h Handler) V1betaListBackupPolicies(ctx context.Context, params gcpgenserver.V1betaListBackupPoliciesParams) (gcpgenserver.V1betaListBackupPoliciesRes, error) {
	if !backupEnabled {
		return &gcpgenserver.V1betaListBackupPoliciesBadRequest{
			Code:    400,
			Message: "Backup feature is currently not enabled.",
		}, nil
	}
	logger := util.GetLogger(ctx)
	_, _, parsingErr := parseAndValidateRegionAndZone(params.LocationId)
	if parsingErr != nil {
		return &gcpgenserver.V1betaListBackupPoliciesBadRequest{
			Code:    parsingErr.Code,
			Message: parsingErr.Message,
		}, nil
	}

	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	listBackupPoliciesParams := &backup_policy.V1betaListBackupPoliciesParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
	}
	res, err := cvpClient.BackupPolicy.V1betaListBackupPolicies(listBackupPoliciesParams)
	if err != nil {
		switch e := err.(type) {
		case *backup_policy.V1betaListBackupPoliciesBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListBackupPoliciesBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaListBackupPoliciesForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListBackupPoliciesForbidden{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaListBackupPoliciesUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListBackupPoliciesUnauthorized{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaListBackupPoliciesNotFound:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListBackupPoliciesNotFound{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaListBackupPoliciesDefault:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListBackupPoliciesInternalServerError{
				Code:    code,
				Message: msg,
			}, nil
		}
	}
	if res == nil || res.Payload == nil {
		return &gcpgenserver.V1betaListBackupPoliciesInternalServerError{
			Code:    500,
			Message: "unknown error during the list backup policies",
		}, nil
	}

	vcpBackupPolicyVolumeCount, vcpBackupPolicies, err := h.Orchestrator.ListBackupPoliciesAndVolumeCount(ctx, params.ProjectNumber, nil)
	if err != nil {
		logger.Errorf("Failed to list backup policies and volume counts: %v", err)
		return &gcpgenserver.V1betaListBackupPoliciesInternalServerError{
			Code:    500,
			Message: "Failed to list backup policies",
		}, nil
	}

	operationResponse := gcpgenserver.V1betaListBackupPoliciesOK{
		BackupPolicies: []gcpgenserver.BackupPolicyV1beta{},
	}
	for _, bp := range res.Payload.BackupPolicies {
		if vcpBackupPolicyVolumeCount[bp.BackupPolicyID] > 0 {
			// Update the backup policy's volume count if volumes are assigned to this policy in VCP
			totalVolumesAssigned := vcpBackupPolicyVolumeCount[bp.BackupPolicyID]
			if bp.VolumeCount != nil {
				totalVolumesAssigned += *bp.VolumeCount
			}
			bp.VolumeCount = &totalVolumesAssigned
		}
		if vcpBackupPolicies[bp.BackupPolicyID] != nil {
			// Update the backup policy state if it exists in VCP
			bp.State = vcpBackupPolicies[bp.BackupPolicyID].State
		}
		operationResponse.BackupPolicies = append(operationResponse.BackupPolicies, convertToBackupPolicyV1beta(bp))
	}
	return &operationResponse, nil
}

func (h Handler) V1betaUpdateBackupPolicy(ctx context.Context, req *gcpgenserver.BackupPolicyUpdateV1beta, params gcpgenserver.V1betaUpdateBackupPolicyParams) (gcpgenserver.V1betaUpdateBackupPolicyRes, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.ProjectNumber, params.LocationId, nil)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	description := req.Description.Value
	enabled := req.Enabled.Value
	dailyBackupLimit := int64(req.DailyBackupLimit.Value)
	monthlyBackupLimit := int64(req.MonthlyBackupLimit.Value)
	weeklyBackupLimit := int64(req.WeeklyBackupLimit.Value)

	backupPolicyScheduleV1beta := models.BackupPolicyScheduleV1beta{
		DailyBackupLimit:   &dailyBackupLimit,
		MonthlyBackupLimit: &monthlyBackupLimit,
		WeeklyBackupLimit:  &weeklyBackupLimit,
	}
	body := &models.BackupPolicyUpdateV1beta{
		BackupPolicyScheduleV1beta: backupPolicyScheduleV1beta,
		Description:                &description,
		Enabled:                    &enabled,
	}

	updateBackupPolicyParams := &backup_policy.V1betaUpdateBackupPolicyParams{
		BackupPolicyID: params.BackupPolicyId,
		Body:           body,
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
	}
	res, _, err := cvpClient.BackupPolicy.V1betaUpdateBackupPolicy(updateBackupPolicyParams)
	if err != nil {
		switch e := err.(type) {
		case *backup_policy.V1betaUpdateBackupPolicyBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateBackupPolicyBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaUpdateBackupPolicyForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateBackupPolicyForbidden{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaUpdateBackupPolicyUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateBackupPolicyUnauthorized{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaUpdateBackupPolicyNotFound:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateBackupPolicyNotFound{
				Code:    code,
				Message: msg,
			}, nil
		case *backup_policy.V1betaUpdateBackupPolicyDefault:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateBackupPolicyInternalServerError{
				Code:    code,
				Message: msg,
			}, nil
		}
	}
	if res == nil || res.Payload == nil {
		return &gcpgenserver.V1betaUpdateBackupPolicyInternalServerError{
			Code:    500,
			Message: "unknown error during the update backup policy",
		}, nil
	}
	return convertToOperationV1betaBackupPolicy(res.Payload), nil
}

func createBackupPolicyParams(req *gcpgenserver.BackupPolicyCreateV1beta, params gcpgenserver.V1betaCreateBackupPolicyParams) *backup_policy.V1betaCreateBackupPolicyParams {
	resourceId := req.ResourceId
	var description string
	if req.Description.IsSet() {
		description = req.Description.Value
	}
	var dailyBackupLimit, monthlyBackupLimit, weeklyBackupLimit int64
	if req.DailyBackupLimit.IsSet() {
		dailyBackupLimit = int64(req.DailyBackupLimit.Value)
	}
	if req.WeeklyBackupLimit.IsSet() {
		weeklyBackupLimit = int64(req.WeeklyBackupLimit.Value)
	}
	if req.MonthlyBackupLimit.IsSet() {
		monthlyBackupLimit = int64(req.MonthlyBackupLimit.Value)
	}
	var enabled bool
	if req.Enabled.IsSet() {
		enabled = req.Enabled.Value
	}
	var correlationID string
	if params.XCorrelationID.IsSet() {
		correlationID = params.XCorrelationID.Value
	}
	return &backup_policy.V1betaCreateBackupPolicyParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &correlationID,
		Body: &models.BackupPolicyCreateV1beta{
			ResourceNameV1beta: models.ResourceNameV1beta{
				ResourceID: &resourceId,
			},
			DescriptionV1beta: models.DescriptionV1beta{
				Description: &description,
			},
			BackupPolicyScheduleV1beta: models.BackupPolicyScheduleV1beta{
				DailyBackupLimit:   &dailyBackupLimit,
				WeeklyBackupLimit:  &weeklyBackupLimit,
				MonthlyBackupLimit: &monthlyBackupLimit,
			},
			Enabled: &enabled,
		},
	}
}

func convertToOperationV1betaBackupPolicy(res *models.OperationV1beta) *gcpgenserver.OperationV1beta {
	return &gcpgenserver.OperationV1beta{
		Name: gcpgenserver.NewOptString(res.Name),
		Done: gcpgenserver.NewOptBool(*res.Done),
	}
}

func convertToBackupPolicyDetailsV1beta(res *backup_policy.V1betaDescribeBackupPolicyOK) *gcpgenserver.BackupPolicyDetailsV1beta {
	state := gcpgenserver.BackupPolicyDetailsV1betaState(res.Payload.State)
	var volumeBackups []gcpgenserver.VolumeBackupDetailsV1beta
	for _, vb := range res.Payload.VolumeBackups {
		volumeBackups = append(volumeBackups, gcpgenserver.VolumeBackupDetailsV1beta{
			VolumeName:           gcpgenserver.NewOptString(vb.VolumeName),
			ScheduledBackupCount: gcpgenserver.NewOptInt(int(vb.ScheduledBackupCount)),
			PolicyEnabled:        gcpgenserver.NewOptBool(*vb.PolicyEnabled),
		})
	}
	return &gcpgenserver.BackupPolicyDetailsV1beta{
		ResourceId:         *res.Payload.ResourceID,
		BackupPolicyId:     gcpgenserver.NewOptString(res.Payload.BackupPolicyID),
		Enabled:            *res.Payload.Enabled,
		Description:        gcpgenserver.NewOptString(*res.Payload.Description),
		CreatedAt:          gcpgenserver.NewOptDateTime(time.Time(*res.Payload.CreatedAt)),
		State:              gcpgenserver.NewOptBackupPolicyDetailsV1betaState(state),
		DailyBackupLimit:   gcpgenserver.NewOptInt(int(*res.Payload.DailyBackupLimit)),
		WeeklyBackupLimit:  gcpgenserver.NewOptInt(int(*res.Payload.WeeklyBackupLimit)),
		MonthlyBackupLimit: gcpgenserver.NewOptInt(int(*res.Payload.MonthlyBackupLimit)),
		VolumeBackups:      volumeBackups,
		VolumeCount:        gcpgenserver.NewOptInt(int(*res.Payload.VolumeCount)),
	}
}

func convertToBackupPolicyV1beta(bp *models.BackupPolicyV1beta) gcpgenserver.BackupPolicyV1beta {
	backupPolicy := gcpgenserver.BackupPolicyV1beta{}

	backupPolicy.BackupPolicyId = gcpgenserver.NewOptString(bp.BackupPolicyID)

	if bp.ResourceID != nil {
		backupPolicy.ResourceId = *bp.ResourceID
	}
	if bp.Enabled != nil {
		backupPolicy.Enabled = *bp.Enabled
	}
	if bp.Description != nil {
		backupPolicy.Description = gcpgenserver.NewOptString(*bp.Description)
	}
	if bp.CreatedAt != nil {
		backupPolicy.CreatedAt = gcpgenserver.NewOptDateTime(time.Time(*bp.CreatedAt))
	}
	if bp.State != "" {
		state := gcpgenserver.BackupPolicyV1betaState(bp.State)
		backupPolicy.State = gcpgenserver.NewOptBackupPolicyV1betaState(state)
	}
	if bp.VolumeCount != nil {
		backupPolicy.VolumeCount = gcpgenserver.NewOptInt(int(*bp.VolumeCount))
	}
	if bp.DailyBackupLimit != nil {
		backupPolicy.DailyBackupLimit = gcpgenserver.NewOptInt(int(*bp.DailyBackupLimit))
	}
	if bp.WeeklyBackupLimit != nil {
		backupPolicy.WeeklyBackupLimit = gcpgenserver.NewOptInt(int(*bp.WeeklyBackupLimit))
	}
	if bp.MonthlyBackupLimit != nil {
		backupPolicy.MonthlyBackupLimit = gcpgenserver.NewOptInt(int(*bp.MonthlyBackupLimit))
	}
	return backupPolicy
}
