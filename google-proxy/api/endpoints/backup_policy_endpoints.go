package api

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_policy"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"time"
)

var createClient = cvp.CreateClient

func (h Handler) V1betaCreateBackupPolicy(ctx context.Context, req *gcpgenserver.BackupPolicyCreateV1beta, params gcpgenserver.V1betaCreateBackupPolicyParams) (gcpgenserver.V1betaCreateBackupPolicyRes, error) {
	logger := utils.GetLoggerFromContext(ctx)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)

	resourceNameV1beta := models.ResourceNameV1beta{
		ResourceID: &req.ResourceId,
	}
	descriptionV1beta := models.DescriptionV1beta{
		Description: &req.Description.Value,
	}
	dailyBackupLimit := int64(req.DailyBackupLimit.Value)
	monthlyBackupLimit := int64(req.MonthlyBackupLimit.Value)
	weeklyBackupLimit := int64(req.WeeklyBackupLimit.Value)

	backupPolicyScheduleV1beta := models.BackupPolicyScheduleV1beta{
		DailyBackupLimit:   &dailyBackupLimit,
		MonthlyBackupLimit: &monthlyBackupLimit,
		WeeklyBackupLimit:  &weeklyBackupLimit,
	}
	body := &models.BackupPolicyCreateV1beta{
		ResourceNameV1beta:         resourceNameV1beta,
		DescriptionV1beta:          descriptionV1beta,
		BackupPolicyScheduleV1beta: backupPolicyScheduleV1beta,
	}
	createBackupPolicyParams := &backup_policy.V1betaCreateBackupPolicyParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
		Body:           body,
	}
	res, err := cvpClient.BackupPolicy.V1betaCreateBackupPolicy(createBackupPolicyParams)
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
	return convertToOperationV1betaBackupPolicy(res.Payload), nil
}

func (h Handler) V1betaDeleteBackupPolicy(ctx context.Context, params gcpgenserver.V1betaDeleteBackupPolicyParams) (gcpgenserver.V1betaDeleteBackupPolicyRes, error) {
	logger := utils.GetLoggerFromContext(ctx)
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
	logger := utils.GetLoggerFromContext(ctx)
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

func (h Handler) V1betaGetMultipleBackupPolicies(ctx context.Context, req *gcpgenserver.BackupPolicyIDListV1beta, params gcpgenserver.V1betaGetMultipleBackupPoliciesParams) (gcpgenserver.V1betaGetMultipleBackupPoliciesRes, error) {
	logger := utils.GetLoggerFromContext(ctx)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)

	var backupPolicyUUIDs []string
	backupPolicyUUIDs = append(backupPolicyUUIDs, req.BackupPolicyUUIDs...)

	body := &models.BackupPolicyIDListV1beta{
		BackupPolicyUUIDs: backupPolicyUUIDs,
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
	operationResponse := gcpgenserver.V1betaGetMultipleBackupPoliciesOK{
		BackupPolicies: []gcpgenserver.BackupPolicyV1beta{},
	}
	for _, bp := range res.Payload.BackupPolicies {
		operationResponse.BackupPolicies = append(operationResponse.BackupPolicies, convertToBackupPolicyV1beta(bp))
	}
	return &operationResponse, nil
}

func (h Handler) V1betaListBackupPolicies(ctx context.Context, params gcpgenserver.V1betaListBackupPoliciesParams) (gcpgenserver.V1betaListBackupPoliciesRes, error) {
	logger := utils.GetLoggerFromContext(ctx)
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
	operationResponse := gcpgenserver.V1betaListBackupPoliciesOK{
		BackupPolicies: []gcpgenserver.BackupPolicyV1beta{},
	}
	for _, bp := range res.Payload.BackupPolicies {
		operationResponse.BackupPolicies = append(operationResponse.BackupPolicies, convertToBackupPolicyV1beta(bp))
	}
	return &operationResponse, nil
}

func (h Handler) V1betaUpdateBackupPolicy(ctx context.Context, req *gcpgenserver.BackupPolicyScheduleV1beta, params gcpgenserver.V1betaUpdateBackupPolicyParams) (gcpgenserver.V1betaUpdateBackupPolicyRes, error) {
	logger := utils.GetLoggerFromContext(ctx)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
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
	state := gcpgenserver.BackupPolicyV1betaState(bp.State)
	backupPolicy := gcpgenserver.BackupPolicyV1beta{
		ResourceId:         *bp.ResourceID,
		BackupPolicyId:     gcpgenserver.NewOptString(bp.BackupPolicyID),
		Enabled:            *bp.Enabled,
		Description:        gcpgenserver.NewOptString(*bp.Description),
		CreatedAt:          gcpgenserver.NewOptDateTime(time.Time(*bp.CreatedAt)),
		State:              gcpgenserver.NewOptBackupPolicyV1betaState(state),
		VolumeCount:        gcpgenserver.NewOptInt(int(*bp.VolumeCount)),
		DailyBackupLimit:   gcpgenserver.NewOptInt(int(*bp.DailyBackupLimit)),
		WeeklyBackupLimit:  gcpgenserver.NewOptInt(int(*bp.WeeklyBackupLimit)),
		MonthlyBackupLimit: gcpgenserver.NewOptInt(int(*bp.MonthlyBackupLimit)),
	}
	return backupPolicy
}
