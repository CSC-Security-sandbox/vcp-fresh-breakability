package api

import (
	"context"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func (h Handler) V1betaCheckKmsConfig(ctx context.Context, params gcpgenserver.V1betaCheckKmsConfigParams) (gcpgenserver.V1betaCheckKmsConfigRes, error) {
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	checkKmsConfigParams := &kms_configurations.V1betaCheckKmsConfigParams{
		KmsConfigID:    params.KmsConfigId,
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
	}
	res, err := cvpClient.KmsConfigurations.V1betaCheckKmsConfig(checkKmsConfigParams)
	if err != nil {
		switch e := err.(type) {
		case *kms_configurations.V1betaCheckKmsConfigConflict:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCheckKmsConfigConflict{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaCheckKmsConfigBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCheckKmsConfigBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaCheckKmsConfigUnprocessableEntity:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCheckKmsConfigUnprocessableEntity{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaCheckKmsConfigUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCheckKmsConfigUnauthorized{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaCheckKmsConfigForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCheckKmsConfigForbidden{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaCheckKmsConfigNotFound:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCheckKmsConfigNotFound{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaCheckKmsConfigTooManyRequests:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCheckKmsConfigTooManyRequests{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaCheckKmsConfigDefault:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCheckKmsConfigInternalServerError{
				Code:    code,
				Message: msg,
			}, nil
		}
	}
	if res == nil || res.Payload == nil {
		return &gcpgenserver.V1betaCheckKmsConfigInternalServerError{
			Code:    500,
			Message: "unknown error during the check kms config",
		}, nil
	}
	checkKmsConfigResponse := convertToKmsConfigCheckV1beta(res)
	return checkKmsConfigResponse, nil
}

func (h Handler) V1betaCreateKmsConfiguration(ctx context.Context, req *gcpgenserver.KmsConfigV1beta, params gcpgenserver.V1betaCreateKmsConfigurationParams) (gcpgenserver.V1betaCreateKmsConfigurationRes, error) {
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	deletedTime := strfmt.DateTime(req.DeletedTime.Value)
	updatedTime := strfmt.DateTime(req.UpdatedTime.Value)
	body := &models.KmsConfigV1beta{
		CreatedTime:         strfmt.DateTime(req.CreatedTime.Value),
		DeletedTime:         &deletedTime,
		Description:         &req.Description.Value,
		Instructions:        req.Instructions.Value,
		KeyFullPath:         &req.KeyFullPath,
		KmsState:            string(req.KmsState.Value),
		KmsStateDetails:     req.KmsStateDetails.Value,
		ResourceID:          &req.ResourceId.Value,
		ServiceAccountEmail: req.ServiceAccountEmail.Value,
		UpdatedTime:         &updatedTime,
		UUID:                req.UUID.Value,
	}
	createKmsConfigParams := &kms_configurations.V1betaCreateKmsConfigurationParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
		Body:           body,
	}
	res, err := cvpClient.KmsConfigurations.V1betaCreateKmsConfiguration(createKmsConfigParams)
	if err != nil {
		switch e := err.(type) {
		case *kms_configurations.V1betaCreateKmsConfigurationUnprocessableEntity:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateKmsConfigurationUnprocessableEntity{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaCreateKmsConfigurationConflict:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateKmsConfigurationConflict{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaCreateKmsConfigurationBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateKmsConfigurationBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaCreateKmsConfigurationUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateKmsConfigurationUnauthorized{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaCreateKmsConfigurationForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateKmsConfigurationForbidden{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaCreateKmsConfigurationTooManyRequests:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateKmsConfigurationTooManyRequests{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaCreateKmsConfigurationDefault:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaCreateKmsConfigurationInternalServerError{
				Code:    code,
				Message: msg,
			}, nil
		}
	}
	if res == nil || res.Payload == nil {
		return &gcpgenserver.V1betaCreateKmsConfigurationInternalServerError{
			Code:    500,
			Message: "unknown error during the create kms configuration",
		}, nil
	}
	return convertToOperationV1beta(res.Payload), nil
}

func (h Handler) V1betaDeleteKmsConfiguration(ctx context.Context, params gcpgenserver.V1betaDeleteKmsConfigurationParams) (gcpgenserver.V1betaDeleteKmsConfigurationRes, error) {
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	deleteKmsConfigParams := &kms_configurations.V1betaDeleteKmsConfigurationParams{
		KmsConfigID:    params.KmsConfigId,
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
	}
	res, _, err := cvpClient.KmsConfigurations.V1betaDeleteKmsConfiguration(deleteKmsConfigParams)
	if err != nil {
		switch e := err.(type) {
		case *kms_configurations.V1betaDeleteKmsConfigurationUnprocessableEntity:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteKmsConfigurationUnprocessableEntity{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaDeleteKmsConfigurationConflict:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteKmsConfigurationConflict{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaDeleteKmsConfigurationBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteKmsConfigurationBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaDeleteKmsConfigurationTooManyRequests:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteKmsConfigurationTooManyRequests{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaDeleteKmsConfigurationForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteKmsConfigurationForbidden{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaDeleteKmsConfigurationUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteKmsConfigurationUnauthorized{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaDeleteKmsConfigurationDefault:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDeleteKmsConfigurationInternalServerError{
				Code:    code,
				Message: msg,
			}, nil
		}
	}
	if res == nil || res.Payload == nil {
		return &gcpgenserver.V1betaDeleteKmsConfigurationInternalServerError{
			Code:    500,
			Message: "unknown error during the delete kms configuration",
		}, nil
	}
	return convertToOperationV1beta(res.Payload), nil
}

func (h Handler) V1betaDescribeKmsConfiguration(ctx context.Context, params gcpgenserver.V1betaDescribeKmsConfigurationParams) (gcpgenserver.V1betaDescribeKmsConfigurationRes, error) {
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	describeKmsConfigParams := &kms_configurations.V1betaDescribeKmsConfigurationParams{
		KmsConfigID:    params.KmsConfigId,
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
	}
	res, err := cvpClient.KmsConfigurations.V1betaDescribeKmsConfiguration(describeKmsConfigParams)
	if err != nil {
		switch e := err.(type) {
		case *kms_configurations.V1betaDescribeKmsConfigurationUnprocessableEntity:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDescribeKmsConfigurationUnprocessableEntity{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaDescribeKmsConfigurationConflict:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDescribeKmsConfigurationConflict{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaDescribeKmsConfigurationBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDescribeKmsConfigurationBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaDescribeKmsConfigurationForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDescribeKmsConfigurationForbidden{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaDescribeKmsConfigurationUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDescribeKmsConfigurationUnauthorized{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaDescribeKmsConfigurationTooManyRequests:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDescribeKmsConfigurationTooManyRequests{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaDescribeKmsConfigurationDefault:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaDescribeKmsConfigurationInternalServerError{
				Code:    code,
				Message: msg,
			}, nil
		}
	}
	if res == nil || res.Payload == nil {
		return &gcpgenserver.V1betaDescribeKmsConfigurationInternalServerError{
			Code:    500,
			Message: "unknown error during the describe kms configuration",
		}, nil
	}
	return convertToKmsConfigV1beta(res.Payload), nil
}

func (h Handler) V1betaListKmsConfigurations(ctx context.Context, params gcpgenserver.V1betaListKmsConfigurationsParams) (gcpgenserver.V1betaListKmsConfigurationsRes, error) {
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	listKmsConfigParams := &kms_configurations.V1betaListKmsConfigurationsParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
	}
	res, err := cvpClient.KmsConfigurations.V1betaListKmsConfigurations(listKmsConfigParams)
	if err != nil {
		switch e := err.(type) {
		case *kms_configurations.V1betaListKmsConfigurationsConflict:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListKmsConfigurationsConflict{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaListKmsConfigurationsBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListKmsConfigurationsBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaListKmsConfigurationsNotFound:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListKmsConfigurationsNotFound{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaListKmsConfigurationsForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListKmsConfigurationsForbidden{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaListKmsConfigurationsUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListKmsConfigurationsUnauthorized{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaListKmsConfigurationsTooManyRequests:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListKmsConfigurationsTooManyRequests{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaListKmsConfigurationsDefault:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaListKmsConfigurationsInternalServerError{
				Code:    code,
				Message: msg,
			}, nil
		}
	}
	if res == nil || res.Payload == nil {
		return &gcpgenserver.V1betaListKmsConfigurationsInternalServerError{
			Code:    500,
			Message: "unknown error during the list kms configurations",
		}, nil
	}
	operationResponse := gcpgenserver.V1betaListKmsConfigurationsOK{
		KmsMinusConfigurations: []gcpgenserver.KmsConfigV1beta{},
	}
	for _, kmsConfig := range res.Payload {
		operationResponse.KmsMinusConfigurations = append(operationResponse.KmsMinusConfigurations, *convertToKmsConfigV1beta(kmsConfig))
	}
	return &operationResponse, nil
}

func (h Handler) V1betaUpdateKmsConfiguration(ctx context.Context, req *gcpgenserver.KmsConfigUpdateV1beta, params gcpgenserver.V1betaUpdateKmsConfigurationParams) (gcpgenserver.V1betaUpdateKmsConfigurationRes, error) {
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	body := &models.KmsConfigUpdateV1beta{
		Description: &req.Description.Value,
		KeyFullPath: req.KeyFullPath.Value,
		ResourceID:  &req.ResourceId.Value,
	}
	updateKmsConfigParams := &kms_configurations.V1betaUpdateKmsConfigurationParams{
		KmsConfigID:    params.KmsConfigId,
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID.Value,
		Body:           body,
	}
	res, err := cvpClient.KmsConfigurations.V1betaUpdateKmsConfiguration(updateKmsConfigParams)
	if err != nil {
		switch e := err.(type) {
		case *kms_configurations.V1betaUpdateKmsConfigurationUnprocessableEntity:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateKmsConfigurationUnprocessableEntity{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaUpdateKmsConfigurationConflict:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateKmsConfigurationConflict{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaUpdateKmsConfigurationBadRequest:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateKmsConfigurationBadRequest{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaUpdateKmsConfigurationNotFound:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateKmsConfigurationNotFound{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaUpdateKmsConfigurationForbidden:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateKmsConfigurationForbidden{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaUpdateKmsConfigurationUnauthorized:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateKmsConfigurationUnauthorized{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaUpdateKmsConfigurationTooManyRequests:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateKmsConfigurationTooManyRequests{
				Code:    code,
				Message: msg,
			}, nil
		case *kms_configurations.V1betaUpdateKmsConfigurationDefault:
			msg := nillable.GetString(&e.Payload.Message, "")
			code := float64(nillable.GetFloat64(&e.Payload.Code, 0))
			return &gcpgenserver.V1betaUpdateKmsConfigurationInternalServerError{
				Code:    code,
				Message: msg,
			}, nil
		}
	}
	if res == nil || res.Payload == nil {
		return &gcpgenserver.V1betaUpdateKmsConfigurationInternalServerError{
			Code:    500,
			Message: "unknown error during the update kms configurations",
		}, nil
	}
	return convertToKmsConfigV1beta(res.Payload), nil
}

func convertToKmsConfigCheckV1beta(res *kms_configurations.V1betaCheckKmsConfigOK) *gcpgenserver.KmsConfigCheckV1beta {
	kmsConfigHealthCheckV1beta := gcpgenserver.KmsConfigHealthCheckV1beta{
		IsHealthy:    *res.Payload.KmsConfigHealthCheck.IsHealthy,
		HealthError:  gcpgenserver.NewOptString(res.Payload.KmsConfigHealthCheck.HealthError),
		Instructions: gcpgenserver.NewOptString(res.Payload.KmsConfigHealthCheck.Instructions),
	}
	return &gcpgenserver.KmsConfigCheckV1beta{
		ServiceAccount:       gcpgenserver.NewOptString(res.Payload.ServiceAccount),
		KmsConfigHealthCheck: gcpgenserver.NewOptKmsConfigHealthCheckV1beta(kmsConfigHealthCheckV1beta),
	}
}

func convertToOperationV1beta(res *models.OperationV1beta) *gcpgenserver.OperationV1beta {
	return &gcpgenserver.OperationV1beta{
		Name: gcpgenserver.NewOptString(res.Name),
		Done: gcpgenserver.NewOptBool(*res.Done),
	}
}

func convertToKmsConfigV1beta(res *models.KmsConfigV1beta) *gcpgenserver.KmsConfigV1beta {
	state := gcpgenserver.KmsConfigV1betaKmsState(res.KmsState)
	kmsConfigV1beta := &gcpgenserver.KmsConfigV1beta{
		UUID:            gcpgenserver.NewOptString(res.UUID),
		KmsState:        gcpgenserver.NewOptKmsConfigV1betaKmsState(state),
		KmsStateDetails: gcpgenserver.NewOptString(res.KmsStateDetails),
		Description:     gcpgenserver.NewOptString(*res.Description),
		CreatedTime:     gcpgenserver.NewOptDateTime(time.Time(res.CreatedTime)),
	}
	if res.KeyFullPath != nil {
		kmsConfigV1beta.KeyFullPath = *res.KeyFullPath
	}
	if res.DeletedTime != nil {
		kmsConfigV1beta.DeletedTime = gcpgenserver.NewOptDateTime(time.Time(*res.DeletedTime))
	}
	if res.Instructions != "" {
		kmsConfigV1beta.Instructions = gcpgenserver.NewOptString(res.Instructions)
	}
	if res.ServiceAccountEmail != "" {
		kmsConfigV1beta.ServiceAccountEmail = gcpgenserver.NewOptString(res.ServiceAccountEmail)
	}
	if res.UpdatedTime != nil {
		kmsConfigV1beta.UpdatedTime = gcpgenserver.NewOptDateTime(time.Time(*res.UpdatedTime))
	}
	if res.ResourceID != nil {
		kmsConfigV1beta.ResourceId = gcpgenserver.NewOptString(*res.ResourceID)
	}
	return kmsConfigV1beta
}
