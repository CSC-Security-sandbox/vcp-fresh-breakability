package sde

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var createClient = cvp.CreateClient

func UpdateSDEKmsConfiguration(ctx context.Context, kmsConfig *datamodel.KmsConfig, params *common.UpdateKmsConfigParams) (gcpgenserver.V1betaUpdateKmsConfigurationRes, error) {
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)

	body := &models.KmsConfigUpdateV1beta{}
	if params.KeyUri != "" {
		body.KeyFullPath = params.KeyUri
	}
	if params.Description != nil {
		body.Description = params.Description
	}
	if params.Name != "" {
		body.ResourceID = &params.Name
	}
	updateKmsConfigParams := &kms_configurations.V1betaUpdateKmsConfigurationParams{
		KmsConfigID:    kmsConfig.KmsAttributes.SdeKmsConfigUUID,
		LocationID:     params.Region,
		ProjectNumber:  params.AccountName,
		XCorrelationID: &params.XCorrelationID,
		Body:           body,
	}
	res, err := cvpClient.KmsConfigurations.V1betaUpdateKmsConfiguration(updateKmsConfigParams)
	if err != nil {
		return convertCvpClientUpdateKmsConfigErrorToVcpError(err), nil
	}
	if res == nil || res.Payload == nil {
		return &gcpgenserver.V1betaUpdateKmsConfigurationInternalServerError{
			Code:    500,
			Message: "unknown error during the update kms configurations",
		}, nil
	}
	return nil, nil
}

func convertCvpClientUpdateKmsConfigErrorToVcpError(cvpErr error) gcpgenserver.V1betaUpdateKmsConfigurationRes {
	getMsg := func(msg *string) string {
		return nillable.GetString(msg, "")
	}
	getCode := func(floatVal *float64) float64 {
		return nillable.GetFloat64(floatVal, 0)
	}
	switch e := cvpErr.(type) {
	case *kms_configurations.V1betaUpdateKmsConfigurationUnprocessableEntity:
		return &gcpgenserver.V1betaUpdateKmsConfigurationUnprocessableEntity{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaUpdateKmsConfigurationConflict:
		return &gcpgenserver.V1betaUpdateKmsConfigurationConflict{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaUpdateKmsConfigurationBadRequest:
		return &gcpgenserver.V1betaUpdateKmsConfigurationBadRequest{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaUpdateKmsConfigurationNotFound:
		return &gcpgenserver.V1betaUpdateKmsConfigurationNotFound{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaUpdateKmsConfigurationForbidden:
		return &gcpgenserver.V1betaUpdateKmsConfigurationForbidden{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaUpdateKmsConfigurationUnauthorized:
		return &gcpgenserver.V1betaUpdateKmsConfigurationUnauthorized{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaUpdateKmsConfigurationTooManyRequests:
		return &gcpgenserver.V1betaUpdateKmsConfigurationTooManyRequests{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaUpdateKmsConfigurationDefault:
		return &gcpgenserver.V1betaUpdateKmsConfigurationInternalServerError{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	default:
		return &gcpgenserver.V1betaUpdateKmsConfigurationInternalServerError{
			Code:    500,
			Message: "unknown error during the update kms configurations",
		}
	}
}
