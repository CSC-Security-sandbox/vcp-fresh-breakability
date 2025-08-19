package sde

import (
	"context"
	"go.temporal.io/sdk/temporal"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
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
	if params.ResourceID != "" {
		body.ResourceID = &params.ResourceID
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
		return convertCvpClientUpdateKmsConfigErrorToVcpError(err), err
	}
	if res == nil || res.Payload == nil {
		return &gcpgenserver.V1betaUpdateKmsConfigurationInternalServerError{
			Code:    500,
			Message: "unknown error during the update kms configurations",
		}, nil
	}
	return convertCvpKmsConfigToGcpServer(res.Payload), nil
}

func DeleteSDEKmsConfiguration(ctx context.Context, kmsConfig *datamodel.KmsConfig, params *common.DeleteKmsConfigParams) (gcpgenserver.V1betaDeleteKmsConfigurationRes, error) {
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetAuthTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)

	deleteKmsConfigParams := &kms_configurations.V1betaDeleteKmsConfigurationParams{
		KmsConfigID:    kmsConfig.KmsAttributes.SdeKmsConfigUUID,
		LocationID:     params.Region,
		ProjectNumber:  params.AccountName,
		XCorrelationID: &params.XCorrelationID,
	}
	res, _, err := cvpClient.KmsConfigurations.V1betaDeleteKmsConfiguration(deleteKmsConfigParams)
	if err != nil {
		return convertCvpClientDeleteKmsConfigErrorToVcpError(err), errors2.WrapAsTemporalApplicationError(errors2.NewVCPError(errors2.ErrKMSDeleteSDE, err))
	}
	if res == nil || res.Payload == nil {
		return &gcpgenserver.V1betaDeleteKmsConfigurationInternalServerError{
			Code:    500,
			Message: "unknown error during the delete kms configurations",
		}, errors2.WrapAsTemporalApplicationError(errors2.NewVCPError(errors2.ErrKMSDeleteSDE, err))
	}
	return convertToOperationV1beta(res.Payload), nil
}

func DescribeSDEJob(ctx context.Context, operationId, region, accountName, correlationId string) error {
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetAuthTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)

	describeOperationParams := &async.V1betaDescribeOperationParams{
		LocationID:     region,
		ProjectNumber:  accountName,
		XCorrelationID: &correlationId,
		OperationID:    operationId,
	}
	res, err := cvpClient.Async.V1betaDescribeOperation(describeOperationParams)
	if err != nil {
		return temporal.NewNonRetryableApplicationError("failed to describe operation", "DescribeOperationError", err)
	}
	if *res.Payload.Done {
		if res.Payload.Error != nil {
			logger.Errorf("job failed in SDE: %v", res.Payload.Error)
			return errors2.WrapAsTemporalApplicationError(errors2.NewVCPError(errors2.ErrDescribingSDEJob, errors2.New("sde job failed")))
		}
		return nil
	}

	return errors2.NewVCPError(errors2.ErrSDEJobNotFinished, errors2.New("sde job not finished"))
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

func convertCvpClientDeleteKmsConfigErrorToVcpError(cvpErr error) gcpgenserver.V1betaDeleteKmsConfigurationRes {
	getMsg := func(msg *string) string {
		return nillable.GetString(msg, "")
	}
	getCode := func(floatVal *float64) float64 {
		return nillable.GetFloat64(floatVal, 0)
	}
	switch e := cvpErr.(type) {
	case *kms_configurations.V1betaDeleteKmsConfigurationUnprocessableEntity:
		return &gcpgenserver.V1betaDeleteKmsConfigurationUnprocessableEntity{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaDeleteKmsConfigurationConflict:
		return &gcpgenserver.V1betaDeleteKmsConfigurationConflict{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaDeleteKmsConfigurationBadRequest:
		return &gcpgenserver.V1betaDeleteKmsConfigurationBadRequest{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaDeleteKmsConfigurationNotFound:
		return &gcpgenserver.V1betaDeleteKmsConfigurationNotFound{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaDeleteKmsConfigurationForbidden:
		return &gcpgenserver.V1betaDeleteKmsConfigurationForbidden{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaDeleteKmsConfigurationUnauthorized:
		return &gcpgenserver.V1betaDeleteKmsConfigurationUnauthorized{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaDeleteKmsConfigurationTooManyRequests:
		return &gcpgenserver.V1betaDeleteKmsConfigurationTooManyRequests{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	case *kms_configurations.V1betaDeleteKmsConfigurationDefault:
		return &gcpgenserver.V1betaDeleteKmsConfigurationInternalServerError{
			Code:    getCode(&e.Payload.Code),
			Message: getMsg(&e.Payload.Message),
		}
	default:
		return &gcpgenserver.V1betaDeleteKmsConfigurationInternalServerError{
			Code:    500,
			Message: "unknown error during the delete kms configurations",
		}
	}
}

func convertToOperationV1beta(res *models.OperationV1beta) *gcpgenserver.OperationV1beta {
	return &gcpgenserver.OperationV1beta{
		Name: gcpgenserver.NewOptString(res.Name),
		Done: gcpgenserver.NewOptBool(*res.Done),
	}
}

// convertCvpKmsConfigToGcpServer converts CVP KmsConfigV1beta to gcpgenserver KmsConfigV1beta
func convertCvpKmsConfigToGcpServer(cvpConfig *models.KmsConfigV1beta) *gcpgenserver.KmsConfigV1beta {
	result := &gcpgenserver.KmsConfigV1beta{}

	result.UUID = gcpgenserver.NewOptString(cvpConfig.UUID)
	result.ServiceAccountEmail = gcpgenserver.NewOptString(cvpConfig.ServiceAccountEmail)

	if cvpConfig.KeyFullPath != nil {
		result.KeyFullPath = *cvpConfig.KeyFullPath
	}

	// Convert the KMS state
	if cvpConfig.KmsState != "" {
		result.KmsState = gcpgenserver.NewOptKmsConfigV1betaKmsState(gcpgenserver.KmsConfigV1betaKmsState(cvpConfig.KmsState))
	}

	// Convert the KMS state
	if cvpConfig.KmsStateDetails != "" {
		result.KmsStateDetails = gcpgenserver.NewOptString(cvpConfig.KmsStateDetails)
	}

	if cvpConfig.Description != nil {
		result.Description = gcpgenserver.NewOptString(*cvpConfig.Description)
	}

	if !cvpConfig.CreatedTime.IsZero() {
		result.CreatedTime = gcpgenserver.NewOptDateTime(time.Time(cvpConfig.CreatedTime))
	}

	if cvpConfig.UpdatedTime != nil && !cvpConfig.UpdatedTime.IsZero() {
		result.UpdatedTime = gcpgenserver.NewOptDateTime(time.Time(*cvpConfig.UpdatedTime))
	}

	if cvpConfig.DeletedTime != nil && !cvpConfig.DeletedTime.IsZero() {
		result.DeletedTime = gcpgenserver.NewOptDateTime(time.Time(*cvpConfig.DeletedTime))
	}

	if cvpConfig.Instructions != "" {
		result.Instructions = gcpgenserver.NewOptString(cvpConfig.Instructions)
	}

	if cvpConfig.ResourceID != nil {
		result.ResourceId = gcpgenserver.NewOptString(*cvpConfig.ResourceID)
	}

	return result
}
