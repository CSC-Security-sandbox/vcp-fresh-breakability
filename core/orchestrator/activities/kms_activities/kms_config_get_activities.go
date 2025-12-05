package kms_activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
)

var (
	GetSDEKmsConfiguration = _getSDEKmsConfiguration
)

// DescribeSDEKmsConfigurationActivity retrieves the KMS configuration details for the given KMS configuration.
func (j *KmsConfigActivity) DescribeSDEKmsConfigurationActivity(ctx context.Context, params *common.GetKmsConfigParams) (*models.KmsConfigV1beta, error) {
	return _getSDEKmsConfiguration(ctx, params)
}

func _getSDEKmsConfiguration(ctx context.Context, params *common.GetKmsConfigParams) (*models.KmsConfigV1beta, error) {
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetAuthTokenFromContext(ctx)
	if jwtToken == "" {
		jwtToken = utils.GetJWTTokenFromContext(ctx)
	}
	cvpClient := createClient(logger, jwtToken)
	xCorrelationID := utils.GetCoRelationIDFromContext(ctx)

	describeKmsConfigParams := kms_configurations.NewV1betaDescribeKmsConfigurationParams()
	describeKmsConfigParams.KmsConfigID = params.UUID
	describeKmsConfigParams.XCorrelationID = &xCorrelationID
	describeKmsConfigParams.LocationID = params.LocationID
	describeKmsConfigParams.ProjectNumber = params.ProjectNumber

	sdeKmsConfigResponse, err := cvpClient.KmsConfigurations.V1betaDescribeKmsConfiguration(describeKmsConfigParams)
	if err != nil {
		return nil, errors2.WrapAsTemporalApplicationError(errors2.NewVCPError(errors2.ErrKmsConfigNotFound, err))
	}
	if sdeKmsConfigResponse == nil || sdeKmsConfigResponse.Payload == nil {
		return nil, errors.New("unknown error during the get kms configuration")
	}
	return sdeKmsConfigResponse.Payload, nil
}

// GetKmsConfigActivity retrieves the KMS configuration by its UUID.
func (j *KmsConfigActivity) GetKmsConfigActivity(ctx context.Context, uuid string) (*datamodel.KmsConfig, error) {
	se := j.SE
	kmsConfig, err := se.GetKmsConfig(ctx, uuid)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return nil, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeKmsConfigNotFound, err)
		}
		return nil, err
	}
	return kmsConfig, err
}

// UpdateKmsConfigAttributesActivity updates the attributes of a KMS configuration in the database.
func (j *KmsConfigActivity) UpdateKmsConfigAttributesActivity(ctx context.Context, kmsConfig *datamodel.KmsConfig, attributes *datamodel.KmsAttributes) (*datamodel.KmsConfig, error) {
	se := j.SE
	kmsConfig, err := se.UpdateKmsConfigAttributes(ctx, kmsConfig.UUID, attributes)
	if err != nil {
		return nil, err
	}
	return kmsConfig, nil
}

func (j *KmsConfigActivity) ListKmsConfigActivity(ctx context.Context, projectNumber string) ([]*datamodel.KmsConfig, error) {
	se := j.SE
	account, err := se.GetAccount(ctx, projectNumber)
	if err != nil {
		return nil, err
	}
	kmsConfigs, err := se.ListKmsConfigByAccountID(ctx, account.ID)
	if err != nil {
		return nil, err
	}
	return kmsConfigs, nil
}

// ConvertToCreateKmsConfigParams transforms from CVP datamodel to VSA datamodel
func ConvertToCreateKmsConfigParams(params *models.KmsConfigV1beta, createPoolParams *common.CreatePoolParams) *common.CreateKmsConfigParams {
	createConfigParams := &common.CreateKmsConfigParams{}

	createConfigParams.ProjectNumber = createPoolParams.AccountName
	createConfigParams.UUID = params.UUID
	createConfigParams.KmsState = params.KmsState
	createConfigParams.KmsStateDetails = params.KmsStateDetails
	createConfigParams.ServiceAccountEmail = params.ServiceAccountEmail
	createConfigParams.Instructions = params.Instructions
	createConfigParams.LocationID = createPoolParams.Region

	if params.Description != nil {
		createConfigParams.Description = *params.Description
	}
	if params.KeyFullPath != nil {
		createConfigParams.KeyFullPath = *params.KeyFullPath
	}
	if params.ResourceID != nil {
		createConfigParams.ResourceID = *params.ResourceID
	}
	return createConfigParams
}
