package kms_activities

import (
	"context"
	"go.temporal.io/sdk/temporal"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// DescribeSDEKmsConfigurationActivity retrieves the KMS configuration details for the given KMS configuration.
func (j *KmsConfigActivity) DescribeSDEKmsConfigurationActivity(ctx context.Context, params *common.GetKmsConfigParams) (*models.KmsConfigV1beta, error) {
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetAuthTokenFromContext(ctx)
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
