package kms_activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// DescribeKmsConfigurationActivity retrieves the KMS configuration details for the given KMS configuration.
func (j *KmsConfigActivity) DescribeKmsConfigurationActivity(ctx context.Context, kmsConfig *datamodel.KmsConfig) (*datamodel.KmsConfig, error) {
	se := j.SE
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	xCorrelationID := utils.GetCoRelationIDFromContext(ctx)

	describeKmsConfigParams := kms_configurations.NewV1betaDescribeKmsConfigurationParams()
	describeKmsConfigParams.KmsConfigID = kmsConfig.KmsAttributes.SdeKmsConfigUUID
	describeKmsConfigParams.XCorrelationID = &xCorrelationID
	describeKmsConfigParams.LocationID = kmsConfig.KeyRingLocation
	describeKmsConfigParams.ProjectNumber = kmsConfig.Account.Name
	sdeKmsConfigResponse, err := cvpClient.KmsConfigurations.V1betaDescribeKmsConfiguration(describeKmsConfigParams)
	if err != nil {
		return nil, err
	}
	if sdeKmsConfigResponse == nil || sdeKmsConfigResponse.Payload == nil {
		return nil, errors.New("unknown error during the get kms configuration")
	}
	return se.UpdateKmsConfigAttributes(ctx, kmsConfig.UUID, &datamodel.KmsAttributes{SdeKmsConfigUUID: sdeKmsConfigResponse.Payload.UUID,
		SdeServiceAccountEmail: sdeKmsConfigResponse.Payload.ServiceAccountEmail,
		Instructions:           sdeKmsConfigResponse.Payload.Instructions})
}
