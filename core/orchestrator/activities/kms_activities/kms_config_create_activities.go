package kms_activities

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	cvpClientModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

const (
	TokenCreatorRole     = "roles/iam.serviceAccountTokenCreator"
	SDEShortTermSAPrefix = "n-" // this is the prefix for the short-term service account which is created in SDE
)

var (
	createClient = cvp.CreateClient
)

// CreateKmsConfigSDEActivity creates a KMS configuration in SDE and polls for its completion.
func (j *KmsConfigActivity) CreateKmsConfigSDEActivity(ctx context.Context, params *common.CreateKmsConfigParams) (*kms_configurations.V1betaCreateKmsConfigurationAccepted, error) {
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := createClient(logger, jwtToken)
	xCorrelationID := utils.GetCoRelationIDFromContext(ctx)
	var body = &cvpClientModels.KmsConfigV1beta{
		ResourceID:  &params.ResourceID,
		KeyFullPath: &params.KeyFullPath,
	}
	createKmsConfigParams := &kms_configurations.V1betaCreateKmsConfigurationParams{
		LocationID:     params.LocationID,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &xCorrelationID,
		Body:           body,
	}

	// Initiate the KMS configuration creation
	response, err := cvpClient.KmsConfigurations.V1betaCreateKmsConfiguration(createKmsConfigParams)
	if err != nil {
		logger.Error("Error creating KMS configuration: ", err)
		return nil, err
	}
	if response == nil || response.Payload == nil {
		return nil, errors.New("unknown error during the create kms configuration")
	}
	return response, nil
}
