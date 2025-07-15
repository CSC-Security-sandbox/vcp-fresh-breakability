package kms_activities

import (
	"context"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	cvpClientModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

const (
	TokenCreatorRole     = "roles/iam.serviceAccountTokenCreator"
	SDEShortTermSAPrefix = "n-" // this is the prefix for the short-term service account which is created in SDE
	netappDomain         = "netapp.com"
)

var (
	createClient = cvp.CreateClient
)

// CreateKmsConfigSDEActivity creates a KMS configuration in SDE and polls for its completion.
func (j *KmsConfigActivity) CreateKmsConfigSDEActivity(ctx context.Context, params *common.CreateKmsConfigParams) (*kms_configurations.V1betaCreateKmsConfigurationAccepted, error) {
	logger := util.GetLogger(ctx)
	jwtToken := utils.GetAuthTokenFromContext(ctx)
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

func (j *KmsConfigActivity) CreateAndSyncKmsConfigActivity(ctx context.Context, params *common.CreateKmsConfigParams) (*datamodel.KmsConfig, error) {
	se := j.SE

	account, err := se.GetAccount(ctx, params.AccountName)
	if err != nil {
		return nil, err
	}

	parsedKeyFullPathResource, err := utils.ParseKeyFullPathResource(params.KeyFullPath)
	if err != nil {
		return nil, err
	}

	dbKmsConfig := &datamodel.KmsConfig{}
	dbKmsConfig.CreatedAt = time.Now()
	dbKmsConfig.UUID = params.UUID
	dbKmsConfig.State = params.KmsState
	dbKmsConfig.StateDetails = params.KmsStateDetails
	dbKmsConfig.AccountID = account.ID
	dbKmsConfig.UpdatedAt = time.Now()
	dbKmsConfig.KeyName = parsedKeyFullPathResource.CryptoKey
	dbKmsConfig.CustomerProjectID = params.ProjectNumber
	dbKmsConfig.KeyRingLocation = parsedKeyFullPathResource.Location
	dbKmsConfig.KeyRing = parsedKeyFullPathResource.KeyRing
	dbKmsConfig.ResourceID = params.ResourceID
	dbKmsConfig.KmsAttributes = &datamodel.KmsAttributes{Instructions: params.Instructions,
		SdeKmsConfigUUID:       params.UUID,
		SdeServiceAccountEmail: params.ServiceAccountEmail,
	}
	dbKmsConfig.KeyProjectID = parsedKeyFullPathResource.ProjectID
	return se.CreateKmsConfig(ctx, dbKmsConfig)
}

func (j *KmsConfigActivity) CreateDnsActivity(ctx context.Context, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider, err := activities.GetProviderByNode(ctx, node)
	if err != nil {
		return err
	}

	googleDnsServers := []string{"8.8.8.8", "8.8.4.4"}
	dnsCreateParams := vsa.CreateDnsParams{
		Domains: []string{netappDomain},
		Servers: googleDnsServers,
	}
	err = provider.CreateDns(dnsCreateParams)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate entry") {
			logger.Info("Create DNS Activity - DNS entry already present in VSA", "error", err)
			return nil
		}
		logger.Error("Failed to create dns", "error", err)
		return err
	}
	return nil
}
