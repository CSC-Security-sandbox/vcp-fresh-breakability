package kms_activities

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	cvpClientModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler"
	helper "github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/api/iam/v1"
)

var (
	cmekGlobalProjectId               = env.GetString("CMEK_GLOBAL_PROJECT_ID", "")
	pollCvpOperationForWorkflow       = _pollCvpOperationForWorkflow
	getGcpService                     = activities.GetGCPService
	gcpServiceCreateServiceAccountKey = _gcpServiceCreateServiceAccountKey
)

type KmsConfigActivity struct {
	SE database.Storage
}

func _pollCvpOperationForWorkflow(ctx context.Context, cvpClient cvpapi.Cvp, operationParams *async.V1betaDescribeOperationParams) (*cvpClientModels.OperationV1beta, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("Polling for operation %s", operationParams.OperationID)
	operationResponse, err := cvpClient.Async.V1betaDescribeOperation(operationParams)
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError("failed to describe operation", "DescribeOperationError", err)
	}
	// Check if the operation is done
	if !*operationResponse.Payload.Done {
		logger.Debug("Operation in progress ", operationParams.OperationID)
		return nil, errors.New(fmt.Sprintf("operation %s in progress, trying again", operationParams.OperationID))
	}
	return operationResponse.Payload, nil
}

// PollKmsConfigOperationActivity polls the KMS configuration operation until it is done.
func (j *KmsConfigActivity) PollKmsConfigOperationActivity(ctx context.Context, kmsConfig *datamodel.KmsConfig, params *common.CreateKmsConfigParams, response *kms_configurations.V1betaCreateKmsConfigurationAccepted) (*datamodel.KmsConfig, error) {
	if response == nil || response.Payload == nil {
		return nil, errors.New("unknown error during the create kms configuration")
	}
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	logger := util.GetLogger(ctx)
	cvpClient := createClient(logger, jwtToken)

	// Check if the operation is done
	if !*response.Payload.Done {
		// Extract the operation UUID
		operationUUID := utils.GetOperationUUID(response.Payload.Name)
		operationParams := async.NewV1betaDescribeOperationParams()
		operationParams.OperationID = operationUUID
		operationParams.ProjectNumber = params.ProjectNumber
		operationParams.LocationID = params.LocationID
		_, err := pollCvpOperationForWorkflow(ctx, cvpClient, operationParams)
		if err != nil {
			return nil, err
		}
	}
	var cvpResponse = gcpserver.KmsConfigV1beta{}
	// Marshal the response field back to JSON
	responseJSON, err := json.Marshal(response.Payload.Response)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(responseJSON, &cvpResponse)
	if err != nil {
		return nil, err
	}
	kmsConfig.KmsAttributes.SdeKmsConfigUUID = cvpResponse.UUID.Value
	return kmsConfig, nil
}

// CreateVSAKmsConfigSAKeyActivity creates a service account key for the given KMS configuration.
func (j *KmsConfigActivity) CreateVSAKmsConfigSAKeyActivity(ctx context.Context, kmsConfig *datamodel.KmsConfig) (*datamodel.KmsConfig, error) {
	se := j.SE
	gcpService, err := getGcpService(ctx)
	if err != nil {
		return nil, err
	}

	vsaEmail := utils.RemovePrefix(kmsConfig.KmsAttributes.SdeServiceAccountEmail, SDEShortTermSAPrefix)
	serviceAccountKey, err := gcpServiceCreateServiceAccountKey(gcpService, ctx, vsaEmail)
	if err != nil {
		return nil, err
	}
	sa, err := se.UpdateServiceAccountEmailAndKey(ctx, kmsConfig.ServiceAccount.UUID, vsaEmail, serviceAccountKey.PrivateKeyData)
	if err != nil {
		return nil, err
	}
	kmsConfig.ServiceAccount = sa
	return kmsConfig, nil
}

func _gcpServiceCreateServiceAccountKey(gcpService hyperscaler.GoogleServices, ctx context.Context, email string) (*iam.ServiceAccountKey, error) {
	// Create a service account key for the given service account email
	return gcpService.CreateServiceAccountKey(ctx, email)
}

// GrantRoleActivity grants the specified role to the service account for the given KMS configuration.
func (j *KmsConfigActivity) GrantRoleActivity(ctx context.Context, kmsConfig *datamodel.KmsConfig) error {
	return helper.GrantRoleToServiceAccount(ctx, cmekGlobalProjectId, kmsConfig.ServiceAccount.ServiceAccountEmail, TokenCreatorRole)
}

// FailedKmsConfigCreateActivity updates the KMS configuration state to "error" with the provided error message.
func (j *KmsConfigActivity) FailedKmsConfigCreateActivity(ctx context.Context, kmsConfig *datamodel.KmsConfig, errMsg string) error {
	se := j.SE
	kmsConfig.State = models.LifeCycleStateError
	kmsConfig.StateDetails = errMsg
	_, err := se.UpdateKmsConfigState(ctx, kmsConfig.UUID, kmsConfig.State, kmsConfig.StateDetails)
	if err != nil {
		return err
	}
	_, err = se.UpdateServiceAccountState(ctx, kmsConfig.ServiceAccount.UUID, models.LifeCycleStateError, errMsg)
	return err
}

// CreatedKmsConfigActivity updates the KMS configuration state to created
func (j *KmsConfigActivity) CreatedKmsConfigActivity(ctx context.Context, kmsConfig *datamodel.KmsConfig) error {
	se := j.SE
	kmsConfig.State = models.LifeCycleStateCreated
	kmsConfig.StateDetails = models.LifeCycleStateCreatedDetails
	_, err := se.UpdateKmsConfigState(ctx, kmsConfig.UUID, kmsConfig.State, kmsConfig.StateDetails)
	if err != nil {
		return err
	}
	_, err = se.UpdateServiceAccountState(ctx, kmsConfig.ServiceAccount.UUID, models.AccountStateEnabled, models.LifeCycleStateReadyDetails)
	return err
}
