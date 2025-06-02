package kms_activities

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	cvpClientModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/google"
	helper "github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
)

var (
	cmekGlobalProjectId         = env.GetString("CMEK_GLOBAL_PROJECT_ID", "")
	pollCvpOperationForWorkflow = _pollCvpOperationForWorkflow
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
	if operationResponse.Payload.Done != nil && *operationResponse.Payload.Done {
		// Check if there is an error in the operation
		if operationResponse.Payload.Error != nil {
			msg := fmt.Errorf("operation failed: %v", operationResponse.Payload.Error)
			return nil, temporal.NewNonRetryableApplicationError("operation failed", "OperationError", msg)
		}
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

	se := j.SE
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
		payload, err := pollCvpOperationForWorkflow(ctx, cvpClient, operationParams)
		if err != nil {
			logger.Errorf("Error polling KMS configuration operation: %s", operationUUID)
			return nil, err
		}
		response.Payload = payload
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
	kmsConfig, err = se.UpdateKmsConfigAttributes(ctx, kmsConfig.UUID, kmsConfig.KmsAttributes)
	if err != nil {
		return nil, err
	}
	return kmsConfig, nil
}

// CreateVSAKmsConfigSAKeyActivity creates a service account key for the given KMS configuration.
func (j *KmsConfigActivity) CreateVSAKmsConfigSAKeyActivity(ctx context.Context, kmsConfig *datamodel.KmsConfig) (*datamodel.KmsConfig, error) {
	se := j.SE
	gcpService := &google.GcpServices{
		Ctx:    ctx,
		Logger: log.NewLogger(),
	}
	err := gcpService.InitializeClients()
	if err != nil || !gcpService.IsAdminClientInitialized() {
		gcpService.Logger.Debug("Initialisation of service failed")
		return nil, errors.New("initialisation of service failed")
	}

	vsaEmail := utils.RemovePrefix(kmsConfig.KmsAttributes.SdeServiceAccountEmail, SDEShortTermSAPrefix)
	serviceAccountKey, err := gcpService.CreateServiceAccountKey(ctx, vsaEmail)
	if err != nil {
		return nil, err
	}
	_, err = se.UpdateServiceAccountEmailAndKey(ctx, kmsConfig.ServiceAccount.UUID, vsaEmail, serviceAccountKey.PrivateKeyData)
	if err != nil {
		return nil, err
	}
	return kmsConfig, nil
}

// GrantRoleActivity grants the specified role to the service account for the given KMS configuration.
func (j *KmsConfigActivity) GrantRoleActivity(ctx context.Context, kmsConfig *datamodel.KmsConfig) error {
	return helper.GrantRoleToServiceAccount(ctx, cmekGlobalProjectId, kmsConfig.KmsAttributes.SdeServiceAccountEmail, TokenCreatorRole)
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
