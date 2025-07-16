package kms_activities

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	cvpClientModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/google"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/retry"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/api/iam/v1"
)

var (
	pollCvpOperationForWorkflow       = _pollCvpOperationForWorkflow
	getGcpService                     = activities.GetGCPService
	gcpServiceCreateServiceAccountKey = _gcpServiceCreateServiceAccountKey
	gcpGrantServiceAccountRole        = _gcpGrantServiceAccountRole
	retryDo                           = retry.RetryDoWithTimeout
	AccessCryptoKey                   = _accessCryptoKey
	getImpersonatedKmsService         = google.GetImpersonatedKmsService
	synchronizeServiceAccountKeys     = _synchronizeServiceAccountKeys
)

const (
	serviceNameCmek                        = "cmek"
	ErrTypeKmsConfigNotFound               = "KmsConfigNotFound"
	ErrTypeKmsConfigNotReachableVsaCluster = "KmsConfigNotReachableVsaCluster"
	ErrTypeDNSExists                       = "DNSEntryExists"
	RetryTimeOutForGetCryptoKey            = 1 * time.Minute
	RetryIntervalForGetCryptoKey           = 5 * time.Second
	GcpKmsConfigHealthError                = "specified key <key_name> in <key_ring> does not exist or service permissions are incorrect"
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
	} else if operationResponse.Payload.Error != nil {
		msg := fmt.Errorf("operation failed: %v", operationResponse.Payload.Error)
		return nil, temporal.NewNonRetryableApplicationError("operation failed", "OperationError", msg)
	}
	return operationResponse.Payload, nil
}

// PollKmsConfigOperationActivity polls the KMS configuration operation until it is done.
func (j *KmsConfigActivity) PollKmsConfigOperationActivity(ctx context.Context, params *common.CreateKmsConfigParams) error {
	jwtToken := utils.GetAuthTokenFromContext(ctx)
	logger := util.GetLogger(ctx)
	cvpClient := createClient(logger, jwtToken)

	// Check if the operation is done
	if !params.OperationDone {
		// Extract the operation UUID
		operationUUID := utils.GetOperationUUID(params.OperationUri)
		operationParams := async.NewV1betaDescribeOperationParams()
		operationParams.OperationID = operationUUID
		operationParams.ProjectNumber = params.ProjectNumber
		operationParams.LocationID = params.LocationID
		operationParams.XCorrelationID = &params.XCorrelationID
		_, err := pollCvpOperationForWorkflow(ctx, cvpClient, operationParams)
		if err != nil {
			return err
		}
	}
	return nil
}

func GetResponseforPollCvpOperation(ctx context.Context, responsePayloadName string, projectNumber string, locationID string) (*cvpClientModels.OperationV1beta, error) {
	jwtToken := utils.GetAuthTokenFromContext(ctx)
	logger := util.GetLogger(ctx)
	cvpClient := createClient(logger, jwtToken)

	operationUUID := utils.GetOperationUUID(responsePayloadName)
	operationParams := async.NewV1betaDescribeOperationParams()
	operationParams.OperationID = operationUUID
	operationParams.ProjectNumber = projectNumber
	operationParams.LocationID = locationID

	payload, err := pollCvpOperationForWorkflow(ctx, cvpClient, operationParams)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// CreateVSAKmsConfigSAKeyActivity creates a service account key for the given KMS configuration.
func (j *KmsConfigActivity) CreateVSAKmsConfigSAKeyActivity(ctx context.Context, kmsConfig *datamodel.KmsConfig) (*datamodel.KmsConfig, error) {
	se := j.SE
	gcpService, err := getGcpService(ctx)
	if err != nil {
		return nil, err
	}
	vsaEmail := utils.RemovePrefix(kmsConfig.KmsAttributes.SdeServiceAccountEmail, SDEShortTermSAPrefix)
	dbAccount, err := se.GetServiceAccountFromEmail(ctx, vsaEmail)
	if err != nil && !errors.IsNotFoundErr(err) {
		return nil, err
	} else if errors.IsNotFoundErr(err) {
		serviceAccountKey, err := gcpServiceCreateServiceAccountKey(gcpService, ctx, vsaEmail)
		if err != nil {
			return nil, err
		}
		sa := &datamodel.ServiceAccount{
			Name:                           kmsConfig.Name,
			Description:                    kmsConfig.Description,
			AccountID:                      kmsConfig.AccountID,
			ServiceName:                    serviceNameCmek,
			ServiceAccountEmail:            vsaEmail,
			ServiceAccountPasswordLocation: serviceAccountKey.PrivateKeyData,
		}
		dbAccount, err = se.CreateKmsServiceAccount(ctx, sa)
		if err != nil {
			return nil, err
		}
	}
	// For accounts where db record already exists, check if password is "" and update it.
	password, err := utils.DecryptPassword(log.Secret(dbAccount.ServiceAccountPasswordLocation))
	if err != nil {
		return nil, err
	}
	if password != nil && *password == "" {
		encryptedKey, err := synchronizeServiceAccountKeys(ctx, gcpService, dbAccount.ServiceAccountEmail)
		if err != nil {
			return nil, err
		}
		dbAccount, err = se.UpdateServiceAccountEmailAndKey(ctx, dbAccount.UUID, dbAccount.ServiceAccountEmail, *encryptedKey)
		if err != nil {
			return nil, err
		}
	}
	_, err = se.UpdateServiceAccountState(ctx, dbAccount.UUID, models.AccountStateEnabled, models.LifeCycleStateAvailableDetails)
	if err != nil {
		return nil, err
	}

	kmsConfig.ServiceAccount = dbAccount
	err = se.UpdateKmsConfig(ctx, kmsConfig.UUID, map[string]interface{}{
		"ServiceAccountID": dbAccount.ID,
	})
	if err != nil {
		return nil, err
	}
	return kmsConfig, nil
}

func _gcpServiceCreateServiceAccountKey(gcpService hyperscaler.GoogleServices, ctx context.Context, email string) (*iam.ServiceAccountKey, error) {
	// Create a service account key for the given service account email
	return gcpService.CreateServiceAccountKey(ctx, email)
}

func _gcpGrantServiceAccountRole(ctx context.Context, gcpService *google.GcpServices, serviceAccountEmail, member, role string) error {
	return gcpService.GrantServiceAccountRole(ctx, serviceAccountEmail, member, role)
}

// GrantRoleActivity grants the specified role to the service account for the given KMS configuration.
func (j *KmsConfigActivity) GrantRoleActivity(ctx context.Context, kmsConfig *datamodel.KmsConfig) error {
	gcpService, err := getGcpService(ctx)
	if err != nil {
		return err
	}
	return gcpGrantServiceAccountRole(ctx, gcpService, kmsConfig.KmsAttributes.SdeServiceAccountEmail, kmsConfig.ServiceAccount.ServiceAccountEmail, TokenCreatorRole)
}

// FailedKmsConfigCreateActivity updates the KMS configuration state to "error" with the provided error message.
func (j *KmsConfigActivity) FailedKmsConfigCreateActivity(ctx context.Context, kmsConfig *datamodel.KmsConfig, errMsg string) error {
	se := j.SE
	kmsConfig.State = models.LifeCycleStateError
	kmsConfig.StateDetails = errMsg
	_, err := se.UpdateKmsConfigState(ctx, kmsConfig.UUID, kmsConfig.State, kmsConfig.StateDetails)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			// If the KMS config is not found, this can mean that creation failed before the KMS config was created
			return nil
		}
		return err
	}
	_, err = se.UpdateServiceAccountState(ctx, kmsConfig.ServiceAccount.UUID, models.LifeCycleStateError, errMsg)
	return err
}

// CreatedKmsConfigActivity updates the KMS configuration state to created
func (j *KmsConfigActivity) CreatedKmsConfigActivity(ctx context.Context, kmsConfig *datamodel.KmsConfig) error {
	se := j.SE
	kmsConfig.State = models.LifeCycleStateREADY
	kmsConfig.StateDetails = models.LifeCycleStateCreatedDetails
	_, err := se.UpdateKmsConfigState(ctx, kmsConfig.UUID, kmsConfig.State, kmsConfig.StateDetails)
	if err != nil {
		return err
	}
	_, err = se.UpdateServiceAccountState(ctx, kmsConfig.ServiceAccount.UUID, models.AccountStateEnabled, models.LifeCycleStateReadyDetails)
	return err
}

func (j *KmsConfigActivity) UpdatePoolWithKmsConfigActivity(ctx context.Context, pool *datamodel.Pool, kmsConfigID string) (*datamodel.Pool, error) {
	se := j.SE
	return se.UpdatePoolWithKmsConfigID(ctx, pool, kmsConfigID)
}

func (j *KmsConfigActivity) AccessCryptoKeyWithImpersonationActivity(ctx context.Context, kmsConfig *datamodel.KmsConfig) error {
	se := j.SE
	err := AccessCryptoKey(ctx, se, kmsConfig)
	if err != nil {
		return err
	}
	return nil
}

func _accessCryptoKey(ctx context.Context, se database.Storage, kmsConfig *datamodel.KmsConfig) error {
	logger := util.GetLogger(ctx)
	var err error
	defer func() {
		if err != nil {
			_, _ = se.UpdateKmsConfigState(ctx, kmsConfig.UUID, models.LifeCycleStateError, err.Error())
		}
	}()

	// Process the service account credentials to get the scope credentials
	scopeCreds, err := utils.ProcessCredentials(ctx, kmsConfig.ServiceAccount.ServiceAccountPasswordLocation)
	if err != nil {
		return err
	}

	kmsService, err := getImpersonatedKmsService(ctx, kmsConfig.KmsAttributes.SdeServiceAccountEmail, scopeCreds)
	if err != nil {
		return fmt.Errorf("failed to create KMS service: %w", err)
	}

	// Define the name of the crypto key you want to get details about
	cryptoKeyPath := utils.ParsedKeyFullPathResource{
		ProjectID: kmsConfig.KeyProjectID,
		Location:  kmsConfig.KeyRingLocation,
		KeyRing:   kmsConfig.KeyRing,
		CryptoKey: kmsConfig.KeyName,
	}.String()

	// Get the crypto key details
	err = retryDo(ctx, RetryTimeOutForGetCryptoKey, RetryIntervalForGetCryptoKey, "AccessCryptoKeyWithImpersonation", func(attempt int) (bool, error) {
		cryptoKey, err := kmsService.Projects.Locations.KeyRings.CryptoKeys.Get(cryptoKeyPath).Context(ctx).Do()
		if err != nil {
			return true, fmt.Errorf("Projects.Locations.KeyRings.CryptoKeys.Get: %v", err)
		}
		logger.Debugf("Successfully got crypto key %s", cryptoKey.Name)
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("failed to access crypto key %s: %w", cryptoKeyPath, err)
	}
	return nil
}

func _synchronizeServiceAccountKeys(ctx context.Context, gcpService hyperscaler.GoogleServices, email string) (*string, error) {
	// If db password is empty, then delete and recreate key
	// Delete existing keys
	err := gcpService.DeleteAllServiceAccountKeys(ctx, email)
	if err != nil {
		return nil, err
	}

	// Create key and update db
	key, err := gcpService.CreateServiceAccountKey(ctx, email)
	if err != nil {
		return nil, err
	}

	return &key.PrivateKeyData, nil
}

// UpdateKmsConfigHealth updates the state and attributes of the KmsConfig based on the results of the Verify operation
func UpdateKmsConfigHealth(ctx context.Context, se database.Storage, kmsConfig *datamodel.KmsConfig, isHealthy bool, healthError string, kmsConfigInUse bool) error {
	var err error
	state := models.LifeCycleStateUnknown
	stateDetails := models.LifeCycleStateUnknownDetails

	switch isHealthy {
	case true:
		state = models.LifeCycleStateREADY
		stateDetails = models.LifeCycleStateReadyDetails
		// Keep the state as in user if the KMS config is in use (in use meaning that there are SVMs using this KMS config)
		if kmsConfigInUse {
			state = models.LifeCycleStateInUse
			stateDetails = models.LifeCycleStateAvailableDetails
		}
	case false:
		// If the KMS config is in error state, do not update the state to ready.
		state = models.LifeCycleStateError
		stateDetails = healthError
		healthErrorMessage := strings.Replace(strings.Replace(GcpKmsConfigHealthError, "<key_name>", kmsConfig.KeyName, 1), "<key_ring>", kmsConfig.KeyRing, 1)
		// Keep the state as created if the health error message indicates that the key does not exist or service permissions are incorrect.
		if strings.Contains(stateDetails, healthErrorMessage) {
			state = models.LifeCycleStateCreated
		}
	}

	// Update the KMS config state and details
	kmsConfig, err = se.UpdateKmsConfigState(ctx, kmsConfig.UUID, state, stateDetails)
	if err != nil {
		return err
	}

	// Update the KMS config Attributes with the health check response
	kmsConfig.KmsAttributes.SdeKmsConfigIsHealthy = isHealthy
	kmsConfig.KmsAttributes.SdeKmsConfigHealthError = healthError
	_, err = se.UpdateKmsConfigAttributes(ctx, kmsConfig.UUID, kmsConfig.KmsAttributes)
	if err != nil {
		return err
	}

	return nil
}

func isKmsConfigInUse(ctx context.Context, se database.Storage, kmsConfig *datamodel.KmsConfig) (bool, error) {
	if kmsConfig.State == models.LifeCycleStateInUse {
		return true, nil
	}
	svms, err := se.GetSvmsByKmsConfigID(ctx, kmsConfig.ID)
	if err != nil && !errors.IsNotFoundErr(err) {
		return false, err
	}
	if len(svms) > 0 {
		return true, nil
	}
	return false, nil
}
