package activities

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"k8s.io/client-go/rest"
)

const (
	adcDeleteEndpointTemplate = "%s/api/endpoints/%s/snapshots/%s"
)

var (
	restHTTPClient       rest.HTTPClient = &http.Client{}
	GetStandardAuthToken                 = _getStandardAuthToken
)

type ADCActivity struct {
	SE database.Storage
}

type BackupDeleteAdcBody struct {
	Container      string `json:"container"`
	Port           int64  `json:"port"`
	AccessKey      string `json:"access_key"`
	SecretPassword string `json:"secret_password"`
	Server         string `json:"server"`
	ProviderType   string `json:"provider_type"`
}

type BackupDeleteAdcReq struct {
	ObjectStore BackupDeleteAdcBody `json:"object_store"`
}

// DeployADCCloudRunService deploys the ADC service to Cloud Run
func (a *ADCActivity) DeployADCCloudRunService(ctx context.Context, params *hyperscalermodels.CloudRunServiceConfig) (*hyperscalermodels.CloudRunOperationResponse, error) {
	cloudService, err := GetCloudService(ctx)
	if err != nil {
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("failed to get GCP service: %w", err))
	}

	response, err := cloudService.CreateCloudRunService(ctx, params)
	if err != nil {
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("failed to deploy Cloud Run service: %w", err))
	}

	return &hyperscalermodels.CloudRunOperationResponse{
		OperationName: response.OperationName,
		Status:        response.Status,
	}, nil
}

// GetADCServiceURL retrieves the URL of the deployed ADC Cloud Run service
func (a *ADCActivity) GetADCServiceURL(ctx context.Context, projectID, region, serviceName string) (string, error) {
	cloudService, err := GetCloudService(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get GCP service: %w", err)
	}

	serviceURL, err := cloudService.GetCloudRunServiceURL(ctx, projectID, region, serviceName)
	if err != nil {
		return "", fmt.Errorf("failed to get Cloud Run service URL: %w", err)
	}

	return serviceURL, nil
}

// CleanupADCCloudRunService deletes the ADC Cloud Run service and returns the operation
func (a *ADCActivity) CleanupADCCloudRunService(ctx context.Context, projectID, region, serviceName string) (*hyperscalermodels.CloudRunOperationResponse, error) {
	cloudService, err := GetCloudService(ctx)
	if err != nil {
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("failed to get GCP service: %w", err))
	}

	response, err := cloudService.DeleteCloudRunService(ctx, projectID, region, serviceName)
	if err != nil {
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("failed to delete Cloud Run service: %w", err))
	}

	return &hyperscalermodels.CloudRunOperationResponse{
		OperationName: response.OperationName,
		Status:        response.Status,
	}, nil
}

func (a *ADCActivity) CreateServiceAccount(ctx context.Context, projectID string, saAccountID string, saDisplayName string) (*hyperscalermodels.ServiceAccount, error) {
	cloudService, err := GetCloudService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	createReq := &hyperscalermodels.CreateServiceAccountRequest{
		AccountId: saAccountID,
		ServiceAccount: &hyperscalermodels.ServiceAccount{
			DisplayName: saDisplayName,
		},
	}

	sa, err := cloudService.CreateServiceAccount(createReq, projectID, saDisplayName)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return sa, nil
}

// AttachRolesToServiceAccount attach roles to service account
func (a *ADCActivity) AttachRolesToServiceAccount(ctx context.Context, projectID string, serviceAccountEmail string, roles []string) error {
	cloudService, err := GetCloudService(ctx)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	err = cloudService.AttachOrUpdateRolesForServiceAccounts(roles, serviceAccountEmail, projectID)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

// IsServiceAccountCreated check if service account is created
func (a *ADCActivity) IsServiceAccountCreated(ctx context.Context, saEmail string) (bool, error) {
	cloudService, err := GetCloudService(ctx)
	if err != nil {
		return false, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	sa, err := cloudService.GetServiceAccountByEmail(saEmail)
	if err != nil {
		return false, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return sa != nil, nil
}

func (a *ADCActivity) DeleteSA(ctx context.Context, projectID string, saAccountID string) error {
	cloudService, err := GetCloudService(ctx)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	logger := cloudService.GetLogger()
	saEmail := utils.ConstructServiceAccountEmail(saAccountID, projectID)
	logger.Infof("Deleting service account %s in project %s", saEmail, projectID)
	err = cloudService.DeleteServiceAccount(projectID, saEmail)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return nil
}

// RemoveRolesFromServiceAccount removes specified roles from a service account
func (a *ADCActivity) RemoveRolesFromServiceAccount(ctx context.Context, projectID string, saAccountID string, roles []string) error {
	cloudService, err := GetCloudService(ctx)
	if err != nil {
		return fmt.Errorf("failed to get GCP service: %w", err)
	}

	saEmail := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", saAccountID, projectID)
	logger := util.GetLogger(ctx)
	logger.Infof("Removing roles from service account %s", saEmail)

	err = cloudService.RemoveRolesFromServiceAccounts(roles, saEmail, projectID)
	if err != nil {
		logger.Errorf("Failed to remove roles from service account %s: %v", saEmail, err)
		return fmt.Errorf("failed to remove roles from service account: %w", err)
	}

	logger.Infof("Successfully removed roles from service account %s", saEmail)
	return nil
}

// InitialDeleteRequestWithCloudRun initiates delete request using Cloud Run ADC service
func (a *ADCActivity) InitialDeleteRequestWithCloudRun(ctx context.Context, adcParams *common.ADCParams, serviceURL string) (*common.ADCResponse, error) {
	logger := util.GetLogger(ctx)
	reqBody, err := ConvertADCParamsToRequest(adcParams)
	if err != nil {
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("failed to convert ADC params to request: %w", err))
	}
	reqBodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("failed to marshal request body: %w", err))
	}

	// Use Cloud Run service URL instead of Kubernetes service URL
	initialURL := fmt.Sprintf(adcDeleteEndpointTemplate,
		serviceURL, adcParams.DestEndpointUUID, adcParams.SnapshotUUID)

	req, err := http.NewRequest("DELETE", initialURL, bytes.NewBuffer(reqBodyBytes))
	if err != nil {
		logger.Errorf("Failed to create request: %v", err)
		return nil, err
	}

	// Generate identity token
	identityToken, err := GetStandardAuthToken(ctx)
	if err != nil {
		logger.Errorf("Failed to get identity token: %v", err)
		return nil, err
	}

	// Add proper headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/hal+json")
	req.Header.Set("Authorization", "Bearer "+identityToken)

	resp, err := restHTTPClient.Do(req)
	if err != nil && (resp == nil || resp.StatusCode != http.StatusTemporaryRedirect) {
		logger.Errorf("ADC delete request error: %v", err)
		return nil, err
	}
	defer func() {
		if resp != nil && resp.Body != nil {
			err = resp.Body.Close()
			if err != nil {
				logger.Error("failed to close response body", "error", err)
			}
		}
	}()

	// Check for error status codes
	if resp.StatusCode >= 400 {
		logger.Errorf("ADC delete request failed with status code: %d", resp.StatusCode)
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("ADC delete request failed with status code: %d", resp.StatusCode))
	}

	redirectURL := resp.Header.Get("Location")

	return &common.ADCResponse{
		StatusCode:  resp.StatusCode,
		RedirectURL: redirectURL,
	}, nil
}

// CheckDeleteStatusWithCloudRun checks delete status using Cloud Run ADC service
func (a *ADCActivity) CheckDeleteStatusWithCloudRun(ctx context.Context, params *common.ADCParams, serviceURL, redirectURL string) (*common.ADCResponse, error) {
	logger := util.GetLogger(ctx)
	if redirectURL == "" {
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("missing redirect URL"))
	}
	reqBody, err := ConvertADCParamsToRequest(params)
	if err != nil {
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("failed to convert ADC params to request: %w", err))
	}
	reqBodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("failed to marshal request body: %w", err))
	}

	// Use Cloud Run service URL for async status check
	asyncURL := fmt.Sprintf("%s%s", serviceURL, redirectURL)
	logger.Debug(fmt.Sprintf("Follow async delete URL: %s", asyncURL))

	// Create the HTTP request
	req, err := http.NewRequest("DELETE", asyncURL, bytes.NewBuffer(reqBodyBytes))
	if err != nil {
		logger.Errorf("Failed to create request: %v", err)
		return nil, err
	}

	// Generate identity token
	identityToken, err := GetStandardAuthToken(ctx)
	if err != nil {
		logger.Errorf("Failed to get identity token: %v", err)
		return nil, err
	}

	// Add proper headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/hal+json")
	req.Header.Set("Authorization", "Bearer "+identityToken)

	resp, err := restHTTPClient.Do(req)
	if err != nil && (resp == nil || resp.StatusCode != http.StatusTemporaryRedirect) {
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("ADC status request error: %w", err))
	}
	defer func() {
		if resp != nil && resp.Body != nil {
			err = resp.Body.Close()
			if err != nil {
				logger.Error("failed to close response body", "error", err)
			}
		}
	}()

	// Check for error status codes
	if resp.StatusCode >= 400 {
		logger.Errorf("ADC delete request failed with status code: %d", resp.StatusCode)
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("ADC status request failed with status code: %d", resp.StatusCode))
	}

	newRedirectURL := resp.Header.Get("Location")

	return &common.ADCResponse{
		StatusCode:  resp.StatusCode,
		RedirectURL: newRedirectURL,
	}, nil
}

// CheckOperationStatus checks the status of a Cloud Run operation
func (a *ADCActivity) CheckOperationStatus(ctx context.Context, operationName string) (bool, error) {
	cloudService, err := GetCloudService(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get GCP service: %w", err)
	}

	isReady, err := cloudService.CheckOperationStatus(ctx, operationName)
	if err != nil {
		return false, fmt.Errorf("failed to check operation status: %w", err)
	}

	return isReady, nil
}

// CreateHmacKeys creates HMAC keys for the service account and returns encoded keys
func (a *ADCActivity) CreateHmacKeys(ctx context.Context, params *common.HmacKeyCreateParams) (*common.HmacKeys, error) {
	cloudService, err := GetCloudService(ctx)
	if err != nil {
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("failed to get GCP service: %w", err))
	}

	accessKey, secretKey, err := cloudService.CreateHmacKey(params.ProjectNumber, params.ServiceAccount)
	if err != nil {
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("failed to create HMAC key: %w", err))
	}
	if accessKey == nil || secretKey == nil {
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("accessKey or secretKey is nil"))
	}

	// Encode the keys to avoid storing sensitive data in Temporal DB
	encodedAccessKey := base64.StdEncoding.EncodeToString([]byte(*accessKey))
	encodedSecretKey := base64.StdEncoding.EncodeToString([]byte(*secretKey))

	return &common.HmacKeys{
		AccessKey: encodedAccessKey,
		SecretKey: encodedSecretKey,
	}, nil
}

// GenerateResourceTimestamp generates a deterministic timestamp for resource naming
// This ensures consistency across workflow replays
func (a *ADCActivity) GenerateResourceTimestamp(ctx context.Context) (string, error) {
	// Use current time - this will be consistent across replays within the same activity execution
	timestamp := time.Now().Format("20060102150405") // YYYYMMDDHHMMSS format
	return timestamp, nil
}

// ConvertADCParamsToRequest converts the ADC parameters to the request friendly structure.
func ConvertADCParamsToRequest(adcParams *common.ADCParams) (BackupDeleteAdcReq, error) {
	accessKeyBytes, err := base64.StdEncoding.DecodeString(adcParams.AccessKey)
	if err != nil {
		return BackupDeleteAdcReq{}, fmt.Errorf("failed to decode access key: %w", err)
	}

	// Decode the secret key
	secretKeyBytes, err := base64.StdEncoding.DecodeString(adcParams.SecretKey)
	if err != nil {
		return BackupDeleteAdcReq{}, fmt.Errorf("failed to decode secret key: %w", err)
	}
	return BackupDeleteAdcReq{
		ObjectStore: BackupDeleteAdcBody{
			Container:      adcParams.BucketName,
			Port:           adcParams.Port,
			AccessKey:      string(accessKeyBytes),
			SecretPassword: string(secretKeyBytes),
			Server:         adcParams.ServerURL,
			ProviderType:   adcParams.ProvideType,
		},
	}, nil
}

// GetStandardAuthToken fetches a standard Google Cloud identity token for authentication
func _getStandardAuthToken(ctx context.Context) (string, error) {
	logger := util.GetLogger(ctx)

	// Get GCP service to use the wrapper method
	cloudService, err := GetCloudService(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get GCP service: %w", err)
	}

	token, err := cloudService.GetIdentityToken()
	if err != nil {
		logger.Errorf("Failed to get identity token: %v", err)
		return "", fmt.Errorf("failed to get identity token: %w", err)
	}

	return token, nil
}
