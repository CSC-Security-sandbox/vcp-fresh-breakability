package active_directory_activities

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/internal_active_directories"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	adHelper "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/activity"
)

type ActiveDirectorySyncActivity struct {
	SE        database.Storage
	Scheduler *scheduler.TemporalScheduler
}

// SyncActiveDirectoryParams contains parameters for syncing Active Directory from CVP to VCP
type SyncActiveDirectoryParams struct {
	ActiveDirectoryID string
	AccountName       string
	LocationID        string
	XCorrelationID    string
	PoolUUID          string
	ActiveDirectory   *models.ActiveDirectory
}

// PushActiveDirectoryPasswordResult holds the CVP operation and the generated secret name.
type PushActiveDirectoryPasswordResult struct {
	Operation  *cvpModels.OperationV1beta `json:"operation"`
	SecretName string                     `json:"secretName"`
}

// PushActiveDirectoryPasswordActivity calls CVP API V1betaPushActiveDirectoryPassword
func (a ActiveDirectorySyncActivity) PushActiveDirectoryPasswordActivity(ctx context.Context, params *SyncActiveDirectoryParams) (*PushActiveDirectoryPasswordResult, error) {
	logger := util.GetLogger(ctx)
	logger.Infof("Pushing Active Directory password to CVP for AD ID: %s", params.ActiveDirectoryID)

	if params.ActiveDirectory == nil {
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrADSyncValidationFailure,
				fmt.Errorf("ActiveDirectory cannot be empty")),
		)
	}

	// Generate secret name for the password
	// We'll use a temporary ID since the AD doesn't exist in VCP yet
	// The actual ID will be set when we create the AD record
	secretName := adHelper.GeneratePasswordSecretId(
		env.SecretManagerProjectID,
		params.AccountName,
		params.ActiveDirectory.AdName,
		params.LocationID,
	)

	// Prepare the request body
	passwordBody := &cvpModels.ActiveDirectoryPasswordV1beta{
		ActiveDirectoryID: params.ActiveDirectoryID,
		SecretName:        secretName,
		SdeProjectID:      env.SecretManagerProjectID,
	}

	// Create CVP client with fresh token to avoid expiration during long-running or retried workflows
	jwtToken, err := getSignedJwtToken(params.AccountName)
	if err != nil {
		logger.Errorf("Failed to get signed JWT token: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrGetSignedToken, err),
		)
	}
	cvpClient := CvpClient(logger, jwtToken)

	// Prepare API parameters
	pushPasswordParams := &internal_active_directories.V1betaPushActiveDirectoryPasswordParams{
		Context:        ctx,
		ProjectNumber:  params.AccountName,
		LocationID:     params.LocationID,
		XCorrelationID: &params.XCorrelationID,
		Body:           passwordBody,
	}

	activity.RecordHeartbeat(ctx, fmt.Sprintf("Calling CVP PushActiveDirectoryPassword for AD %s", params.ActiveDirectoryID))
	// Call CVP API
	response, err := cvpClient.InternalActiveDirectories.V1betaPushActiveDirectoryPassword(pushPasswordParams)
	if err != nil {
		var conflictErr *internal_active_directories.V1betaPushActiveDirectoryPasswordConflict
		if errors.As(err, &conflictErr) {
			logger.Warn("CVP returned 409: Active Directory operation already in progress, will retry")
			return nil, vsaerrors.WrapAsTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrADSyncADOperationInProgress,
					fmt.Errorf("SDE returned 409: %v", err)),
			)
		}

		logger.Errorf("Failed to push Active Directory password to CVP: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrADSyncSDECommunicationFailure,
				fmt.Errorf("failed to push Active Directory password to SDE: %v", err)),
		)
	}

	if response == nil || response.Payload == nil {
		logger.Error("Empty response from CVP push Active Directory password")
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrADSyncValidationFailure,
				fmt.Errorf("empty response from CVP push Active Directory password")),
		)
	}

	logger.Infof("Successfully pushed Active Directory password to CVP, operation: %s", response.Payload.Name)
	return &PushActiveDirectoryPasswordResult{
		Operation:  response.Payload,
		SecretName: secretName,
	}, nil
}

// PollPushPasswordOperationActivity polls the CVP operation until it completes
func (a ActiveDirectorySyncActivity) PollPushPasswordOperationActivity(ctx context.Context, params *SyncActiveDirectoryParams, operation *cvpModels.OperationV1beta) error {
	logger := util.GetLogger(ctx)
	logger.Infof("Polling push password operation for AD ID: %s", params.ActiveDirectoryID)

	if operation == nil {
		logger.Warn("PollPushPasswordOperationActivity called with nil operation, skipping poll")
		return nil
	}

	// Check if operation is already done (synchronous completion)
	if operation.Done != nil && *operation.Done {
		logger.Info("Operation already completed synchronously, skipping poll")
		if operation.Error != nil {
			logger.Errorf("Operation completed with error: %v", operation.Error)
			// Mask CVP error to a generic error for internal api
			return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrADSyncSDECommunicationFailure,
					fmt.Errorf("SDE push password operation failed: %s", operation.Error.Message)),
			)
		}
		return nil
	}

	// For async operations, we need the operation name to poll
	if operation.Name == "" {
		logger.Error("Operation name is empty, cannot poll")
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrADSyncValidationFailure,
				fmt.Errorf("operation name is empty")),
		)
	}

	logger.Debugf("Polling async operation: %s", operation.Name)
	jwtToken, err := getSignedJwtToken(params.AccountName)
	if err != nil {
		logger.Errorf("Failed to get signed JWT token for PollPushPasswordOperationActivity: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrGetSignedToken, err),
		)
	}
	cvpClient := CvpClient(logger, jwtToken)

	// Extract the operation UUID
	operationUUID := utils.GetOperationUUID(operation.Name)
	logger.Infof("Extracted operation UUID: %s", operationUUID)

	operationParams := async.NewV1betaDescribeOperationParams()
	operationParams.OperationID = operationUUID
	operationParams.ProjectNumber = params.AccountName
	operationParams.LocationID = params.LocationID
	operationParams.XCorrelationID = &params.XCorrelationID

	logger.Debugf("Polling CVP operation with params: ProjectNumber=%s, LocationID=%s, OperationID=%s",
		params.AccountName, params.LocationID, operationUUID)

	activity.RecordHeartbeat(ctx, fmt.Sprintf("Polling CVP operation %s for AD %s", operationUUID, params.ActiveDirectoryID))
	res, err := pollCvpOperationForWorkflow(ctx, cvpClient, operationParams)
	if err != nil {
		logger.Errorf("Failed to poll CVP operation %s: %v", operationUUID, err)
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrADSyncPollOperationFailure, err),
		)
	}

	logger.Debugf("Poll response for operation %s: Done=%v, Error=%v",
		operationUUID, res.Done, res.Error != nil)

	if res.Done != nil && *res.Done {
		if res.Error != nil {
			logger.Errorf("Operation %s failed with error: %v", operationUUID, res.Error)
			return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrADSyncSDECommunicationFailure,
					fmt.Errorf("SDE push password operation failed: %s", res.Error.Message)),
			)
		}
		logger.Infof("Operation %s completed successfully", operationUUID)
		return nil
	}

	logger.Debugf("Operation %s not yet finished, will retry", operationUUID)
	return vsaerrors.WrapAsTemporalApplicationError(
		vsaerrors.NewVCPError(vsaerrors.ErrSDEJobNotFinished, customerrors.New("job not finished")),
	)
}

// CreateActiveDirectoryInVCPActivity creates the ActiveDirectory entry in VCP database
func (a ActiveDirectorySyncActivity) CreateActiveDirectoryInVCPActivity(ctx context.Context, params *SyncActiveDirectoryParams, secretCredentialPath string) (*datamodel.ActiveDirectory, error) {
	logger := util.GetLogger(ctx)
	logger.Infof("Creating Active Directory entry in VCP for AD ID: %s", params.ActiveDirectoryID)

	if params.ActiveDirectory == nil {
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrADSyncValidationFailure,
				fmt.Errorf("ActiveDirectory model is nil")),
		)
	}

	activity.RecordHeartbeat(ctx, fmt.Sprintf("Fetching account for %s", params.AccountName))
	// Get account ID
	account, err := a.SE.GetAccount(ctx, params.AccountName)
	if err != nil {
		logger.Errorf("Failed to get account: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, fmt.Sprintf("Checking for existing AD %s in account %d", params.ActiveDirectoryID, account.ID))
	// Return existing ActiveDirectory if present to avoid duplicates
	existingAD, err := a.SE.GetActiveDirectoryByUuidAndAccountId(ctx, params.ActiveDirectoryID, account.ID)
	if err != nil && !customerrors.IsNotFoundErr(err) {
		logger.Errorf("Failed to fetch Active Directory by UUID: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	if existingAD != nil {
		logger.Infof("Active Directory already exists in VCP with ID: %d", existingAD.ID)
		return existingAD, nil
	}

	// Convert models.ActiveDirectory to datamodel.ActiveDirectory
	adRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			UUID: params.ActiveDirectoryID,
		},
		AdName:       params.ActiveDirectory.AdName,
		Username:     params.ActiveDirectory.Username,
		Domain:       params.ActiveDirectory.Domain,
		DNS:          params.ActiveDirectory.DNS,
		NetBIOS:      params.ActiveDirectory.NetBIOS,
		State:        datamodel.LifeCycleStateREADY,
		StateDetails: datamodel.LifeCycleStateReadyDetails,
		AccountId:    account.ID,
	}

	// Set ActiveDirectoryAttributes if available
	if params.ActiveDirectory.ActiveDirectoryAttributes != nil {
		adRecord.ActiveDirectoryAttributes = &datamodel.ActiveDirectoryAttributes{
			OrganizationalUnit: params.ActiveDirectory.ActiveDirectoryAttributes.OrganizationalUnit,
			Site:               params.ActiveDirectory.ActiveDirectoryAttributes.Site,
			AdUsers: map[string][]string{
				utils.ActiveDirectoryGroupBuiltInBackupOperators: params.ActiveDirectory.ActiveDirectoryAttributes.BackupOperators,
				utils.ActiveDirectoryGroupBuiltInAdministrators:  params.ActiveDirectory.ActiveDirectoryAttributes.Administrators,
				utils.ActiveDirectorySeSecurityPrivilege:         params.ActiveDirectory.ActiveDirectoryAttributes.SecurityOperators,
			},
			KdcIP:                      params.ActiveDirectory.ActiveDirectoryAttributes.KdcIP,
			KdcHostname:                params.ActiveDirectory.ActiveDirectoryAttributes.KdcHostname,
			AesEncryption:              params.ActiveDirectory.ActiveDirectoryAttributes.AesEncryption,
			EncryptDCConnections:       params.ActiveDirectory.ActiveDirectoryAttributes.EncryptDCConnections,
			LdapSigning:                params.ActiveDirectory.ActiveDirectoryAttributes.LdapSigning,
			AllowLocalNFSUsersWithLdap: params.ActiveDirectory.ActiveDirectoryAttributes.AllowLocalNFSUsersWithLdap,
			Description:                params.ActiveDirectory.ActiveDirectoryAttributes.Description,
			PrimaryAD:                  true,
		}
	}

	activity.RecordHeartbeat(ctx, fmt.Sprintf("Creating AD record for %s in VCP database", params.ActiveDirectoryID))
	// Create the ActiveDirectory record first to get the ID
	createdAD, err := a.SE.CreateActiveDirectory(ctx, adRecord)
	if err != nil {
		logger.Errorf("Failed to create Active Directory in VCP: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	createdAD.CredentialPath = secretCredentialPath

	activity.RecordHeartbeat(ctx, fmt.Sprintf("Updating credential path for AD %d", createdAD.ID))
	_, err = a.SE.UpdateActiveDirectory(ctx, createdAD)
	if err != nil {
		logger.Errorf("Failed to update Active Directory credential path: %v", err)
		// Don't fail the whole operation, just log the error
	}

	logger.Infof("Successfully created Active Directory in VCP with ID: %d", createdAD.ID)
	return createdAD, nil
}

// UpdatePoolActiveDirectoryIDActivity updates the pool table's activedirectoryID with the newly created AD Int ID
func (a ActiveDirectorySyncActivity) UpdatePoolActiveDirectoryIDActivity(ctx context.Context, params *SyncActiveDirectoryParams, adID int64) error {
	logger := util.GetLogger(ctx)
	logger.Infof("Updating pool %s with ActiveDirectory ID: %d", params.PoolUUID, adID)

	// Update pool's activedirectoryID
	updates := map[string]interface{}{
		"active_directory_id": sql.NullInt64{Int64: adID, Valid: true},
	}

	activity.RecordHeartbeat(ctx, fmt.Sprintf("Updating pool %s with AD ID %d", params.PoolUUID, adID))
	err := a.SE.UpdatePoolFields(ctx, params.PoolUUID, updates)
	if err != nil {
		logger.Errorf("Failed to update pool ActiveDirectory ID: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Successfully updated pool %s with ActiveDirectory ID: %d", params.PoolUUID, adID)
	return nil
}
