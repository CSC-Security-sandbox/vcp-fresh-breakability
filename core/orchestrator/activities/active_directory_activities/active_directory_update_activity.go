package active_directory_activities

import (
	"context"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/google"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"strconv"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/active_directories"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/resource_events"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
)

type ActiveDirectoryUpdateActivity struct {
	SE        database.Storage
	Scheduler *scheduler.TemporalScheduler
}

const (
	ErrTypeResourceNotFound = "NotFoundErr"
	ErrInvalidRequest       = "InvalidRequestErr"
)

var (
	updatePasswordSecret = _updatePasswordSecret
)

func (a ActiveDirectoryUpdateActivity) MarkVcpAdToUpdatingActivity(ctx context.Context, params *common.UpdateActiveDirectoryParams, oldAd *models.ActiveDirectory) error {
	Logger := util.GetLogger(ctx)
	Logger.Debug("Updating VCP ActiveDirectory DB Record to UPDATING state")

	account, err := a.SE.GetAccount(ctx, params.AccountId)
	if err != nil {
		return errors.New("Could not fetch related Account for Active Directory update")
	}

	olAdVcpDbRecord, _ := a.SE.GetActiveDirectoryByNameAndAccountID(ctx, oldAd.AdName, account.ID)
	if olAdVcpDbRecord == nil {
		Logger.Info("Active Directory from SDE not found in VCP, skipping VCP update.")
		return nil
	}

	olAdVcpDbRecord.State = models.LifeCycleStateUpdating
	olAdVcpDbRecord.StateDetails = models.LifeCycleStateUpdatingDetails

	_, err = a.SE.UpdateActiveDirectory(ctx, olAdVcpDbRecord)
	if err != nil {
		return err
	}
	Logger.Info("Marked VCP ActiveDirectory as updating")

	return nil
}

func (a ActiveDirectoryUpdateActivity) MarkVcpAdToErrorActivity(ctx context.Context, params *common.UpdateActiveDirectoryParams, oldAd *models.ActiveDirectory) error {
	Logger := util.GetLogger(ctx)
	Logger.Debug("Updating VCP ActiveDirectory DB Record to ERROR state")

	account, err := a.SE.GetAccount(ctx, params.AccountId)
	if err != nil {
		return errors.New("Could not fetch related Account for Active Directory update")
	}

	olAdVcpDbRecord, _ := a.SE.GetActiveDirectoryByNameAndAccountID(ctx, oldAd.AdName, account.ID)
	if olAdVcpDbRecord == nil {
		Logger.Info("Active Directory from SDE not found in VCP, skipping VCP update.")
		return nil
	}

	olAdVcpDbRecord.State = models.LifeCycleStateError
	olAdVcpDbRecord.StateDetails = models.LifeCycleStateUpdateErrorDetails

	_, err = a.SE.UpdateActiveDirectory(ctx, olAdVcpDbRecord)
	if err != nil {
		return err
	}
	Logger.Info("Marked VCP ActiveDirectory as Error")

	return nil
}

func (a ActiveDirectoryUpdateActivity) UpdateVcpActiveDirectory(ctx context.Context, params *common.UpdateActiveDirectoryParams, oldAd *models.ActiveDirectory, changeId string) error {
	Logger := util.GetLogger(ctx)
	Logger.Debug("Updating VCP ActiveDirectory")

	account, err := a.SE.GetAccount(ctx, params.AccountId)
	if err != nil {
		return errors.New("Could not fetch related Account for Active Directory update")
	}

	olAdVcpDbRecord, err := a.SE.GetActiveDirectoryByNameAndAccountID(ctx, oldAd.AdName, account.ID)
	if err != nil {
		Logger.Errorf("Failed to fetch Active Directory by name and account ID: %v", err)
		return errors.New("Could not fetch Active Directory from VCP database")
	}
	if olAdVcpDbRecord == nil {
		Logger.Info("Active Directory from SDE not found in VCP, skipping VCP update.")
		return nil
	}

	updatedAd := convertUpdateParamsToModel(params, oldAd)

	if params.Password != nil {
		err := updatePasswordSecret(ctx, *params.Password, olAdVcpDbRecord.CredentialPath)
		if err != nil {
			return err
		}
		updatedAd.CredentialPath = olAdVcpDbRecord.CredentialPath
	}

	updatedAd.ChangeId = changeId
	updatedAd.ID = olAdVcpDbRecord.ID
	_, err = a.SE.UpdateActiveDirectory(ctx, updatedAd)
	if err != nil {
		return err
	}
	Logger.Debug("Updated VCP ActiveDirectory")

	return nil
}

func (a ActiveDirectoryUpdateActivity) PollSdeUpdateActivity(ctx context.Context, params *common.UpdateActiveDirectoryParams, result *cvpModels.OperationV1beta) error {
	logger := util.GetLogger(ctx)
	logger.Infof("Starting PollSdeUpdateActivity for account %s, location %s", params.AccountId, params.LocationId)

	if result == nil {
		logger.Warn("PollSdeUpdateActivity called with nil result, skipping poll")
		return nil
	}

	// Check if operation is already done (synchronous completion)
	if result.Done != nil && *result.Done {
		logger.Info("Operation already completed synchronously, skipping poll")
		return nil
	}

	// For async operations, we need the operation name to poll
	if result.Name == "" {
		logger.Error("Operation name is empty, cannot poll")
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrInvalidOperationName, errors.New("operation name is nil")))
	}

	logger.Debugf("Polling async operation: %s", result.Name)
	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := CvpClient(logger, jwtToken)

	// Extract the operation UUID
	operationUUID := utils.GetOperationUUID(result.Name)
	logger.Infof("Extracted operation UUID: %s", operationUUID)

	operationParams := async.NewV1betaDescribeOperationParams()
	operationParams.OperationID = operationUUID
	operationParams.ProjectNumber = params.AccountId
	operationParams.LocationID = params.LocationId

	logger.Debugf("Polling CVP operation with params: ProjectNumber=%s, LocationID=%s, OperationID=%s",
		params.AccountId, params.LocationId, operationUUID)

	res, err := pollCvpOperationForWorkflow(ctx, cvpClient, operationParams)
	if err != nil {
		logger.Errorf("Failed to poll CVP operation %s: %v", operationUUID, err)
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrCVPClientHandleResourceEventError, err),
		)
	}

	logger.Debugf("Poll response for operation %s: Done=%v, Error=%v", operationUUID, res.Done, res.Error)

	if res.Done != nil && *res.Done {
		if res.Error != nil {
			logger.Errorf("Operation %s completed with error: Code=%d, Message=%s",
				operationUUID, int(res.Error.Code), res.Error.Message)

			switch int(res.Error.Code) {
			case common.HTTPStatusBadRequest:
				return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorBadRequest,
						fmt.Errorf("Bad request while polling operation %s: %s", operationUUID, res.Error.Message)),
				)

			case common.HTTPStatusUnauthorized:
				return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorUnauthorized,
						fmt.Errorf("Unauthorized while polling operation %s: %s", operationUUID, res.Error.Message)),
				)

			case common.HTTPStatusForbidden:
				return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorForbidden,
						fmt.Errorf("Forbidden while polling operation %s: %s", operationUUID, res.Error.Message)),
				)

			case common.HTTPStatusNotFound:
				return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorNotFound,
						fmt.Errorf("Operation %s not found while polling: %s", operationUUID, res.Error.Message)),
				)

			case common.HTTPStatusInternalServerError:
				return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorInternalServerError,
						fmt.Errorf("Internal server error while polling operation %s: %s", operationUUID, res.Error.Message)),
				)

			case common.HTTPStatusTooManyRequests:
				return vsaerrors.WrapAsTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorTooManyRequests,
						fmt.Errorf("Too many requests while polling operation %s: %s", operationUUID, res.Error.Message)),
				)

			default:
				logger.Warnf("Unknown error code while polling operation %s: %d - %s", operationUUID, int(res.Error.Code), res.Error.Message)
				return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrCVPClientStartProjectEventError,
						fmt.Errorf("SDE polling failed for operation %s: %s", operationUUID, res.Error.Message)),
				)
			}
		}
		logger.Infof("Operation %s completed successfully", operationUUID)
		return nil
	}

	logger.Debugf("Operation %s not yet finished, will retry", operationUUID)
	return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrSDEJobNotFinished, errors.New("job not finished")))
}

func (a ActiveDirectoryUpdateActivity) PushUpdatesDownstreamActivity(ctx context.Context, oldAd *models.ActiveDirectory, changeId string) error {
	Logger := util.GetLogger(ctx)

	pools, err := a.SE.GetPoolsByActiveDirectoryId(ctx, strconv.FormatInt(oldAd.ID, 10))
	if err != nil {
		Logger.Warnf("Failed to fetch pools for Active Directory %s: %v", oldAd.AdName, err)
		return err
	}

	// Update pools with the new AD change ID
	for _, pool := range pools {
		pool.ActiveDirectoryChangeId = changeId
		if err := a.SE.UpdatePoolFields(ctx, pool.UUID, map[string]interface{}{"active_directory_change_id": changeId}); err != nil {
			Logger.Warnf("Failed to update pool %s with new AD change ID: %v", pool.UUID, err)
			return err
		}
	}

	// TODO: Code to push updates to downstream systems like SVMs

	return nil
}

func (a ActiveDirectoryUpdateActivity) UpdateSdeActiveDirectory(ctx context.Context, params *common.UpdateActiveDirectoryParams) (*cvpModels.OperationV1beta, error) {
	logger := util.GetLogger(ctx)
	helper.AddLabelerAttributes(ctx, params.AccountId, params.LocationId, nil)

	body := &cvpModels.ActiveDirectoryUpdateV1beta{}

	if params.Username != nil {
		body.Username = *params.Username
	}
	if params.Description != nil {
		body.Description = params.Description
	}
	if params.Password != nil {
		body.Password = *params.Password
	}
	if params.Domain != nil {
		body.Domain = *params.Domain
	}
	if params.DNS != nil {
		body.DNS = *params.DNS
	}
	if params.NetBIOS != nil {
		body.NetBIOS = *params.NetBIOS
	}
	if params.OrganizationalUnit != nil {
		body.OrganizationalUnit = params.OrganizationalUnit
	}
	if params.Site != nil {
		body.Site = params.Site
	}
	if params.KdcIP != nil {
		body.KdcIP = *params.KdcIP
	}
	if params.KdcHostname != nil {
		body.KdcHostname = *params.KdcHostname
	}
	if params.LdapSigning != nil {
		body.LdapSigning = params.LdapSigning
	}
	if params.AllowLocalNFSUsersWithLdap != nil {
		body.AllowLocalNFSUsersWithLdap = params.AllowLocalNFSUsersWithLdap
	}
	if params.EncryptDCConnections != nil {
		body.EncryptDCConnections = params.EncryptDCConnections
	}
	if params.AesEncryption != nil {
		body.AesEncryption = params.AesEncryption
	}
	if len(params.SecurityOperators) > 0 {
		body.SecurityOperators = params.SecurityOperators
	}
	if len(params.BackupOperators) > 0 {
		body.BackupOperators = params.BackupOperators
	}
	if len(params.Administrators) > 0 {
		body.Administrators = params.Administrators
	}

	updateParams := &active_directories.V1betaUpdateActiveDirectoryParams{
		ActiveDirectoryID: params.ActiveDirectoryId,
		LocationID:        params.LocationId,
		ProjectNumber:     params.AccountId,
		XCorrelationID:    &params.XCorrelationId,
		Body:              body,
	}

	jwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := CvpClient(logger, jwtToken)
	sdeResponse, err := cvpClient.ActiveDirectories.V1betaUpdateActiveDirectory(updateParams)
	if err != nil {
		logger.Errorf("Failed to update Active Directory in SDE: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.New(err.Error()))
	}
	return sdeResponse.Payload, nil
}

// convertUpdateParamsToModel converts UpdateActiveDirectoryParams to a model, merging with oldAd
func convertUpdateParamsToModel(params *common.UpdateActiveDirectoryParams, oldAd *models.ActiveDirectory) *datamodel.ActiveDirectory {
	ad := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			UUID:      oldAd.UUID,
			CreatedAt: oldAd.CreatedAt,
		},
		AdName:       oldAd.AdName,
		Username:     oldAd.Username,
		Domain:       oldAd.Domain,
		DNS:          oldAd.DNS,
		NetBIOS:      oldAd.NetBIOS,
		State:        models.LifeCycleStateREADY,
		StateDetails: models.LifeCycleStateReadyDetails,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			OrganizationalUnit: oldAd.ActiveDirectoryAttributes.OrganizationalUnit,
			Site:               oldAd.ActiveDirectoryAttributes.Site,
			AdUsers: map[string][]string{
				utils.ActiveDirectorySeSecurityPrivilege:         oldAd.ActiveDirectoryAttributes.SecurityOperators,
				utils.ActiveDirectoryGroupBuiltInBackupOperators: oldAd.ActiveDirectoryAttributes.BackupOperators,
				utils.ActiveDirectoryGroupBuiltInAdministrators:  oldAd.ActiveDirectoryAttributes.Administrators,
			},
			KdcIP:                      oldAd.ActiveDirectoryAttributes.KdcIP,
			KdcHostname:                oldAd.ActiveDirectoryAttributes.KdcHostname,
			AesEncryption:              oldAd.ActiveDirectoryAttributes.AesEncryption,
			EncryptDCConnections:       oldAd.ActiveDirectoryAttributes.EncryptDCConnections,
			LdapSigning:                oldAd.ActiveDirectoryAttributes.LdapSigning,
			AllowLocalNFSUsersWithLdap: oldAd.ActiveDirectoryAttributes.AllowLocalNFSUsersWithLdap,
			Description:                oldAd.ActiveDirectoryAttributes.Description,
		},
	}

	// Update with non-empty params from request
	if params.Username != nil && *params.Username != "" {
		ad.Username = *params.Username
	}
	if params.Domain != nil && *params.Domain != "" {
		ad.Domain = *params.Domain
	}
	if params.DNS != nil && *params.DNS != "" {
		ad.DNS = *params.DNS
	}
	if params.NetBIOS != nil && *params.NetBIOS != "" {
		ad.NetBIOS = *params.NetBIOS
	}
	if params.OrganizationalUnit != nil && *params.OrganizationalUnit != "" {
		ad.ActiveDirectoryAttributes.OrganizationalUnit = *params.OrganizationalUnit
	}
	if params.Site != nil && *params.Site != "" {
		ad.ActiveDirectoryAttributes.Site = *params.Site
	}
	if len(params.SecurityOperators) > 0 {
		ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectorySeSecurityPrivilege] = params.SecurityOperators
	}
	if len(params.BackupOperators) > 0 {
		ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectoryGroupBuiltInBackupOperators] = params.BackupOperators
	}
	if len(params.Administrators) > 0 {
		ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectoryGroupBuiltInAdministrators] = params.Administrators
	}
	if params.KdcIP != nil && *params.KdcIP != "" {
		ad.ActiveDirectoryAttributes.KdcIP = *params.KdcIP
	}
	if params.KdcHostname != nil && *params.KdcHostname != "" {
		ad.ActiveDirectoryAttributes.KdcHostname = *params.KdcHostname
	}
	if params.AesEncryption != nil {
		ad.ActiveDirectoryAttributes.AesEncryption = *params.AesEncryption
	}
	if params.EncryptDCConnections != nil {
		ad.ActiveDirectoryAttributes.EncryptDCConnections = *params.EncryptDCConnections
	}
	if params.LdapSigning != nil {
		ad.ActiveDirectoryAttributes.LdapSigning = *params.LdapSigning
	}
	if params.AllowLocalNFSUsersWithLdap != nil {
		ad.ActiveDirectoryAttributes.AllowLocalNFSUsersWithLdap = *params.AllowLocalNFSUsersWithLdap
	}
	if params.Description != nil && *params.Description != "" {
		ad.ActiveDirectoryAttributes.Description = *params.Description
	}

	return ad
}

func pollCvpOperationForWorkflow(ctx context.Context, cvpClient cvpapi.Cvp, operationParams *async.V1betaDescribeOperationParams) (*cvpModels.OperationV1beta, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("Polling for operation %s", operationParams.OperationID)
	operationResponse, err := cvpClient.Async.V1betaDescribeOperation(operationParams)
	if err != nil {
		if _, isNotFound := err.(*resource_events.V1betaResourceStateUpdateNotFound); isNotFound {
			logger.Infof("SDE HandleResourceEvent returned 404 (resource not found), treating as non-retryable: %v", err)
			return nil, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeResourceNotFound, err)
		} else if _, isBadRequest := err.(*resource_events.V1betaResourceStateUpdateBadRequest); isBadRequest {
			logger.Infof("SDE HandleResourceEvent returned 400 (bad request), treating as non-retryable: %v", err)
			return nil, temporal.NewNonRetryableApplicationError(err.Error(), ErrInvalidRequest, err)
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDescribingSDEJob, err)
	}

	return operationResponse.Payload, nil
}

func _updatePasswordSecret(ctx context.Context, password string, secretID string) error {
	logger := util.GetLogger(ctx)

	gcpService, err := hyperscaler.GetGCPService(ctx)
	if err != nil {
		logger.Error("Failed to get GCP service", "error", err)
		return vsaerrors.New(err.Error())
	}

	_, err = google.AddSecretVersion(gcpService, env.SecretManagerProjectID, secretID, password)
	if err != nil {
		logger.Error("Failed to add secret version", "error", err, "secretID", secretID)
		return vsaerrors.New(err.Error())
	}

	logger.Info("Successfully updated secret with new version", "secretID", secretID)

	return nil
}
