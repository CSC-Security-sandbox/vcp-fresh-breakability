package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	adHelper "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	customValidators "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/validator"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"strconv"
)

var (
	createActiveDirectory        = _createActiveDirectory
	getActiveDirectory           = _getActiveDirectory
	listActiveDirectories        = _listActiveDirectories
	getMultipleActiveDirectories = _getMultipleActiveDirectories
	storePasswordSecret          = _storePasswordSecret
)

const (
	DefaultOrganizationalUnit = "CN=Computers"

	// ActiveDirectoryGroupBuiltInBackupOperators defines the name of the built-in backup operators group
	ActiveDirectoryGroupBuiltInBackupOperators = `BUILTIN\Backup Operators`

	// ActiveDirectoryGroupBuiltInAdministrators defines the name of the built-in administrators group
	ActiveDirectoryGroupBuiltInAdministrators = `BUILTIN\Administrators`

	// ActiveDirectorySeSecurityPrivilege defines the name of the SE security privilege
	ActiveDirectorySeSecurityPrivilege = `SeSecurityPrivilege`
)

// _createActiveDirectory orchestrates the creation of an Active Directory resource.
// It validates input, creates a job, and starts the corresponding Temporal workflow.
func _createActiveDirectory(
	ctx context.Context,
	se database.Storage,
	temporal client.Client,
	params *common.CreateActiveDirectoryParams,
) (*models.ActiveDirectory, string, error) {
	logger := util.GetLogger(ctx)

	adValidator := customValidators.NewActiveDirectoryValidator(ctx, se)
	err := adValidator.RegisterValidators()
	if err != nil {
		return nil, "", err
	}
	err = adValidator.ValidateParams(params)
	if err != nil {
		errMsg := strings.Join(func() []string {
			var messages []string
			for _, validationErr := range err.(validator.ValidationErrors) {
				messages = append(messages, validationErr.Translate(adValidator.Translator))
			}
			return messages
		}(), "; ")
		return nil, "", customerrors.NewUserInputValidationErr(errMsg)
	}

	account, err := getOrCreateAccount(ctx, se, params.AccountId)
	if err != nil {
		return nil, "", err
	}

	if params.OrganizationalUnit == "" {
		params.OrganizationalUnit = DefaultOrganizationalUnit
	}

	var adRecord *datamodel.ActiveDirectory

	if cvp.CVP_HOST == "" {
		adRecord, err = createVCPActiveDirectoryDBRecord(ctx, se, params, account.ID)
		if err != nil {
			return nil, "", err
		}
	}

	var resourceUUID string
	if adRecord != nil {
		resourceUUID = adRecord.UUID
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeCreateActiveDirectory),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.ResourceId,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: resourceUUID,
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	controlWorkflowID := fmt.Sprintf("Account_%d_ActiveDirectory_%s", account.ID, params.ResourceId)
	err = workflowsExecuteWorkflowSequentially(
		temporal,
		ctx,
		client.StartWorkflowOptions{
			TaskQueue: workflowengine.CustomerTaskQueue,
			ID:        controlWorkflowID,
		},
		workflows.CreateActiveDirectoryWorkflow,
		workflow.ChildWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			WorkflowID:            createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		params,
		adRecord,
	)
	if err != nil {
		logger.Error("Failed to start create active directory workflow: ", "error", err)
		if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
			logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
		}
		return nil, "", err
	}

	if cvp.CVP_HOST == "" {
		return convertDatastoreActiveDirectoryToModel(adRecord), createdJob.UUID, nil
	}

	return convertActiveDirectoryParamsToModel(params), createdJob.UUID, nil
}

// _getActiveDirectory retrieves an Active Directory resource by UUID from the database.
func _getActiveDirectory(
	ctx context.Context,
	se database.Storage,
	activeDirectoryUUID string,
) (*models.ActiveDirectory, error) {
	logger := util.GetLogger(ctx)

	// Get ActiveDirectory by UUID from database
	ad, err := se.GetActiveDirectoryByUUID(ctx, activeDirectoryUUID)
	if err != nil {
		logger.Error("Failed to retrieve ActiveDirectory from database", "uuid", activeDirectoryUUID, "error", err)
		return nil, err
	}

	if ad == nil {
		logger.Warn("ActiveDirectory not found", "uuid", activeDirectoryUUID)
		return nil, customerrors.NewNotFoundErr("ActiveDirectory", &activeDirectoryUUID)
	}

	// Convert datamodel to model
	return convertDatastoreActiveDirectoryToModel(ad), nil
}

// convertDatastoreActiveDirectoryToModel converts datamodel.ActiveDirectory to models.ActiveDirectory
func convertDatastoreActiveDirectoryToModel(ad *datamodel.ActiveDirectory) *models.ActiveDirectory {
	if ad == nil {
		return nil
	}

	model := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			UUID:      ad.UUID,
			CreatedAt: ad.CreatedAt,
			UpdatedAt: ad.UpdatedAt,
		},
		AdName:       ad.AdName,
		Username:     ad.Username,
		Password:     log.PasswordMask,
		Domain:       ad.Domain,
		DNS:          ad.DNS,
		NetBIOS:      ad.NetBIOS,
		State:        ad.State,
		StateDetails: ad.StateDetails,
	}

	// Convert ActiveDirectoryAttributes if available
	if ad.ActiveDirectoryAttributes != nil {
		model.ActiveDirectoryAttributes = &models.ActiveDirectoryAttributes{
			OrganizationalUnit:         ad.ActiveDirectoryAttributes.OrganizationalUnit,
			Site:                       ad.ActiveDirectoryAttributes.Site,
			SecurityOperators:          ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectorySeSecurityPrivilege],
			BackupOperators:            ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectoryGroupBuiltInBackupOperators],
			Administrators:             ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectoryGroupBuiltInAdministrators],
			KdcIP:                      ad.ActiveDirectoryAttributes.KdcIP,
			KdcHostname:                ad.ActiveDirectoryAttributes.KdcHostname,
			AesEncryption:              ad.ActiveDirectoryAttributes.AesEncryption,
			EncryptDCConnections:       ad.ActiveDirectoryAttributes.EncryptDCConnections,
			LdapSigning:                ad.ActiveDirectoryAttributes.LdapSigning,
			AllowLocalNFSUsersWithLdap: ad.ActiveDirectoryAttributes.AllowLocalNFSUsersWithLdap,
			Description:                ad.ActiveDirectoryAttributes.Description,
		}
	}

	return model
}

// CreateActiveDirectory is the public orchestrator method for creating an Active Directory resource.
func (o *Orchestrator) CreateActiveDirectory(
	ctx context.Context,
	params *common.CreateActiveDirectoryParams,
) (*models.ActiveDirectory, string, error) {
	ad, jobUUID, err := createActiveDirectory(ctx, o.storage, o.temporal, params)
	if err != nil {
		return nil, "", err
	}
	return ad, jobUUID, nil
}

// _listActiveDirectories retrieves a list of Active Directory resources for an account.
func _listActiveDirectories(
	ctx context.Context,
	se database.Storage,
	accountName string,
) ([]*models.ActiveDirectory, error) {
	logger := util.GetLogger(ctx)

	// Get account by ID first
	account, err := getOrCreateAccount(ctx, se, accountName)
	if err != nil {
		return nil, err
	}

	// List ActiveDirectories for the account
	ads, err := se.ListActiveDirectories(ctx, account.ID)
	if err != nil {
		logger.Error("Failed to list ActiveDirectories from database", "accountID", account.ID, "error", err)
		return nil, err
	}

	// Convert datamodel to model
	var result []*models.ActiveDirectory
	for _, ad := range ads {
		result = append(result, convertDatastoreActiveDirectoryToModel(ad))
	}

	return result, nil
}

// GetActiveDirectory is the public orchestrator method for retrieving an Active Directory resource by UUID.
func (o *Orchestrator) GetActiveDirectory(
	ctx context.Context,
	activeDirectoryUUID string,
) (*models.ActiveDirectory, error) {
	ad, err := getActiveDirectory(ctx, o.storage, activeDirectoryUUID)
	if err != nil {
		return nil, err
	}
	return ad, nil
}

// _getMultipleActiveDirectories retrieves multiple Active Directory resources by UUIDs.
func _getMultipleActiveDirectories(
	ctx context.Context,
	se database.Storage,
	uuids []string,
) ([]*models.ActiveDirectory, error) {
	logger := util.GetLogger(ctx)

	// Get multiple ActiveDirectories by UUIDs from database
	ads, err := se.GetMultipleActiveDirectoriesByUUIDs(ctx, uuids)
	if err != nil {
		logger.Error("Failed to retrieve multiple ActiveDirectories from database", "uuids", uuids, "error", err)
		return nil, err
	}

	// Convert datamodel to model
	var result []*models.ActiveDirectory
	for _, ad := range ads {
		result = append(result, convertDatastoreActiveDirectoryToModel(ad))
	}

	return result, nil
}

// ListActiveDirectories is the public orchestrator method for listing Active Directory resources for an account.
func (o *Orchestrator) ListActiveDirectories(
	ctx context.Context,
	accountName string,
) ([]*models.ActiveDirectory, error) {
	ads, err := listActiveDirectories(ctx, o.storage, accountName)
	if err != nil {
		return nil, err
	}
	return ads, nil
}

// GetMultipleActiveDirectories is the public orchestrator method for retrieving multiple Active Directory resources by UUIDs.
func (o *Orchestrator) GetMultipleActiveDirectories(
	ctx context.Context,
	uuids []string,
) ([]*models.ActiveDirectory, error) {
	ads, err := getMultipleActiveDirectories(ctx, o.storage, uuids)
	if err != nil {
		return nil, err
	}
	return ads, nil
}

// GetADConfig retrieves an Active Directory resource by account ID and resource name.
func (o *Orchestrator) GetADConfig(ctx context.Context, params *common.GetADParams) (*models.ActiveDirectory, error) {
	account, err := getAccountWithName(ctx, o.storage, params.AccountName)
	if err != nil {
		return nil, err
	}
	adConfig, err2 := o.storage.GetActiveDirectoryByUuidAndAccountId(ctx, params.UUID, account.ID)
	if err2 != nil {
		return nil, err2
	}

	return convertActiveDirectoryToModel(adConfig), nil
}

func (o *Orchestrator) GetSDEActiveDirectory(ctx context.Context, getADParams *common.GetADParams) (*cvpmodels.ActiveDirectoryV1beta, error) {
	// Phase 2 implementation
	return nil, nil
}

func convertActiveDirectoryToModel(ad *datamodel.ActiveDirectory) *models.ActiveDirectory {
	if ad == nil {
		return nil
	}

	model := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			ID:        ad.ID,
			UUID:      ad.UUID,
			CreatedAt: ad.CreatedAt,
			UpdatedAt: ad.UpdatedAt,
		},
		AdName:       ad.AdName,
		Username:     ad.Username,
		Password:     ad.CredentialPath,
		Domain:       ad.Domain,
		DNS:          ad.DNS,
		NetBIOS:      ad.NetBIOS,
		State:        ad.State,
		StateDetails: ad.StateDetails,
	}

	// Convert ActiveDirectoryAttributes if available
	if ad.ActiveDirectoryAttributes != nil {
		model.ActiveDirectoryAttributes = &models.ActiveDirectoryAttributes{
			OrganizationalUnit:         ad.ActiveDirectoryAttributes.OrganizationalUnit,
			Site:                       ad.ActiveDirectoryAttributes.Site,
			SecurityOperators:          ad.ActiveDirectoryAttributes.AdUsers["SeSecurityPrivilege"],
			BackupOperators:            ad.ActiveDirectoryAttributes.AdUsers[`BUILTIN\Backup Operators`],
			Administrators:             ad.ActiveDirectoryAttributes.AdUsers[`BUILTIN\Administrators`],
			KdcIP:                      ad.ActiveDirectoryAttributes.KdcIP,
			KdcHostname:                ad.ActiveDirectoryAttributes.KdcHostname,
			AesEncryption:              ad.ActiveDirectoryAttributes.AesEncryption,
			EncryptDCConnections:       ad.ActiveDirectoryAttributes.EncryptDCConnections,
			LdapSigning:                ad.ActiveDirectoryAttributes.LdapSigning,
			AllowLocalNFSUsersWithLdap: ad.ActiveDirectoryAttributes.AllowLocalNFSUsersWithLdap,
			Description:                ad.ActiveDirectoryAttributes.Description,
		}
	}

	return model
}

// createAdRecordForNonSDE creates a database record for Active Directory when SDE is disabled.
// This ensures the AD record exists in the DB for job tracking purposes.
func createAdRecordForNonSDE(
	ctx context.Context,
	se database.Storage,
	params *common.CreateActiveDirectoryParams,
	accountID int64,
	SecretID string,
) (*datamodel.ActiveDirectory, error) {
	adRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			UUID: utils.RandomUUID(),
		},
		AdName:         params.ResourceId,
		Username:       params.Username,
		Domain:         params.Domain,
		DNS:            params.DNS,
		NetBIOS:        params.NetBIOS,
		State:          models.LifeCycleStateCreating,
		StateDetails:   models.LifeCycleStateCreatingDetails,
		AccountId:      accountID,
		CredentialPath: SecretID,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			OrganizationalUnit: params.OrganizationalUnit,
			Site:               params.Site,
			AdUsers: map[string][]string{
				ActiveDirectoryGroupBuiltInBackupOperators: params.BackupOperators,
				ActiveDirectoryGroupBuiltInAdministrators:  params.Administrators,
				ActiveDirectorySeSecurityPrivilege:         params.SecurityOperators,
			},
			KdcIP:                      params.KdcIP,
			KdcHostname:                params.KdcHostname,
			AesEncryption:              params.AesEncryption,
			EncryptDCConnections:       params.EncryptDCConnections,
			LdapSigning:                params.LdapSigning,
			AllowLocalNFSUsersWithLdap: params.AllowLocalNFSUsersWithLdap,
			Description:                params.Description,
			PrimaryAD:                  true,
		},
	}

	return se.CreateActiveDirectory(ctx, adRecord)
}

func _storePasswordSecret(ctx context.Context, password string, secretID string) error {
	logger := util.GetLogger(ctx)

	gcpService, err := hyperscaler.GetGCPService(ctx)
	if err != nil {
		logger.Error("Failed to get GCP service", "error", err)
		return err
	}

	existingSecret, err := gcpService.GetSecretWithLatestVersion(env.SecretManagerProjectID, secretID)
	if err != nil {
		logger.Error("Failed to check existing secret", "secretID", secretID, "error", err)
		return err
	}

	// Only create secret if it doesn't exist
	if existingSecret == nil {
		projectID := env.SecretManagerProjectID
		secret, err := gcpService.CreateSecret(projectID, env.Region, secretID, password)
		if err != nil {
			logger.Error("Failed to create secret", "secretID", secretID, "error", err)
			return err
		}
		logger.Info("Successfully created new secret", "secretID", secretID)
		common.AddToUserAuthCache(secretID, secret.SecretVersion.Value)
	} else {
		logger.Info("Secret already exists, skipping creation", "secretID", secretID)
		// Add existing secret to cache for consistency
		common.AddToUserAuthCache(secretID, existingSecret.SecretVersion.Value)
	}

	return nil
}

// createVCPActiveDirectoryDBRecord handles Active Directory creation for non-SDE environments.
// It manages password secret storage in GCP Secret Manager and creates the AD database record.
func createVCPActiveDirectoryDBRecord(
	ctx context.Context,
	se database.Storage,
	params *common.CreateActiveDirectoryParams,
	accountID int64,
) (*datamodel.ActiveDirectory, error) {
	logger := util.GetLogger(ctx)

	// Generate secret ID before creating AD record since we need accountID
	// Note: adRecord doesn't exist yet, so we use params values
	secretId := adHelper.GeneratePasswordSecretId(
		env.SecretManagerProjectID,
		strconv.FormatInt(accountID, 10),
		params.ResourceId,
		env.Region,
	)

	err := storePasswordSecret(ctx, params.Password, secretId)
	if err != nil {
		return nil, err
	}

	adRecord, err := createAdRecordForNonSDE(ctx, se, params, accountID, secretId)
	if err != nil {
		logger.Error("Failed to create Active Directory record in database", "error", err)
		return nil, err
	}

	return adRecord, nil
}

func convertActiveDirectoryParamsToModel(params *common.CreateActiveDirectoryParams) *models.ActiveDirectory {
	ad := &models.ActiveDirectory{
		AdName:       params.ResourceId,
		Username:     params.Username,
		Domain:       params.Domain,
		DNS:          params.DNS,
		NetBIOS:      params.NetBIOS,
		State:        models.LifeCycleStateCreating,
		StateDetails: models.LifeCycleStateCreatingDetails,
		ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{
			OrganizationalUnit:         params.OrganizationalUnit,
			Site:                       params.Site,
			SecurityOperators:          params.SecurityOperators,
			BackupOperators:            params.BackupOperators,
			Administrators:             params.Administrators,
			KdcIP:                      params.KdcIP,
			KdcHostname:                params.KdcHostname,
			AesEncryption:              params.AesEncryption,
			EncryptDCConnections:       params.EncryptDCConnections,
			LdapSigning:                params.LdapSigning,
			AllowLocalNFSUsersWithLdap: params.AllowLocalNFSUsersWithLdap,
			Description:                params.Description,
		},
	}
	return ad
}
