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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	customValidators "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/validator"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

var (
	createActiveDirectory        = _createActiveDirectory
	updateActiveDirectory        = _updateActiveDirectory
	getActiveDirectory           = _getActiveDirectory
	listActiveDirectories        = _listActiveDirectories
	getMultipleActiveDirectories = _getMultipleActiveDirectories
	deleteActiveDirectory        = _deleteActiveDirectory
	checkIfDomainUpdateAllowed   = _checkIfDomainUpdateAllowed
	validateMultiADConstraints   = _validateMultiADConstraints
)

const (
	DefaultOrganizationalUnit        = "CN=Computers"
	MultiADNotAllowedError           = "Multiple Active Directories are not allowed."
	MaxADLimitReachedForAccountError = "Maximum Active Directory limit reached for this account."
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

	err = validateMultiADConstraints(ctx, se, account.ID)
	if err != nil {
		return nil, "", err
	}

	var adRecord *datamodel.ActiveDirectory

	if cvp.CVP_HOST == "" || utils.CreateCommonResourcesInVCP {
		adRecord, err = createAdRecordForNonSDE(ctx, se, params, account.ID)
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

	if cvp.CVP_HOST == "" || utils.CreateCommonResourcesInVCP {
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
	ad, err := se.GetActiveDirectoryByUuidAndAccountId(ctx, activeDirectoryUUID, 0)
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
			ID:        ad.ID,
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

	return convertDatastoreActiveDirectoryToModel(adConfig), nil
}

func (o *Orchestrator) GetSDEActiveDirectory(ctx context.Context, getADParams *common.GetADParams) (*cvpmodels.ActiveDirectoryV1beta, error) {
	// Phase 2 implementation
	return nil, nil
}

// createAdRecordForNonSDE creates a database record for Active Directory when SDE is disabled.
// This ensures the AD record exists in the DB for job tracking purposes.
func createAdRecordForNonSDE(
	ctx context.Context,
	se database.Storage,
	params *common.CreateActiveDirectoryParams,
	accountID int64,
) (*datamodel.ActiveDirectory, error) {
	adRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			UUID: utils.RandomUUID(),
		},
		AdName:       params.ResourceId,
		Username:     params.Username,
		Domain:       params.Domain,
		DNS:          params.DNS,
		NetBIOS:      params.NetBIOS,
		State:        models.LifeCycleStateCreating,
		StateDetails: models.LifeCycleStateCreatingDetails,
		AccountId:    accountID,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			OrganizationalUnit: params.OrganizationalUnit,
			Site:               params.Site,
			AdUsers: map[string][]string{
				utils.ActiveDirectoryGroupBuiltInBackupOperators: params.BackupOperators,
				utils.ActiveDirectoryGroupBuiltInAdministrators:  params.Administrators,
				utils.ActiveDirectorySeSecurityPrivilege:         params.SecurityOperators,
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

// UpdateActiveDirectory is the public orchestrator method for updating an Active Directory resource.
func (o *Orchestrator) UpdateActiveDirectory(
	ctx context.Context,
	params *common.UpdateActiveDirectoryParams,
) (*models.ActiveDirectory, string, error) {
	ad, jobUUID, err := updateActiveDirectory(ctx, o.storage, o.temporal, params)
	if err != nil {
		return nil, "", err
	}
	return ad, jobUUID, nil
}

// _updateActiveDirectory orchestrates the creation of an Active Directory resource.
// It validates input, creates a job, and starts the corresponding Temporal workflow.
func _updateActiveDirectory(
	ctx context.Context,
	se database.Storage,
	temporal client.Client,
	params *common.UpdateActiveDirectoryParams,
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

	ad, err := getActiveDirectory(ctx, se, params.ActiveDirectoryId)
	if err != nil {
		return nil, "", err
	}

	if params.Domain != nil && *params.Domain != ad.Domain {
		// Check if domain update is allowed
		err = checkIfDomainUpdateAllowed(ctx, se, ad)
		if err != nil {
			return nil, "", err
		}
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeUpdateActiveDirectory),
		State:         string(models.JobsStateNEW),
		ResourceName:  ad.AdName,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: params.ActiveDirectoryId,
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	controlWorkflowID := fmt.Sprintf("Account_%d_ActiveDirectory_%s", account.ID, ad.AdName)
	err = workflowsExecuteWorkflowSequentially(
		temporal,
		ctx,
		client.StartWorkflowOptions{
			TaskQueue: workflowengine.CustomerTaskQueue,
			ID:        controlWorkflowID,
		},
		workflows.UpdateActiveDirectoryWorkflow,
		workflow.ChildWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			WorkflowID:            createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		params,
		ad,
	)
	if err != nil {
		logger.Error("Failed to start update active directory workflow: ", "error", err)
		if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
			logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
		}
		return nil, "", err
	}

	if cvp.CVP_HOST == "" || utils.CreateCommonResourcesInVCP {
		adRecord, _ := se.GetActiveDirectoryByNameAndAccountID(ctx, ad.AdName, account.ID)
		if adRecord == nil {
			return nil, "", customerrors.NewNotFoundErr("ActiveDirectory", &params.ActiveDirectoryId)
		}
		adRecord.State = models.LifeCycleStateUpdating
		adRecord.StateDetails = models.LifeCycleStateUpdatingDetails
		adRecord, _ = se.UpdateActiveDirectory(ctx, adRecord)
	}

	ad.State = models.LifeCycleStateUpdating
	ad.StateDetails = models.LifeCycleStateUpdatingDetails
	return ad, createdJob.UUID, nil
}

func _checkIfDomainUpdateAllowed(ctx context.Context, se database.Storage, oldAd *models.ActiveDirectory) error {
	logger := util.GetLogger(ctx)

	svms, err := se.GetSVMsUsingActiveDirectory(ctx, oldAd.ID)
	if err != nil {
		logger.Error("Failed to check SVMs using Active Directory", "error", err, "active_directory_id", oldAd.ID)
		return err
	}

	if len(svms) > 0 {
		logger.Errorf("Active Directory is in use by %d SVM(s)", len(svms))
		return customerrors.NewBadRequestErr("Active Directory domain cannot be updated while it is in use")
	}

	return nil
}

// _deleteActiveDirectory orchestrates the deletion of an Active Directory resource.
// It creates a job and starts the corresponding Temporal workflow to delete the AD.
// Returns empty string if the AD is already deleted (indicating operation is already done).
func _deleteActiveDirectory(ctx context.Context, se database.Storage, temporal client.Client, params *common.DeleteActiveDirectoryParams) (string, error) {
	logger := util.GetLogger(ctx)

	// Get account
	account, err := getOrCreateAccount(ctx, se, params.ProjectNumber)
	if err != nil {
		logger.Error("Failed to get account", "error", err, "project ", params.ProjectNumber)
		return "", err
	}

	params.AccountId = account.ID

	// Get Active Directory to check if it exists and its state
	ad, err := se.GetActiveDirectoryByUuidAndAccountId(ctx, params.ActiveDirectoryUUID, params.AccountId)
	if err != nil && !customerrors.IsNotFoundErr(err) {
		logger.Error("Failed to get Active Directory", "error", err, "active_directory_id", params.ActiveDirectoryUUID)
		return "", err
	}
	resourceName := ad.AdName

	if ad != nil {
		// Check if the Active Directory is already in deleted state
		if ad.State == models.LifeCycleStateDeleted {
			logger.Info("Active Directory is already deleted", "active_directory_uuid", params.ActiveDirectoryUUID, "state", ad.State)
			return "", nil
		}

		// Check if the Active Directory is already being deleted
		if ad.State == models.LifeCycleStateDeleting {
			logger.Info("Active Directory is already being deleted", "active_directory_uuid", params.ActiveDirectoryUUID, "state", ad.State)
			// Check if there's an existing job
			existingJob, err := se.GetJobByResourceUUID(ctx, params.ActiveDirectoryUUID, string(models.JobTypeDeleteActiveDirectory))
			if err == nil && existingJob != nil {
				logger.Info("Returning existing job UUID", "job_uuid", existingJob.UUID)
				return existingJob.UUID, nil
			}
			logger.Warn("Active Directory is in Deleting state but no job found, proceeding with new job creation")
		}
	}

	// Create a job for tracking the deletion
	job := &datamodel.Job{
		Type:          string(models.JobTypeDeleteActiveDirectory),
		State:         string(models.JobsStateNEW),
		ResourceName:  resourceName,
		AccountID:     sql.NullInt64{Int64: params.AccountId, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: params.ActiveDirectoryUUID,
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return "", err
	}

	// Start the delete workflow
	controlWorkflowID := fmt.Sprintf("Account_%d_ActiveDirectory_%s_Delete", params.AccountId, params.ActiveDirectoryUUID)
	err = workflowsExecuteWorkflowSequentially(
		temporal,
		ctx,
		client.StartWorkflowOptions{
			TaskQueue: workflowengine.CustomerTaskQueue,
			ID:        controlWorkflowID,
		},
		workflows.DeleteActiveDirectoryWorkflow,
		workflow.ChildWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			WorkflowID:            createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		params,
	)
	if err != nil {
		logger.Error("Failed to start delete active directory workflow: ", "error", err)
		if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
			logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
		}
		return "", err
	}

	return createdJob.UUID, nil
}

// DeleteActiveDirectory is the public orchestrator method for deleting an Active Directory resource.
func (o *Orchestrator) DeleteActiveDirectory(ctx context.Context, params *common.DeleteActiveDirectoryParams) (string, error) {
	jobUUID, err := deleteActiveDirectory(ctx, o.storage, o.temporal, params)
	if err != nil {
		return "", err
	}
	return jobUUID, nil
}

func _validateMultiADConstraints(ctx context.Context, se database.Storage, accountID int64) error {
	logger := util.GetLogger(ctx)
	ads, err := se.ListActiveDirectories(ctx, accountID)
	if err != nil {
		return err
	}
	maxADsPerAccount := utils.MaxNumberOfADPerAccount
	multiADEnabled := utils.EnableMultiAD

	// If multi-AD is disabled, only allow creation if no ADs exist (len(ads) == 0)
	// This ensures that after creation, there will be exactly 1 AD total (including the one being created)
	if !multiADEnabled {
		if len(ads) > 0 {
			logger.Error(MultiADNotAllowedError)
			return customerrors.NewUserInputValidationErr(MultiADNotAllowedError)
		}
		return nil
	}

	// If multi-AD is enabled, check if adding one more AD would exceed the limit
	// We check len(ads) < maxADsPerAccount because after creation, len(ads) + 1 <= maxADsPerAccount
	// This ensures the total number of ADs (existing + the one being created) does not exceed maxADsPerAccount
	if len(ads) >= maxADsPerAccount {
		logger.Error(MaxADLimitReachedForAccountError)
		return customerrors.NewConflictErr(MaxADLimitReachedForAccountError)
	}
	return nil
}
