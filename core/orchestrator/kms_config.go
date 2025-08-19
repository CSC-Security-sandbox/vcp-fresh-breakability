package orchestrator

import (
	"context"
	"database/sql"
	"strings"
	"time"

	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	models2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/kms_workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/sde"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

var (
	createKmsConfig               = _createKmsConfig
	getKmsConfig                  = _getKmsConfig
	parseKeyFullPathResource      = utils.ParseKeyFullPathResource
	updateKmsConfig               = _updateKmsConfig
	validateUpdateKmsConfigParams = _validateUpdateKmsConfigParams
	getKmsConfigByKeyFullPath     = _getKmsConfigByKeyFullPath
	validateDeleteKmsConfigParams = _validateDeleteKmsConfigParams
	ConvertKmsConfigStateV1beta   = convertKmsConfigStateV1beta
	updateSDEKmsConfiguration     = sde.UpdateSDEKmsConfiguration
)

type KmsConfigInterface interface {
	CreateKmsConfig(ctx context.Context, params *common.CreateKmsConfigParams) (*models.KmsConfig, string, error)
	GetKmsConfig(ctx context.Context, params *common.GetKmsConfigParams) (*models.KmsConfig, error)
	GetKmsConfigByKeyFullPath(ctx context.Context, params *common.GetKmsConfigParams) (*models.KmsConfig, error)
	GetMultipleKMSConfigs(ctx context.Context, kmsConfigIDList []string) ([]*models.KmsConfig, error)
	UpdateKmsConfig(ctx context.Context, params *common.UpdateKmsConfigParams) (*models.KmsConfig, error)
	CheckAndUpdateKmsConfigHealth(ctx context.Context, params *models.KmsConfigCheck) (*models.KmsConfig, error)
	AccessCryptoKeyWithImpersonation(ctx context.Context, kmsConfig *models.KmsConfig) error
	DeleteKmsConfig(ctx context.Context, params *common.DeleteKmsConfigParams) (*models.KmsConfig, string, error)
	MigrateKmsConfig(ctx context.Context, params *common.MigrateKmsConfigParams) (string, error)
	RotateKmsConfig(ctx context.Context, params *common.RotateKmsConfigParams) (*models.KmsConfig, *models.Job, error)
}

// CreateKmsConfig creates a new KMS configuration.
func (o *Orchestrator) CreateKmsConfig(ctx context.Context, params *common.CreateKmsConfigParams) (*models.KmsConfig, string, error) {
	return createKmsConfig(ctx, o.storage, o.temporal, params)
}

func _createKmsConfig(ctx context.Context, se database.Storage, temporal client.Client, params *common.CreateKmsConfigParams) (*models.KmsConfig, string, error) {
	logger := util.GetLogger(ctx)
	account, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return nil, "", err
	}
	parsedKeyFullPath, err := parseKeyFullPathResource(params.KeyFullPath)
	if err != nil {
		return nil, "", err
	}
	dbKmsConfig := &datamodel.KmsConfig{}
	dbKmsConfig.CreatedAt = time.Now()
	dbKmsConfig.UUID = params.UUID // Use the uuid of the sde; this is to make sure CCFE gets the same UUID as SDE for PO volume creation
	dbKmsConfig.State = models.LifeCycleStateCreating
	dbKmsConfig.StateDetails = models.LifeCycleStateCreatingDetails
	dbKmsConfig.AccountID = account.ID
	dbKmsConfig.UpdatedAt = time.Now()
	dbKmsConfig.KeyName = parsedKeyFullPath.CryptoKey
	dbKmsConfig.CustomerProjectID = params.ProjectNumber
	dbKmsConfig.KeyRingLocation = parsedKeyFullPath.Location
	dbKmsConfig.KeyRing = parsedKeyFullPath.KeyRing
	dbKmsConfig.ResourceID = params.ResourceID
	dbKmsConfig.KeyProjectID = parsedKeyFullPath.ProjectID
	dbKmsConfig.KmsAttributes = &datamodel.KmsAttributes{SdeKmsConfigUUID: params.UUID}
	dbKmsConfig.Description = params.Description

	dbKmsConfig, err = se.CreateKmsConfig(ctx, dbKmsConfig)
	if err != nil {
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeCreateKmsConfig),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.ResourceID,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: dbKmsConfig.UUID},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err.Error())
		return nil, "", err
	}

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		kms_workflows.CreateKmsConfigWorkflow,
		params,
		dbKmsConfig,
	)
	if err != nil {
		logger.Error("Failed to start create kms workflow: ", "error", err)
		return nil, "", err
	}
	return convertDatastoreKmsConfigToModel(dbKmsConfig), createdJob.UUID, nil
}

// GetKmsConfig retrieves a KMS configuration by its UUID.
func (o *Orchestrator) GetKmsConfig(ctx context.Context, params *common.GetKmsConfigParams) (*models.KmsConfig, error) {
	return getKmsConfig(ctx, o.storage, o.temporal, params)
}

func _getKmsConfig(ctx context.Context, se database.Storage, temporal client.Client, params *common.GetKmsConfigParams) (*models.KmsConfig, error) {
	_, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return nil, err
	}
	dbKmsConfig, err := se.GetKmsConfigByUUID(ctx, params.UUID)
	if err != nil {
		return nil, err
	}
	return convertDatastoreKmsConfigToModel(dbKmsConfig), nil
}

// GetMultipleKMSConfigs gets KMS config records for the UUIDs provided
func (o *Orchestrator) GetMultipleKMSConfigs(ctx context.Context, kmsConfigUUIDList []string) ([]*models.KmsConfig, error) {
	se := o.storage

	conditions := [][]interface{}{{"uuid in ?", kmsConfigUUIDList}}
	kmsConfigDataStoreList, err := se.GetMultipleKmsConfigs(ctx, conditions)
	if err != nil {
		return nil, err
	}
	var kmsConfigModelList []*models.KmsConfig
	for _, kmsConfigDataStore := range kmsConfigDataStoreList {
		kmsConfigModel := convertDataStoreKmsConfigToModel(kmsConfigDataStore)
		kmsConfigModelList = append(kmsConfigModelList, kmsConfigModel)
	}

	return kmsConfigModelList, nil
}

// UpdateKmsConfig updates the specified kms configuration.
func (o *Orchestrator) UpdateKmsConfig(ctx context.Context, params *common.UpdateKmsConfigParams) (*models.KmsConfig, error) {
	return updateKmsConfig(ctx, o.storage, params)
}

func _updateKmsConfig(ctx context.Context, se database.Storage, params *common.UpdateKmsConfigParams) (*models.KmsConfig, error) {
	logger := util.GetLogger(ctx)

	_, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		return nil, err
	}

	kmsConfig, err := se.GetKmsConfig(ctx, params.KmsConfigID)
	if err != nil {
		if !errors.IsNotFoundErr(err) {
			return nil, err
		}
		logger.Info("Kms config not found in vcp database", "error", err)

		// For KmsConfig not found, directly call SDE & return
		kmsConfig = &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{
				SdeKmsConfigUUID: params.KmsConfigID,
			},
		}

		sdeRes, err := updateSDEKmsConfiguration(ctx, kmsConfig, params)
		if err != nil {
			logger.Error("Failed to update KMS configuration in SDE", "error", err)
			return nil, err
		}

		sdeKmsConfig, ok := sdeRes.(*gcpserver.KmsConfigV1beta)
		if ok && sdeKmsConfig != nil {
			return convertSDEResponseToKmsConfig(sdeKmsConfig), nil
		}

		return nil, errors.New("Failed to update KMS configuration in SDE")
	}

	err = validateUpdateKmsConfigParams(ctx, se, kmsConfig, params)
	if err != nil {
		return nil, err
	}

	_, err = updateSDEKmsConfiguration(ctx, kmsConfig, params)
	if err != nil {
		logger.Error("Failed to update KMS configuration in SDE", "error", err)
		return nil, err
	}

	// Update the database with success state
	if kmsConfig.UUID != "" {
		// Parse KeyUri if provided to set individual key components
		if params.KeyUri != "" {
			parsedKeyFullPath, parseErr := parseKeyFullPathResource(params.KeyUri)
			if parseErr == nil {
				params.KeyName = parsedKeyFullPath.CryptoKey
				params.KeyRingLocation = parsedKeyFullPath.Location
				params.KeyRing = parsedKeyFullPath.KeyRing
				params.KeyProjectID = parsedKeyFullPath.ProjectID
			}
		}

		// Use the activity function to update the KMS config in database
		err = kms_activities.UpdateKmsConfig(se, ctx, kmsConfig, params)
		if err != nil {
			logger.Error("Failed to update KMS config in database", "error", err)
			return nil, err
		}

		// Get the updated config from database
		updatedKmsConfig, err := se.GetKmsConfig(ctx, params.KmsConfigID)
		if err != nil {
			logger.Error("Failed to get updated KMS config from database", "error", err)
			return nil, err
		}
		return convertDataStoreKmsConfigToModel(updatedKmsConfig), nil
	}

	return convertDataStoreKmsConfigToModel(kmsConfig), nil
}

// DeleteKmsConfig updates the specified kms configuration.
func (o *Orchestrator) DeleteKmsConfig(ctx context.Context, params *common.DeleteKmsConfigParams) (*models.KmsConfig, string, error) {
	return _deleteKmsConfig(ctx, o.storage, o.temporal, params)
}

func _deleteKmsConfig(ctx context.Context, se database.Storage, temporal client.Client, params *common.DeleteKmsConfigParams) (*models.KmsConfig, string, error) {
	logger := util.GetLogger(ctx)

	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		return nil, "", err
	}

	kmsConfig, err := se.GetKmsConfig(ctx, params.KmsConfigID)
	if err == nil {
		// If KMS config is already in DELETING state, check for existing delete job
		if kmsConfig.State == models.LifeCycleStateDeleting {
			existingJob, jobErr := se.GetJobByResourceUUID(ctx, params.KmsConfigID, string(models.JobTypeDeleteKmsConfig))
			if jobErr == nil && existingJob != nil {
				// Check if it's a delete job that's not done
				if existingJob.Type == string(models.JobTypeDeleteKmsConfig) && existingJob.State != string(models.JobsStateDONE) {
					return convertDataStoreKmsConfigToModel(kmsConfig), existingJob.UUID, nil
				}
			}
		}

		err = validateDeleteKmsConfigParams(ctx, se, kmsConfig, params)
		if err != nil {
			return nil, "", err
		}

		kmsConfig, err = se.UpdateKmsConfigState(ctx, kmsConfig.UUID, models.LifeCycleStateDeleting, models.LifeCycleStateDeletingDetails)
		if err != nil {
			return nil, "", err
		}
	} else {
		if !errors.IsNotFoundErr(err) {
			return nil, "", err
		}
		logger.Error("Failed to get kms config from database", "error", err)
		kmsConfig = &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{
				SdeKmsConfigUUID: params.KmsConfigID,
			},
		}
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeDeleteKmsConfig),
		State:         string(models.JobsStateNEW),
		ResourceName:  kmsConfig.ResourceID,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: params.KmsConfigID},
		CorrelationID: params.XCorrelationID,
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		kms_workflows.DeleteKmsConfigWorkflow,
		kmsConfig,
		params,
	)
	if err != nil {
		logger.Error("Failed to start update kms config workflow: ", "error", err)
		return nil, "", err
	}

	return convertDataStoreKmsConfigToModel(kmsConfig), createdJob.UUID, nil
}

func (o *Orchestrator) MigrateKmsConfig(ctx context.Context, params *common.MigrateKmsConfigParams) (string, error) {
	return migrateKmsConfig(ctx, o.storage, o.temporal, params)
}

func migrateKmsConfig(ctx context.Context, se database.Storage, temporal client.Client, params *common.MigrateKmsConfigParams) (string, error) {
	logger := util.GetLogger(ctx)

	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		return "", err
	}

	localEntryPresent := false
	params.SdeUUID = params.UUID
	dbKmsConfig, err := se.GetKmsConfigByUUID(ctx, params.UUID)
	if err != nil {
		if !errors.IsNotFoundErr(err) {
			return "", err
		}
	} else {
		if dbKmsConfig.KmsAttributes != nil && dbKmsConfig.KmsAttributes.SdeKmsConfigUUID != "" {
			params.SdeUUID = dbKmsConfig.KmsAttributes.SdeKmsConfigUUID
			localEntryPresent = true
		} else {
			return "", errors.New("KmsAttributes property not present within KmsConfig DB entry in VCP")
		}
	}

	// Check for validation errors before starting the workflow because SDE does not return...
	// ...validation errors as part of operation response. Also use this to return ongoing migration job (if present)
	ongoingJobUuid, errValidate := validateKmsConfigState(ctx, se, params.State, account.ID, localEntryPresent)
	if errValidate != nil {
		return "", errValidate
	} else if ongoingJobUuid != "" {
		return ongoingJobUuid, nil
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeMigrateKmsConfig),
		State:        string(models.JobsStateNEW),
		ResourceName: params.Name,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
	}
	createdJob, errJob := se.CreateJob(ctx, job)
	if errJob != nil {
		logger.Error("Failed to create job in database", "error", errJob)
		return "", errJob
	}

	if localEntryPresent {
		_, errUpdateState := se.UpdateKmsConfigState(ctx, params.UUID, models.LifeCycleStateMigrating, models.LifeCycleStateMigratingDetails)
		if errUpdateState != nil {
			return "", errUpdateState
		}
	}

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		kms_workflows.MigrateKmsConfigWorkflow,
		params,
	)
	if err != nil {
		logger.Error("Failure encountered during CMEK migrate workflow: ", "error", err)
		return "", err
	}

	return createdJob.UUID, nil
}

// RotateKmsConfig rotates the service account key for a KMS configuration.
func (o *Orchestrator) RotateKmsConfig(ctx context.Context, params *common.RotateKmsConfigParams) (*models.KmsConfig, *models.Job, error) {
	return rotateKmsConfig(ctx, o.storage, o.temporal, params)
}

func rotateKmsConfig(ctx context.Context, se database.Storage, temporal client.Client, params *common.RotateKmsConfigParams) (*models.KmsConfig, *models.Job, error) {
	logger := util.GetLogger(ctx)

	// Validate the KMS config exists
	kmsConfig, err := se.GetKmsConfig(ctx, params.KmsConfigID)
	if err != nil {
		logger.Error("Failed to get KMS config", "KmsConfigID", params.KmsConfigID, "Error", err)
		return nil, nil, err
	}

	// Get account info using UUID instead of name
	// Using AccountName as UUID for now - this may need to be adjusted based on your actual requirement
	account, err := getAccountFromUUID(ctx, se, params.AccountName)
	if err != nil {
		return nil, nil, err
	}

	if kmsConfig.State != models2.KmsConfigV1betaKmsStateINUSE && kmsConfig.State != models2.KmsConfigV1betaKmsStateREADY {
		return nil, nil, errors.New("Concerned GCP KMS config is not in a state(ready/in use) to rotate the service account key")
	}

	// Create a new job for the rotation
	job := &datamodel.Job{
		Type:         string(models.JobTypeRotateKmsConfig),
		State:        string(models.JobsStateNEW),
		ResourceName: kmsConfig.Name,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: params.KmsConfigID,
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create rotation job", "Error", err)
		return nil, nil, err
	}

	// Start the rotation workflow
	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		kms_workflows.RotateKmsConfigWorkflow,
		params,
	)
	if err != nil {
		logger.Error("Failed to start rotation workflow", "Error", err)
		return nil, nil, err
	}

	logger.Info("Started KMS config rotation workflow",
		"WorkflowID", createdJob.WorkflowID,
		"KmsConfigID", params.KmsConfigID,
		"JobID", createdJob.UUID)

	// Convert the KMS config to model and return it along with job model
	kmsConfigModel := convertDataStoreKmsConfigToModel(kmsConfig)
	jobModel := convertDatastoreJobToModel(createdJob)
	return kmsConfigModel, jobModel, nil
}

func convertDataStoreKmsConfigToModel(kmsConfig *datamodel.KmsConfig) *models.KmsConfig {
	if kmsConfig == nil || kmsConfig.UUID == "" {
		return nil
	}
	state, stateDetails := convertKmsConfigStateV1beta(kmsConfig.State, kmsConfig.StateDetails)
	kmsModel := &models.KmsConfig{
		BaseModel: models.BaseModel{
			UUID:      kmsConfig.UUID,
			CreatedAt: kmsConfig.CreatedAt,
			UpdatedAt: kmsConfig.UpdatedAt,
			DeletedAt: DeletedAtOrNil(kmsConfig.DeletedAt),
		},
		Name:              kmsConfig.Name,
		Description:       kmsConfig.Description,
		State:             state,
		StateDetails:      stateDetails,
		KeyRing:           kmsConfig.KeyRing,
		KeyRingLocation:   kmsConfig.KeyRingLocation,
		KeyName:           kmsConfig.KeyName,
		AccountID:         kmsConfig.AccountID,
		CustomerProjectID: kmsConfig.CustomerProjectID,
		KeyProjectID:      kmsConfig.KeyProjectID,
		ServiceAccountID:  kmsConfig.ServiceAccountID,
		ResourceID:        kmsConfig.ResourceID,
	}
	if kmsConfig.KmsAttributes != nil {
		kmsModel.KmsAttributes = &models.KmsAttributes{
			SdeKmsConfigUUID:       kmsConfig.KmsAttributes.SdeKmsConfigUUID,
			SdeServiceAccountEmail: kmsConfig.KmsAttributes.SdeServiceAccountEmail,
			Instructions:           kmsConfig.KmsAttributes.Instructions,
		}
	}
	return kmsModel
}

func _validateUpdateKmsConfigParams(ctx context.Context, se database.Storage, kmsConfig *datamodel.KmsConfig, params *common.UpdateKmsConfigParams) error {
	if kmsConfig.State == models.LifeCycleStateCreating || kmsConfig.State == models.LifeCycleStateError {
		return errors.NewConflictErr("can not update a gcpKmsConfig which is in creating or error state.")
	}

	isConfigInUse, err := se.IsKmsConfigInUse(ctx, kmsConfig.UUID)
	if err != nil {
		return err
	}

	if isConfigInUse && !nillable.IsNilOrEmpty(&params.KeyName) {
		return errors.NewConflictErr("can not update key details while kms config is in use")
	}
	return nil
}

func _validateDeleteKmsConfigParams(ctx context.Context, se database.Storage, kmsConfig *datamodel.KmsConfig, params *common.DeleteKmsConfigParams) error {
	if kmsConfig.State == models.LifeCycleStateCreating || kmsConfig.State == models.LifeCycleStateError {
		return errors.NewConflictErr("can not delete a gcpKmsConfig which is in creating or error state.")
	}

	isConfigInUse, err := se.IsKmsConfigInUse(ctx, kmsConfig.UUID)
	if err != nil {
		return err
	}

	if isConfigInUse {
		return errors.NewConflictErr("can not delete this policy as it is still in use")
	}

	findOngoingJobs, err := se.ListOngoingPoolJobsWithKmsConfigId(ctx, kmsConfig.ID, kmsConfig.AccountID)
	if err != nil {
		return err
	}

	if len(findOngoingJobs) > 0 {
		return errors.NewConflictErr("can not delete this policy as there are ongoing pool creation using it")
	}
	return nil
}

// CheckAndUpdateKmsConfigHealth UpdateKmsConfigHealth checks the health of a KMS configuration and updates its status accordingly.
func (o *Orchestrator) CheckAndUpdateKmsConfigHealth(ctx context.Context, configCheck *models.KmsConfigCheck) (*models.KmsConfig, error) {
	se := o.storage
	kmsConfig, err := kms_activities.UpdateKmsConfigHealth(ctx, se, configCheck)
	if err != nil {
		return nil, err
	}
	return convertDataStoreKmsConfigToModel(kmsConfig), nil
}

// AccessCryptoKeyWithImpersonation use impersonation to retrieve the details of a specific KMS crypto key.
func (o *Orchestrator) AccessCryptoKeyWithImpersonation(ctx context.Context, kmsConfig *models.KmsConfig) error {
	se := o.storage
	dbKmsConfig, err := se.GetKmsConfig(ctx, kmsConfig.UUID)
	if err != nil {
		return err
	}
	return kms_activities.AccessCryptoKey(ctx, dbKmsConfig, dbKmsConfig.ServiceAccount.ServiceAccountPasswordLocation)
}

func (o *Orchestrator) GetKmsConfigByKeyFullPath(ctx context.Context, params *common.GetKmsConfigParams) (*models.KmsConfig, error) {
	return getKmsConfigByKeyFullPath(ctx, o.storage, params)
}

func _getKmsConfigByKeyFullPath(ctx context.Context, se database.Storage, params *common.GetKmsConfigParams) (*models.KmsConfig, error) {
	account, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return nil, err
	}
	dbKmsConfig, err := se.GetKmsConfigByKeyFullPath(ctx, params.KeyFullPath, account.ID)
	if err != nil {
		return nil, err
	}
	return convertDatastoreKmsConfigToModel(dbKmsConfig), nil
}

func convertDatastoreKmsConfigToModel(kmsConfig *datamodel.KmsConfig) *models.KmsConfig {
	return &models.KmsConfig{
		BaseModel: models.BaseModel{
			UUID:      kmsConfig.UUID,
			CreatedAt: kmsConfig.CreatedAt,
			UpdatedAt: kmsConfig.UpdatedAt,
		},
		CustomerProjectID: kmsConfig.CustomerProjectID,
		KeyProjectID:      kmsConfig.KeyProjectID,
		State:             kmsConfig.State,
		StateDetails:      kmsConfig.StateDetails,
		Description:       kmsConfig.Description,
		ResourceID:        kmsConfig.ResourceID,
		AccountID:         kmsConfig.AccountID,
		ServiceAccountID:  kmsConfig.ServiceAccountID,
		KmsAttributes: &models.KmsAttributes{
			SdeKmsConfigUUID:       kmsConfig.KmsAttributes.SdeKmsConfigUUID,
			SdeServiceAccountEmail: kmsConfig.KmsAttributes.SdeServiceAccountEmail,
			Instructions:           kmsConfig.KmsAttributes.Instructions,
		},
		ServiceAccount:  convertDatastoreServiceAccountToModel(kmsConfig.ServiceAccount),
		Name:            kmsConfig.Name,
		KeyRing:         kmsConfig.KeyRing,
		KeyRingLocation: kmsConfig.KeyRingLocation,
		KeyName:         kmsConfig.KeyName,
	}
}

func convertDatastoreServiceAccountToModel(sa *datamodel.ServiceAccount) *models.ServiceAccount {
	if sa == nil {
		return nil
	}
	return &models.ServiceAccount{
		BaseModel: models.BaseModel{
			UUID:      sa.UUID,
			CreatedAt: sa.CreatedAt,
			UpdatedAt: sa.UpdatedAt,
			DeletedAt: DeletedAtOrNil(sa.DeletedAt),
		},
		Name:                           sa.Name,
		Description:                    sa.Description,
		State:                          sa.State,
		StateDetails:                   sa.StateDetails,
		AccountID:                      sa.AccountID,
		ServiceName:                    sa.ServiceName,
		ServiceAccountEmail:            sa.ServiceAccountEmail,
		ServiceAccountPasswordLocation: sa.ServiceAccountPasswordLocation,
	}
}

// convertDatastoreJobToModel converts a datastore Job to a model Job
func convertDatastoreJobToModel(job *datamodel.Job) *models.Job {
	if job == nil {
		return nil
	}

	modelJob := &models.Job{
		BaseModel: models.BaseModel{
			UUID:      job.UUID,
			CreatedAt: job.CreatedAt,
			UpdatedAt: job.UpdatedAt,
			DeletedAt: DeletedAtOrNil(job.DeletedAt),
		},
		CorrelationID: job.CorrelationID,
		RequestID:     job.RequestID,
		Type:          models.JobType(job.Type),
		State:         models.JobState(job.State),
		StateDetails:  "", // datamodel.Job doesn't have StateDetails field
		TrackingID:    job.TrackingID,
		ErrorDetails:  []byte(job.ErrorDetails),
		AccountID:     job.AccountID,
		IsAdminJob:    job.IsAdminJob,
		WorkflowID:    job.WorkflowID,
		ScheduledAt:   job.ScheduledAt,
		ResourceName:  job.ResourceName,
	}

	if job.JobAttributes != nil {
		modelJob.JobAttributes = &models.JobAttributes{
			ResourceUUID: job.JobAttributes.ResourceUUID,
			PoolUUID:     job.JobAttributes.PoolUUID,
		}
	}

	return modelJob
}

func validateKmsConfigState(ctx context.Context, se database.Storage, kmsConfigState string, accountId int64, localEntry bool) (string, error) {
	if kmsConfigState == models.LifeCycleStateUpdating || kmsConfigState == models.LifeCycleStateMigrating {
		jobMigration, dbErr := se.GetOngoingMigrateKmsConfigJob(ctx, accountId)
		if dbErr != nil {
			if errors.IsNotFoundErr(dbErr) {
				// In this case there is an Update job that is ongoing, which is not a migration job
				return "", errors.NewBadRequestErr("CMEK Configuration is undergoing an Update operation")
			}
			return "", dbErr
		}
		return jobMigration.UUID, nil
	}
	if kmsConfigState != models.LifeCycleStateREADY && kmsConfigState != models.LifeCycleStateInUse {
		return "", errors.NewBadRequestErr("CMEK Configuration needs to be in either Ready or In_Use state for migration")
	}
	return "", nil
}

func convertKmsConfigStateV1beta(status, stateDetails string) (state, details string) {
	switch status {
	case models.LifeCycleStateCreated, models.LifeCycleStateKeyCheckPending:
		return cvpModels.KmsConfigV1betaKmsStateKEYCHECKPENDING, "Credentials created and key check pending"
	case models.LifeCycleStateInUse:
		return cvpModels.KmsConfigV1betaKmsStateINUSE, "Kms config in use"
	case models.LifeCycleStateDeleted:
		return cvpModels.KmsConfigV1betaKmsStateDELETED, "Kms config deleted"
	case models.LifeCycleStateUpdating:
		return cvpModels.KmsConfigV1betaKmsStateUPDATING, "Updating Kms config"
	case models.LifeCycleStateDeleting:
		return cvpModels.KmsConfigV1betaKmsStateDELETING, "Deleting Kms config"
	case models.LifeCycleStateCreating:
		return cvpModels.KmsConfigV1betaKmsStateCREATING, "Creating Kms config"
	case models.LifeCycleStateREADY:
		return cvpModels.KmsConfigV1betaKmsStateREADY, "Kms config is ready for use"
	case models.LifeCycleStateMigrating:
		return cvpModels.KmsConfigV1betaKmsStateMIGRATING, "Kms config is in migrating state"
	default:
		if strings.Contains(status, "error") {
			return cvpModels.KmsConfigV1betaKmsStateERROR, strings.TrimPrefix(stateDetails, "error - ")
		}
		return status, ""
	}
}

// convertSDEResponseToKmsConfig converts SDE response to models.KmsConfig
func convertSDEResponseToKmsConfig(kmsConfig *gcpserver.KmsConfigV1beta) *models.KmsConfig {
	modelKmsConfig := &models.KmsConfig{
		BaseModel: models.BaseModel{
			UUID: kmsConfig.UUID.Value,
		},
	}

	if kmsConfig.Description.IsSet() {
		modelKmsConfig.Description = kmsConfig.Description.Value
	}

	if kmsConfig.ResourceId.IsSet() {
		modelKmsConfig.ResourceID = kmsConfig.ResourceId.Value
	}

	if kmsConfig.KeyFullPath != "" {
		// Parse key full path to extract individual components
		if parsedKey, err := utils.ParseKeyFullPathResource(kmsConfig.KeyFullPath); err == nil {
			modelKmsConfig.KeyName = parsedKey.CryptoKey
			modelKmsConfig.KeyRing = parsedKey.KeyRing
			modelKmsConfig.KeyRingLocation = parsedKey.Location
			modelKmsConfig.KeyProjectID = parsedKey.ProjectID
		}
	}

	if kmsConfig.KmsState.IsSet() {
		// Convert SDE state to internal state
		switch kmsConfig.KmsState.Value {
		case gcpserver.KmsConfigV1betaKmsStateREADY:
			modelKmsConfig.State = models.LifeCycleStateREADY
		case gcpserver.KmsConfigV1betaKmsStateKEYCHECKPENDING:
			modelKmsConfig.State = models.LifeCycleStateKeyCheckPending
		case gcpserver.KmsConfigV1betaKmsStateUPDATING:
			modelKmsConfig.State = models.LifeCycleStateUpdating
		case gcpserver.KmsConfigV1betaKmsStateINUSE:
			modelKmsConfig.State = models.LifeCycleStateInUse
		case gcpserver.KmsConfigV1betaKmsStateERROR:
			modelKmsConfig.State = models.LifeCycleStateError
		default:
			modelKmsConfig.State = models.KmsConfigV1betaKmsStateKEYSTATEUNSPECIFIED
		}
	} else {
		modelKmsConfig.State = models.KmsConfigV1betaKmsStateKEYSTATEUNSPECIFIED
	}

	if kmsConfig.KmsStateDetails.IsSet() {
		modelKmsConfig.StateDetails = kmsConfig.KmsStateDetails.Value
	} else {
		modelKmsConfig.StateDetails = "Updated successfully"
	}

	if kmsConfig.ServiceAccountEmail.IsSet() {
		modelKmsConfig.KmsAttributes = &models.KmsAttributes{
			SdeKmsConfigUUID:       kmsConfig.UUID.Value,
			SdeServiceAccountEmail: kmsConfig.ServiceAccountEmail.Value,
		}
	} else {
		modelKmsConfig.KmsAttributes = &models.KmsAttributes{
			SdeKmsConfigUUID: kmsConfig.UUID.Value,
		}
	}

	if kmsConfig.CreatedTime.IsSet() {
		modelKmsConfig.CreatedAt = kmsConfig.CreatedTime.Value
	}

	if kmsConfig.UpdatedTime.IsSet() {
		modelKmsConfig.UpdatedAt = kmsConfig.UpdatedTime.Value
	}

	return modelKmsConfig
}
