package orchestrator

import (
	"context"
	"database/sql"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/kms_workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
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
	UpdateKmsConfig               = _updateKmsConfig
	validateUpdateKmsConfigParams = _validateUpdateKmsConfigParams
	isKmsConfigInUse              = _isKmsConfigInUse
	getKmsConfigByKeyFullPath     = _getKmsConfigByKeyFullPath
	validateDeleteKmsConfigParams = _validateDeleteKmsConfigParams
)

const (
	GcpKmsConfigHealthError = "specified key <key_name> in <key_ring> does not exist or service permissions are incorrect"
)

type KmsConfigInterface interface {
	CreateKmsConfig(ctx context.Context, params *common.CreateKmsConfigParams) (*models.KmsConfig, string, error)
	GetKmsConfig(ctx context.Context, params *common.GetKmsConfigParams) (*models.KmsConfig, error)
	GetKmsConfigByKeyFullPath(ctx context.Context, params *common.GetKmsConfigParams) (*models.KmsConfig, error)
	GetMultipleKMSConfigs(ctx context.Context, kmsConfigIDList []string) ([]*models.KmsConfig, error)
	UpdateKmsConfig(ctx context.Context, params *common.UpdateKmsConfigParams) (*models.KmsConfig, string, error)
	CheckAndUpdateKmsConfigHealth(ctx context.Context, params *models.KmsConfigCheck) (*models.KmsConfig, error)
	AccessCryptoKeyWithImpersonation(ctx context.Context, kmsConfig *models.KmsConfig) error
	DeleteKmsConfig(ctx context.Context, params *common.DeleteKmsConfigParams) (*models.KmsConfig, string, error)
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
	dbKmsConfig.UUID = utils.RandomUUID()
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
	dbKmsConfig.KmsAttributes = &datamodel.KmsAttributes{}
	dbKmsConfig, err = se.CreateKmsConfig(ctx, dbKmsConfig)
	if err != nil {
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeCreateKmsConfig),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.Name,
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
		Name:            kmsConfig.Name,
		KeyRing:         kmsConfig.KeyRing,
		KeyRingLocation: kmsConfig.KeyRingLocation,
		KeyName:         kmsConfig.KeyName,
	}
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

// GetMultipleKMSConfigs gets KMS Config records for the UUIDs provided
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
func (o *Orchestrator) UpdateKmsConfig(ctx context.Context, params *common.UpdateKmsConfigParams) (*models.KmsConfig, string, error) {
	return UpdateKmsConfig(ctx, o.storage, o.temporal, params)
}

func _updateKmsConfig(ctx context.Context, se database.Storage, temporal client.Client, params *common.UpdateKmsConfigParams) (*models.KmsConfig, string, error) {
	logger := util.GetLogger(ctx)

	account, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return nil, "", err
	}

	kmsConfig, err := se.GetKmsConfig(ctx, params.KmsConfigID)
	if err == nil {
		err = validateUpdateKmsConfigParams(ctx, se, kmsConfig, params)
		if err != nil {
			return nil, "", err
		}

		kmsConfig, err = se.UpdateKmsConfigState(ctx, kmsConfig.UUID, models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails)
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
		Type:          string(models.JobTypeUpdateKmsConfig),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
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
		},
		kms_workflows.UpdateKmsConfigWorkflow,
		kmsConfig,
		params,
	)
	if err != nil {
		logger.Error("Failed to start update kms config workflow: ", "error", err)
		return nil, "", err
	}

	kmsConfig.State = models.LifeCycleStateUpdating
	kmsConfig.StateDetails = models.LifeCycleStateUpdatingDetails
	return convertDataStoreKmsConfigToModel(kmsConfig), createdJob.UUID, nil
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
		Type:         string(models.JobTypeDeleteKmsConfig),
		State:        string(models.JobsStateNEW),
		ResourceName: kmsConfig.ResourceID,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
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

func convertDataStoreKmsConfigToModel(kmsConfig *datamodel.KmsConfig) *models.KmsConfig {
	if kmsConfig == nil || kmsConfig.UUID == "" {
		return nil
	}
	kmsModel := &models.KmsConfig{
		BaseModel: models.BaseModel{
			UUID:      kmsConfig.UUID,
			CreatedAt: kmsConfig.CreatedAt,
			UpdatedAt: kmsConfig.UpdatedAt,
			DeletedAt: DeletedAtOrNil(kmsConfig.DeletedAt),
		},
		Name:              kmsConfig.Name,
		Description:       kmsConfig.Description,
		State:             kmsConfig.State,
		StateDetails:      kmsConfig.StateDetails,
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
	if kmsConfig.State == models.LifeCycleStateCreating ||
		kmsConfig.State == models.LifeCycleStateError {
		return errors.NewConflictErr("can not update a gcpKmsConfig which is in creating or error state.")
	}

	isConfigInUse, err := isKmsConfigInUse(ctx, se, kmsConfig)
	if err != nil {
		return err
	}

	if isConfigInUse && !nillable.IsNilOrEmpty(&params.KeyName) {
		return errors.NewConflictErr("can not update key details while kms config is in use")
	}
	return nil
}

func _validateDeleteKmsConfigParams(ctx context.Context, se database.Storage, kmsConfig *datamodel.KmsConfig, params *common.DeleteKmsConfigParams) error {
	if kmsConfig.State == models.LifeCycleStateCreating {
		return errors.NewConflictErr("can not delete a gcpKmsConfig which is in creating state.")
	}

	isConfigInUse, err := isKmsConfigInUse(ctx, se, kmsConfig)
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

func _isKmsConfigInUse(ctx context.Context, se database.Storage, kmsConfig *datamodel.KmsConfig) (bool, error) {
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

// CheckAndUpdateKmsConfigHealth UpdateKmsConfigHealth checks the health of a KMS configuration and updates its status accordingly.
func (o *Orchestrator) CheckAndUpdateKmsConfigHealth(ctx context.Context, configCheck *models.KmsConfigCheck) (*models.KmsConfig, error) {
	se := o.storage
	kmsConfig, err := se.GetKmsConfig(ctx, configCheck.KmsConfig.UUID)
	if err != nil {
		return nil, err
	}
	kmsConfigInUse, err := isKmsConfigInUse(ctx, se, kmsConfig)
	if err != nil {
		return nil, err
	}

	state := models.LifeCycleStateUnknown
	stateDetails := models.LifeCycleStateUnknownDetails

	switch configCheck.IsHealthy {
	case true:
		state = models.LifeCycleStateREADY
		stateDetails = models.LifeCycleStateReadyDetails
		// keep the state as in user if the KMS config is in use (in use meaning that there are SVMs using this KMS config)
		if kmsConfigInUse {
			state = models.LifeCycleStateInUse
			stateDetails = models.LifeCycleStateAvailableDetails
		}
	case false:
		// If the KMS config is in error state, do not update the state to ready.
		state = models.LifeCycleStateError
		stateDetails = configCheck.HealthError
		healthErrorMessage := strings.Replace(strings.Replace(GcpKmsConfigHealthError, "<key_name>", kmsConfig.KeyName, 1), "<key_ring>", kmsConfig.KeyRing, 1)
		// Keep the state as created if the health error message indicates that the key does not exist or service permissions are incorrect.
		if strings.Contains(stateDetails, healthErrorMessage) {
			state = models.LifeCycleStateCreated
		}
	}

	// Update the KMS config state and details
	kmsConfig, err = se.UpdateKmsConfigState(ctx, kmsConfig.UUID, state, stateDetails)
	if err != nil {
		return nil, err
	}

	// Update the KMS config Attributes with the health check response
	kmsConfig.KmsAttributes.SdeKmsConfigIsHealthy = configCheck.IsHealthy
	kmsConfig.KmsAttributes.SdeKmsConfigHealthError = configCheck.HealthError
	kmsConfig, err = se.UpdateKmsConfigAttributes(ctx, kmsConfig.UUID, kmsConfig.KmsAttributes)
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
	return kms_activities.AccessCryptoKey(ctx, se, dbKmsConfig)
}

func (o *Orchestrator) GetKmsConfigByKeyFullPath(ctx context.Context, params *common.GetKmsConfigParams) (*models.KmsConfig, error) {
	return getKmsConfigByKeyFullPath(ctx, o.storage, params)
}

func _getKmsConfigByKeyFullPath(ctx context.Context, se database.Storage, params *common.GetKmsConfigParams) (*models.KmsConfig, error) {
	_, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return nil, err
	}
	dbKmsConfig, err := se.GetKmsConfigByKeyFullPath(ctx, params.KeyFullPath)
	if err != nil {
		return nil, err
	}
	return convertDatastoreKmsConfigToModel(dbKmsConfig), nil
}
