package orchestrator

import (
	"context"
	"database/sql"
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
	createKmsConfig          = _createKmsConfig
	getKmsConfig             = _getKmsConfig
	parseKeyFullPathResource = utils.ParseKeyFullPathResource
)

type KmsConfigInterface interface {
	CreateKmsConfig(ctx context.Context, params *common.CreateKmsConfigParams) (*models.KmsConfig, string, error)
	GetKmsConfig(ctx context.Context, params *common.GetKmsConfigParams) (*models.KmsConfig, error)
	GetMultipleKMSConfigs(ctx context.Context, kmsConfigIDList []string) ([]*models.KmsConfig, error)
	UpdateKmsConfig(ctx context.Context, params *common.UpdateKmsConfigParams) (*models.KmsConfig, string, error)
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
	dbKmsConfig.CustomerProjectID = parsedKeyFullPath.ProjectID
	dbKmsConfig.KeyRingLocation = parsedKeyFullPath.Location
	dbKmsConfig.KeyRing = parsedKeyFullPath.KeyRing
	dbKmsConfig.ResourceID = params.ResourceID
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

var (
	UpdateKmsConfig               = _updateKmsConfig
	validateUpdateKmsConfigParams = _validateUpdateKmsConfigParams
	isKmsConfigInUse              = _isKmsConfigInUse
)

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
		Type:         string(models.JobTypeUpdateKmsConfig),
		State:        string(models.JobsStateNEW),
		ResourceName: params.Name,
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
