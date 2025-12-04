package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/validators"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/client"
	"gorm.io/gorm"
)

var (
	// Pool size limits
	minQuotaInBytesPool = utils.MinQuotaInBytesPool
	maxQuotaInBytesPool = utils.MaxQuotaInBytesPool
	// Function variables
	createPool                     = _createPool
	updatePool                     = _updatePool
	ValidatePoolParams             = _validatePoolParams
	ValidateCreatePoolParams       = _validateCreatePoolParams
	ValidateAndSetUpdatePoolParams = _validateAndSetUpdatePoolParams
	deletePool                     = _deletePool
	GetPoolByName                  = _getPoolByName
)

const (
	ServiceLevelNameFLEX                 = "FLEX"
	QosTypeAuto                          = "auto"
	TieringFullnessThresholdOntapDefault = 50
	VCP_ADMIN_CERT_UN_SUFFIX             = "_admin" // Suffix for VCP admin user certificate
)

// CreatePool creates the specified pool and adds it to the list of pools belonging to the specified owner
func (o *Orchestrator) CreatePool(ctx context.Context, params *commonparams.CreatePoolParams) (*models.Pool, string, error) {
	return createPool(ctx, o.storage, o.temporal, params)
}

// createPool creates a new pool and triggers asynchronous creation processes.
func _createPool(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.CreatePoolParams) (*models.Pool, string, error) {
	logger := util.GetLogger(ctx)
	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)

	account, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return nil, "", err
	}
	err = ValidateCreatePoolParams(params)
	if err != nil {
		return nil, "", err
	}

	dbPool, err2 := CreatePoolInDB(ctx, se, params, account, logger, err)
	if err2 != nil {
		logger.Error("Failed to create pool in database", "error", err2)
		// Check if it's a specific error that should be passed through
		if customerrors.IsConflictErr(err2) {
			return nil, "", err2
		}
		return nil, "", errors.New("unable to process request, please try again later")
	}

	if params.KmsConfig != nil {
		// move the kms config to in-use state
		if _, err = se.UpdateKmsConfigState(ctx, params.KmsConfig.UUID, models.LifeCycleStateInUse, models.LifeCycleStateInUseDetails); err != nil {
			logger.Error("Failed to update KMS config state to InUse", "KMSConfigID", params.KmsConfig.ID, "error", err)
			return nil, "", errors.New("unable to process request, please try again later")
		}
	}

	defer func() {
		if err != nil {
			if poolDeleteErr := se.DeletePool(ctx, dbPool); poolDeleteErr != nil {
				logger.Error("Failed to delete pool", "PoolID", dbPool.UUID, "error", poolDeleteErr)
			}
		}
	}()
	poolCategory := models.GetPoolCategory(params.LargeCapacity)
	jobType := string(models.GetResourceJobType(models.ResourceTypePool, models.ResourceOperationCreate, poolCategory))
	job := &datamodel.Job{
		Type:          jobType,
		State:         string(models.JobsStateNEW),
		ResourceName:  params.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: dbPool.UUID,
		},
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", errors.New("unable to process request, please try again later")
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		createdJob.WorkflowID,
		workflowengine.CustomerTaskQueue,
		workflows.CreatePoolWorkflow,
		params,
		dbPool,
	)
	if err != nil {
		logger.Error("Failed to start pool create workflow: ", "error", err)
		return nil, "", err
	}

	poolView := database.ConvertPoolToPoolView(dbPool)
	return convertDatastorePoolToModel(poolView, account.Name), createdJob.UUID, nil
}

func CreatePoolInDB(ctx context.Context, se database.Storage, params *commonparams.CreatePoolParams, account *datamodel.Account, logger log.Logger, err error) (*datamodel.Pool, error) {
	poolObj := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: utils.RandomUUID(),
		},
		Name:             params.Name,
		Account:          account,
		AccountID:        account.ID,
		VendorID:         params.VendorID,
		Network:          params.VendorSubNetID,
		SizeInBytes:      int64(params.SizeInBytes),
		AllowAutoTiering: params.AllowAutoTiering,
		Description:      params.Description,
		ServiceLevel:     params.ServiceLevel,
		QosType:          params.QosType,
		LargeCapacity:    params.LargeCapacity,
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			HotTierSizeInBytes:       int64(params.HotTierSizeInBytes),
			EnableHotTierAutoResize:  params.EnableHotTierAutoResize,
			TieringStatus:            datamodel.TieringStatusResumed,
			TieringFullnessThreshold: TieringFullnessThresholdOntapDefault,
		},
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: params.CustomPerformanceParams.ThroughputMibps,
			Iops:            *params.CustomPerformanceParams.Iops,
			PrimaryZone:     params.PrimaryZone,
			SecondaryZone:   params.SecondaryZone,
			Labels:          params.Labels,
			IsRegionalHA:    params.IsRegionalHA,
			LdapEnabled:     params.LdapEnabled,
		},
		APIAccessMode: params.Mode,
	}

	if params.KmsConfig != nil {
		poolObj.KmsConfigID = sql.NullInt64{
			Int64: params.KmsConfig.ID,
			Valid: true,
		}
	}

	if params.ActiveDirectoryId != "" {
		poolObj.ActiveDirectoryID = sql.NullInt64{
			Int64: params.ActiveDirectory.ID,
			Valid: true,
		}
	}

	poolObj.DeploymentName = utils.GenerateDeterministicDeploymentName(poolObj.AccountID, poolObj.UUID, params.Region)
	logger.Infof("generated deployment name: %s", poolObj.DeploymentName)

	userName := utils.GenerateUniqueUsername(poolObj.DeploymentName)
	switch env.AuthType {
	case env.USER_CERTIFICATE:
		poolObj.PoolCredentials = &datamodel.PoolCredentials{
			SecretID:      fmt.Sprintf("%s-secret", poolObj.DeploymentName),
			CertificateID: fmt.Sprintf("%s-cert", poolObj.DeploymentName),
			Password:      "",
			AuthType:      env.USER_CERTIFICATE,

			// Certificate-related configuration (stored from environment variables during pool creation)
			// Format: ca_pool_deployed_project_id/ca_pool_name/ca_name
			// Note: Region and VCPAdmin remain as environment variables
			CaURI:    env.BuildCaURI(env.CaPoolDeployedProjectID, env.CaPoolName, env.CaName),
			Username: fmt.Sprintf("%s%s", userName, VCP_ADMIN_CERT_UN_SUFFIX),
		}
	case env.USERNAME_PWD_SEC_MGR:
		poolObj.PoolCredentials = &datamodel.PoolCredentials{
			SecretID:      fmt.Sprintf("%s-secret", poolObj.DeploymentName),
			CertificateID: "",
			Password:      "",
			AuthType:      env.USERNAME_PWD_SEC_MGR,
			Username:      fmt.Sprintf("%s%s", userName, VCP_ADMIN_CERT_UN_SUFFIX),
		}
	default:
		poolObj.PoolCredentials = &datamodel.PoolCredentials{
			SecretID:      "",
			CertificateID: "",
			Password:      env.NodePassword,
			AuthType:      env.USERNAME_PWD,
			Username:      fmt.Sprintf("%s%s", userName, VCP_ADMIN_CERT_UN_SUFFIX),
		}
	}

	if params.Mode == workflows.ONTAPMode {
		poolObj.ExpertModeCredentials = createExpertModeUser(poolObj, env.ExpertModeUser)
	}
	dbPool, err := se.CreatingPool(ctx, poolObj)
	if err != nil {
		logger.Error("Failed to create pool in database", "error", err)
		return nil, err
	}
	return dbPool, nil
}

func createExpertModeUser(poolObj *datamodel.Pool, userName string) *datamodel.ExpertModeCredentials {
	switch env.AuthType {
	case env.USER_CERTIFICATE:
		return &datamodel.ExpertModeCredentials{
			ExpertModeCredential: []*datamodel.ExpertModeCredential{
				// first user id expert user
				{
					SecretID:      "",
					CertificateID: fmt.Sprintf("%s-cert-%s", poolObj.DeploymentName, userName),
					Password:      "",
					AuthType:      env.USER_CERTIFICATE,
					Username:      userName,
				},
			},
		}
	case env.USERNAME_PWD_SEC_MGR:
		return &datamodel.ExpertModeCredentials{
			ExpertModeCredential: []*datamodel.ExpertModeCredential{
				// first user id expert user
				{
					SecretID:      fmt.Sprintf("%s-secret-%s", poolObj.DeploymentName, userName),
					CertificateID: "",
					Password:      "",
					AuthType:      env.USERNAME_PWD_SEC_MGR,
					Username:      userName,
				},
			},
		}
	default:
		return &datamodel.ExpertModeCredentials{
			ExpertModeCredential: []*datamodel.ExpertModeCredential{
				// first user id expert user
				{
					SecretID:      "",
					CertificateID: "",
					// TODO change this to  expert mode password
					Password: env.NodePassword,
					AuthType: env.USERNAME_PWD,
					Username: userName,
				},
			},
		}
	}
}

// UpdatePool updates the specified pool
func (o *Orchestrator) UpdatePool(ctx context.Context, params *commonparams.UpdatePoolParams) (*models.Pool, string, error) {
	return updatePool(ctx, o.storage, o.temporal, params)
}

// _updatePool updates an existing pool
func _updatePool(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.UpdatePoolParams) (*models.Pool, string, error) {
	logger := util.GetLogger(ctx)
	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)
	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		return nil, "", err
	}

	// Get the pool by ID
	dbPoolView, err := se.GetPool(ctx, params.PoolId, account.ID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, "", customerrors.NewNotFoundErr("pool not found", nil)
		}
		return nil, "", err
	}
	dbPool := database.ConvertPoolViewToPool(dbPoolView)
	err = ValidateAndSetUpdatePoolParams(params, dbPool)
	if err != nil {
		return nil, "", err
	}

	if params.ActiveDirectoryConfigId != "" {
		activeDir, adErr := se.GetActiveDirectoryByUuidAndAccountId(ctx, params.ActiveDirectoryConfigId, account.ID)
		if adErr != nil {
			var notFoundErr *customerrors.NotFoundErr
			if errors.As(adErr, &notFoundErr) {
				return nil, "", customerrors.NewUserInputValidationErr(fmt.Sprintf("Active Directory Config with ID %s not found", params.ActiveDirectoryConfigId))
			}
			return nil, "", adErr
		}

		if dbPool.ActiveDirectoryID.Valid && dbPool.ActiveDirectoryID.Int64 != activeDir.ID {
			return nil, "", customerrors.NewUserInputValidationErr("Active Directory configuration cannot be changed for pools already associated with an Active Directory")
		}

		dbPool.ActiveDirectoryID = sql.NullInt64{
			Int64: activeDir.ID,
			Valid: true,
		}
	}

	poolCategory := models.GetPoolCategory(dbPool.LargeCapacity)
	job := &datamodel.Job{
		Type:          string(models.GetResourceJobType(models.ResourceTypePool, models.ResourceOperationUpdate, poolCategory)),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.PoolId,
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	var poolMarkedAsUpdating bool
	previousState := dbPool.State
	previousStateDetails := dbPool.StateDetails
	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
			// Mark pool in error state only if it was successfully marked as updating
			if poolMarkedAsUpdating {
				if _, poolErr := se.UpdatePoolState(ctx, dbPool, previousState, previousStateDetails); poolErr != nil {
					logger.Error("Failed to update pool state to ERROR", "poolID", dbPool.UUID, "error", poolErr)
				}
			}
		}
	}()

	pool, err := se.UpdatingPool(ctx, dbPool)
	if err != nil {
		return nil, "", err
	}

	poolMarkedAsUpdating = true
	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		createdJob.WorkflowID,
		workflowengine.CustomerTaskQueue,
		workflows.UpdatePoolWorkflow,
		params,
		pool,
		nil,
	)
	if err != nil {
		logger.Error("Failed to start pool update workflow: ", "error", err)
		return nil, "", err
	}
	poolView := database.ConvertPoolToPoolView(pool)
	poolModel := convertDatastorePoolToModel(poolView, account.Name)
	// Merge update params into the pool model for 202 response
	// Params already contain merged values (either new from request or existing from pool)
	mergedPoolModel := mergeUpdateParamsIntoPoolModel(poolModel, params)
	return mergedPoolModel, createdJob.UUID, nil
}

// mergeUpdateParamsIntoPoolModel merges update params into a pool model.
// Only auto tiering parameters and pool size are updated from params if they are provided.
// Reuses poolModel and only creates a shallow copy to avoid mutating the original.
func mergeUpdateParamsIntoPoolModel(poolModel *models.Pool, params *commonparams.UpdatePoolParams) *models.Pool {
	// Shallow copy poolModel to avoid mutating the original
	merged := *poolModel

	// Copy PoolAttributes to avoid mutating the original
	if merged.PoolAttributes != nil {
		poolAttributesCopy := *merged.PoolAttributes
		// Copy Labels map to avoid sharing the same map reference
		if poolAttributesCopy.Labels != nil {
			labelsCopy := make(map[string]string)
			for k, v := range poolAttributesCopy.Labels {
				labelsCopy[k] = v
			}
			poolAttributesCopy.Labels = labelsCopy
		}
		merged.PoolAttributes = &poolAttributesCopy
	}

	// Update pool size if provided in params
	if params.SizeInBytes > 0 {
		merged.SizeInBytes = params.SizeInBytes
	}

	// Update AutoTieringConfig only if auto tiering parameters are provided in params
	if params.AllowAutoTiering || params.HotTierSizeInBytes > 0 {
		merged.AllowAutoTiering = params.AllowAutoTiering
		// Create a copy of AutoTieringConfig if it exists, otherwise create new
		if merged.AutoTieringConfig != nil {
			// Shallow copy the existing config
			autoTieringConfigCopy := *merged.AutoTieringConfig
			merged.AutoTieringConfig = &autoTieringConfigCopy
		} else {
			// Create new AutoTieringConfig if it doesn't exist
			merged.AutoTieringConfig = &models.AutoTieringConfig{}
		}

		// Update only the fields that are provided in params
		if params.HotTierSizeInBytes > 0 {
			merged.AutoTieringConfig.HotTierSizeInBytes = params.HotTierSizeInBytes
		}
		merged.AutoTieringConfig.EnableHotTierAutoResize = params.EnableHotTierAutoResize
	}

	return &merged
}

// GetPool gets the specified pool
func (o *Orchestrator) DescribePool(ctx context.Context, poolId string, accountName string) (*models.Pool, error) {
	se := o.storage

	account, err := getAccountWithName(ctx, se, accountName)
	if err != nil {
		return nil, err
	}

	pool, err := se.DescribePool(ctx, poolId, account.ID)
	if err != nil {
		return nil, err
	}

	return convertDatastorePoolToModel(pool, pool.Account.Name), nil
}

// _validatePoolParams is a unified validation function that works for both create and update operations
func _validatePoolParams(perf *validators.CustomPerformance, serviceLevel string) error {
	// Validate service level for create operations (only needed for create)
	if serviceLevel != "" && serviceLevel != ServiceLevelNameFLEX {
		return customerrors.NewUserInputValidationErr("Given service level not supported. Supported service level is " + ServiceLevelNameFLEX)
	}

	// First validate common parameters
	if err := validators.ValidateCommonPoolParams(perf); err != nil {
		return err
	}

	// Then validate pool-specific parameters
	poolValidator := validators.NewPoolValidator(perf.LargeCapacity)
	pipeline := validators.NewValidationPipeline(poolValidator)
	return pipeline.Execute(perf)
}

// _validateCreatePoolParams now just builds CustomPerformance and calls the unified validator
func _validateCreatePoolParams(params *commonparams.CreatePoolParams) error {
	// Build CustomPerformance params first
	perf := validators.NewCustomPerformanceFromCreate(params)

	// Call unified validation with service level check
	err := ValidatePoolParams(perf, params.ServiceLevel)
	if err != nil {
		return err
	}

	if params.KmsConfig != nil {
		switch params.KmsConfig.State {
		case models.LifeCycleStateREADY, models.LifeCycleStateInUse:
			break
		default:
			return customerrors.NewUserInputValidationErr(fmt.Sprintf("invalid KMS configuration state: %s", params.KmsConfig.State))
		}
	}

	params.CustomPerformanceParams.Iops = perf.Iops
	return nil
}

// _validateAndSetUpdatePoolParams now just builds CustomPerformance and calls the unified validator
func _validateAndSetUpdatePoolParams(params *commonparams.UpdatePoolParams, pool *datamodel.Pool) error {
	// Auto_tier checks for update with existing pool values
	if params.AllowAutoTiering {
		if !pool.AllowAutoTiering && params.HotTierSizeInBytes < uint64(pool.SizeInBytes) {
			return customerrors.NewUserInputValidationErr("Given hot tier size is not supported. Hot tier size cannot be less than existing pool size")
		} else if pool.AllowAutoTiering && pool.AutoTieringConfig != nil && params.HotTierSizeInBytes < uint64(pool.AutoTieringConfig.HotTierSizeInBytes) {
			return customerrors.NewUserInputValidationErr("Given hot tier size is not supported. Hot tier size must be greater than existing hot tier size")
		}
	} else if pool.AllowAutoTiering {
		return customerrors.NewUserInputValidationErr("Auto tiering disable operation is not supported")
	}
	// Build CustomPerformance params first
	perf := validators.NewCustomPerformanceFromUpdate(params)
	perf.LargeCapacity = pool.LargeCapacity // Use existing pool type for validation
	// Prevent changing pool type
	if params.LargeCapacity != nil && (*params.LargeCapacity != pool.LargeCapacity) {
		return customerrors.NewUserInputValidationErr("Given large capacity value is not supported. Large capacity cannot be changed for existing pool")
	}

	// Call unified validation (no service level check needed for updates)
	err := ValidatePoolParams(perf, "")
	if err != nil {
		return err
	}
	params.TotalIops = perf.Iops
	return nil
}

// DeletePool deletes the specified pool
func (o *Orchestrator) DeletePool(ctx context.Context, params *commonparams.DeletePoolParams) (*models.Pool, string, error) {
	return deletePool(ctx, o.temporal, o.storage, params)
}

// _deletePool deletes the specified pool and its associated resources.
func _deletePool(ctx context.Context, temporal client.Client, se database.Storage, params *commonparams.DeletePoolParams) (*models.Pool, string, error) {
	logger := util.GetLogger(ctx)
	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)
	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		return nil, "", err
	}

	// Get the pool by ID
	pool, err := se.GetPool(ctx, params.PoolID, account.ID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, "", customerrors.NewNotFoundErr("pool not found", nil)
		}
		return nil, "", err
	}

	if pool.VolumeCount > 0 {
		return nil, "", customerrors.NewConflictErr("pool cannot be deleted with active volumes")
	}

	poolCategory := models.GetPoolCategory(pool.LargeCapacity)
	job := &datamodel.Job{
		Type:          string(models.GetResourceJobType(models.ResourceTypePool, models.ResourceOperationDelete, poolCategory)),
		State:         string(models.JobsStateNEW),
		ResourceName:  pool.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: pool.UUID,
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	dbpool := database.ConvertPoolViewToPool(pool)
	previousState := dbpool.State
	previousStateDetails := dbpool.StateDetails

	if err = se.DeletingPool(ctx, dbpool); err != nil {
		return nil, "", err
	}

	defer func() {
		if err != nil {
			if _, poolErr := se.UpdatePoolState(ctx, dbpool, previousState, previousStateDetails); poolErr != nil {
				logger.Error("Failed to update pool state to ERROR", "poolID", dbpool.UUID, "error", poolErr)
			}
		}
	}()

	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		createdJob.WorkflowID,
		workflowengine.CustomerTaskQueue,
		workflows.DeletePoolWorkflow,
		params,
		dbpool,
	)
	if err != nil {
		logger.Error("Failed to start pool delete workflow: ", "error", err)
		return nil, "", err
	}

	pool.State = dbpool.State
	pool.StateDetails = dbpool.StateDetails
	return convertDatastorePoolToModel(pool, account.Name), createdJob.UUID, nil
}

// ListPools returns list of pools belonging to the specified owner
func (o *Orchestrator) ListPools(ctx context.Context, accountName string, includeDeleted bool) ([]*models.Pool, error) {
	se := o.storage

	account, err := getAccountWithName(ctx, se, accountName)
	if err != nil {
		return nil, err
	}

	filter := utils2.CreateFilterWithConditions(utils2.NewFilterCondition("account_id", "=", account.ID))
	filter.SetIncludeDeleted(includeDeleted)
	pools, err := se.ListPools(ctx, filter)
	if err != nil {
		return nil, err
	}

	return convertDatastorePoolsToModel(pools, account.Name), nil
}

// ListAllPools returns list of non-deleted pools
func (o *Orchestrator) ListAllPools(ctx context.Context) ([]*models.Pool, error) {
	se := o.storage

	pools, err := se.ListPools(ctx, nil)
	if err != nil {
		return nil, err
	}

	return convertDatastorePoolsToModelWithoutAccountNameParam(pools), nil
}

// GetMultiplePools returns multiple pools with uuids belonging to the specified owner
func (o *Orchestrator) GetMultiplePools(ctx context.Context, accountName string, poolUUIDs []string) ([]*models.Pool, error) {
	se := o.storage

	account, err := getAccountWithName(ctx, se, accountName)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			util.GetLogger(ctx).Warnf("Account with name %s not found in VCP, checking in CVP", accountName)
			return []*models.Pool{}, nil
		}
		return nil, err
	}

	filter := utils2.CreateFilterWithConditions(
		utils2.NewFilterCondition("uuid", "in", poolUUIDs),
		utils2.NewFilterCondition("account_id", "=", account.ID))
	pools, err := se.ListPools(ctx, filter)
	if err != nil {
		return nil, err
	}

	return convertDatastorePoolsToModel(pools, account.Name), nil
}

// GetPoolByVendorID retrieves a pool by its VendorID.
func (o *Orchestrator) GetPoolByVendorID(ctx context.Context, vendorID string, accountName string) (*models.Pool, error) {
	se := o.storage
	account, err := getOrCreateAccount(ctx, se, accountName)
	if err != nil {
		return nil, err
	}
	pool, err := se.GetPoolByVendorID(ctx, vendorID, account.ID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, customerrors.NewNotFoundErr("pool not found", nil)
		}
		return nil, err
	}
	return convertDatastorePoolToModel(pool, pool.Account.Name), nil
}

// GetPoolByName retrieves a pool with the specified name and owner.
func (o *Orchestrator) GetPoolByName(ctx context.Context, poolName string, accountName string, queryDepth int) (*models.Pool, error) {
	return GetPoolByName(ctx, o.storage, poolName, accountName, queryDepth)
}

func _getPoolByName(ctx context.Context, se database.Storage, poolName string, accountName string, queryDepth int) (*models.Pool, error) {
	account, err := getAccountWithName(ctx, se, accountName)
	if err != nil {
		return nil, err
	}

	conditions := [][]interface{}{{"name = ?", poolName}}
	conditions = append(conditions, []interface{}{"account_id = ?", account.ID})
	pools, err := se.GetPoolByName(ctx, conditions)
	if err != nil {
		return nil, err
	}

	if queryDepth > 0 {
		return convertDatastorePoolToModelWithClusterDetails(pools, accountName), nil
	}

	return convertDatastorePoolToModel(pools, accountName), nil
}

func convertDatastorePoolToModelWithClusterDetails(pools *datamodel.PoolView, accountName string) *models.Pool {
	pool := convertDatastorePoolToModel(pools, accountName)

	pool.ClusterDetails = &models.ClusterDetails{
		InterClusterLifs:      pools.ClusterDetails.InterclusterLifIPs,
		ExternalName:          pools.ClusterDetails.ExternalName,
		Network:               pools.ClusterDetails.Network,
		SubnetNames:           pools.ClusterDetails.SubnetNames,
		RegionalTenantProject: pools.ClusterDetails.RegionalTenantProject,
		SnHostProject:         pools.ClusterDetails.SnHostProject,
	}

	return pool
}

// CustomPerformanceParams is used to specify the custom performance parameters for a pool
type CustomPerformanceParams struct {
	Enabled    bool
	Throughput float64
	Iops       int64
}

func convertDatastorePoolsToModel(pools []*datamodel.PoolView, accountName string) []*models.Pool {
	var poolsList []*models.Pool
	for _, pool := range pools {
		p := convertDatastorePoolToModel(pool, accountName)
		poolsList = append(poolsList, p)
	}
	return poolsList
}

func convertDatastorePoolsToModelWithoutAccountNameParam(pools []*datamodel.PoolView) []*models.Pool {
	var poolsList []*models.Pool
	for _, pool := range pools {
		accountName := pool.Account.Name
		p := convertDatastorePoolToModel(pool, accountName)
		poolsList = append(poolsList, p)
	}
	return poolsList
}

func convertDatastorePoolToModel(pool *datamodel.PoolView, accountName string) *models.Pool {
	logger := util.GetLogger(context.Background())
	ldapEnabled := false
	if pool.PoolAttributes != nil {
		ldapEnabled = pool.PoolAttributes.LdapEnabled
	}
	logger.Infof("LDAP enabled: %v", ldapEnabled)
	labels := make(map[string]string)
	if pool.PoolAttributes != nil && pool.PoolAttributes.Labels != nil {
		labels = convertJSONBToMap(pool.PoolAttributes.Labels)
	}

	var autoTieringConfig *models.AutoTieringConfig
	if pool.AutoTieringConfig != nil {
		autoTieringConfig = &models.AutoTieringConfig{
			HotTierSizeInBytes:      uint64(pool.AutoTieringConfig.HotTierSizeInBytes),
			EnableHotTierAutoResize: pool.AutoTieringConfig.EnableHotTierAutoResize,
			HotTierConsumption:      pool.AutoTieringConfig.HotTierConsumption,
			ColdTierConsumption:     pool.AutoTieringConfig.ColdTierConsumption,
		}
	}

	poolRes := &models.Pool{
		BaseModel: models.BaseModel{
			UUID:      pool.UUID,
			CreatedAt: pool.CreatedAt,
			UpdatedAt: pool.UpdatedAt,
			DeletedAt: DeletedAtOrNil(pool.DeletedAt),
		},
		AccountName:      accountName,
		Name:             pool.Name,
		Description:      pool.Description,
		SizeInBytes:      uint64(pool.SizeInBytes),
		State:            pool.State,
		StateDetails:     pool.StateDetails,
		AllowAutoTiering: pool.AllowAutoTiering,
		VendorSubNetID:   pool.Network,
		ServiceLevel:     pool.ServiceLevel,
		QosType:          pool.QosType,
		DeploymentName:   pool.DeploymentName,
		LargeCapacity:    pool.LargeCapacity,
		PoolAttributes: &models.PoolAttributes{
			AllocatedBytes:  float64(pool.QuotaInBytes),
			NumberOfVolumes: pool.VolumeCount,
			PrimaryZone:     pool.PoolAttributes.PrimaryZone,
			SecondaryZone:   pool.PoolAttributes.SecondaryZone,
			Labels:          labels,
			IsRegionalHA:    pool.PoolAttributes.IsRegionalHA,
			LdapEnabled:     ldapEnabled,
		},
		AutoTieringConfig: autoTieringConfig,
		CustomPerformanceParams: &models.CustomPerformanceParams{
			Enabled:    true,
			Throughput: float64(pool.PoolAttributes.ThroughputMibps),
			Iops:       pool.PoolAttributes.Iops,
		},
		Account: &models.Account{
			Name: accountName,
		},
		SatisfiesPzi:  pool.SatisfyZI,
		SatisfiesPzs:  pool.SatisfyZS,
		APIAccessMode: pool.APIAccessMode,
	}

	if pool.Account != nil && &pool.Account.ID != nil {
		poolRes.Account.ID = pool.Account.ID
	}

	if pool.ActiveDirectory != nil {
		poolRes.ActiveDirectoryConfigId = pool.ActiveDirectory.UUID
		poolRes.ActiveDirectoryResourceId = pool.ActiveDirectory.AdName
	}

	if pool.KmsConfig != nil {
		poolRes.KmsConfig = &models.KmsConfig{
			BaseModel: models.BaseModel{
				UUID:      pool.KmsConfig.UUID,
				CreatedAt: pool.KmsConfig.CreatedAt,
				UpdatedAt: pool.KmsConfig.UpdatedAt,
				DeletedAt: DeletedAtOrNil(pool.KmsConfig.DeletedAt),
			},
			Name:              pool.KmsConfig.Name,
			Description:       pool.KmsConfig.Description,
			State:             pool.KmsConfig.State,
			StateDetails:      pool.KmsConfig.StateDetails,
			KeyRing:           pool.KmsConfig.KeyRing,
			KeyRingLocation:   pool.KmsConfig.KeyRingLocation,
			KeyName:           pool.KmsConfig.KeyName,
			AccountID:         pool.KmsConfig.AccountID,
			CustomerProjectID: pool.KmsConfig.CustomerProjectID,
			KeyProjectID:      pool.KmsConfig.KeyProjectID,
			ResourceID:        pool.KmsConfig.ResourceID,
		}
	}
	if pool.AssetMetadata != nil {
		poolRes.AssetMetadata = &models.AssetMetadata{}
		for _, originalPoolAssetMetadata := range pool.AssetMetadata.ChildAssets {
			var assetMetadata models.ChildAsset
			assetMetadata.AssetNames = originalPoolAssetMetadata.AssetNames
			assetMetadata.AssetType = originalPoolAssetMetadata.AssetType
			poolRes.AssetMetadata.ChildAssets = append(poolRes.AssetMetadata.ChildAssets, assetMetadata)
		}
	}
	return poolRes
}

func DeletedAtOrNil(deletedAt *gorm.DeletedAt) *time.Time {
	if deletedAt != nil && deletedAt.Valid {
		return &deletedAt.Time
	}
	return nil
}

func convertJSONBToMap(jsonb *datamodel.JSONB) map[string]string {
	result := make(map[string]string)
	if jsonb == nil {
		return result
	}

	for k, v := range *jsonb {
		// attempt a type assertion
		if strVal, ok := v.(string); ok {
			result[k] = strVal
		} else {
			// fallback: convert using fmt.Sprintf
			result[k] = fmt.Sprintf("%v", v)
		}
	}
	return result
}

func (o *Orchestrator) GetExpertModePoolCreds(ctx context.Context, poolUUID string, accountName string, userName string) (*models.UserCredentials, error) {
	se := o.storage

	account, err := getAccountWithName(ctx, se, accountName)
	if err != nil {
		return nil, err
	}
	pool, err := se.GetPool(ctx, poolUUID, account.ID)
	if err != nil {
		return nil, err
	}

	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return nil, err
	}

	if pool.ExpertModeCredentials == nil || pool.ExpertModeCredentials.ExpertModeCredential == nil || len(pool.ExpertModeCredentials.ExpertModeCredential) == 0 {
		return nil, nil
	}

	for _, expertModeCredential := range pool.ExpertModeCredentials.ExpertModeCredential {
		if expertModeCredential.Username == userName {
			useHostDNS := expertModeCredential.AuthType == env.USER_CERTIFICATE
			endpointMappings := buildOntapEndpoints(nodes, useHostDNS)
			return &models.UserCredentials{
				SecretID:       expertModeCredential.SecretID,
				CertificateID:  expertModeCredential.CertificateID,
				Password:       expertModeCredential.Password,
				AuthType:       expertModeCredential.AuthType,
				OntapEndpoints: endpointMappings,
				CaURI:          pool.PoolCredentials.GetCaURIWithFallback(),
			}, nil
		}
	}

	return nil, errors.New("expert mode user not found")
}

func buildOntapEndpoints(nodes []*datamodel.Node, useHostDNS bool) []models.OntapEndpoint {
	var endpointMappings []models.OntapEndpoint
	for _, node := range nodes {
		mapping := models.OntapEndpoint{
			IP: node.EndpointAddress,
		}
		if useHostDNS {
			mapping.DNS = node.HostDNSName
		} else {
			mapping.DNS = node.EndpointAddress
		}
		endpointMappings = append(endpointMappings, mapping)
	}
	return endpointMappings
}
