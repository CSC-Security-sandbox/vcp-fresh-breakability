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
	"go.temporal.io/api/enums/v1"
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
	ServiceLevelNameFLEX = "FLEX"
	QosTypeAuto          = "auto"
)

// CreatePool creates the specified pool and adds it to the list of pools belonging to the specified owner
func (o *Orchestrator) CreatePool(ctx context.Context, params *commonparams.CreatePoolParams) (*models.Pool, string, error) {
	return createPool(ctx, o.storage, o.temporal, params)
}

// createPool creates a new pool and triggers asynchronous creation processes.
func _createPool(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.CreatePoolParams) (*models.Pool, string, error) {
	logger := util.GetLogger(ctx)

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

	defer func() {
		if err != nil {
			if _, jobErr := se.UpdatePoolState(ctx, dbPool, string(models.JobsStateERROR), err.Error()); jobErr != nil {
				logger.Error("Failed to update pool status to error", "PoolID", dbPool.UUID, "error", jobErr)
			}
		}
	}()

	job := &datamodel.Job{
		Type:          string(models.JobTypeCreatePool),
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

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
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
			HotTierSizeInBytes:      int64(params.HotTierSizeInBytes),
			EnableHotTierAutoResize: params.EnableHotTierAutoResize,
		},
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: params.CustomPerformanceParams.ThroughputMibps,
			Iops:            *params.CustomPerformanceParams.Iops,
			PrimaryZone:     params.PrimaryZone,
			SecondaryZone:   params.SecondaryZone,
			Labels:          params.Labels,
			IsRegionalHA:    params.IsRegionalHA,
		},
	}
	poolObj.DeploymentName = utils.GenerateDeterministicDeploymentName(poolObj.AccountID, poolObj.UUID, params.Region)
	logger.Infof("generated deployment name: %s", poolObj.DeploymentName)
	switch env.AuthType {
	case env.USER_CERTIFICATE:
		poolObj.PoolCredentials = &datamodel.PoolCredentials{
			SecretID:      fmt.Sprintf("%s-secret", poolObj.DeploymentName),
			CertificateID: fmt.Sprintf("%s-cert", poolObj.DeploymentName),
			Password:      "",
			AuthType:      env.USER_CERTIFICATE,
		}
	case env.USERNAME_PWD_SEC_MGR:
		poolObj.PoolCredentials = &datamodel.PoolCredentials{
			SecretID:      fmt.Sprintf("%s-secret", poolObj.DeploymentName),
			CertificateID: "",
			Password:      "",
			AuthType:      env.USERNAME_PWD_SEC_MGR,
		}
	default:
		poolObj.PoolCredentials = &datamodel.PoolCredentials{
			SecretID:      "",
			CertificateID: "",
			Password:      env.NodePassword,
			AuthType:      env.USERNAME_PWD,
		}
	}

	dbPool, err := se.CreatingPool(ctx, poolObj)
	if err != nil {
		logger.Error("Failed to create pool in database", "error", err)
		return nil, err
	}
	return dbPool, nil
}

// UpdatePool updates the specified pool
func (o *Orchestrator) UpdatePool(ctx context.Context, params *commonparams.UpdatePoolParams) (*models.Pool, string, error) {
	return updatePool(ctx, o.storage, o.temporal, params)
}

// _updatePool updates an existing pool
func _updatePool(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.UpdatePoolParams) (*models.Pool, string, error) {
	logger := util.GetLogger(ctx)
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

	job := &datamodel.Job{
		Type:         string(models.JobTypeUpdatePool),
		State:        string(models.JobsStateNEW),
		ResourceName: params.PoolId,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
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
	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		workflows.UpdatePoolWorkflow,
		params, // this contains the parameters for the update operation
		pool,
	)
	if err != nil {
		logger.Error("Failed to start pool update workflow: ", "error", err)
		return nil, "", err
	}
	poolView := database.ConvertPoolToPoolView(pool)
	return convertDatastorePoolToModel(poolView, account.Name), createdJob.UUID, nil
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

	job := &datamodel.Job{
		Type:          string(models.JobTypeDeletePool),
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
	if err = se.DeletingPool(ctx, dbpool); err != nil {
		return nil, "", err
	}

	previousState := dbpool.State
	previousStateDetails := dbpool.StateDetails
	defer func() {
		if err != nil {
			if _, poolErr := se.UpdatePoolState(ctx, dbpool, previousState, previousStateDetails); poolErr != nil {
				logger.Error("Failed to update pool state to ERROR", "poolID", dbpool.UUID, "error", poolErr)
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
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

	pool.ClusterAttributes = &models.ClusterAttributes{
		InterClusterLifs: pools.ClusterDetails.InterclusterLifIPs,
		ExternalName:     pools.ClusterDetails.ExternalName,
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
	labels := make(map[string]string)
	if pool.PoolAttributes != nil && pool.PoolAttributes.Labels != nil {
		labels = convertJSONBToMap(pool.PoolAttributes.Labels)
	}

	var autoTieringConfig *models.AutoTieringConfig
	if pool.AutoTieringConfig != nil {
		autoTieringConfig = &models.AutoTieringConfig{
			HotTierSizeInBytes:      uint64(pool.AutoTieringConfig.HotTierSizeInBytes),
			EnableHotTierAutoResize: pool.AutoTieringConfig.EnableHotTierAutoResize,
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
		},
		AutoTieringConfig: autoTieringConfig,
		CustomPerformanceParams: &models.CustomPerformanceParams{
			Enabled:    true,
			Throughput: float64(pool.PoolAttributes.ThroughputMibps),
			Iops:       pool.PoolAttributes.Iops,
		},
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

func (o *Orchestrator) GetExpertModePoolCreds(ctx context.Context, poolId string, accountName string, userName string) (*models.UserCredentials, error) {
	se := o.storage

	account, err := getAccountWithName(ctx, se, accountName)
	if err != nil {
		return nil, err
	}

	pool, err := se.DescribePool(ctx, poolId, account.ID)
	if err != nil {
		return nil, err
	}

	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return nil, err
	}

	// TODO: Return expert mode credentials when VLM changes are available
	if pool.PoolCredentials == nil {
		return nil, nil
	}

	useHostDNS := pool.PoolCredentials.AuthType == env.USER_CERTIFICATE
	endpointMappings := buildOntapEndpoints(nodes, useHostDNS)

	result := &models.UserCredentials{
		SecretID:       pool.PoolCredentials.SecretID,
		CertificateID:  pool.PoolCredentials.CertificateID,
		Password:       pool.PoolCredentials.Password,
		AuthType:       pool.PoolCredentials.AuthType,
		OntapEndpoints: endpointMappings,
	}

	return result, nil
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
