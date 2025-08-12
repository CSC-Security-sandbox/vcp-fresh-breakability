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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"gorm.io/gorm"
)

var (
	minQuotaInBytesPool      = env.GetUint64("MIN_QUOTA_IN_BYTES_POOL", 2*TibInBytes)   // 2TiB
	maxQuotaInBytesPool      = env.GetUint64("MAX_QUOTA_IN_BYTES_POOL", 500*TibInBytes) // 500TiB
	minCustomThroughput      = env.GetUint64("MIN_CUSTOM_THROUGHPUT", 64)               // 64 MiBps
	minCustomIops            = env.GetUint64("MIN_CUSTOM_IOPS", 1024)
	minSizeGranularity       = env.GetUint64("MIN_SIZE_GRANULARITY", GibInBytes) // 1 GiB
	createPool               = _createPool
	updatePool               = _updatePool
	ValidateCreatePoolParams = _validateCreatePoolParams
	ValidateUpdatePoolParams = _validateUpdatePoolParams
	deletePool               = _deletePool
	GetPoolByName            = _getPoolByName
	autoTieringEnabled       = env.GetBool("AUTO_TIERING_ENABLED", false)
)

const (
	ServiceLevelNameFLEX = "FLEX"
	QosTypeAuto          = "auto"
	GibInBytes           = 1073741824
	TibInBytes           = 1099511627776
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

	job := &datamodel.Job{
		Type:          string(models.JobTypeCreatePool),
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
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			HotTierSizeInBytes:      int64(params.HotTierSizeInBytes),
			EnableHotTierAutoResize: params.EnableHotTierAutoResize,
		},
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: params.CustomPerformanceParams.ThroughputMibps,
			Iops:            params.CustomPerformanceParams.Iops,
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
		return nil, "", err
	}

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
	err = ValidateUpdatePoolParams(params, dbPool)
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

	pool, err := se.UpdatingPool(ctx, dbPool)
	if err != nil {
		return nil, "", err
	}
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

func _validateCreatePoolParams(params *commonparams.CreatePoolParams) error {
	if params.ServiceLevel != ServiceLevelNameFLEX {
		return customerrors.NewUserInputValidationErr("Given service level not supported. Supported service level is " + ServiceLevelNameFLEX)
	}

	if minQuotaInBytesPool > params.SizeInBytes {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf("Given pool size not supported. Pool size must be greater than %s and a multiple of 1GiB", utils.FmtUint64Bytes(minQuotaInBytesPool)))
	}

	if params.SizeInBytes > maxQuotaInBytesPool {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf("Given pool size not supported. Pool size must be less than %s", utils.FmtUint64Bytes(maxQuotaInBytesPool)))
	}

	if params.SizeInBytes%minSizeGranularity != 0 {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf("Given pool size must be a multiple of %s", utils.FmtUint64Bytes(minSizeGranularity)))
	}

	if params.QosType != QosTypeAuto {
		return customerrors.NewUserInputValidationErr("Given QoS type not supported for Unified Flex Storage Pool. Supported QoS type is " + QosTypeAuto)
	}

	// CustomPerformanceParams is always set in endpoints layer
	if minCustomThroughput > uint64(params.CustomPerformanceParams.ThroughputMibps) {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf("TotalThroughputMibps must be set and must be greater than %d MiBps for Unified Flex Storage Pool", minCustomThroughput))
	}

	if minCustomIops > uint64(params.CustomPerformanceParams.Iops) {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf("TotalIops must be greater than %d for Unified Flex Storage Pool", minCustomIops))
	}
	return nil
}

func _validateUpdatePoolParams(params *commonparams.UpdatePoolParams, pool *datamodel.Pool) error {
	if pool.QosType == QosTypeAuto && params.QosType != QosTypeAuto {
		return customerrors.NewUserInputValidationErr("Cannot change qos type from auto to manual")
	}

	if minQuotaInBytesPool > params.SizeInBytes {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf("Given pool size not supported. Pool size must be greater than %s and a multiple of 1GiB", utils.FmtUint64Bytes(minQuotaInBytesPool)))
	}

	if params.SizeInBytes > maxQuotaInBytesPool {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf("Given pool size not supported. Pool size must be less than %s", utils.FmtUint64Bytes(maxQuotaInBytesPool)))
	}

	if params.SizeInBytes%minSizeGranularity != 0 {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf("Given pool size must be a multiple of %s", utils.FmtUint64Bytes(minSizeGranularity)))
	}

	if !autoTieringEnabled && (params.AllowAutoTiering || params.HotTierSizeInBytes > 0) {
		return customerrors.NewUserInputValidationErr("Auto-Tiering feature is currently not enabled.")
	}

	if params.AllowAutoTiering {
		if !pool.AllowAutoTiering && params.HotTierSizeInBytes < uint64(pool.SizeInBytes) {
			return customerrors.NewUserInputValidationErr("Given hot tier size is not supported. Hot tier size cannot be less than existing pool size")
		} else if pool.AllowAutoTiering && pool.AutoTieringConfig != nil && params.HotTierSizeInBytes < uint64(pool.AutoTieringConfig.HotTierSizeInBytes) {
			return customerrors.NewUserInputValidationErr("Given hot tier size is not supported. Hot tier size must be greater than existing hot tier size")
		}
	} else if pool.AllowAutoTiering {
		return customerrors.NewUserInputValidationErr("Auto tiering disable operation is not supported")
	}

	if minCustomThroughput > uint64(params.TotalThroughputMibps) {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf("TotalThroughputMibps must be set and must be greater than %d MiBps for Unified Flex Storage Pool", minCustomThroughput))
	}

	if minCustomIops > uint64(params.TotalIops) {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf("TotalIops must be greater than %d for Unified Flex Storage Pool", minCustomIops))
	}

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
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}
	dbpool := database.ConvertPoolViewToPool(pool)
	if err = se.DeletingPool(ctx, dbpool); err != nil {
		return nil, "", err
	}

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

func (o *Orchestrator) GetAccount(ctx context.Context, accountName string) (*datamodel.Account, error) {
	se := o.storage
	account, err := getAccountWithName(ctx, se, accountName)
	if err != nil {
		return nil, err
	}
	if account.DeletedAt != nil || account.State == models.AccountStateDisabled {
		return nil, customerrors.NewNotFoundErr("account not found or disabled", nil)
	}
	return account, nil
}
