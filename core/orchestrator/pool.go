package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/repository"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
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
	minQuotaInBytesPool          = env.GetUint64("MIN_QUOTA_IN_BYTES_POOL", 2199023255552) // 2TiB
	createPool                   = _createPool
	ValidateCreatePoolParams     = _validateCreatePoolParams
	deletePool                   = _deletePool
	nodeUsername                 = env.GetString("VSA_NODE_USERNAME", "")
	nodePassword                 = env.GetString("VSA_NODE_PASSWORD", "")
	getInterClusterLifsFromONTAP = _getInterClusterLifsFromONTAP
	GetPoolByName                = _getPoolByName
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
	job := &datamodel.Job{
		Type:         string(models.JobTypeCreatePool),
		State:        string(models.JobsStateNEW),
		ResourceName: params.Name,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}
	poolObj := &datamodel.Pool{
		Name:                    params.Name,
		Account:                 account,
		AccountID:               account.ID,
		VendorID:                params.VendorID,
		Network:                 params.VendorSubNetID,
		SizeInBytes:             int64(params.SizeInBytes),
		AllowAutoTiering:        params.AllowAutoTiering,
		HotTierSizeInBytes:      int64(params.HotTierSizeInBytes),
		EnableHotTierAutoResize: params.EnableHotTierAutoResize,
		Description:             params.Description,
		ServiceLevel:            params.ServiceLevel,
		QosType:                 params.QosType,
		Username:                nodeUsername,
		Password:                nodePassword,
	}
	dbPool, err := se.CreatingPool(ctx, poolObj)
	if err != nil {
		return nil, "", err
	}
	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		workflows.CreatePoolWorkflow,
		params,
		dbPool,
	)
	if err != nil {
		logger.Error("Failed to start pool create workflow: ", "error", err)
		return nil, "", err
	}
	poolView := repository.ConvertPoolToPoolView(dbPool)
	return convertDatastorePoolToModel(poolView, account.Name), createdJob.UUID, nil
}

// GetPool gets the specified pool
func (o *Orchestrator) GetPool(ctx context.Context, poolId string, accountName string) (*models.Pool, error) {
	se := o.storage

	account, err := getAccountWithName(ctx, se, accountName)
	if err != nil {
		return nil, err
	}

	pool, err := se.GetPool(ctx, poolId, account.ID)
	if err != nil {
		return nil, err
	}

	return convertDatastorePoolToModel(pool, pool.Account.Name), nil
}

func _validateCreatePoolParams(params *commonparams.CreatePoolParams) error {
	if params.SizeInBytes < minQuotaInBytesPool {
		return customerrors.NewUserInputValidationErr("Given pool size not supported. Pool size can't be less than " + utils.FmtUint64Bytes(minQuotaInBytesPool))
	}

	if params.ServiceLevel != ServiceLevelNameFLEX {
		return customerrors.NewUserInputValidationErr("Given service level not supported. Supported service level is " + ServiceLevelNameFLEX)
	}

	if params.QosType != QosTypeAuto {
		return customerrors.NewUserInputValidationErr("Given QoS type not supported. Supported QoS type is " + QosTypeAuto)
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

	volumeCount, err := se.GetVolumeCountByPoolID(ctx, pool.ID)
	if err != nil {
		return nil, "", err
	}
	if volumeCount > 0 {
		return nil, "", customerrors.NewConflictErr("pool cannot be deleted with active volumes")
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeDeletePool),
		State:        string(models.JobsStateNEW),
		ResourceName: pool.Name,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}
	dbpool := repository.ConvertPoolViewToPool(pool)
	if err = se.DeletingPool(ctx, dbpool); err != nil {
		return nil, "", err
	}

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		workflows.DeletePoolWorkflow,
		params,
		pool,
	)
	if err != nil {
		logger.Error("Failed to start pool create workflow: ", "error", err)
		return nil, "", err
	}

	return convertDatastorePoolToModel(pool, account.Name), createdJob.UUID, nil
}

// ListPools returns list of pools belonging to the specified owner
func (o *Orchestrator) ListPools(ctx context.Context, accountName string) ([]*models.Pool, error) {
	se := o.storage

	account, err := getAccountWithName(ctx, se, accountName)
	if err != nil {
		return nil, err
	}

	conditions := [][]interface{}{{"account_id = ?", account.ID}}
	pools, err := se.ListPools(ctx, conditions)
	if err != nil {
		return nil, err
	}

	return convertDatastorePoolsToModel(pools, account.Name), nil
}

// GetMultiplePools returns multiple pools with uuids belonging to the specified owner
func (o *Orchestrator) GetMultiplePools(ctx context.Context, accountName string, poolUUIDs []string) ([]*models.Pool, error) {
	se := o.storage

	account, err := getAccountWithName(ctx, se, accountName)
	if err != nil {
		return nil, err
	}

	conditions := [][]interface{}{{"uuid in (?)", poolUUIDs}}
	conditions = append(conditions, []interface{}{"account_id = ?", account.ID})
	pools, err := se.ListPools(ctx, conditions)
	if err != nil {
		return nil, err
	}

	return convertDatastorePoolsToModel(pools, account.Name), nil
}

// GetPoolByVendorID retrieves a pool by its VendorID.
func (o *Orchestrator) GetPoolByVendorID(ctx context.Context, vendorID string) (*models.Pool, error) {
	se := o.storage
	pool, err := se.GetPoolByVendorID(ctx, vendorID)
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
	logger := util.GetLogger(ctx)
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

	nodes, err := se.GetNodesByPoolID(ctx, pools.ID)
	if err != nil {
		return nil, err
	}

	if len(nodes) == 0 {
		return nil, customerrors.NewNotFoundErr("node", nil)
	}

	if queryDepth > 0 {
		interClusterLifs, err := getInterClusterLifsFromONTAP(ctx, nodes, pools)
		if err != nil {
			logger.Error("Failed to get interCluster lifs", "error", err)
			return nil, err
		}

		return convertDatastorePoolToModelWithIClifdetails(pools, interClusterLifs, accountName), nil
	}

	return convertDatastorePoolToModel(pools, pools.Name), nil
}

// getInterClusterLifFromONTAP retrieves inter-cluster LIFs from ONTAP.
func _getInterClusterLifsFromONTAP(ctx context.Context, nodes []*datamodel.Node, pools *datamodel.PoolView) ([]*vsa.InterclusterLif, error) {
	logger := util.GetLogger(ctx)
	node := prepareNodeForProvider(nodes[0], pools)
	provider := GetProviderByNode(ctx, node)

	interClusterLifs, err := provider.GetInterclusterLIFs("default-intercluster")
	if err != nil {
		logger.Error("Failed to get interCluster lifs", "error", err)
		return nil, err
	}
	return interClusterLifs, nil
}

func prepareNodeForProvider(nodes *datamodel.Node, pools *datamodel.PoolView) *models.Node {
	return &models.Node{
		Name:            nodes.Name,
		EndpointAddress: nodes.EndpointAddress,
		Username:        pools.Username,
		Password:        pools.Password,
		Zone:            nodes.ZoneName,
		InstanceType:    nodes.NodeAttributes.InstanceType,
	}
}

func convertDatastorePoolToModelWithIClifdetails(pools *datamodel.PoolView, interClusterLifs []*vsa.InterclusterLif, accountName string) *models.Pool {
	pool := convertDatastorePoolToModel(pools, accountName)

	var icLifs []string
	for _, icLif := range interClusterLifs {
		icLifs = append(icLifs, string(icLif.Address))
	}

	pool.ClusterAttributes = &models.ClusterAttributes{
		InterClusterLifs: icLifs,
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

func convertDatastorePoolToModel(pool *datamodel.PoolView, accountName string) *models.Pool {
	return &models.Pool{
		BaseModel: models.BaseModel{
			UUID:      pool.UUID,
			CreatedAt: pool.CreatedAt,
			UpdatedAt: pool.UpdatedAt,
			DeletedAt: DeletedAtOrNil(pool.DeletedAt),
		},
		AccountName:             accountName,
		Name:                    pool.Name,
		Description:             pool.Description,
		SizeInBytes:             uint64(pool.SizeInBytes),
		State:                   pool.State,
		StateDetails:            pool.StateDetails,
		AllowAutoTiering:        pool.AllowAutoTiering,
		VendorSubNetID:          pool.Network,
		ServiceLevel:            pool.ServiceLevel,
		QosType:                 pool.QosType,
		HotTierSizeInBytes:      uint64(pool.HotTierSizeInBytes),
		EnableHotTierAutoResize: pool.EnableHotTierAutoResize,
		PoolAttributes: &models.PoolAttributes{
			AllocatedBytes:  float64(pool.QuotaInBytes),
			NumberOfVolumes: pool.VolumeCount,
		},
	}
}

func DeletedAtOrNil(deletedAt *gorm.DeletedAt) *time.Time {
	if deletedAt != nil && deletedAt.Valid {
		return &deletedAt.Time
	}
	return nil
}
