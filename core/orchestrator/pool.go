package orchestrator

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"gorm.io/gorm"
)

var (
	minQuotaInBytesPool      = env.GetUint64("MIN_QUOTA_IN_BYTES_POOL", 2199023255552) // 2TiB
	createPool               = _createPool
	createPoolAsync          = _createPoolAsync
	validateCreatePoolParams = _validateCreatePoolParams
)

// CreatePool creates the specified pool and adds it to the list of pools belonging to the specified owner
func (o *Orchestrator) CreatePool(ctx context.Context, params *CreatePoolParams) (*models.Pool, error) {
	return createPool(ctx, o.storage, params)
}

func _createPool(ctx context.Context, se database.Storage, params *CreatePoolParams) (*models.Pool, error) {
	account, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return nil, err
	}

	err = validateCreatePoolParams(se, params)
	if err != nil {
		return nil, err
	}

	dbPool := &datamodel.Pool{
		Name:         params.Name,
		Account:      account,
		AccountID:    account.ID,
		VendorID:     params.VendorID,
		Network:      params.VendorSubNetID,
		SizeInBytes:  int64(params.SizeInBytes),
		CoolAccess:   params.CoolAccess,
		Description:  params.Description,
		ServiceLevel: params.ServiceLevel,
	}
	pool, err := se.CreatePool(ctx, dbPool)
	if err != nil {
		return nil, err
	}

	go func() {
		err := createPoolAsync(ctx, se, params)
		if err != nil {
			return
		}
	}()
	// implement storage engine and data store separately
	// use the params and do operations in db or call temporal workflow
	return convertDatastorePoolToModel(pool, account.Name), nil
}

func _createPoolAsync(ctx context.Context, se database.Storage, params *CreatePoolParams) error {
	clusterName := params.Name + "vsa"
	vsaCluster, err := common.DeploymentsInsert(clusterName)
	if err != nil {
		return err
	}

	// retrieve the external ip address of the cluster
	externalIpAddress := vsaCluster[0]["ExternalIP"]
	internalIpAddress := vsaCluster[0]["InternalIP"]
	instanceType := vsaCluster[0]["Name"]
	clusterDetails := &datamodel.ClusterDetails{
		ExternalName: clusterName,
		Nodes: []datamodel.Node{
			{
				ExternalIpAddress: externalIpAddress,
				InternalIpAddress: internalIpAddress,
				InstanceType:      instanceType,
			},
		},
	}

	// TODO: create provider using externl IP, username and password
	// check for vsa cluster creation to be successful
	// TODO: get nodes using the provider and stores node details in the db nodes table

	return se.SavePoolWithVsaClusterDetails(context.Background(), params.Name, params.AccountName, clusterDetails)
}

// GetPool gets the specified pool
func (o *Orchestrator) GetPool(ctx context.Context, poolId string) (*models.Pool, error) {
	se := o.storage

	pool, err := se.GetPool(ctx, poolId)
	if err != nil {
		return nil, err
	}

	return convertDatastorePoolToModel(pool, pool.Account.Name), nil
}

func _validateCreatePoolParams(se database.Storage, params *CreatePoolParams) error {
	if params.SizeInBytes < minQuotaInBytesPool {
		return customerrors.NewUserInputValidationErr("Given pool size not supported. Pool size can't be less than " + utils.FmtUint64Bytes(minQuotaInBytesPool))
	}

	return nil
}

// GetPoolByVendorID retrieves a pool by its VendorID.
func (o *Orchestrator) GetPoolByVendorID(ctx context.Context, vendorID string) (*models.Pool, error) {
	se := o.storage
	pool, err := se.GetPoolByVendorID(ctx, vendorID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("pool not found")
		}
		return nil, err
	}
	return convertDatastorePoolToModel(pool, pool.Account.Name), nil
}

// CreatePoolParams describes parameters supplied to CreatePool
type CreatePoolParams struct {
	AccountName             string
	Region                  string
	Name                    string
	Description             string
	VendorID                string
	ServiceLevel            string
	QosType                 string
	Tags                    string
	SizeInBytes             uint64
	CoolAccess              bool
	CurrentZone             string
	VendorSubNetID          string
	Zones                   []string
	CustomThroughputMibps   uint64
	HostUUID                string
	CustomPerformanceParams *CustomPerformanceParams
}

// CustomPerformanceParams is used to specify the custom performance parameters for a pool
type CustomPerformanceParams struct {
	Enabled    bool
	Throughput float64
	Iops       int64
}

func convertDatastorePoolToModel(pool *datamodel.Pool, accountName string) *models.Pool {
	return &models.Pool{
		BaseModel: models.BaseModel{
			UUID:      pool.UUID,
			CreatedAt: pool.CreatedAt,
			UpdatedAt: pool.UpdatedAt,
			DeletedAt: DeletedAtOrNil(pool.DeletedAt),
		},
		AccountName:    accountName,
		Name:           pool.Name,
		Description:    pool.Description,
		SizeInBytes:    uint64(pool.SizeInBytes),
		State:          pool.State,
		StateDetails:   pool.StateDetails,
		CoolAccess:     pool.CoolAccess,
		VendorSubNetID: pool.Network,
		ServiceLevel:   pool.ServiceLevel,
	}
}

func DeletedAtOrNil(deletedAt *gorm.DeletedAt) *time.Time {
	if deletedAt != nil && deletedAt.Valid {
		return &deletedAt.Time
	}
	return nil
}

func ListPool(ctx context.Context, params gcpgenserver.V1betaDescribePoolParams, orchestrator *Orchestrator) (gcpgenserver.V1betaDescribePoolRes, error) {
	// 1. Prevalidation steps needs to be implemented

	// 2. Create a job in the database
	job, err := orchestrator.storage.CreateJob(ctx, &datamodel.Job{})
	if err != nil {
		return nil, err
	}

	// 3. Create a workflow execution
	retryPolicy := workflowengine.GetRetryPolicy(&workflowengine.RetryPolicyConfig{})
	_, err = orchestrator.temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			ID:                    job.ID,
			TaskQueue:             workflowengine.CustomerTaskQueue,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			RetryPolicy:           retryPolicy,
		},
		workflows.CreatePool,
		params,
	)
	if err != nil {
		return nil, err
	}

	// 3. Implement workflow response processing
	return nil, nil
}
