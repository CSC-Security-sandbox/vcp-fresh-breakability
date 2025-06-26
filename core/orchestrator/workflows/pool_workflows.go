package workflows

import (
	"time"

	hyperscaler_models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/api/iam/v1"
	"netapp.com/vsa/lifecycle-manager/pkg/vlmconfig"
)

var (
	secretManagerEnabled    = env.GetBool("SECRET_MANAGER_ENABLED", false)
	setupNwHeartbeatTimeout = env.GetUint64("SETUP_NW_HEARTBEAT_TIMEOUT_SEC", 300)
	vmrsConfigPath          = env.GetString("VMRS_CONFIG_PATH", "config/vmrs_gcp.yaml")
)

type createPoolWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

// Enforcing the WorkflowInterface on createPoolWorkflow
var _ WorkflowInterface = &createPoolWorkflow{}

// const customerActionTimeout = 30 * time.Minute

// CreatePoolWorkflow processes pool related requests from a customer.
func CreatePoolWorkflow(ctx workflow.Context, params *common.CreatePoolParams, pool *datamodel.Pool) (gcpgenserver.V1betaDescribePoolRes, error) {
	createPoolWF := new(createPoolWorkflow)
	err := createPoolWF.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	createPoolWF.Status = WorkflowStatusRunning
	err = createPoolWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, err = createPoolWF.Run(ctx, params, pool)
	if err != nil {
		createPoolWF.Status = WorkflowStatusFailed
		err = createPoolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err != nil {
			return nil, err
		}
	}
	createPoolWF.Status = WorkflowStatusCompleted
	err = createPoolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
}

func (wf *createPoolWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createPoolParams := input.(*common.CreatePoolParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = createPoolParams.AccountName
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

func (wf *createPoolWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	params := args[0].(*common.CreatePoolParams)
	pool := args[1].(*datamodel.Pool)
	poolActivity := &activities.PoolActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	dbPool := pool

	rollbackManager := common.NewRollbackManager()

	defer func() {
		if err != nil {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	rollbackManager.Add(poolActivity.ErroredPool, dbPool)
	rollbackManager.Add(poolActivity.DeletePoolResourcesOnRollback, dbPool)
	secret := &hyperscaler_models.CustomSecret{}
	if secretManagerEnabled {
		err = workflow.ExecuteActivity(ctx, poolActivity.CreateSecret, params.Region, pool.SecretID).Get(ctx, secret)
		if err != nil {
			return nil, err
		}
	}

	tenancyDetails := &common.TenancyInfo{}
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateTenancy, params).Get(ctx, &tenancyDetails)
	if err != nil {
		return nil, err
	}

	rollbackManager.Add(poolActivity.ReleaseSubnet, dbPool)
	setupNwCtx := workflow.WithHeartbeatTimeout(ctx, time.Duration(setupNwHeartbeatTimeout)*time.Second)
	err = workflow.ExecuteActivity(setupNwCtx, poolActivity.SetupNetwork, params.Region, tenancyDetails.RegionalTenantProject, tenancyDetails.SnHostProject, tenancyDetails.Network).Get(setupNwCtx, nil)
	if err != nil {
		return nil, err
	}

	serviceAccount := &iam.ServiceAccount{}
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateServiceAccountWithStorageRole, tenancyDetails.RegionalTenantProject, pool.ServiceAccountId, pool.Name).Get(ctx, serviceAccount)
	if err != nil {
		return nil, err
	}
	rollbackManager.Add(poolActivity.DeleteServiceAccount, tenancyDetails.RegionalTenantProject, pool.ServiceAccountId)

	err = workflow.ExecuteActivity(ctx, poolActivity.CreateAutoTierBucket, pool.AutoTierBucketName, params.Region, tenancyDetails.RegionalTenantProject).Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	rollbackManager.Add(poolActivity.DeleteAutoTierBucket, pool.AutoTierBucketName)

	clusterName := params.Name + "-" + params.AccountName
	sizeInGB := utils.BytesToGigabytes(params.SizeInBytes)

	// Convert CustomPerformanceParams to CustomerRequestedPerformance.
	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{
		DesiredIOPS:             params.CustomPerformanceParams.Iops,
		DesiredThroughputInMiBs: params.CustomPerformanceParams.ThroughputMibps,
		DesiredCapacityInGiB:    int64(sizeInGB),
	}

	// Find the optimal VMs based on the customer requested performance.
	vlmConfig := &vlmconfig.VLMConfig{}

	var vsaClusterPassword string
	if secretManagerEnabled {
		vsaClusterPassword = secret.SecretVersion.Value
	} else {
		vsaClusterPassword = pool.Password
	}

	poolWithClusterDetails := dbPool
	poolWithClusterDetails.ClusterDetails = datamodel.ClusterDetails{
		ExternalName:          clusterName,
		RegionalTenantProject: tenancyDetails.RegionalTenantProject,
		SnHostProject:         tenancyDetails.SnHostProject,
		Network:               tenancyDetails.Network}
	rollbackManager.Add(poolActivity.DeleteVSADeployment, poolWithClusterDetails)

	err = workflow.ExecuteActivity(ctx, poolActivity.IdentifyVMs, vmrsConfigPath, customerRequestedPerformance, vlmConfig, clusterName, params.Region, params.PrimaryZone, params.SecondaryZone, tenancyDetails.Network, tenancyDetails.SubnetworkName, tenancyDetails.RegionalTenantProject, tenancyDetails.SnHostProject, vsaClusterPassword, serviceAccount.Email, pool.AutoTierBucketName).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.CreateVSACluster, vlmConfig).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.SaveVSANodeDetails, dbPool, vlmConfig).Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, err
	}
	node := CreateNodeForProviderWithPool(dbNodes, pool)

	node.Username = pool.Username
	if secretManagerEnabled {
		node.SecretID = pool.SecretID
	} else {
		node.Password = pool.Password
	}

	var ontapVersion string
	err = workflow.ExecuteActivity(ctx, poolActivity.GetOntapVersion, node).Get(ctx, &ontapVersion)
	if err != nil {
		return nil, err
	}

	clusterDetails := &datamodel.ClusterDetails{
		ExternalName:          clusterName,
		OntapVersion:          ontapVersion,
		RegionalTenantProject: tenancyDetails.RegionalTenantProject,
		SnHostProject:         tenancyDetails.SnHostProject,
		Network:               tenancyDetails.Network,
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.SavePoolWithClusterDetails, dbPool, clusterDetails).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.CreateVSASVM, dbPool, vlmConfig).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.CreatedPool, dbPool).Get(ctx, nil)
	return nil, err
}

type updatePoolWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

// Enforcing the WorkflowInterface on createPoolWorkflow
var _ WorkflowInterface = &updatePoolWorkflow{}

// const customerActionTimeout = 30 * time.Minute

// UpdatePoolWorkflow processes pool related requests from a customer.
func UpdatePoolWorkflow(ctx workflow.Context, params *common.UpdatePoolParams, pool *datamodel.Pool) (gcpgenserver.V1betaDescribePoolRes, error) {
	updatePoolWF := new(updatePoolWorkflow)
	err := updatePoolWF.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	updatePoolWF.Status = WorkflowStatusRunning
	err = updatePoolWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, err = updatePoolWF.Run(ctx, params, pool)
	if err != nil {
		updatePoolWF.Status = WorkflowStatusFailed
		err = updatePoolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err != nil {
			return nil, err
		}
		return nil, err
	}
	updatePoolWF.Status = WorkflowStatusCompleted
	err = updatePoolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
}

func (wf *updatePoolWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	updatePoolParams := input.(*common.UpdatePoolParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = updatePoolParams.AccountName
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

func (wf *updatePoolWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	updatePoolParams := args[0].(*common.UpdatePoolParams)
	pool := args[1].(*datamodel.Pool)
	poolActivity := &activities.PoolActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	dbPool := pool
	wf.Logger.Info("Updating pool with new parameters", "params", updatePoolParams) // Update the pool with the new parameters

	poolObj := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: dbPool.UUID,
		},
		VendorID:                dbPool.VendorID,
		Network:                 dbPool.Network,
		SizeInBytes:             int64(updatePoolParams.SizeInBytes),
		AllowAutoTiering:        updatePoolParams.AllowAutoTiering,
		QosType:                 updatePoolParams.QosType,
		HotTierSizeInBytes:      int64(updatePoolParams.HotTierSizeInBytes),
		EnableHotTierAutoResize: updatePoolParams.EnableHotTierAutoResize,
		Description:             updatePoolParams.Description,
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: int64(updatePoolParams.TotalThroughputMibps),
			Iops:            int64(updatePoolParams.TotalIops),
		},
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.UpdatedPool, poolObj).Get(ctx, nil) // replace with the actual activity to update the pool
	return nil, err
}

type deletePoolWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

// Enforcing the WorkflowInterface on deletePoolWorkflow
var _ WorkflowInterface = &deletePoolWorkflow{}

// DeletePoolWorkflow runs delete workflow for a pool.
func DeletePoolWorkflow(ctx workflow.Context, params *common.DeletePoolParams, pool *datamodel.Pool) (gcpgenserver.V1betaDescribePoolRes, error) {
	deletePoolWF := new(deletePoolWorkflow)
	err := deletePoolWF.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	deletePoolWF.Status = WorkflowStatusRunning
	err = deletePoolWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, err = deletePoolWF.Run(ctx, params, pool)
	if err != nil {
		deletePoolWF.Status = WorkflowStatusFailed
		err = deletePoolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err != nil {
			return nil, err
		}
	}
	deletePoolWF.Status = WorkflowStatusCompleted
	err = deletePoolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		return nil, err
	}
	return nil, err
}

func (wf *deletePoolWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	deletePoolParams := input.(*common.DeletePoolParams)
	info := workflow.GetInfo(ctx)
	wf.CustomerID = deletePoolParams.AccountName
	wf.Status = WorkflowStatusCreated
	wf.ID = info.WorkflowExecution.ID
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

func (wf *deletePoolWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	params := args[0].(*common.DeletePoolParams)
	poolActivity := &activities.PoolActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	rollbackManager := common.NewRollbackManager()

	defer func() {
		if err != nil {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	dbPool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: params.PoolID},
	}
	err = workflow.ExecuteActivity(ctx, poolActivity.GetPool, dbPool).Get(ctx, &dbPool)
	if err != nil {
		return nil, err
	}

	// Add the cleanup / rollback activity using this rollback.Add() method instead of writing multiple defer statements,
	// this rollback manager will be invoked whenever there is an error and it will start calling clean up activities in LIFO manner ***/
	rollbackManager.Add(poolActivity.FailedPool, dbPool)

	err = workflow.ExecuteActivity(ctx, poolActivity.DeletingPoolResources, dbPool).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.DeleteVSADeployment, dbPool).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.DeleteAutoTierBucket, dbPool.AutoTierBucketName).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.DeleteServiceAccount, dbPool.ClusterDetails.RegionalTenantProject, dbPool.ServiceAccountId).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.ReleaseSubnet, dbPool).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.DeletePoolResources, dbPool).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
}
