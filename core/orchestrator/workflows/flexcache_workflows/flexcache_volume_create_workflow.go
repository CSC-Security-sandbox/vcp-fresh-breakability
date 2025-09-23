package flexcache_workflows

import (
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/flexcache_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/flexcache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	clusterPeerTimeout  = env.GetDuration("CLUSTER_PEER_TIMEOUT", 60*time.Minute)
	clusterPeerInterval = env.GetDuration("CLUSTER_PEER_INTERVAL", 15*time.Second)

	getClusterPeerTimeout  = _getClusterPeerTimeout
	getClusterPeerInterval = _getClusterPeerInterval
)

func _getClusterPeerTimeout() time.Duration {
	return clusterPeerTimeout
}

func _getClusterPeerInterval() time.Duration {
	return clusterPeerInterval
}

type flexCacheCreateWorkflow struct {
	workflows.BaseWorkflow
}

var _ workflows.WorkflowInterface = &flexCacheCreateWorkflow{}

// CreateFlexCacheWorkflow Volume Workflow process volume related requests from a customer.
func CreateFlexCacheWorkflow(ctx workflow.Context, params *common.CreateVolumeParams, volume *datamodel.Volume) error {
	log := util.GetLogger(ctx)
	flexCacheWf := new(flexCacheCreateWorkflow)
	err := flexCacheWf.Setup(ctx, params)
	if err != nil {
		log.Errorf("Failed to setup CreateFlexCacheWorkflow: %v", err)
		return err
	}
	flexCacheWf.Status = workflows.WorkflowStatusRunning
	err = flexCacheWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Processing for CreateFlexCacheWorkflow: %v", err)
		return err
	}
	_, customErr := flexCacheWf.Run(ctx, volume, params)
	if customErr != nil {
		log.Errorf("CreateFlexCacheWorkflow completed with error: %v", customErr)
		flexCacheWf.Status = workflows.WorkflowStatusFailed
		err2 := flexCacheWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if err2 != nil {
			log.Errorf("Failed to update job status to Done with error for CreateFlexCacheWorkflow: %v", err2)
			return err2
		}
		return customErr
	}
	flexCacheWf.Status = workflows.WorkflowStatusCompleted
	err = flexCacheWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Done for CreateFlexCacheWorkflow: %v", err)
	}
	return err
}

func (wf *flexCacheCreateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createPoolParams := input.(*common.CreateVolumeParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = createPoolParams.AccountName
	wf.Status = workflows.WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

func (wf *flexCacheCreateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	log := util.GetLogger(ctx)
	dbVolume := args[0].(*datamodel.Volume)
	flexCacheVolumeCreateActivity := &flexcache_activities.FlexCacheVolumeCreateActivity{}
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
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
	flexcacheResult := flexcache.CreateFlexCacheResult{
		DBVolume: dbVolume,
	}

	rollbackManager := common.NewRollbackManager()
	defer func() {
		if err != nil {
			updateStateDetailsAndCode(&flexcacheResult, workflows.ConvertToVSAError(err))
			err2 := workflow.ExecuteActivity(ctx, flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, &flexcacheResult).Get(ctx, nil)
			if err2 != nil {
				log.Errorf("Failed to update cache parameters in DB: %v", err2)
			}
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &dbVolume.Pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	flexcacheResult.Node = hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{Nodes: dbNodes, Password: dbVolume.Pool.PoolCredentials.Password, SecretID: dbVolume.Pool.PoolCredentials.SecretID, DeploymentName: dbVolume.Pool.DeploymentName, CertificateID: dbVolume.Pool.PoolCredentials.CertificateID, AuthType: dbVolume.Pool.PoolCredentials.AuthType})

	err = workflow.ExecuteActivity(ctx, flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, &flexcacheResult).Get(ctx, &flexcacheResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, &flexcacheResult).Get(ctx, &flexcacheResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	waitCtx := getWaitContext(ctx, dbVolume.CacheParameters)
	err = workflow.ExecuteActivity(waitCtx, flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, &flexcacheResult).Get(ctx, &flexcacheResult)
	if err != nil {
		if temporal.IsTimeoutError(err) {
			err = vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrClusterPeerTimeout, err))
		}

		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, &flexcacheResult).Get(ctx, &flexcacheResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, activities.VolumeCreateActivity.UpdateVolumeDetails, &dbVolume, &flexcacheResult.VolumeResponse).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	return nil, nil
}

func updateStateDetailsAndCode(result *flexcache.CreateFlexCacheResult, err error) {
	var vsaErr *vsaerrors.CustomError
	if errors.As(err, &vsaErr) {
		switch vsaErr.TrackingID {
		case vsaerrors.ErrClusterPeerTimeout:
			result.DBVolume.CacheParameters.CacheStateDetailsCode = coremodels.ClusterPeeringExpiredCode
			result.DBVolume.CacheParameters.CacheStateDetails = coremodels.ClusterPeeringExpired
		default:
			result.DBVolume.CacheParameters.CacheStateDetailsCode = coremodels.ErrorDuringClusterPeerCode
			result.DBVolume.CacheParameters.CacheStateDetails = coremodels.ErrorDuringClusterPeer
		}
	}
}

func getWaitContext(ctx workflow.Context, cacheParams *datamodel.CacheParameters) workflow.Context {
	expiry := getClusterPeerTimeout()
	if cacheParams.CommandExpiryTime != nil && cacheParams.CommandExpiryTime.After(time.Now()) {
		expiry = time.Until(*cacheParams.CommandExpiryTime)
	}

	interval := getClusterPeerInterval()
	activityOptions := workflow.ActivityOptions{
		// Total timeout for the activity
		ScheduleToCloseTimeout: expiry,
		// Timeout for a single activity execution
		StartToCloseTimeout: interval,
		// With an interval of 15s it will take ~10 mins to reach the maximum interval of 45s
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    interval,
			BackoffCoefficient: 1.05,
			MaximumInterval:    interval * 3,
		},
	}

	return workflow.WithActivityOptions(ctx, activityOptions)
}
