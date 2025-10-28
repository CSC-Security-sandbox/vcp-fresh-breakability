package flexcache_workflows

import (
	"errors"
	"fmt"
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
func CreateFlexCacheWorkflow(ctx workflow.Context, params *common.CreateVolumeParams, volume *datamodel.Volume) (retErr error) {
	log := util.GetLogger(ctx)
	flexCacheWf := new(flexCacheCreateWorkflow)
	// Guard to detect whether we entered Run.
	enteredRun := false

	// Defer to catch panics and pre-Run failures, so we don’t leave the job stuck in PROCESSING.
	defer func() {
		if r := recover(); r != nil {
			// Panic before Run or during Run. If it was during Run,
			// Run’s own defer should handle job failure; we only handle pre-Run here.
			log.Errorf("panic in CreateFlexCacheWorkflow: %v", r)
			if !enteredRun {
				_ = flexCacheWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), fmt.Errorf("panic: %v", r))
			}
			retErr = fmt.Errorf("panic: %v", r)
		}
	}()

	if err := flexCacheWf.Setup(ctx, params); err != nil {
		log.Errorf("Failed to setup CreateFlexCacheWorkflow: %v", err)
		return err
	}
	flexCacheWf.Status = workflows.WorkflowStatusRunning
	if err := flexCacheWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil); err != nil {
		log.Errorf("Failed to update job status to Processing for CreateFlexCacheWorkflow: %v", err)
		return err
	}

	// Hand-off job failure to Run’s defer.
	enteredRun = true
	_, customErr := flexCacheWf.Run(ctx, volume, params)
	if customErr != nil {
		log.Errorf("CreateFlexCacheWorkflow completed with error: %v", customErr)
		flexCacheWf.Status = workflows.WorkflowStatusFailed
		return customErr
	}
	flexCacheWf.Status = workflows.WorkflowStatusCompleted
	return nil
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
	params := args[1].(*common.CreateVolumeParams)
	flexCacheVolumeCreateActivity := &flexcache_activities.FlexCacheVolumeCreateActivity{}
	flexCacheVolumeDeleteActivity := &flexcache_activities.FlexCacheVolumeDeleteActivity{}
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

	wfInfo := workflow.GetInfo(ctx)
	flexcacheResult := flexcache.CreateFlexCacheResult{
		DBVolume:      dbVolume,
		ActiveJobType: models.JobTypeFlexCacheCreateVolume,
		JobInput: &flexcache.JobActivityInput{
			ResourceName: dbVolume.Name,
			ResourceUUID: dbVolume.UUID,
			AccountID:    dbVolume.AccountID,
			WorkflowID:   wfInfo.WorkflowExecution.ID,
			Metadata: map[string]interface{}{
				"poolID": dbVolume.Pool.ID,
			},
		},
	}

	rollbackManager := common.NewRollbackManager()
	defer func() {
		if err != nil {
			vsaErr := workflows.ConvertToVSAError(err)
			updateStateDetailsAndCode(&flexcacheResult, vsaErr)
			if err2 := workflow.ExecuteActivity(
				ctx,
				flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity,
				&flexcacheResult,
			).Get(ctx, nil); err2 != nil {
				log.Errorf("Failed to update cache parameters in DB: %v", err2)
			}

			if flexcacheResult.ActiveJobType != "" {
				flexcacheResult.ErrorTrackingID = vsaErr.TrackingID
				flexcacheResult.ErrorMessage = vsaErr.Error()
				_ = workflow.ExecuteActivity(
					ctx,
					flexCacheVolumeCreateActivity.FailJobActivity,
					&flexcacheResult,
				).Get(ctx, nil)
			}

			// Execute rollback in disconnected context
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	// Active job helpers
	setActiveJob := func(jobType models.JobType) {
		flexcacheResult.ActiveJobType = jobType
	}

	clearActiveJob := func() {
		flexcacheResult.ActiveJobType = ""
	}

	// Copy input cache parameters to DBVolume cache parameters
	// This is needed to set the CommandExpiryTime in DBVolume cache parameters
	// as we are using same workflow for establish peering, we can override the values here
	// if user has provided different values during create flexcache volume
	// We copy only these two parameters, other parameters are set during volume creation
	// and should not be overwritten
	if params != nil && params.CacheParameters != nil && dbVolume.CacheParameters != nil {
		flexcacheResult = *copyInputCacheParameters(params, &flexcacheResult)
	}

	// CCFE compatibility: Handle create job
	if err = workflow.ExecuteActivity(
		ctx,
		flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity,
		&flexcacheResult,
	).Get(ctx, nil); err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	clearActiveJob()

	// CCFE compatibility: Establish Peering job
	if err = workflow.ExecuteActivity(
		ctx,
		flexCacheVolumeCreateActivity.CreatePeeringJobActivity,
		&flexcacheResult,
	).Get(ctx, nil); err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	setActiveJob(coremodels.JobTypeFlexCacheEstablishPeering)

	// Fetch nodes needed for FlexCache creation
	var dbNodes []*datamodel.Node
	if err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &dbVolume.Pool.ID).Get(ctx, &dbNodes); err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	flexcacheResult.Node = hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
		Nodes:          dbNodes,
		Password:       dbVolume.Pool.PoolCredentials.Password,
		SecretID:       dbVolume.Pool.PoolCredentials.SecretID,
		DeploymentName: dbVolume.Pool.DeploymentName,
		CertificateID:  dbVolume.Pool.PoolCredentials.CertificateID,
		AuthType:       dbVolume.Pool.PoolCredentials.AuthType,
	})

	// check the status of cluster peer before creating new cluster peer
	err = workflow.ExecuteActivity(ctx, flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, &flexcacheResult).Get(ctx, &flexcacheResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	// delete cluster peer on ONTAP if already created in previous attempt and has problem before creating new one
	if flexcacheResult.ClusterPeerAction == flexcache.ActionCreate && flexcacheResult.ClusterPeer != nil {
		log.Debugf("Deleting existing cluster peer on ONTAP before creating a new one as a part of "+
			"Establish Peering for flexcache volume %s", dbVolume.Name)
		err = workflow.ExecuteActivity(ctx, flexCacheVolumeDeleteActivity.DeleteClusterPeerInOntapActivity,
			convertCreateResultToDeleteResult(&flexcacheResult)).Get(ctx, nil)
		if err != nil {
			log.Errorf("Failed to delete cluster peer on ONTAP before creating a new one: %v", err)
			return nil, workflows.ConvertToVSAError(err)
		}
		flexcacheResult.DBVolume.ClusterPeerUUID = nil
		flexcacheResult.ClusterPeer = nil
		// TODO: Update DB for consistency in case subsequent steps fail, as Volume table is gonna change in future. We can add the logic for table change later
		// https://jira.ngage.netapp.com/browse/VSCP-1217
	}

	if flexcacheResult.ClusterPeerAction == flexcache.ActionCreate {
		if err = workflow.ExecuteActivity(ctx, flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, &flexcacheResult).Get(ctx, &flexcacheResult); err != nil {
			return nil, workflows.ConvertToVSAError(err)
		}

		if err = workflow.ExecuteActivity(ctx, flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, &flexcacheResult).Get(ctx, &flexcacheResult); err != nil {
			return nil, workflows.ConvertToVSAError(err)
		}
	}

	// CCFE compatibility: Complete peering job
	if err = workflow.ExecuteActivity(
		ctx,
		flexCacheVolumeCreateActivity.CompletePeeringJobActivity,
		&flexcacheResult,
	).Get(ctx, nil); err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	clearActiveJob()

	// CCFE compatibility: Internal job
	if err = workflow.ExecuteActivity(
		ctx,
		flexCacheVolumeCreateActivity.StartInternalJobActivity,
		&flexcacheResult,
	).Get(ctx, nil); err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	setActiveJob(models.JobTypeFlexCacheInternalPeering)

	if flexcacheResult.ClusterPeerAction == flexcache.ActionCreate || flexcacheResult.ClusterPeerAction == flexcache.ActionWait {
		clusterPeerWaitCtx := getWaitContext(ctx, dbVolume.CacheParameters)
		if err = workflow.ExecuteActivity(clusterPeerWaitCtx, flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, &flexcacheResult).Get(ctx, &flexcacheResult); err != nil {
			if temporal.IsTimeoutError(err) {
				err = vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrClusterPeerTimeout, err))
			}
			return nil, workflows.ConvertToVSAError(err)
		}
	}

	err = workflow.ExecuteActivity(ctx, flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, &flexcacheResult).Get(ctx, &flexcacheResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	// delete SVM peer on ONTAP if already created in previous attempt and has problem before creating new one
	if flexcacheResult.SVMPeerAction == flexcache.ActionCreate && flexcacheResult.SVMPeer != nil {
		err = workflow.ExecuteActivity(ctx, flexCacheVolumeDeleteActivity.DeleteSVMPeeringInOntapActivity,
			convertCreateResultToDeleteResult(&flexcacheResult)).Get(ctx, nil)
		if err != nil {
			return nil, workflows.ConvertToVSAError(err)
		}
		flexcacheResult.DBVolume.SvmPeerUUID = nil
		flexcacheResult.SVMPeer = nil

		// TODO: Update DB for consistency in case subsequent steps fail, as table is gonna change in future. We can add the logic for table change later
		// https://jira.ngage.netapp.com/browse/VSCP-1217
	}

	if flexcacheResult.SVMPeerAction == flexcache.ActionCreate {
		if err = workflow.ExecuteActivity(ctx, flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, &flexcacheResult).Get(ctx, &flexcacheResult); err != nil {
			return nil, workflows.ConvertToVSAError(err)
		}

		if err = workflow.ExecuteActivity(ctx, flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, &flexcacheResult).Get(ctx, &flexcacheResult); err != nil {
			return nil, workflows.ConvertToVSAError(err)
		}
	}

	if flexcacheResult.SVMPeerAction == flexcache.ActionCreate || flexcacheResult.SVMPeerAction == flexcache.ActionWait {
		svmPeerWaitCtx := getWaitContext(ctx, nil)
		if err = workflow.ExecuteActivity(svmPeerWaitCtx, flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, &flexcacheResult).Get(ctx, &flexcacheResult); err != nil {
			if temporal.IsTimeoutError(err) {
				err = vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrSVMPeerTimeout, err))
			}
			return nil, workflows.ConvertToVSAError(err)
		}
	}

	if err = workflow.ExecuteActivity(ctx, flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, &flexcacheResult).Get(ctx, &flexcacheResult); err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	if err = workflow.ExecuteActivity(ctx, activities.VolumeCreateActivity.CreateExportPolicyInOntap, &flexcacheResult.DBVolume, &flexcacheResult.Node).Get(ctx, nil); err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	if err = workflow.ExecuteActivity(ctx, flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeDetailsActivity, &flexcacheResult).Get(ctx, &flexcacheResult); err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	// CCFE compatibility: Complete internal job
	if err = workflow.ExecuteActivity(
		ctx,
		flexCacheVolumeCreateActivity.CompleteInternalJobActivity,
		&flexcacheResult,
	).Get(ctx, nil); err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	clearActiveJob()

	return nil, nil
}

func updateStateDetailsAndCode(result *flexcache.CreateFlexCacheResult, err error) {
	var vsaErr *vsaerrors.CustomError
	if errors.As(err, &vsaErr) {
		switch vsaErr.TrackingID {
		case vsaerrors.ErrClusterPeerTimeout:
			result.DBVolume.CacheParameters.CacheStateDetailsCode = coremodels.ClusterPeeringExpiredCode
			result.DBVolume.CacheParameters.CacheStateDetails = coremodels.ClusterPeeringExpired
		case vsaerrors.ErrSVMPeerTimeout:
			result.DBVolume.CacheParameters.CacheStateDetailsCode = coremodels.SVMPeeringExpiredCode
			result.DBVolume.CacheParameters.CacheStateDetails = coremodels.SVMPeeringExpired
		case vsaerrors.ErrSVMPeerError:
			result.DBVolume.CacheParameters.CacheStateDetailsCode = coremodels.ErrorDuringSVMPeeringCode
			result.DBVolume.CacheParameters.CacheStateDetails = coremodels.ErrorDuringSVMPeering
		default:
			result.DBVolume.CacheParameters.CacheStateDetailsCode = coremodels.ErrorDuringClusterPeerCode
			result.DBVolume.CacheParameters.CacheStateDetails = coremodels.ErrorDuringClusterPeer
		}
	}
}

func getWaitContext(ctx workflow.Context, cacheParams *datamodel.CacheParameters) workflow.Context {
	expiry := getClusterPeerTimeout()
	if cacheParams != nil && cacheParams.CommandExpiryTime != nil && cacheParams.CommandExpiryTime.After(workflow.Now(ctx)) {
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

func convertCreateResultToDeleteResult(createResult *flexcache.CreateFlexCacheResult) *flexcache.DeleteFlexCacheResult {
	return &flexcache.DeleteFlexCacheResult{
		DBVolume: createResult.DBVolume,
		Node:     createResult.Node,
	}
}

func copyInputCacheParameters(params *common.CreateVolumeParams,
	flexcacheResult *flexcache.CreateFlexCacheResult) *flexcache.CreateFlexCacheResult {
	in := params.CacheParameters
	out := flexcacheResult.DBVolume.CacheParameters

	// use the input cache parameters only for CommandExpiryTime
	// assign nil if PeerExpiryTime is not provided in input, which sets the expiry time
	// to default value in flexcache create activity to 1 hour from current time
	if in.PeerExpiryTime != nil {
		t := *in.PeerExpiryTime
		out.CommandExpiryTime = &t
	} else {
		out.CommandExpiryTime = nil
	}
	return flexcacheResult
}
