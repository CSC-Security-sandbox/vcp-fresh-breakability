package replicationWorkflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type internalVolumeReplicationStopWorkflow struct {
	workflows.BaseWorkflow
	SE *database.Storage
}

var _ workflows.WorkflowInterface = &internalVolumeReplicationStopWorkflow{}

func StopInternalVolumeReplicationWorkflow(ctx workflow.Context, replicationDb *datamodel.VolumeReplication, forceStop bool) (*vsa.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	stopRepWf := new(internalVolumeReplicationStopWorkflow)
	err := stopRepWf.Setup(ctx, replicationDb)
	if err != nil {
		return nil, err
	}
	stopRepWf.Status = workflows.WorkflowStatusRunning
	err = stopRepWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		stopRepWf.Status = workflows.WorkflowStatusFailed
		err = stopRepWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		return nil, err
	}
	_, customErr := stopRepWf.Run(ctx, replicationDb, forceStop)
	if customErr != nil {
		logger.Info("Internal Stop Volume Replication workflow run executed with error", "error", customErr)
		stopRepWf.Status = workflows.WorkflowStatusFailed
		err = stopRepWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		return nil, err
	}
	stopRepWf.Status = workflows.WorkflowStatusCompleted
	err = stopRepWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
}

func (wf *internalVolumeReplicationStopWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	replicationParams := input.(*datamodel.VolumeReplication)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = replicationParams.Account.Name
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

func (wf *internalVolumeReplicationStopWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	dbReplication := args[0].(*datamodel.VolumeReplication)
	forceStop := args[1].(bool)
	replicationActivity := &replicationActivities.InternalStopVolumeReplicationActivity{}
	replicationCommonActivity := &replicationActivities.VolumeReplicationCreateActivity{}
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"NonRetryableError", "PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	log := util.GetLogger(ctx)

	// Defer function to mark the database entry in error state if any error occurs
	defer func() {
		if err != nil {
			// On panic, mark volume replication in error state
			dbReplication.State = models.LifeCycleStateError
			dbReplication.StateDetails = err.Error()
			err2 := workflow.ExecuteActivity(ctx, replicationCommonActivity.UpdateReplicationState, *dbReplication).Get(ctx, nil)
			if err2 != nil {
				log.Errorf("Failed to update volume state in DB to error: %v", err2)
			}
		}
	}()

	var vsaReplication *vsa.VolumeReplication
	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, dbReplication.Volume.PoolID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
		Nodes:            dbNodes,
		DeploymentName:   dbReplication.Volume.Pool.DeploymentName,
		OntapCredentials: dbReplication.Volume.Pool.PoolCredentials,
	})

	err = workflow.ExecuteActivity(ctx, replicationActivity.AbortVolumeReplication, dbReplication, node, forceStop).Get(ctx, &vsaReplication)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.BreakVolumeReplication, dbReplication, node).Get(ctx, &vsaReplication)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	err = workflow.ExecuteActivity(ctx, replicationActivity.GetSnapMirrorFromOntap, dbReplication, node).Get(ctx, &vsaReplication)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	err = workflow.ExecuteActivity(ctx, replicationActivity.UpdateVolumeReplicationStopDetails, dbReplication, vsaReplication).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	err = workflow.ExecuteActivity(ctx, replicationActivity.UpdateVolumeToNonDPVolume, dbReplication).Get(ctx, nil)
	return nil, workflows.ConvertToVSAError(err)
}
