package replicationWorkflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type ReplicationStopWorkflow struct {
	workflows.BaseWorkflow
	SE *database.Storage
}

var _ workflows.WorkflowInterface = &ReplicationStopWorkflow{}

func StopReplicationWorkflow(ctx workflow.Context, params *commonparams.StopReplicationParams, event *replication.StopReplicationEvent) (*vsa.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	repWf := new(ReplicationStopWorkflow)
	err := repWf.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	repWf.Status = workflows.WorkflowStatusRunning
	err = repWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err == nil {
			repWf.Status = workflows.WorkflowStatusCompleted
			err = repWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		} else {
			repWf.Status = workflows.WorkflowStatusFailed
			err = repWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		}
	}()
	_, err = repWf.Run(ctx, event)
	if err != nil {
		logger.Info("Stop Replication workflow run executed with error", "error", err)
	}
	return nil, err
}

func (wf *ReplicationStopWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	stopReplicationParams := input.(*commonparams.StopReplicationParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = stopReplicationParams.AccountName
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

func (wf *ReplicationStopWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	event := args[0].(*replication.StopReplicationEvent)
	replicationActivity := &replicationActivities.StopVolumeReplicationActivity{}
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"NonRetryableError"},
		},
	}
	ao1 := ao
	ao1.RetryPolicy.MaximumAttempts = int32(ReplicationJobsRetryMaxAttempts)

	ctx = workflow.WithActivityOptions(ctx, ao)
	ctx1 := workflow.WithActivityOptions(ctx, ao1)

	replicationResult := replication.StopReplicationResult{
		SrcProjectNumber: &event.SourceProjectNumber,
		DstProjectNumber: &event.DestinationProjectNumber,
		Event:            event,
		DbVolReplication: event.ReplicationModel,
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetSrcBasePathStop, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetDstBasePathStop, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetSignedSrcTokenStop, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetSignedDstTokenStop, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.StopReplicationOnDestination, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx1, replicationActivity.DescribeDestJobStop, &replicationResult).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
}
