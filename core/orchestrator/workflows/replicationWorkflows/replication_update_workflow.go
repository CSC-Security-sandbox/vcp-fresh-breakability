package replicationWorkflows

import (
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type ReplicationUpdateWorkflow struct {
	workflows.BaseWorkflow
	SE *database.Storage
}

var _ workflows.WorkflowInterface = &ReplicationUpdateWorkflow{}

func UpdateVolumeReplicationWorkflow(ctx workflow.Context, params *commonparams.UpdateReplicationParams, event *replication.UpdateReplicationEvent) (*vsa.VolumeReplication, error) {
	repWf := new(ReplicationUpdateWorkflow)
	err := repWf.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	repWf.Status = workflows.WorkflowStatusRunning
	err = repWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		repWf.Status = workflows.WorkflowStatusFailed
		err = repWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		return nil, err
	}
	_, customErr := repWf.Run(ctx, event)
	if customErr != nil {
		repWf.Status = workflows.WorkflowStatusFailed
		err = repWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		return nil, err
	}
	repWf.Status = workflows.WorkflowStatusCompleted
	err = repWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
}

func (wf *ReplicationUpdateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	updateReplicationParams := input.(*commonparams.UpdateReplicationParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = updateReplicationParams.AccountName
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

func (wf *ReplicationUpdateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	event := args[0].(*replication.UpdateReplicationEvent)
	replicationActivity := &replicationActivities.VolumeReplicationUpdateActivity{}
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
			NonRetryableErrorTypes: []string{"NonRetryableErr", "PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	ao1 := ao
	ao1.RetryPolicy.MaximumAttempts = int32(ReplicationJobsRetryMaxAttempts)

	ctx1 := workflow.WithActivityOptions(ctx, ao1)

	replicationResult := replication.UpdateReplicationResult{
		SrcProjectNumber: &event.SourceProjectNumber,
		DstProjectNumber: &event.DestinationProjectNumber,
		Event:            event,
		DbVolReplication: event.ReplicationModel,
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetSrcBasePathUpdate, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetDstBasePathUpdate, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetSignedSrcTokenUpdate, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetSignedDstTokenUpdate, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.UpdateReplicationOnDestination, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx1, replicationActivity.DescribeRemoteUpdateJob, &replicationResult).Get(ctx, nil)
	return nil, workflows.ConvertToVSAError(err)
}
