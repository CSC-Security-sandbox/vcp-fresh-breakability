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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	ReverseReplicationJobsRetryMaxAttempts = env.GetInt("REPLICATION_JOBS_RETRY_MAX_ATTEMPTS", 10)
)

type ReverseReplicationWorkflow struct {
	workflows.BaseWorkflow
	SE *database.Storage
}

var _ workflows.WorkflowInterface = &ReverseReplicationWorkflow{}

func ReverseAndResumeVolumeReplicationWorkflow(ctx workflow.Context, params *commonparams.ReverseAndResumeReplicationParams, event *replication.ReverseReplicationEvent) (*vsa.VolumeReplication, error) {
	repWf := new(ReverseReplicationWorkflow)
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
	_, customErr := repWf.Run(ctx, event, params)
	if customErr != nil {
		repWf.Status = workflows.WorkflowStatusFailed
		err = repWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		return nil, err
	}
	repWf.Status = workflows.WorkflowStatusCompleted
	err = repWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
}

func (wf *ReverseReplicationWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	reverseReplicationParams := input.(*commonparams.ReverseAndResumeReplicationParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = reverseReplicationParams.AccountName
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

func (wf *ReverseReplicationWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	event := args[0].(*replication.ReverseReplicationEvent)
	params := args[1].(*commonparams.ReverseAndResumeReplicationParams)
	replicationActivity := &replicationActivities.ReverseVolumeReplicationActivity{}
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
	ao1.RetryPolicy.MaximumAttempts = int32(ReverseReplicationJobsRetryMaxAttempts)

	ctx1 := workflow.WithActivityOptions(ctx, ao1)

	reverseResult := replication.ReverseReplicationResult{
		Event:            event,
		SrcProjectNumber: &event.SourceProjectNumber,
		DstProjectNumber: &event.DestinationProjectNumber,
		DbVolReplication: event.ReplicationModel,
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetSrcBasePathReverse, &reverseResult).Get(ctx, &reverseResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetDstBasePathReverse, &reverseResult).Get(ctx, &reverseResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetSignedSrcTokenReverse, &reverseResult).Get(ctx, &reverseResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetSignedDstTokenReverse, &reverseResult).Get(ctx, &reverseResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.VerifyNewDstVolume, &reverseResult).Get(ctx, &reverseResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.ResizeNewDstVolumeIfNeeded, &reverseResult).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.ReverseAndResumeReplication, &reverseResult, params).Get(ctx, &reverseResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx1, replicationActivity.DescribeRemoteJobOnDst, &reverseResult).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	// Update the database on the current destination (which becomes the new source after reverse)
	err = workflow.ExecuteActivity(ctx, replicationActivity.UpdateVolumeReplicationAttributes, &reverseResult).Get(ctx, &reverseResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx1, replicationActivity.DescribeRemoteJobOnSrc, &reverseResult).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.MountReplicationAfterReverse, &reverseResult).Get(ctx, &reverseResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	return nil, nil
}
