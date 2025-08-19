package replicationWorkflows

import (
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
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

type replicationDeleteWorkflow struct {
	workflows.BaseWorkflow
	SE *database.Storage
}

var _ workflows.WorkflowInterface = &replicationDeleteWorkflow{}

func ReplicationDeleteWorkflow(ctx workflow.Context, params *commonparams.DeleteReplicationParams, event *replication.DeleteReplicationEvent) (*vsa.VolumeReplication, error) {
	repWf := new(replicationDeleteWorkflow)
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

func (wf *replicationDeleteWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	resumeReplicationParams := input.(*commonparams.DeleteReplicationParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = resumeReplicationParams.AccountName
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

func (wf *replicationDeleteWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	event := args[0].(*replication.DeleteReplicationEvent)
	replicationActivity := &replicationActivities.DeleteVolumeReplicationActivity{}
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
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	ao1 := ao
	ao1.RetryPolicy.MaximumAttempts = int32(ReplicationJobsRetryMaxAttempts)

	ctx1 := workflow.WithActivityOptions(ctx, ao1)

	replicationResult := replication.DeleteReplicationResult{
		SrcProjectNumber: &event.SourceProjectNumber,
		DstProjectNumber: &event.DestinationProjectNumber,
		Event:            event,
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetSrcBasePathDelete, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetDstBasePathDelete, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetSignedSrcTokenDelete, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetSignedDstTokenDelete, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetReplicationOnDestinationForDelete, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.DeleteReplicationOnDestination, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx1, replicationActivity.DescribeRemoteJobForDelete, &replicationResult).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.ReleaseReplicationOnSource, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx1, replicationActivity.DescribeSourceJobForDelete, &replicationResult).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx1, replicationActivity.DeleteSnapmirrorSnapshotsOnDestination, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx1, replicationActivity.DescribeRemoteJobForDelete, &replicationResult).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx1, replicationActivity.DeleteSnapmirrorSnapshotsOnSource, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx1, replicationActivity.DescribeSourceJobForDelete, &replicationResult).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.DeHydrateDestinationVolumeReplication, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	if replicationResult.DstReplication.MirrorState.IsSet() && replicationResult.DstReplication.MirrorState.Value == googleproxyclient.VolumeReplicationInternalV1betaMirrorStateUNINITIALIZED {
		err = workflow.ExecuteActivity(ctx, replicationActivity.DeleteVolumeOnDestination, &replicationResult).Get(ctx, &replicationResult)
		if err != nil {
			return nil, workflows.ConvertToVSAError(err)
		}

		err = workflow.ExecuteActivity(ctx1, replicationActivity.DescribeRemoteJobForDelete, &replicationResult).Get(ctx, nil)
		if err != nil {
			return nil, workflows.ConvertToVSAError(err)
		}

		err = workflow.ExecuteActivity(ctx, replicationActivity.DeHydrateDestinationVolume, &replicationResult).Get(ctx, &replicationResult)
		if err != nil {
			return nil, workflows.ConvertToVSAError(err)
		}
	}

	return nil, workflows.ConvertToVSAError(err)
}
