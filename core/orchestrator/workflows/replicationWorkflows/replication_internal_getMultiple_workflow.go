package replicationWorkflows

import (
	"time"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type replicationInternalGetMultipleWorkflow struct {
	workflows.BaseWorkflow
	SE *database.Storage
}

var _ workflows.WorkflowInterface = &replicationInternalGetMultipleWorkflow{}

func GetMultipleReplicationsInternalWorkflow(ctx workflow.Context, params *common.ReplicationInternalGetMultipleParams) error {
	repWf := new(replicationInternalGetMultipleWorkflow)
	err := repWf.Setup(ctx, params)
	if err != nil {
		return err
	}
	repWf.Status = workflows.WorkflowStatusRunning
	err = repWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		repWf.Status = workflows.WorkflowStatusFailed
		err = repWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		return err
	}

	_, customErr := repWf.Run(ctx, params)
	if customErr != nil {
		repWf.Status = workflows.WorkflowStatusFailed
		err = repWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		return err
	}
	repWf.Status = workflows.WorkflowStatusCompleted
	err = repWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return err
}

func (wf *replicationInternalGetMultipleWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	tParams := input.(*common.ReplicationInternalGetMultipleParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = tParams.AccountName
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

func (wf *replicationInternalGetMultipleWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	params := args[0].(*common.ReplicationInternalGetMultipleParams)
	replicationActivity := &replicationActivities.ReplicationInternalGetMultipleActivity{}
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Duration(workflows.StartToCloseTimeoutForReplicationActivities) * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     workflows.BackoffCoefficientForReplicationActivities,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(1),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Get all replications from DB
	err = workflow.ExecuteActivity(ctx, replicationActivity.GetReplicationsFromDB, &params).Get(ctx, &params)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	// Get nodes for all pools
	err = workflow.ExecuteActivity(ctx, replicationActivity.GetNodesForPools, &params).Get(ctx, &params)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	// Execute activity to get replication details from Ontap
	err = workflow.ExecuteActivity(ctx, replicationActivity.GetReplicationsFromOntap, &params).Get(ctx, &params)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	// Execute activity to update DB
	err = workflow.ExecuteActivity(ctx, replicationActivity.UpdateReplicationsInDB, &params).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	return nil, workflows.ConvertToVSAError(err)
}
