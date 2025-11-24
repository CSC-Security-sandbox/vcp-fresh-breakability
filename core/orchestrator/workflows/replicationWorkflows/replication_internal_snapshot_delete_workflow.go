package replicationWorkflows

import (
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type internalSnapshotDeleteWorkflow struct {
	workflows.BaseWorkflow
	SE *database.Storage
}

// Enforcing the WorkflowInterface on snapshotDeleteWorkflow
var _ workflows.WorkflowInterface = &internalSnapshotDeleteWorkflow{}

// DeleteInternalSnapshotWorkflow Delete Snapshot Workflow process snapshot related requests from a customer.
func DeleteInternalSnapshotWorkflow(ctx workflow.Context, params *common.SnapshotsInternalDeleteParams) (gcpgenserver.V1betaDescribeSnapshotRes, error) {
	logger := util.GetLogger(ctx)
	snapshotWf := new(internalSnapshotDeleteWorkflow)
	err := snapshotWf.Setup(ctx, params)
	if err != nil {
		logger.Infof("Snapshot Delete workflow setup executed with error: %v", err)
		return nil, err
	}
	snapshotWf.Status = workflows.WorkflowStatusRunning
	err = snapshotWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		logger.Infof("Update job status for snapshot executed with error: %v", err)
		snapshotWf.Status = workflows.WorkflowStatusFailed
		err = snapshotWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		return nil, err
	}
	_, customErr := snapshotWf.Run(ctx, params)
	if customErr != nil {
		logger.Infof("Snapshot delete workflow run executed with error: %v", customErr)
		snapshotWf.Status = workflows.WorkflowStatusFailed
		err = snapshotWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		return nil, err
	}
	logger.Info("Snapshot workflow completed successfully")
	snapshotWf.Status = workflows.WorkflowStatusCompleted
	err = snapshotWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
}

func (wf *internalSnapshotDeleteWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	deleteSnapshotParams := input.(*common.SnapshotsInternalDeleteParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = deleteSnapshotParams.AccountName
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

func (wf *internalSnapshotDeleteWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	logger := util.GetLogger(ctx)
	params := args[0].(*common.SnapshotsInternalDeleteParams)
	replicationActivity := &replicationActivities.InternalSnapshotsDeleteActivity{}
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

	logger.Infof("Starting the snapshot deletion workflow for snapshots")
	err = workflow.ExecuteActivity(ctx, replicationActivity.GetNodeFromDB, &params).Get(ctx, &params)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
		Nodes:            params.Nodes,
		DeploymentName:   params.Volume.Pool.DeploymentName,
		OntapCredentials: params.Volume.Pool.PoolCredentials,
	})

	err = workflow.ExecuteActivity(ctx, replicationActivity.ListSnapshotInONTAP, &params, &node).Get(ctx, &params)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	err = workflow.ExecuteActivity(ctx, replicationActivity.DeleteSnapshotsInONTAP, &params, &node).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	err = workflow.ExecuteActivity(ctx, replicationActivity.ListSnapshotFromDB, &params).Get(ctx, &params)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	err = workflow.ExecuteActivity(ctx, replicationActivity.DehydrateSnapshots, &params).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	err = workflow.ExecuteActivity(ctx, replicationActivity.UpdateSnapshotRecordInDB, &params).Get(ctx, nil)

	return nil, workflows.ConvertToVSAError(err)
}
