package backgroundworkflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

func SyncVSASnapshotsWorkflow(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return err
	}
	// TODO: Based on the data volume, we might need to adjust the timeouts and retry policies.
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

	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}

	var pools []*datamodel.Pool
	err = workflow.ExecuteActivity(ctx, syncSnapshotActivity.ListPools).Get(ctx, &pools)
	if err != nil {
		logger.Error("ListPools activity failed.", "Error", err)
		return err
	}

	err = workflow.ExecuteActivity(ctx, syncSnapshotActivity.SynchronizeSnapshots, pools).Get(ctx, nil)
	if err != nil {
		logger.Error("SynchronizeSnapshots activity execution failed.", "Error", err)
		return err
	}

	return nil
}
