package backgroundworkflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// TrialAccountSyncWorkflow periodically reconciles trial metadata from Google into VCP.
// Scheduling is gated by TRIAL_ACCOUNT_SYNC_ENABLED in LoadJobSpecs (google-proxy).
func TrialAccountSyncWorkflow(ctx workflow.Context) error {
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return err
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

	activity := &backgroundactivities.TrialAccountSyncActivity{}
	return workflow.ExecuteActivity(ctx, activity.SyncTrialAccounts).Get(ctx, nil)
}
