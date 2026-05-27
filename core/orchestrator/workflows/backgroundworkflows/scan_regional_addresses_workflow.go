package backgroundworkflows

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/ipscan"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ScanRegionalAddressesWorkflow is registered on the vcp-background-worker
// pod (BackgroundTaskQueue) and called by the leaked-resources internal
// reserved-IP detector running in the core pod. It is a thin Temporal
// wrapper around the ScanRegionalAddresses activity: the activity does the
// actual work; the workflow exists to give the call standard Temporal
// retry/timeout semantics and to be addressable by name from other pods.
//
// Registered in worker/main.go RegisterBackgroundWorkflowsAndActivities.
// The string identifier used by the detector to submit this workflow is
// ipscan.WorkflowName, kept identical to the function name so the Temporal
// registry resolves it without alias configuration.
func ScanRegionalAddressesWorkflow(ctx workflow.Context, in ipscan.ScanInput) (*ipscan.ScanOutput, error) {
	logger := util.GetLogger(ctx)
	logger.Infof("ScanRegionalAddressesWorkflow started: targets=%d", len(in.Targets))

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        5 * time.Second,
			BackoffCoefficient:     2.0,
			MaximumInterval:        time.Minute,
			MaximumAttempts:        3,
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	a := &backgroundactivities.ScanRegionalAddressesActivity{}
	var out ipscan.ScanOutput
	if err := workflow.ExecuteActivity(ctx, a.ScanRegionalAddresses, in).Get(ctx, &out); err != nil {
		logger.Errorf("ScanRegionalAddressesWorkflow: activity failed: %v", err)
		return nil, err
	}

	logger.Infof("ScanRegionalAddressesWorkflow finished: result_groups=%d partial_failures=%d",
		len(out.Results), len(out.PartialFailures))
	return &out, nil
}
