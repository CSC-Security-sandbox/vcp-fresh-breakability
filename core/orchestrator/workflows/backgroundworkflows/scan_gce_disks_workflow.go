package backgroundworkflows

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/diskscan"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ScanGCEDisksWorkflow is registered on the vcp-background-worker pod
// (BackgroundTaskQueue) and called by the leaked-resources disk detector
// running in the core pod. It is a thin Temporal wrapper around the
// ScanGCEDisks activity: the activity does the actual work; the
// workflow exists to give the call standard Temporal retry/timeout
// semantics and to be addressable by name from other pods.
//
// Registered in worker/main.go RegisterBackgroundWorkflowsAndActivities.
// The string identifier used by the detector to submit this workflow is
// diskscan.WorkflowName, kept identical to the function name so the Temporal
// registry resolves it without alias configuration.
func ScanGCEDisksWorkflow(ctx workflow.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error) {
	logger := util.GetLogger(ctx)
	logger.Infof("ScanGCEDisksWorkflow started: projects=%d", len(in.ProjectIDs))

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

	a := &backgroundactivities.ScanGCEDisksActivity{}
	var out diskscan.ScanOutput
	if err := workflow.ExecuteActivity(ctx, a.ScanGCEDisks, in).Get(ctx, &out); err != nil {
		logger.Errorf("ScanGCEDisksWorkflow: activity failed: %v", err)
		return nil, err
	}

	logger.Infof("ScanGCEDisksWorkflow finished: total_disks=%d partial_failures=%d",
		len(out.Items), len(out.PartialFailures))
	return &out, nil
}
