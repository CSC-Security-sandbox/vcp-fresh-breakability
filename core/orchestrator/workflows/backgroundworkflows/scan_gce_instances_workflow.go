package backgroundworkflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/vmscan"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"time"
)

// ScanGCEInstancesWorkflow is registered on the vcp-background-worker pod
// (BackgroundTaskQueue) and called by the leaked-resources VM detector
// running in the core pod. It is a thin Temporal wrapper around the
// ScanGCEInstances activity: the activity does the actual work; the
// workflow exists to give the call standard Temporal retry/timeout
// semantics and to be addressable by name from other pods.
//
// Registered in worker/main.go RegisterBackgroundWorkflowsAndActivities.
// The string identifier used by the detector to submit this workflow is
// vmscan.WorkflowName, kept identical to the function name so the Temporal
// registry resolves it without alias configuration.
func ScanGCEInstancesWorkflow(ctx workflow.Context, in vmscan.ScanInput) (*vmscan.ScanOutput, error) {
	logger := util.GetLogger(ctx)
	logger.Infof("ScanGCEInstancesWorkflow started: projects=%d", len(in.ProjectIDs))

	ao := workflow.ActivityOptions{
		// One Compute aggregatedList per project; 10 minutes is generous even for
		// large fleets and matches the WorkflowExecutionTimeout the detector sets.
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

	a := &backgroundactivities.ScanGCEInstancesActivity{}
	var out vmscan.ScanOutput
	if err := workflow.ExecuteActivity(ctx, a.ScanGCEInstances, in).Get(ctx, &out); err != nil {
		logger.Errorf("ScanGCEInstancesWorkflow: activity failed: %v", err)
		return nil, err
	}

	logger.Infof("ScanGCEInstancesWorkflow finished: total_instances=%d partial_failures=%d",
		len(out.Items), len(out.PartialFailures))
	return &out, nil
}
