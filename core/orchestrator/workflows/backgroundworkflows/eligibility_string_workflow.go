package backgroundworkflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/metrics"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

func EligibilityStringWorkflow(ctx workflow.Context) ([]metrics.VolumeDetails, error) {
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	// Set activity options with timeout and optional retry policy
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

	EligibilityStringActivity := &backgroundactivities.EligibilityStringActivity{}

	// Execute the activity to get non-deleted volumes
	var volumes []*datamodel.Volume
	err = workflow.ExecuteActivity(ctx, EligibilityStringActivity.GetEligibilityString).Get(ctx, &volumes)

	if err != nil {
		return nil, err
	}
	var details []metrics.VolumeDetails
	for _, v := range volumes {
		details = append(details, metrics.VolumeDetails{
			Name:  v.Name,
			State: v.State,
		})
	}
	return details, nil
}
