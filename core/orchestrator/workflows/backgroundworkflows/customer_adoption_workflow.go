package backgroundworkflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/metrics"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// VolumeDetailsWorkflow is a Temporal workflow that retrieves details of non-deleted volumes.
func VolumeDetailsWorkflow(ctx workflow.Context) ([]metrics.VolumeDetails, error) {
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	// Set activity options with timeout and optional retry policy
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout, // adjust as needed
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	CustomerAdoptionActivity := &backgroundactivities.CustomerAdoptionActivity{}

	// Execute the activity to get non-deleted volumes
	var volumes []*datamodel.Volume
	err = workflow.ExecuteActivity(ctx, CustomerAdoptionActivity.GetActiveVolumesActivity).Get(ctx, &volumes)

	if err != nil {
		return nil, err
	}
	var details []metrics.VolumeDetails
	for _, v := range volumes {
		details = append(details, metrics.VolumeDetails{
			Name:      v.Name,
			State:     v.State,
			AccountID: v.AccountID,
		})
	}
	return details, nil
}
