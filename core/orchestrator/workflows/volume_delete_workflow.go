package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type volumeDeleteWorkflow struct {
	// add fields needed for volume workflow
	ID         string
	customerID string
	status     string
	logger     log.Logger
}

type volumeDeleteWorkflowStatus struct {
	ID         string
	customerID string
	status     string
}

// DeleteVolumeWorkflow Delete Volume Workflow process volume related requests from a customer.
func DeleteVolumeWorkflow(ctx workflow.Context, volume *datamodel.Volume) (gcpgenserver.V1betaDescribeVolumeRes, error) {
	volumeWf := new(volumeDeleteWorkflow)
	err := volumeWf.Setup(ctx, volume)
	if err != nil {
		return nil, err
	}
	volumeWf.status = WorkflowStatusRunning
	// err = poolWF.UpdateStatus(ctx, string(models.JobsStatePROCESSING), "")
	// if err != nil {
	//	return nil, err
	// }
	_, err = volumeWf.Run(ctx, volume)
	if err != nil {
		volumeWf.status = WorkflowStatusFailed
	}
	// poolWF.status = WorkflowStatusCompleted
	// err = poolWF.UpdateStatus(ctx, string(models.JobsStateDONE), "")
	// if err != nil {
	//	return nil, err
	// }
	return nil, err
}

func (wf *volumeDeleteWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	volume := input.(*datamodel.Volume)
	wf.customerID = volume.Account.Name
	wf.status = "created"
	wf.logger = log.With(
		workflow.GetLogger(ctx),
		"workflowID", wf.ID,
		"customerID", wf.customerID,
	)

	return workflow.SetQueryHandler(ctx, "status", func() (*volumeDeleteWorkflowStatus, error) {
		return &volumeDeleteWorkflowStatus{
			ID:         wf.ID,
			status:     wf.status,
			customerID: wf.customerID,
		}, nil
	})
}

func (wf *volumeDeleteWorkflow) Run(ctx workflow.Context, volume *datamodel.Volume) (interface{}, error) {
	deleteActivity := &activities.VolumeDeleteActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	options := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, options)

	var dbNode *datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &volume.Pool.ID).Get(ctx, &dbNode)
	if err != nil {
		return nil, err
	}

	node := createNodeForProvider(dbNode, volume)

	err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteVolumeInONTAP, &volume, &node).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteVolume, &volume).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
}
