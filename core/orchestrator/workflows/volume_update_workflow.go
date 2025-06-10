package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type volumeUpdateWorkflow struct {
	// add fields needed for volume workflow
	BaseWorkflow
}

// Enforcing the WorkflowInterface on volumeUpdateWorkflow
var _ WorkflowInterface = &volumeUpdateWorkflow{}

// UpdateVolumeWorkflow Update Volume Workflow process volume related requests from a customer.
func UpdateVolumeWorkflow(ctx workflow.Context, params *common.UpdateVolumeParams, volume *datamodel.Volume) (gcpgenserver.V1betaDescribeVolumeRes, error) {
	log := util.GetLogger(ctx)
	volumeWf := new(volumeUpdateWorkflow)
	err := volumeWf.Setup(ctx, volume)
	if err != nil {
		return nil, err
	}
	volumeWf.Status = WorkflowStatusRunning
	err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			volumeWf.Status = WorkflowStatusFailed
			err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		} else {
			volumeWf.Status = WorkflowStatusCompleted
			err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		}
	}()

	_, err = volumeWf.Run(ctx, params, volume)
	if err != nil {
		log.Errorf("Volume update workflow completed with error: %v", err)
		return nil, err
	}
	log.Infof("Volume update workflow completed successfully")
	return nil, err
}

func (wf *volumeUpdateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	volume := input.(*datamodel.Volume)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = volume.Account.Name
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

func (wf *volumeUpdateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	params := args[0].(*common.UpdateVolumeParams)
	volume := args[1].(*datamodel.Volume)
	updateActivity := &activities.VolumeUpdateActivity{}

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

	node := CreateNodeForProvider(dbNode, volume)

	err = workflow.ExecuteActivity(ctx, updateActivity.UpdateVolumeInONTAP, &volume, &params, &node).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	// Avoid updating the lun if the size is not changed
	if params.QuotaInBytes > volume.SizeInBytes {
		err = workflow.ExecuteActivity(ctx, updateActivity.UpdateLun, &volume, &params, &node).Get(ctx, nil)
		if err != nil {
			return nil, err
		}
	}

	err = workflow.ExecuteActivity(ctx, updateActivity.UpdateVolumeInDB, &volume, &params).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	// ToDo: Update HostGroup Activity,
	// Need to find the delta from the existing host groups in volume and the new host groups from params
	// Then Find out which host groups to add and which to remove

	return nil, err
}
