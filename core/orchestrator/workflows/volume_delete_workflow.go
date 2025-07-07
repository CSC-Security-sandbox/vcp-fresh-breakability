package workflows

import (
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type volumeDeleteWorkflow struct {
	// add fields needed for volume workflow
	BaseWorkflow
}

// Enforcing the WorkflowInterface on volumeDeleteWorkflow
var _ WorkflowInterface = &volumeDeleteWorkflow{}

// DeleteVolumeWorkflow Delete Volume Workflow process volume related requests from a customer.
func DeleteVolumeWorkflow(ctx workflow.Context, volume *datamodel.Volume) (gcpgenserver.V1betaDescribeVolumeRes, error) {
	log := util.GetLogger(ctx)
	volumeWf := new(volumeDeleteWorkflow)
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

	_, err = volumeWf.Run(ctx, volume)
	if err != nil {
		log.Errorf("Volume delete workflow completed with error: %v", err)
		return nil, err
	}
	log.Infof("Volume delete workflow completed successfully")
	return nil, err
}

func (wf *volumeDeleteWorkflow) Setup(ctx workflow.Context, input interface{}) error {
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

func (wf *volumeDeleteWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	log := util.GetLogger(ctx)
	volume := args[0].(*datamodel.Volume)
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
	rollbackManager := common.NewRollbackManager()
	defer func() {
		if err != nil {
			err2 := workflow.ExecuteActivity(ctx, activities.VolumeCreateActivity.UpdateVolumeStateInDB, volume.UUID, models.LifeCycleStateError, models.LifeCycleStateDeletionErrorDetails).Get(ctx, nil)
			if err2 != nil {
				log.Errorf("Failed to update volume state in DB to error: %v", err2)
			}
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &volume.Pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, err
	}

	node := common.CreateNodeForProvider(common.NodeProviderInput{Nodes: dbNodes, Username: volume.Pool.Username, Password: volume.Pool.Password, SecretID: volume.Pool.SecretID})

	var ontapAsyncResponse *vsa.OntapAsyncResponse
	err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteSnapmirrorInONTAP, volume.UUID, &node).Get(ctx, &ontapAsyncResponse)
	if err != nil {
		return nil, err
	}

	err = WaitForONTAPJob(ctx, ontapAsyncResponse, node, time.Minute*10)
	if err != nil {
		return nil, fmt.Errorf("failed to delete snapmirror in ontap: %w", err)
	}

	err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteVolumeInONTAP, volume.VolumeAttributes.ExternalUUID, volume.Name, node).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	SnapshotPolicyName := getSnapshotPolicyName(volume)
	err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteSnapshotPolicyInONTAP, SnapshotPolicyName, &node).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteVolume, &volume).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
}
