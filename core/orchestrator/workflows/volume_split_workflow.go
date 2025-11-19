package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type volumeSplitWorkflow struct {
	// add fields needed for split volume workflow
	BaseWorkflow
}

// Enforcing the WorkflowInterface on volumeSplitWorkflow
var _ WorkflowInterface = &volumeSplitWorkflow{}

// SplitVolumeWorkflow orchestrates the process of splitting a volume as part of a customer request.
func SplitVolumeWorkflow(ctx workflow.Context, volume *datamodel.Volume) error {
	log := util.GetLogger(ctx)
	volumeWf := new(volumeSplitWorkflow)
	err := volumeWf.Setup(ctx, volume)
	if err != nil {
		log.Errorf("Volume update workflow setup executed with error: %v", err)
		return err
	}
	volumeWf.Status = WorkflowStatusRunning
	err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Processing for SplitVolumeWorkflow: %v", err)
		return err
	}

	_, errRun := volumeWf.Run(ctx, volume)
	if errRun != nil {
		log.Errorf("SplitVolumeWorkflow completed with error: %v", errRun)
		volumeWf.Status = WorkflowStatusFailed
		err2 := volumeWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), errRun)
		if err2 != nil {
			log.Errorf("Failed to update job status to Done with err for SplitVolumeWorkflow: %v", err2)
			return err2
		}
		return errRun
	}

	volumeWf.Status = WorkflowStatusCompleted
	err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Done for SplitVolumeWorkflow: %v", err)
	}
	return nil
}

func (wf *volumeSplitWorkflow) Setup(ctx workflow.Context, input interface{}) error {
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

func (wf *volumeSplitWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	log := util.GetLogger(ctx)
	volume := args[0].(*datamodel.Volume)

	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	options := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, options)

	defer func() {
		err2 := workflow.ExecuteActivity(ctx, activities.VolumeCreateActivity.UpdateVolumeStateInDB, volume.UUID, models.LifeCycleStateREADY, models.LifeCycleStateAvailableDetails).Get(ctx, nil)
		if err2 != nil {
			log.Errorf("Failed to update volume state in DB to READY: %v", err2)
		}
	}()

	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, volume.Pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{Nodes: dbNodes, Password: volume.Pool.PoolCredentials.Password, SecretID: volume.Pool.PoolCredentials.SecretID, DeploymentName: volume.Pool.DeploymentName, CertificateID: volume.Pool.PoolCredentials.CertificateID, AuthType: volume.Pool.PoolCredentials.AuthType})

	err = workflow.ExecuteActivity(ctx, activities.VolumeCreateActivity.InitiateSplitForVolume, &volume, &node, nil).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Update cloneSharedBytes to 0 in the database
	err = workflow.ExecuteActivity(ctx, activities.VolumeSplitActivity.UpdateCloneSharedBytesInDB, volume.UUID, uint64(0)).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	return nil, nil
}
