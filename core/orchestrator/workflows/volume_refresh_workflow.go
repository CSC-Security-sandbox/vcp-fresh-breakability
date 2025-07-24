package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type volumeMetricHydrationWorkflow struct {
	BaseWorkflow
}

// Enforcing the WorkflowInterface on volumeMetricHydrationWorkflow
var _ WorkflowInterface = &volumeMetricHydrationWorkflow{}

// VolumeRefreshWorkflow updates the volume fields by fetching the volume details from ONTAP
func VolumeRefreshWorkflow(ctx workflow.Context, volume *datamodel.Volume) error {
	log := util.GetLogger(ctx)
	volumeWf := new(volumeMetricHydrationWorkflow)
	err := volumeWf.Setup(ctx, volume.Account.Name)
	if err != nil {
		log.Errorf("Failed to setup VolumeRefreshWorkflow: %v", err)
		return err
	}
	volumeWf.Status = WorkflowStatusRunning
	err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		log.Errorf("Failed to update job status for VolumeRefreshWorkflow: %v", err)
		return err
	}
	_, err = volumeWf.Run(ctx, volume)
	if err != nil {
		log.Errorf("Failed to run VolumeRefreshWorkflow: %v", err)
		volumeWf.Status = WorkflowStatusFailed
		err2 := volumeWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err2 != nil {
			log.Errorf("Failed to update job status with error details for VolumeRefreshWorkflow: %v", err2)
			return err2
		}
		return err
	}
	volumeWf.Status = WorkflowStatusCompleted
	err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		log.Errorf("Failed to update job status with done for VolumeRefreshWorkflow: %v", err)
	}
	return err
}

func (wf *volumeMetricHydrationWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	accountName := input.(string)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = accountName
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

func (wf *volumeMetricHydrationWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	log := util.GetLogger(ctx)
	dbVolume := args[0].(*datamodel.Volume)
	volumeActivity := &activities.VolumeUpdateActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, dbVolume.Pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, err
	}
	node := common.CreateNodeForProvider(common.NodeProviderInput{Nodes: dbNodes, Password: dbVolume.Pool.PoolCredentials.Password, SecretID: dbVolume.Pool.PoolCredentials.SecretID, DeploymentName: dbVolume.Pool.DeploymentName, CertificateID: dbVolume.Pool.PoolCredentials.CertificateID, AuthType: dbVolume.Pool.PoolCredentials.AuthType})

	volResponse := &vsa.VolumeResponse{}
	err = workflow.ExecuteActivity(ctx, volumeActivity.GetVolumeFromONTAP, dbVolume, node).Get(ctx, &volResponse)
	if err != nil {
		return nil, err
	}

	log.Debugf("Volume %v retrieved from ONTAP successfully", dbVolume)

	err = workflow.ExecuteActivity(ctx, volumeActivity.RefreshVolumeFieldsInDB, dbVolume.UUID, volResponse).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
}
