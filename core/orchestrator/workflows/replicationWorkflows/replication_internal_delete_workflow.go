package replicationWorkflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type internalVolumeReplicationDeleteWorkflow struct {
	workflows.BaseWorkflow
	SE *database.Storage
}

var _ workflows.WorkflowInterface = &internalVolumeReplicationDeleteWorkflow{}

func DeleteInternalVolumeReplicationWorkflow(ctx workflow.Context, replication *datamodel.VolumeReplication) (*vsa.VolumeReplication, error) {
	repWf := new(internalVolumeReplicationDeleteWorkflow)
	err := repWf.Setup(ctx, replication)
	if err != nil {
		return nil, err
	}
	repWf.Status = workflows.WorkflowStatusRunning
	err = repWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		repWf.Status = workflows.WorkflowStatusFailed
		err = repWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		return nil, err
	}
	_, customErr := repWf.Run(ctx, replication)
	if customErr != nil {
		repWf.Status = workflows.WorkflowStatusFailed
		err = repWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		return nil, err
	}
	repWf.Status = workflows.WorkflowStatusCompleted
	err = repWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
}

func (wf *internalVolumeReplicationDeleteWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	deleteReplicationParams := input.(*datamodel.VolumeReplication)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = deleteReplicationParams.Account.Name
	wf.Status = workflows.WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

func (wf *internalVolumeReplicationDeleteWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	log := util.GetLogger(ctx)
	replication := args[0].(*datamodel.VolumeReplication)
	replicationActivity := &replicationActivities.InternalVolumeReplicationDeleteActivity{}
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"NonRetryableErr", "PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	defer func() {
		if err != nil {
			err2 := workflow.ExecuteActivity(ctx, replicationActivity.UpdateReplicationStateInDBForDelete, replication).Get(ctx, nil)
			if err2 != nil {
				log.Errorf("Failed to update volume state in DB to error: %v", err2)
			}
		}
	}()

	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, replication.Volume.PoolID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{Nodes: dbNodes, Password: replication.Volume.Pool.PoolCredentials.Password, SecretID: replication.Volume.Pool.PoolCredentials.SecretID, CertificateID: replication.Volume.Pool.PoolCredentials.CertificateID, DeploymentName: replication.Volume.Pool.DeploymentName, AuthType: replication.Volume.Pool.PoolCredentials.AuthType})

	var replicationDeleteResponse *vsa.VolumeReplication
	err = workflow.ExecuteActivity(ctx, replicationActivity.DeleteVolumeReplication, replication, node).Get(ctx, &replicationDeleteResponse)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.UpdateVolumeReplicationDetailsForDelete, replication).Get(ctx, nil)
	return nil, workflows.ConvertToVSAError(err)
}
