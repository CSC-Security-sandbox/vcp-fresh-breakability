package replicationWorkflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type internalVolumeReplicationCreateWorkflow struct {
	workflows.BaseWorkflow
	SE *database.Storage
}

var _ workflows.WorkflowInterface = &internalVolumeReplicationCreateWorkflow{}

func CreateInternalVolumeReplicationWorkflow(ctx workflow.Context, params *common.CreateVolumeReplicationParams, replication *datamodel.VolumeReplication) (*vsa.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	repWf := new(internalVolumeReplicationCreateWorkflow)
	err := repWf.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	repWf.Status = workflows.WorkflowStatusRunning
	err = repWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err == nil {
			repWf.Status = workflows.WorkflowStatusCompleted
			err = repWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		} else {
			repWf.Status = workflows.WorkflowStatusFailed
			err = repWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		}
	}()
	_, err = repWf.Run(ctx, params, replication)
	if err != nil {
		logger.Info("Internal Volume Replication workflow run executed with error", "error", err)
	}
	return nil, err
}

func (wf *internalVolumeReplicationCreateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createReplicationParams := input.(*common.CreateVolumeReplicationParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = createReplicationParams.VolumeReplication.Account.Name
	wf.Status = "created"
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

func (wf *internalVolumeReplicationCreateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	params := args[0].(*common.CreateVolumeReplicationParams)
	replication := args[1].(*datamodel.VolumeReplication)
	replicationActivity := &replicationActivities.InternalVolumeReplicationActivity{}
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
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

	var dbNode *datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, replication.Volume.PoolID).Get(ctx, &dbNode)
	if err != nil {
		return nil, err
	}

	node := createNodeForProvider(dbNode, replication.Volume)

	var replicationCreateResponse *vsa.VolumeReplication
	volumeExternalUUID := replication.Volume.VolumeAttributes.ExternalUUID
	err = workflow.ExecuteActivity(ctx, replicationActivity.CreateVolumeReplicationInternal, params, node, volumeExternalUUID).Get(ctx, &replicationCreateResponse)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.UpdateVolumeReplicationDetails, replication, replicationCreateResponse).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	// TODO: Add activity for hydrating to CCFE

	return nil, err
}

func createNodeForProvider(dbNode *datamodel.Node, volume *datamodel.Volume) *models.Node {
	node := &models.Node{
		EndpointAddress: dbNode.EndpointAddress,
		Username:        volume.Pool.Username,
		Password:        volume.Pool.Password,
	}
	return node
}
