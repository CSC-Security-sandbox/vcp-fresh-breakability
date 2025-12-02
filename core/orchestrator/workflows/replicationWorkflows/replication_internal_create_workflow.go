package replicationWorkflows

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type internalVolumeReplicationCreateWorkflow struct {
	workflows.BaseWorkflow
	SE *database.Storage
}

var _ workflows.WorkflowInterface = &internalVolumeReplicationCreateWorkflow{}

func CreateInternalVolumeReplicationWorkflow(ctx workflow.Context, params *common.CreateVolumeReplicationInternalParams, replication *datamodel.VolumeReplication) (*vsa.VolumeReplication, error) {
	repWf := new(internalVolumeReplicationCreateWorkflow)
	err := repWf.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	if err = repWf.EnsureJobState(ctx, models.JobsStateNEW); err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	repWf.Status = workflows.WorkflowStatusRunning
	err = repWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		repWf.Status = workflows.WorkflowStatusFailed
		err = repWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		return nil, err
	}
	_, customErr := repWf.Run(ctx, params, replication)
	if customErr != nil {
		repWf.Status = workflows.WorkflowStatusFailed
		err = repWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		return nil, err
	}
	repWf.Status = workflows.WorkflowStatusCompleted
	err = repWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
}

func (wf *internalVolumeReplicationCreateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createReplicationParams := input.(*common.CreateVolumeReplicationInternalParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = createReplicationParams.VolumeReplication.Account.Name
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

func (wf *internalVolumeReplicationCreateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	params := args[0].(*common.CreateVolumeReplicationInternalParams)
	replication := args[1].(*datamodel.VolumeReplication)
	replicationActivity := &replicationActivities.InternalVolumeReplicationActivity{}
	replicationCreateActivity := &replicationActivities.VolumeReplicationCreateActivity{}
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Duration(workflows.StartToCloseTimeoutForReplicationActivities) * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     workflows.BackoffCoefficientForReplicationActivities,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	log := util.GetLogger(ctx)

	// Defer function to mark the database entry in error state if any error occurs
	defer func() {
		if err != nil {
			// On panic, mark volume replication in error state
			replication.State = models.LifeCycleStateError
			replication.StateDetails = err.Error()
			err2 := workflow.ExecuteActivity(ctx, replicationCreateActivity.UpdateReplicationState, *replication).Get(ctx, nil)
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

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
		Nodes:            dbNodes,
		DeploymentName:   replication.Volume.Pool.DeploymentName,
		OntapCredentials: replication.Volume.Pool.PoolCredentials,
	})

	var replicationCreateResponse *vsa.VolumeReplication
	volumeExternalUUID := replication.Volume.VolumeAttributes.ExternalUUID
	err = workflow.ExecuteActivity(ctx, replicationActivity.CreateVolumeReplicationInternal, params, node, volumeExternalUUID).Get(ctx, &replicationCreateResponse)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.UpdateVolumeReplicationDetails, replication, replicationCreateResponse, nil).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.HydrateReplicationCreate, replication, params.VolumeReplication.Account.Name).Get(ctx, nil)

	return nil, workflows.ConvertToVSAError(err)
}
