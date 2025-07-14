package workflows

import (
	"errors"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	nodesInfoNotAvailable    = "no Available Nodes to unregister"
	nodeGroupMapNotAvailable = "node group map not available"
)

type unRegisterNodeFromHarvestFarmParams struct {
	PoolID int64
}

type unRegisterNodeFromHarvestFarmWorkflow struct {
	BaseWorkflow
	SE database.Storage
}

// Enforcing the WorkflowInterface on unRegisterNodeToHarvestFarmWorkflow
var _ WorkflowInterface = &unRegisterNodeFromHarvestFarmWorkflow{}

// UnRegisterNodeToHarvestFarmWorkflow is a Temporal workflow that un-registers a node to the Harvest farm
func UnRegisterNodeFromHarvestFarmWorkflow(ctx workflow.Context, params *unRegisterNodeFromHarvestFarmParams) error {
	wf := new(unRegisterNodeFromHarvestFarmWorkflow)
	err := wf.Setup(ctx, params)
	if err != nil {
		return err
	}
	wf.Status = WorkflowStatusRunning
	// Optionally update job status here if needed
	_, err = wf.Run(ctx, params)
	if err != nil {
		wf.Status = WorkflowStatusFailed
		// Optionally update job status here if needed
		return err
	}
	wf.Status = WorkflowStatusCompleted
	// Optionally update job status here if needed
	return nil
}

func (wf *unRegisterNodeFromHarvestFarmWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = "" // Set if you have a customer/account field
	wf.Status = WorkflowStatusCreated
	// Add logger or extra fields as needed
	return nil
}

func (wf *unRegisterNodeFromHarvestFarmWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	input := args[0].(*unRegisterNodeFromHarvestFarmParams)
	unRegisterActivity := &activities.UnRegisterNodeFromHarvestActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{nodeGroupMapNotAvailable, nodeGroupMapNotAvailable},
		},
	}
	poolID := input.PoolID
	ctx = workflow.WithActivityOptions(ctx, ao)

	var dbNodes []*datamodel.Node
	// 1. Validate and Get Nodes from DB which are in Deleted state
	err = workflow.ExecuteActivity(ctx, unRegisterActivity.ValidateAndGetNodes, poolID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, err
	}

	if len(dbNodes) == 0 {
		return nil, errors.New(nodesInfoNotAvailable)
	}

	var nodeGroupMap []*datamodel.NodeNodeGroupMap
	// 2. Get node and harvest mapping details which are not in soft deleted state
	err = workflow.ExecuteActivity(ctx, unRegisterActivity.GetNodeGroupMapping, dbNodes).Get(ctx, &nodeGroupMap)
	if err != nil {
		return nil, err
	}

	// If No nodeGroupMap exists, i.e., pollers are already deleted from harvest farm
	if len(nodeGroupMap) == 0 {
		return nil, errors.New(nodeGroupMapNotAvailable)
	}

	// 3.Delete NodeGroupMapping from DB i.e., soft delete
	err = workflow.ExecuteActivity(ctx, unRegisterActivity.DeleteNodeGroupMapping, nodeGroupMap).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	// 4. Delete node pollers from harvest farm, i.e., node harvest yaml files will be deleted from PVC

	err = workflow.ExecuteActivity(ctx, unRegisterActivity.DeletePollersFromHarvestFarm, nodeGroupMap).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	// 5. Validate and delete kubernetes lease information from DB, i.e., if no nodes are assigned to
	// harvest farm then HPA will delete the pod having poller count to 0
	err = workflow.ExecuteActivity(ctx, unRegisterActivity.ValidateAndReleaseLease, nodeGroupMap).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
