package workflows

import (
	"errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type unRegisterNodeFromHarvestFarmParams struct {
	PoolID            int64
	CustomerProjectID string
	TenantProjectID   string
}

type unRegisterNodeFromHarvestFarmWorkflow struct {
	BaseWorkflow
	SE database.Storage
}

// Enforcing the WorkflowInterface on unRegisterNodeToHarvestFarmWorkflow
var _ WorkflowInterface = &unRegisterNodeFromHarvestFarmWorkflow{}

// UnRegisterNodeFromHarvestFarmWorkflow is a Temporal workflow that un-registers a node to the Harvest farm
func UnRegisterNodeFromHarvestFarmWorkflow(ctx workflow.Context, params *unRegisterNodeFromHarvestFarmParams) error {
	wf := new(unRegisterNodeFromHarvestFarmWorkflow)
	err := wf.Setup(ctx, params)
	if err != nil {
		return err
	}
	wf.Status = WorkflowStatusRunning
	_, err = wf.Run(ctx, params)
	if err != nil {
		wf.Status = WorkflowStatusFailed
		return err
	}
	wf.Status = WorkflowStatusCompleted
	return nil
}

func (wf *unRegisterNodeFromHarvestFarmWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	workflowInput, ok := input.(*unRegisterNodeFromHarvestFarmParams)
	if !ok {
		return errors.New("unable to type cast input as unRegisterNodeFromHarvestFarmParams")
	}
	wf.CustomerID = workflowInput.CustomerProjectID
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger
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
			NonRetryableErrorTypes: activities.UnRegisterNodeFromHarvestFarmNonRetryableErrors,
		},
	}
	activityParams := &activities.UnRegisterNodeFromHarvestActivityParams{
		PoolID:            input.PoolID,
		CustomerProjectID: input.CustomerProjectID,
		TenantProjectID:   input.TenantProjectID,
	}

	ctx = workflow.WithActivityOptions(ctx, ao)

	var dbNodes []*datamodel.Node
	// 1. Validate and Get Nodes from DB which are in Deleted state
	err = workflow.ExecuteActivity(ctx, unRegisterActivity.ValidateAndGetNodes, activityParams).Get(ctx, &dbNodes)
	if err != nil {
		var appErr *temporal.ApplicationError
		if errors.As(err, &appErr) && appErr.NonRetryable() && appErr.Type() == activities.UnRegisterNodesInfoNotAvailable {
			wf.Logger.Infof("no nodes available to perform unregister from harvest farm")
			return nil, nil
		}
		return nil, err
	}

	// If No Nodes exist, i.e., pollers are already deleted from harvest farm hence mark the wf as success
	if len(dbNodes) == 0 {
		return nil, nil
	}
	// Update Nodes info to activity params
	activityParams.Nodes = dbNodes

	var nodeGroupMap []*datamodel.NodeNodeGroupMap
	// 2. Get node and harvest mapping details which are not in soft deleted state
	err = workflow.ExecuteActivity(ctx, unRegisterActivity.GetNodeGroupMapping, activityParams).Get(ctx, &nodeGroupMap)
	if err != nil {
		var appErr *temporal.ApplicationError
		if errors.As(err, &appErr) && appErr.NonRetryable() && appErr.Type() == activities.UnRegisterNodeGroupMapNotAvailable {
			wf.Logger.Infof("no nodeGroupMap available to perform unregister from harvest farm")
			return nil, nil
		}
		return nil, err
	}

	// If No nodeGroupMap exists, i.e., pollers are already deleted from harvest farm hence mark the wf as success
	if len(nodeGroupMap) == 0 {
		return nil, nil
	}
	// Update NodeLeaseMap info to activity params
	activityParams.NodeGroupsMap = nodeGroupMap

	// 3.Delete NodeGroupMapping from DB i.e., soft delete
	err = workflow.ExecuteActivity(ctx, unRegisterActivity.DeleteNodeGroupMapping, activityParams).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	// 4. Delete node pollers from harvest farm, i.e., node harvest yaml files will be deleted from PVC
	err = workflow.ExecuteActivity(ctx, unRegisterActivity.DeletePollersFromHarvestFarm, activityParams).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	// 5. Validate and delete kubernetes lease information from DB, i.e., if no nodes are assigned to
	// harvest farm then HPA will delete the pod having poller count to 0
	err = workflow.ExecuteActivity(ctx, unRegisterActivity.ValidateAndReleaseLease, activityParams).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
