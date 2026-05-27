package workflows

import (
	"errors"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	refreshURL = fmt.Sprintf("%s://%s/config/refresh", harvestRestProtocol, harvestHost)
)

// HarvestPollerUpgradeParams is the workflow input for HarvestPollerUpgradeWorkFlow (empty; reserved for future use).
type HarvestPollerUpgradeParams struct {
}

// HarvestPollerUpgradeWorkflowID is the stable Temporal workflow ID for harvest template / poller refresh.
const HarvestPollerUpgradeWorkflowID = "harvest-poller-upgrade"

type harvestNodesRefreshWorkFlow struct {
	BaseWorkflow
	SE database.Storage
}

// Enforcing the WorkflowInterface on HarvestWorkflow
var _ WorkflowInterface = &harvestNodesRefreshWorkFlow{}

// HarvestPollerUpgradeWorkFlow is a Temporal workflow that refreshes the harvest pollers
func HarvestPollerUpgradeWorkFlow(ctx workflow.Context, params *HarvestPollerUpgradeParams) error {
	wf := new(harvestNodesRefreshWorkFlow)
	err := wf.Setup(ctx, params)
	if err != nil {
		return err
	}
	wf.Status = WorkflowStatusRunning
	_, err = wf.Run(ctx, params)
	if e, ok := err.(*vsaerrors.CustomError); ok && e != nil {
		wf.Status = WorkflowStatusFailed
		return err
	}
	wf.Status = WorkflowStatusCompleted
	return nil
}

func (wf *harvestNodesRefreshWorkFlow) Setup(ctx workflow.Context, input interface{}) error {
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	_, ok := input.(*HarvestPollerUpgradeParams)
	if !ok {
		return errors.New("unable to type cast input as HarvestPollerUpgradeParams")
	}
	wf.CustomerID = ""
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger
	return nil
}

func (wf *harvestNodesRefreshWorkFlow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	harvestNodesRefreshActivity := &activities.HarvestNodesRefreshActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
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
	refreshParams := &activities.HarvestNodesRefreshActivityParams{}

	defer func() {
		if err != nil {
			_ = workflow.ExecuteActivity(ctx, harvestNodesRefreshActivity.AlertHarvestRefreshFailure, err.Error()).Get(ctx, nil)
		}
	}()

	ctx = workflow.WithActivityOptions(ctx, ao)

	var nodeGroupsMap []*datamodel.NodeNodeGroupMap

	// 1. Get all NodeGroupsMap records which are in active state
	err = workflow.ExecuteActivity(ctx, harvestNodesRefreshActivity.GetNodeGroupMaps, refreshParams).Get(ctx, &nodeGroupsMap)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// If No node GroupMap exist, i.e., no pollers are registered in harvest farm
	if len(nodeGroupsMap) == 0 {
		return nil, nil
	}
	// Update nodeGroupsMap info to activity params
	refreshParams.NodeGroupMaps = nodeGroupsMap
	refreshParams.RefreshURL = refreshURL

	// 2. Upload the updated harvest nodes yaml files to harvest farm and refresh the harvest farm
	err = workflow.ExecuteActivity(ctx, harvestNodesRefreshActivity.RefreshHarvestNodes, refreshParams).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	return nil, nil
}
