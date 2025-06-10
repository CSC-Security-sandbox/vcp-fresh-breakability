package workflows

import (
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type clusterPeerWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

var _ WorkflowInterface = &clusterPeerWorkflow{}

func AcceptClusterPeerWorkflow(ctx workflow.Context, params *common.ClusterPeerParams, pool *datamodel.Pool) error {
	clusterPeerWF := new(clusterPeerWorkflow)
	err := clusterPeerWF.Setup(ctx, params)
	if err != nil {
		return err
	}
	clusterPeerWF.Status = WorkflowStatusRunning
	err = clusterPeerWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return err
	}
	_, err = clusterPeerWF.Run(ctx, params, pool)
	if err != nil {
		clusterPeerWF.Status = WorkflowStatusFailed
		err = clusterPeerWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		return err
	}
	clusterPeerWF.Status = WorkflowStatusCompleted
	err = clusterPeerWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return err
}

func (wf *clusterPeerWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	acceptClusterPeerParams := input.(*common.ClusterPeerParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = acceptClusterPeerParams.AccountName
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

func (wf *clusterPeerWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	params := args[0].(*common.ClusterPeerParams)
	pool := args[1].(*datamodel.Pool)
	clusterPeerActivity := &activities.ClusterPeerActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var dbNode *datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, pool.ID).Get(ctx, &dbNode)
	if err != nil {
		return nil, err
	}
	node := CreateNodeForProviderWithPool(dbNode, pool)

	clusterPeer := &common.ClusterPeerParams{}
	err = workflow.ExecuteActivity(ctx, clusterPeerActivity.AcceptClusterPeer, params, node).Get(ctx, &clusterPeer)
	if err != nil {
		return nil, err
	}
	return clusterPeer, nil
}
