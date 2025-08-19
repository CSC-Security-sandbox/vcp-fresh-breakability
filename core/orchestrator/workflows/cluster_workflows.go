package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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
	_, customErr := clusterPeerWF.Run(ctx, params, pool)
	if customErr != nil {
		clusterPeerWF.Status = WorkflowStatusFailed
		err = clusterPeerWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
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

func (wf *clusterPeerWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	params := args[0].(*common.ClusterPeerParams)
	pool := args[1].(*datamodel.Pool)
	clusterPeerActivity := &activities.ClusterPeerActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:        1,
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{Nodes: dbNodes, Password: pool.PoolCredentials.Password, SecretID: pool.PoolCredentials.SecretID, DeploymentName: pool.DeploymentName, CertificateID: pool.PoolCredentials.CertificateID, AuthType: pool.PoolCredentials.AuthType})

	clusterPeer := &common.ClusterPeerParams{}
	err = workflow.ExecuteActivity(ctx, clusterPeerActivity.AcceptClusterPeer, params, node).Get(ctx, &clusterPeer)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	return clusterPeer, nil
}
