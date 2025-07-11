package replicationWorkflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type mountCheckWorkflow struct {
	workflows.BaseWorkflow
	SE *database.Storage
}

var _ workflows.WorkflowInterface = &mountCheckWorkflow{}

func PerformMountCheckWorkflow(ctx workflow.Context, replicationUUID string, accountName string) error {
	mountCheckWF := new(mountCheckWorkflow)
	err := mountCheckWF.Setup(ctx, accountName)
	if err != nil {
		return err
	}
	mountCheckWF.Status = workflows.WorkflowStatusRunning
	err = mountCheckWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return err
	}
	_, err = mountCheckWF.Run(ctx, replicationUUID, accountName)
	if err != nil {
		mountCheckWF.Status = workflows.WorkflowStatusFailed
		err = mountCheckWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		return err
	}
	mountCheckWF.Status = workflows.WorkflowStatusCompleted
	err = mountCheckWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return err
}

func (wf *mountCheckWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	AccountName := input.(string)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = AccountName
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

func (wf *mountCheckWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	replicationUUID := args[0].(string)
	accountName := args[1].(string)
	mountJobActivity := &replicationActivities.MountJobActivity{}
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

	var replication *datamodel.VolumeReplication
	err = workflow.ExecuteActivity(ctx, mountJobActivity.GetReplication, replicationUUID).Get(ctx, &replication)
	if err != nil {
		return nil, err
	}

	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, replication.Volume.PoolID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, err
	}
	node := common.CreateNodeForProvider(common.NodeProviderInput{Nodes: dbNodes, Password: replication.Volume.Pool.PoolCredentials.Password, SecretID: replication.Volume.Pool.PoolCredentials.SecretID, CertificateID: replication.Volume.Pool.PoolCredentials.CertificateID, DeploymentName: replication.Volume.Pool.DeploymentName})

	ao1 := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			NonRetryableErrorTypes: []string{"NonRetryableErr"},
		},
	}
	ctx1 := workflow.WithActivityOptions(ctx, ao1)
	err = workflow.ExecuteActivity(ctx1, mountJobActivity.CheckMountJob, replication, node, accountName).Get(ctx1, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, mountJobActivity.GetReplicationFromOntap, replication, node, accountName).Get(ctx, &replication)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, mountJobActivity.UpdateReplicationInDB, replication).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
