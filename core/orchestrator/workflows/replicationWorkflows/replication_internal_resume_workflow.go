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

type internalVolumeReplicationResumeWorkflow struct {
	workflows.BaseWorkflow
	SE *database.Storage
}

var _ workflows.WorkflowInterface = &internalVolumeReplicationResumeWorkflow{}

func ResumeInternalVolumeReplicationWorkflow(ctx workflow.Context, replicationDb *datamodel.VolumeReplication, forceResume bool) (*vsa.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	repWf := new(internalVolumeReplicationResumeWorkflow)
	err := repWf.Setup(ctx, replicationDb)
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
	_, err = repWf.Run(ctx, replicationDb, forceResume)
	if err != nil {
		logger.Info("Internal Resume Replication workflow run executed with error", "error", err)
	}
	return nil, err
}

func (wf *internalVolumeReplicationResumeWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	replicationParams := input.(*datamodel.VolumeReplication)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = replicationParams.Account.Name
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

func (wf *internalVolumeReplicationResumeWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	replication := args[0].(*datamodel.VolumeReplication)
	forceResume := args[1].(bool)
	replicationActivity := &replicationActivities.InternalVolumeReplicationResumeActivity{}
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
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
			NonRetryableErrorTypes: []string{"NonRetryableErr"},
		},
	}

	ctx = workflow.WithActivityOptions(ctx, ao)

	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, replication.Volume.PoolID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, err
	}

	node := common.CreateNodeForProvider(common.NodeProviderInput{Nodes: dbNodes, Password: replication.Volume.Pool.PoolCredentials.Password, SecretID: replication.Volume.Pool.PoolCredentials.SecretID, CertificateID: replication.Volume.Pool.PoolCredentials.CertificateID, DeploymentName: replication.Volume.Pool.DeploymentName, AuthType: replication.Volume.Pool.PoolCredentials.AuthType})
	var replicationResumeResponse *vsa.VolumeReplication
	err = workflow.ExecuteActivity(ctx, replicationActivity.ResumeVolumeReplication, replication, node, forceResume).Get(ctx, &replicationResumeResponse)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetSnapmirrorDetails, replication, node).Get(ctx, &replicationResumeResponse)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.UpdateVolumeReplicationResumeDetails, replication, replicationResumeResponse).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.UpdateVolumeType, replication).Get(ctx, nil)
	return nil, err
}
