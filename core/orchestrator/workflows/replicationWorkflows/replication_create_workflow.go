package replicationWorkflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	ReplicationJobsRetryMaxAttempts = env.GetInt("REPLICATION_JOBS_RETRY_MAX_ATTEMPTS", 10)
)

type createVolumeReplicationWorkflow struct {
	workflows.BaseWorkflow
	SE *database.Storage
}

// Enforcing the WorkflowInterface on createVolumeReplicationWorkflow
var _ workflows.WorkflowInterface = &createVolumeReplicationWorkflow{}

// CreateVolumeReplicationWorkflow Workflow process volume replication related request from a customer.
func CreateVolumeReplicationWorkflow(ctx workflow.Context, params *common.CreateVolumeReplicationParams, volumeRep *datamodel.VolumeReplication, event *replication.CreateReplicationEvent) (gcpgenserver.V1betaDescribeVolumeRes, error) {
	volumeRepWf := new(createVolumeReplicationWorkflow)
	err := volumeRepWf.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	volumeRepWf.Status = workflows.WorkflowStatusRunning
	err = volumeRepWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		volumeRepWf.Status = workflows.WorkflowStatusFailed
		err = volumeRepWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		return nil, err
	}
	_, customErr := volumeRepWf.Run(ctx, volumeRep, event)
	if customErr != nil {
		volumeRepWf.Status = workflows.WorkflowStatusFailed
		err = volumeRepWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		return nil, err
	}
	volumeRepWf.Status = workflows.WorkflowStatusCompleted
	err = volumeRepWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
}

func (wf *createVolumeReplicationWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createReplicationParams := input.(*common.CreateVolumeReplicationParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = createReplicationParams.AccountName
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

func (wf *createVolumeReplicationWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	log := util.GetLogger(ctx)
	volumeReplication := args[0].(*datamodel.VolumeReplication)
	event := args[1].(*replication.CreateReplicationEvent)
	replicationActivity := &replicationActivities.VolumeReplicationCreateActivity{}
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
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	ao1 := ao
	ao1.RetryPolicy.MaximumAttempts = int32(ReplicationJobsRetryMaxAttempts)

	ctx1 := workflow.WithActivityOptions(ctx, ao1)
	dbVolumeRep := volumeReplication

	replicationResult := replication.CreateReplicationResult{
		SrcProjectNumber: &event.SourceProjectNumber,
		DstProjectNumber: &event.DestinationProjectNumber,
		Event:            event,
		DbVolReplication: dbVolumeRep,
	}

	// Defer function to mark the database entry in error state if any error occurs
	defer func() {
		if err != nil {
			// On panic, mark volume replication in error state
			volumeReplication.State = models.LifeCycleStateError
			volumeReplication.StateDetails = models.LifeCycleStateCreationErrorDetails
			err2 := workflow.ExecuteActivity(ctx, replicationActivity.UpdateReplicationState, *volumeReplication).Get(ctx, nil)
			if err2 != nil {
				log.Errorf("Failed to update volume state in DB to error: %v", err2)
			}
		}
	}()

	var dbNodes []*datamodel.Node

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetSrcBasePath, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetDstBasePath, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetSignedSrcToken, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetSignedDstToken, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &replicationResult.Event.SourcePool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	srcNode := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{Nodes: dbNodes, Password: replicationResult.Event.SourcePool.PoolCredentials.Password, SecretID: replicationResult.Event.SourcePool.PoolCredentials.SecretID, CertificateID: replicationResult.Event.SourcePool.PoolCredentials.CertificateID, DeploymentName: replicationResult.Event.SourcePool.DeploymentName, AuthType: replicationResult.Event.SourcePool.PoolCredentials.AuthType})

	replicationResult.SrcNode = srcNode
	err = workflow.ExecuteActivity(ctx, replicationActivity.GetSourceInterclusterLifs, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetDestinationPoolDetails, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.CreateClusterPeering, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.AcceptClusterPeering, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx1, replicationActivity.DescribeRemoteJob, &replicationResult).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	dbVolumeRep.StateDetails = "Cluster Peered"
	err = workflow.ExecuteActivity(ctx, replicationActivity.UpdateReplicationState, dbVolumeRep).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.CreateDestinationVolume, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx1, replicationActivity.DescribeRemoteJob, &replicationResult).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.HydrateDestinationVolume, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	dbVolumeRep.StateDetails = "Remote volume created"
	err = workflow.ExecuteActivity(ctx, replicationActivity.UpdateReplicationState, dbVolumeRep).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.GetVolumeSVMNames, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.CreateReplicationOnDestination, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.AcceptSvmPeer, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx1, replicationActivity.DescribeRemoteJob, &replicationResult).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.UpdateReplicationDetails, &replicationResult).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, replicationActivity.MountReplication, &replicationResult).Get(ctx, &replicationResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	return nil, workflows.ConvertToVSAError(err)
}
