package flexcache_workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/flexcache_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type flexCacheCreateWorkflow struct {
	workflows.BaseWorkflow
}

var _ workflows.WorkflowInterface = &flexCacheCreateWorkflow{}

// CreateFlexCacheWorkflow Volume Workflow process volume related requests from a customer.
func CreateFlexCacheWorkflow(ctx workflow.Context, params *common.CreateVolumeParams, volume *datamodel.Volume) error {
	log := util.GetLogger(ctx)
	flexCacheWf := new(flexCacheCreateWorkflow)
	err := flexCacheWf.Setup(ctx, params)
	if err != nil {
		log.Errorf("Failed to setup CreateFlexCacheWorkflow: %v", err)
		return err
	}
	flexCacheWf.Status = workflows.WorkflowStatusRunning
	err = flexCacheWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Processing for CreateFlexCacheWorkflow: %v", err)
		return err
	}
	_, customErr := flexCacheWf.Run(ctx, volume, params)
	if customErr != nil {
		log.Errorf("CreateFlexCacheWorkflow completed with error: %v", customErr)
		flexCacheWf.Status = workflows.WorkflowStatusFailed
		err2 := flexCacheWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if err2 != nil {
			log.Errorf("Failed to update job status to Done with error for CreateFlexCacheWorkflow: %v", err2)
			return err2
		}
		return customErr
	}
	flexCacheWf.Status = workflows.WorkflowStatusCompleted
	err = flexCacheWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Done for CreateFlexCacheWorkflow: %v", err)
	}
	return err
}

func (wf *flexCacheCreateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createPoolParams := input.(*common.CreateVolumeParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = createPoolParams.AccountName
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

func (wf *flexCacheCreateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *errors.CustomError) {
	log := util.GetLogger(ctx)
	dbVolume := args[0].(*datamodel.Volume)
	flexCacheVolumeCreateActivity := &flexcache_activities.FlexCacheVolumeCreateActivity{}
	volumeCreateActivity := &activities.VolumeCreateActivity{}
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
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

	rollbackManager := common.NewRollbackManager()
	defer func() {
		if err != nil {
			err2 := workflow.ExecuteActivity(ctx, volumeCreateActivity.UpdateVolumeStateInDB, dbVolume.UUID, models.LifeCycleStateError, models.LifeCycleStateCreationErrorDetails).Get(ctx, nil)
			if err2 != nil {
				log.Errorf("Failed to update volume state in DB to error: %v", err2)
			}
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &dbVolume.Pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{Nodes: dbNodes, Password: dbVolume.Pool.PoolCredentials.Password, SecretID: dbVolume.Pool.PoolCredentials.SecretID, DeploymentName: dbVolume.Pool.DeploymentName, CertificateID: dbVolume.Pool.PoolCredentials.CertificateID, AuthType: dbVolume.Pool.PoolCredentials.AuthType})

	volCreateResponse := &vsa.VolumeResponse{}
	err = workflow.ExecuteActivity(ctx, flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, &dbVolume, &node).Get(ctx, &volCreateResponse)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, volumeCreateActivity.UpdateVolumeDetails, &dbVolume, &volCreateResponse).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	return nil, nil
}
