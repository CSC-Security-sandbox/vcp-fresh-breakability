package flexcache_workflows

import (
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/flexcache_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/flexcache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"time"
)

type flexCacheVolumeDeleteWorkflow struct {
	workflows.BaseWorkflow
}

// Enforcing the WorkflowInterface on flexCacheVolumeDeleteWorkflow
var _ workflows.WorkflowInterface = &flexCacheVolumeDeleteWorkflow{}

// DeleteFlexCacheVolumeWorkflow Delete FlexCache Volume Workflow process volume related requests from a customer.
func DeleteFlexCacheVolumeWorkflow(ctx workflow.Context, volume *datamodel.Volume) error {
	log := util.GetLogger(ctx)
	flexCacheWf := new(flexCacheVolumeDeleteWorkflow)
	err := flexCacheWf.Setup(ctx, volume)
	if err != nil {
		log.Errorf("FlexCache volume delete workflow setup executed with error: %v", err)
		return err
	}
	flexCacheWf.Status = workflows.WorkflowStatusRunning
	err = flexCacheWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Processing for DeleteFlexCacheVolumeWorkflow: %v", err)
		return err
	}

	_, customErr := flexCacheWf.Run(ctx, volume)
	if customErr != nil {
		log.Errorf("DeleteFlexCacheVolumeWorkflow completed with error: %v", customErr)
		flexCacheWf.Status = workflows.WorkflowStatusFailed
		err2 := flexCacheWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if err2 != nil {
			log.Errorf("Failed to update job status to Done with err for DeleteFlexCacheVolumeWorkflow: %v", err2)
			return err2
		}
		return customErr
	}

	flexCacheWf.Status = workflows.WorkflowStatusCompleted
	err = flexCacheWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Done for DeleteFlexCacheVolumeWorkflow: %v", err)
	}
	return err
}

func (wf *flexCacheVolumeDeleteWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	volume := input.(*datamodel.Volume)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = volume.Account.Name
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

func (wf *flexCacheVolumeDeleteWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	log := util.GetLogger(ctx)
	dbVolume := args[0].(*datamodel.Volume)
	deleteActivity := &flexcache_activities.FlexCacheVolumeDeleteActivity{}
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	options := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, options)
	rollbackManager := common.NewRollbackManager()
	defer func() {
		if err != nil {
			err2 := workflow.ExecuteActivity(ctx, activities.VolumeCreateActivity.UpdateVolumeStateInDB, dbVolume.UUID, models.LifeCycleStateError, models.LifeCycleStateDeletionErrorDetails).Get(ctx, nil)
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

	flexCacheResult := flexcache.DeleteFlexCacheResult{
		DBVolume: dbVolume,
		Node:     node,
	}

	err = workflow.ExecuteActivity(ctx, deleteActivity.UnmountVolumeInOntapActivity, &flexCacheResult).Get(ctx, &flexCacheResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	// If the volume was never created in ontap, there will be no unmount job to wait for
	if flexCacheResult.UnmountJobResponse != nil {
		err = workflows.WaitForONTAPJob(ctx, flexCacheResult.UnmountJobResponse, node, time.Minute*10)
		if err != nil {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to unmount volume: %w", err))
		}
	}

	err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteFlexCacheVolumeInOntapActivity, &flexCacheResult).Get(ctx, &flexCacheResult)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	// If the volume was never created in ontap, there will be no delete job to wait for
	if flexCacheResult.DeleteJobResponse != nil {
		err = workflows.WaitForONTAPJob(ctx, flexCacheResult.DeleteJobResponse, node, time.Minute*10)
		if err != nil {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to delete FlexCache volume: %w", err))
		}
	}

	err = workflow.ExecuteActivity(ctx, activities.VolumeDeleteActivity.DeleteVolume, &dbVolume).Get(ctx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	return nil, nil
}
