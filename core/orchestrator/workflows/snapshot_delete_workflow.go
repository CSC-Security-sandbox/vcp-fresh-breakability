package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type snapshotDeleteWorkflow struct {
	BaseWorkflow
}

// Enforcing the WorkflowInterface on snapshotDeleteWorkflow
var _ WorkflowInterface = &snapshotDeleteWorkflow{}

// DeleteSnapshotWorkflow Delete Snapshot Workflow process snapshot related requests from a customer.
func DeleteSnapshotWorkflow(ctx workflow.Context, params *common.DeleteSnapshotParams, snapshot *datamodel.Snapshot) (gcpgenserver.V1betaDescribeSnapshotRes, error) {
	logger := util.GetLogger(ctx)
	snapshotWf := new(snapshotDeleteWorkflow)
	err := snapshotWf.Setup(ctx, params)
	if err != nil {
		logger.Infof("Snapshot Delete workflow setup executed with error: %v", err)
		return nil, err
	}
	snapshotWf.Status = WorkflowStatusRunning
	err = snapshotWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		logger.Infof("Update job status for snapshot executed with error: %v", err)
		return nil, err
	}
	defer func() {
		if err == nil {
			snapshotWf.Status = WorkflowStatusCompleted
			err = snapshotWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		} else {
			snapshotWf.Status = WorkflowStatusFailed
			err = snapshotWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		}
	}()
	_, err = snapshotWf.Run(ctx, snapshot)
	if err != nil {
		logger.Infof("Snapshot delete workflow run executed with error: %v", err)
		return nil, err
	}
	logger.Info("Snapshot workflow completed successfully")
	return nil, err
}

func (wf *snapshotDeleteWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	deleteSnapshotParams := input.(*common.DeleteSnapshotParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = deleteSnapshotParams.AccountName
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

// shouldUpdateSnapshotStateToError checks if the error should trigger updating the snapshot state to error
// Some errors are legitimate business logic errors that should not mark the snapshot as having a deletion error
func shouldUpdateSnapshotStateToError(err error) bool {
	// Check for specific VCP errors that should not update snapshot state to error
	var customError *vsaerrors.CustomError
	if vsaerrors.As(err, &customError) {
		// ErrDeleteSnapshot (7001) - snapshot has owners/clones/replication, legitimate business error
		if customError.TrackingID == vsaerrors.ErrDeleteSnapshot {
			return false
		}
		// ErrVolumeNotOnlineForSnapshotDelete (7002) - volume is not online, legitimate business error
		if customError.TrackingID == vsaerrors.ErrVolumeNotOnlineForSnapshotDelete {
			return false
		}
	}

	// For all other errors, update the snapshot state to error
	return true
}

func (wf *snapshotDeleteWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	logger := util.GetLogger(ctx)
	snapshot := args[0].(*datamodel.Snapshot)
	deleteActivity := &activities.SnapshotDeleteActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	options := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, options)

	dbSnapshot := snapshot
	logger.Infof("Starting the snapshot deletion workflow for snapshot: %s", dbSnapshot.Name)

	var dbNodes []*datamodel.Node
	defer func() {
		if err != nil {
			if shouldUpdateSnapshotStateToError(err) {
				dbSnapshot.State = models.LifeCycleStateError
				dbSnapshot.StateDetails = models.LifeCycleStateDeletionErrorDetails
			} else {
				dbSnapshot.State = models.LifeCycleStateREADY
				dbSnapshot.StateDetails = models.LifeCycleStateAvailableDetails
			}
			workflow.ExecuteActivity(ctx, deleteActivity.UpdateDeleteSnapshotDetails, &dbSnapshot)
		}
	}()
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &dbSnapshot.Volume.PoolID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, err
	}

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{Nodes: dbNodes, Password: dbSnapshot.Volume.Pool.PoolCredentials.Password, SecretID: dbSnapshot.Volume.Pool.PoolCredentials.SecretID, DeploymentName: dbSnapshot.Volume.Pool.DeploymentName, CertificateID: dbSnapshot.Volume.Pool.PoolCredentials.CertificateID, AuthType: dbSnapshot.Volume.Pool.PoolCredentials.AuthType})

	err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteSnapshotInONTAP, &dbSnapshot, &node).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteSnapshot, &dbSnapshot).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
}
