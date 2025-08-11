package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type snapshotUpdateWorkflow struct {
	BaseWorkflow
	SE database.Storage
}

var _ WorkflowInterface = &snapshotUpdateWorkflow{}

// UpdateSnapshotWorkflow Snapshot Workflow process snapshot related requests from a customer.
func UpdateSnapshotWorkflow(ctx workflow.Context, snapshot *datamodel.Snapshot) (gcpgenserver.V1betaCreateSnapshotRes, error) {
	logger := util.GetLogger(ctx)
	snapshotWf := new(snapshotUpdateWorkflow)
	err := snapshotWf.Setup(ctx, snapshot)
	if err != nil {
		logger.Infof("Snapshot update workflow setup executed with error: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	snapshotWf.Status = WorkflowStatusRunning
	err = snapshotWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		logger.Infof("Update job status for snapshot executed with error: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
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
	_, errRun := snapshotWf.Run(ctx, snapshot)
	if errRun != nil {
		logger.Infof("Snapshot update workflow run executed with error: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(errRun)
	}
	logger.Debug("Snapshot update workflow completed successfully")
	return nil, nil
}

// Setup initializes the workflow with the necessary parameters and sets up a query handler for status updates.
func (wf *snapshotUpdateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	snapshotParams := input.(*datamodel.Snapshot)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = snapshotParams.Account.Name
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

// Run executes the snapshot creation workflow, including creating the snapshot and updating its details.
func (wf *snapshotUpdateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	snapshot := args[0].(*datamodel.Snapshot)
	snapshotUpdateActivity := &activities.SnapshotUpdateActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	err = workflow.ExecuteActivity(ctx, snapshotUpdateActivity.UpdateSnapshot, snapshot).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	return nil, nil
}
