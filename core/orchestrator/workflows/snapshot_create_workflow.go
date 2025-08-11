package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type snapshotCreateWorkflow struct {
	BaseWorkflow
	SE database.Storage
}

var _ WorkflowInterface = &snapshotCreateWorkflow{}

// CreateSnapshotWorkflow Snapshot Workflow process snapshot related requests from a customer.
func CreateSnapshotWorkflow(ctx workflow.Context, params *common.CreateSnapshotParams, snapshot *datamodel.Snapshot) (gcpgenserver.V1betaCreateSnapshotRes, error) {
	logger := util.GetLogger(ctx)
	snapshotWf := new(snapshotCreateWorkflow)
	err := snapshotWf.Setup(ctx, params)
	if err != nil {
		logger.Infof("Snapshot workflow setup executed with error: %v", err)
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
	_, errRun := snapshotWf.Run(ctx, snapshot)
	if errRun != nil {
		logger.Infof("Snapshot workflow run executed with error: %v", err)
		return nil, errRun
	}
	logger.Debug("Snapshot workflow completed successfully")
	return nil, nil
}

// Setup initializes the workflow with the necessary parameters and sets up a query handler for status updates.
func (wf *snapshotCreateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createSnapshotParams := input.(*common.CreateSnapshotParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = createSnapshotParams.AccountName
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
func (wf *snapshotCreateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	snapshot := args[0].(*datamodel.Snapshot)
	logger := util.GetLogger(ctx)
	snapshotActivity := &activities.SnapshotCreateActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	dbSnapshot := snapshot
	snapshotDescription := dbSnapshot.Description // Storing the description in a variable such that we can update this in DB.
	dbSnapshot.Description = wf.ID                // Storing the job UUID in the comments param while requesting ONTAP

	logger.Infof("Starting the snapshot creation workflow for snapshot: %s", dbSnapshot.Name)
	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &dbSnapshot.Volume.PoolID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{Nodes: dbNodes, Password: dbSnapshot.Volume.Pool.PoolCredentials.Password, SecretID: dbSnapshot.Volume.Pool.PoolCredentials.SecretID, DeploymentName: dbSnapshot.Volume.Pool.DeploymentName, CertificateID: dbSnapshot.Volume.Pool.PoolCredentials.CertificateID, AuthType: dbSnapshot.Volume.Pool.PoolCredentials.AuthType})

	var snapshotCreateResponse *vsa.SnapshotProviderResponse
	defer func() {
		dbSnapshot.Description = snapshotDescription
		updateErr := workflow.ExecuteActivity(ctx, snapshotActivity.UpdateSnapshotDetails, &dbSnapshot, snapshotCreateResponse).Get(ctx, nil)
		if updateErr != nil {
			// Since activity has failed, activity will reflect the error in the temporal workflow.
			logger.Errorf("Error updating snapshot details: %v", updateErr)
		}
	}()
	err = workflow.ExecuteActivity(ctx, snapshotActivity.CreateSnapshotInONTAP, &dbSnapshot, &node).Get(ctx, &snapshotCreateResponse)
	if err != nil {
		logger.Errorf("Failed to update snapshot details: %v", err)
		return nil, ConvertToVSAError(err)
	}
	return nil, ConvertToVSAError(err)
}
