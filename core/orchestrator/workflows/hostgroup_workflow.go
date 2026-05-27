package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type hostGroupUpdateWorkflow struct {
	// add fields needed for volume workflow
	BaseWorkflow
	SE *database.Storage
}

// Enforcing the WorkflowInterface on hostGroupUpdateWorkflow
var _ WorkflowInterface = &hostGroupUpdateWorkflow{}

// UpdateHostGroupWorkflow updates the specified host group and returns it
func UpdateHostGroupWorkflow(ctx workflow.Context, hostGroup *datamodel.HostGroup) (gcpgenserver.V1betaUpdateHostGroupRes, error) {
	log := util.GetLogger(ctx)
	hgWf := new(hostGroupUpdateWorkflow)
	err := hgWf.Setup(ctx, hostGroup)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	hgWf.Status = WorkflowStatusRunning
	err = hgWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	defer func() {
		if err != nil {
			hgWf.Status = WorkflowStatusFailed
			err = hgWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		} else {
			hgWf.Status = WorkflowStatusCompleted
			err = hgWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		}
	}()

	_, errRun := hgWf.Run(ctx, hostGroup)
	if errRun != nil {
		log.Errorf("HostGroup update workflow completed with error: %v", err)
		return nil, ConvertToVSAError(errRun)
	}
	log.Infof("HostGroup update workflow completed successfully")
	return nil, nil
}

func (wf *hostGroupUpdateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	hostGroup := input.(*datamodel.HostGroup)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = hostGroup.Account.Name
	wf.Status = "created"
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

func (wf *hostGroupUpdateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	hg := args[0].(*datamodel.HostGroup)
	updateActivity := &activities.HostGroupUpdateActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
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

	err = workflow.ExecuteActivity(ctx, updateActivity.UpdateIGroups, &hg).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	return nil, nil
}
