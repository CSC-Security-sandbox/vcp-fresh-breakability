package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type smcTokenRotationWorkflow struct {
	BaseWorkflow
}

func CreateSMCTokenRotationWorkflow(ctx workflow.Context, params *common.CreateSMCTokenRotationParams) error {
	smcWf := new(smcTokenRotationWorkflow)
	err := smcWf.Setup(ctx, params)
	if err != nil {
		return err
	}
	smcWf.Status = WorkflowStatusRunning
	err = smcWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return err
	}
	_, err = smcWf.Run(ctx, params)
	if err != nil {
		smcWf.Status = WorkflowStatusFailed
		err2 := smcWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err2 != nil {
			return err2
		}
		return err
	}
	smcWf.Status = WorkflowStatusCompleted
	err = smcWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return err
}

var _ WorkflowInterface = &smcTokenRotationWorkflow{}

func (wf *smcTokenRotationWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	params := input.(*common.CreateSMCTokenRotationParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = params.AccountName
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

func (wf *smcTokenRotationWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	// 1. Get SMC License from Cloud
	retryPolicy, err := PopulateRetryPolicyParams()
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
	smcTokenRotationActivity := &activities.SmcTokenRotationActivity{}
	var license string
	wf.Logger.Info("Executing GetSMCLicenseFromCloud activity")
	err = workflow.ExecuteActivity(ctx, smcTokenRotationActivity.GetSMCLicenseFromCloud).Get(ctx, &license)
	if err != nil {
		wf.Logger.Error("Failed to get SMC license from cloud", "error", err)
		return nil, temporal.NewApplicationError("failed to get SMC license", "", err)
	}
	if license == "" {
		wf.Logger.Error("Failed to get SMC license from cloud", "error", err)
		return nil, temporal.NewApplicationError("SMC license is empty", "", nil)
	}

	return "SMC token rotation completed", nil
}
