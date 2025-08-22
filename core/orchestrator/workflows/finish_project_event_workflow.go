package workflows

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/resource_events_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	finishProjectCVPJobRetryMaxAttempts           = env.GetInt("FINISH_PROJECT_CVP_CLIENT_RETRY_MAX_ATTEMPTS", 10)
	finishProjectInitialRetryIntervalForCVPClient = env.GetString("FINISH_PROJECT_CVP_CLIENT_RETRY_INTERVAL", "30s")
	finishProjectBackoffCoefficientForCVPClient   = env.GetFloat64("FINISH_PROJECT_CVP_CLIENT_BACKOFF_COEFFICIENT", 1.0)
)

// FinishProjectEventDeleteStateWorkflow is a workflow that handles the DELETE state for FinishProjectEvent.
type finishProjectEventDeleteStateWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

// FinishProjectEventDeleteStateWorkflow is a workflow that handles the DELETE state for FinishProjectEvent.
func FinishProjectEventDeleteStateWorkflow(ctx workflow.Context, params *common.FinishProjectEventParams) (interface{}, error) {
	log := util.GetLogger(ctx)
	finishProjectEventWorkflow := new(finishProjectEventDeleteStateWorkflow)
	err := finishProjectEventWorkflow.Setup(ctx, params)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	finishProjectEventWorkflow.Status = WorkflowStatusRunning
	err = finishProjectEventWorkflow.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	defer func() {
		if err != nil {
			finishProjectEventWorkflow.Status = WorkflowStatusFailed
			err = finishProjectEventWorkflow.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		} else {
			finishProjectEventWorkflow.Status = WorkflowStatusCompleted
			err = finishProjectEventWorkflow.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		}
	}()

	_, errRun := finishProjectEventWorkflow.Run(ctx, params)
	if errRun != nil {
		log.Errorf("finishProjectEventDeleteStateWorkflow completed with error: %v", errRun)
		return nil, ConvertToVSAError(errRun)
	}
	log.Infof("finishProjectEventDeleteStateWorkflow completed successfully")
	return nil, nil
}

func (s *finishProjectEventDeleteStateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	finishProjectEventDeleteStateParams := input.(*common.FinishProjectEventParams)
	info := workflow.GetInfo(ctx)
	s.CustomerID = finishProjectEventDeleteStateParams.ProjectNumber
	s.Status = WorkflowStatusCreated
	s.ID = info.WorkflowExecution.ID
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": s.ID, "customerID": s.CustomerID})
	logger := util.GetLogger(ctx)
	s.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         s.ID,
			Status:     s.Status,
			CustomerID: s.CustomerID,
		}, nil
	})
}

func (s *finishProjectEventDeleteStateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	finishProjectEventParams := args[0].(*common.FinishProjectEventParams)
	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
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

	aoCVP := ao
	aoCVP.RetryPolicy.InitialInterval, err = time.ParseDuration(finishProjectInitialRetryIntervalForCVPClient)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	aoCVP.RetryPolicy.MaximumAttempts = int32(finishProjectCVPJobRetryMaxAttempts)
	aoCVP.RetryPolicy.BackoffCoefficient = finishProjectBackoffCoefficientForCVPClient

	ctx = workflow.WithActivityOptions(ctx, ao)
	ctxCVP := workflow.WithActivityOptions(ctx, aoCVP)

	if cvp.CVP_HOST == "" {
		return nil, nil
	}

	// TODO: add VSA cluster power on activity
	var result *common.FinishProjectEventResult
	err = workflow.ExecuteActivity(ctx, finishProjectEventActivity.FinishProjectEventForSDEActivity, finishProjectEventParams).Get(ctx, &result)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctxCVP, finishProjectEventActivity.PollFinishProjectEventSDEOperationActivity, finishProjectEventParams, &result).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if isPreAGA {
		s.Logger.Info("In pre-AGA stage, skipping resource deletion from VCP.")
		// If the project is in pre-AGA state, we will not delete the resources from VCP.
		return nil, nil
	}

	// TODO: Delete Active directory from VCP. As this is common resource it might have deleted in SDE handle resource delete activity.

	// Delete KMS config from VCP. As this is common resource it might have deleted in SDE handle resource delete activity.
	kmsActivities := &kms_activities.KmsConfigActivity{}
	var kmsConfigs []*datamodel.KmsConfig
	err = workflow.ExecuteActivity(ctx, kmsActivities.ListKmsConfigActivity, finishProjectEventParams.ProjectNumber).Get(ctx, &kmsConfigs)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	// For now, we will have only one KMS config per project.
	if len(kmsConfigs) > 0 && kmsConfigs[0] != nil {
		kmsConfig := kmsConfigs[0]
		err = workflow.ExecuteActivity(ctx, kmsActivities.DeleteKmsConfig, kmsConfig,
			&common.DeleteKmsConfigParams{KmsConfigID: kmsConfig.UUID}).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}
	return nil, ConvertToVSAError(err)
}
