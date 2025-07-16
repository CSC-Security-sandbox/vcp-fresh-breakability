package workflows

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/resource_events_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"netapp.com/vsa/lifecycle-manager/pkg/log"
)

var (
	CVPJobRetryMaxAttempts           = env.GetInt("CVP_JOB_RETRY_MAX_ATTEMPTS", 10)
	InitialRetryIntervalForCVPClient = env.GetString("CVP_CLIENT_RETRY_INTERVAL", "60s")
)

// StartProjectEventOffStateWorkflow is a workflow that handles the OFF state for StartProjectEvent.
type startProjectEventOffStateWorkflow struct {
	BaseWorkflow
}

// Enforcing the WorkflowInterface on startProjectEventOffStateWorkflow
var _ WorkflowInterface = &startProjectEventOffStateWorkflow{}

// StartProjectEventOffStateWorkflow is a workflow that handles the OFF state for StartProjectEvent.
func StartProjectEventOffStateWorkflow(ctx workflow.Context, params *common.StartProjectEventParams) (interface{}, error) {
	startProjectEventWorkflow := new(startProjectEventOffStateWorkflow)
	err := startProjectEventWorkflow.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	startProjectEventWorkflow.Status = WorkflowStatusRunning
	err = startProjectEventWorkflow.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			startProjectEventWorkflow.Status = WorkflowStatusFailed
			err = startProjectEventWorkflow.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		} else {
			startProjectEventWorkflow.Status = WorkflowStatusCompleted
			err = startProjectEventWorkflow.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		}
	}()

	_, err = startProjectEventWorkflow.Run(ctx, params)
	if err != nil {
		log.Errorf("startProjectEventOffStateWorkflow workflow completed with error: %v", err)
		return nil, err
	}
	log.Infof("startProjectEventOffStateWorkflow workflow completed successfully")
	return nil, err
}

func (s *startProjectEventOffStateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	startProjectEventOffStateParams := input.(*common.StartProjectEventParams)
	info := workflow.GetInfo(ctx)
	s.CustomerID = startProjectEventOffStateParams.ProjectNumber
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

func (s *startProjectEventOffStateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	// add activities to disable account, list pools, turn off clusters, forward call to SDE, poll job
	startProjectEventParams := args[0].(*common.StartProjectEventParams)
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{}
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
	ao1 := ao
	ao1.RetryPolicy.MaximumAttempts = int32(CVPJobRetryMaxAttempts)
	ao1.RetryPolicy.InitialInterval, err = time.ParseDuration(InitialRetryIntervalForCVPClient)
	if err != nil {
		return nil, err
	}

	ctx = workflow.WithActivityOptions(ctx, ao)
	ctx1 := workflow.WithActivityOptions(ctx, ao1)

	if cvp.CVP_HOST == "" {
		return nil, nil
	}

	// TODO: add VSA cluster power off activity
	var result *common.StartProjectEventResult
	err = workflow.ExecuteActivity(ctx, startProjectEventActivity.StartProjectEventForSDEActivity, startProjectEventParams).Get(ctx, &result)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx1, startProjectEventActivity.PollStartProjectEventSDEOperationActivity, startProjectEventParams, &result).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
}

// StartProjectEventOnStateWorkflow is a workflow that handles the ON state for StartProjectEvent.
type startProjectEventOnStateWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

// StartProjectEventOnStateWorkflow is a workflow that handles the OFF state for StartProjectEvent.
func StartProjectEventOnStateWorkflow(ctx workflow.Context, params *common.StartProjectEventParams) (interface{}, error) {
	startProjectEventWorkflow := new(startProjectEventOnStateWorkflow)
	err := startProjectEventWorkflow.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	startProjectEventWorkflow.Status = WorkflowStatusRunning
	err = startProjectEventWorkflow.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			startProjectEventWorkflow.Status = WorkflowStatusFailed
			err = startProjectEventWorkflow.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		} else {
			startProjectEventWorkflow.Status = WorkflowStatusCompleted
			err = startProjectEventWorkflow.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		}
	}()

	_, err = startProjectEventWorkflow.Run(ctx, params)
	if err != nil {
		log.Errorf("startProjectEventOnStateWorkflow workflow completed with error: %v", err)
		return nil, err
	}
	log.Infof("startProjectEventOnStateWorkflow workflow completed successfully")
	return nil, err
}

func (s *startProjectEventOnStateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	startProjectEventOnStateParams := input.(*common.StartProjectEventParams)
	info := workflow.GetInfo(ctx)
	s.CustomerID = startProjectEventOnStateParams.ProjectNumber
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

func (s *startProjectEventOnStateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	// add activities to enable account, list pools, turn on clusters, forward call to SDE, poll job
	startProjectEventParams := args[0].(*common.StartProjectEventParams)
	startProjectEventActivity := &resource_events_activities.StartProjectEventActivity{}
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

	ao1 := ao
	ao1.RetryPolicy.MaximumAttempts = int32(CVPJobRetryMaxAttempts)
	ao1.RetryPolicy.InitialInterval, err = time.ParseDuration(InitialRetryIntervalForCVPClient)
	if err != nil {
		return nil, err
	}

	ctx = workflow.WithActivityOptions(ctx, ao)
	ctx1 := workflow.WithActivityOptions(ctx, ao1)

	if cvp.CVP_HOST == "" {
		return nil, nil
	}

	// TODO: add VSA cluster power on activity
	var result *common.StartProjectEventResult
	err = workflow.ExecuteActivity(ctx, startProjectEventActivity.StartProjectEventForSDEActivity, startProjectEventParams).Get(ctx, &result)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx1, startProjectEventActivity.PollStartProjectEventSDEOperationActivity, startProjectEventParams, &result).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
}
