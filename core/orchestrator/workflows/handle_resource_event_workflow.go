package workflows

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	errorcore "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/resource_events_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	isPreAGA = env.GetBool("IS_PRE_AGA", false)
)

const (
	handleResourceVCPJobMaxRetryAttempts = 5
	handleResourceVCPJobMaxRetryInterval = "30s"
)

type updateResourceStateONWorkflow struct {
	BaseWorkflow
	ResourceID string
}

func UpdateResourceStateONWorkflow(ctx workflow.Context, params *common.UpdateResourceStateParams) (interface{}, error) {
	log := util.GetLogger(ctx)
	updateResourceWF := new(updateResourceStateONWorkflow)
	err := updateResourceWF.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	updateResourceWF.Status = WorkflowStatusRunning
	err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			updateResourceWF.Status = WorkflowStatusFailed
			err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		} else {
			updateResourceWF.Status = WorkflowStatusCompleted
			err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		}
	}()

	_, err = updateResourceWF.Run(ctx, params)
	if err != nil {
		log.Errorf("handleResourceEventOffStateWorkflow workflow completed with error: %v", err)
		return nil, err
	}
	log.Infof("handleResourceEventOffStateWorkflow workflow completed successfully")
	return nil, err
}

func (wf *updateResourceStateONWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	updateResourceStateParams := input.(*common.UpdateResourceStateParams)
	info := workflow.GetInfo(ctx)
	wf.CustomerID = updateResourceStateParams.ProjectNumber
	wf.ID = info.WorkflowExecution.ID
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

func (s updateResourceStateONWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	handleResourceEventParams := args[0].(*common.UpdateResourceStateParams)
	handleResourceEventActivity := &resource_events_activities.ResourceEventsActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	interval, err := time.ParseDuration(handleResourceVCPJobMaxRetryInterval)
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        interval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        handleResourceVCPJobMaxRetryAttempts,
			NonRetryableErrorTypes: []string{"NotFoundErr"},
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

	if isPreAGA {
		err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventCheckForVCPActivity, handleResourceEventParams).Get(ctx, nil)
		if err == nil {
			return nil, errors.NewNotImplementedYetErr()
		}
	} else {
		err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventsONForVCPActivity, handleResourceEventParams).Get(ctx, nil)
		if err == nil {
			return nil, nil
		}
	}
	var applicationErr *temporal.ApplicationError
	if errorcore.As(err, &applicationErr) && applicationErr.NonRetryable() && applicationErr.Type() != resource_events_activities.ErrTypeResourceNotFound {
		return nil, err
	}

	var result *common.HandleResourceEventResult
	err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventsForSDEActivity, handleResourceEventParams).Get(ctx, &result)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx1, handleResourceEventActivity.PollHandleResourceEventSDEOperationActivity, handleResourceEventParams, &result).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
}

type updateResourceStateOFFWorkflow struct {
	BaseWorkflow
	ResourceID string
}

func UpdateResourceStateOFFWorkflow(ctx workflow.Context, params *common.UpdateResourceStateParams) (interface{}, error) {
	log := util.GetLogger(ctx)
	updateResourceWF := new(updateResourceStateOFFWorkflow)
	err := updateResourceWF.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	updateResourceWF.Status = WorkflowStatusRunning
	err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			updateResourceWF.Status = WorkflowStatusFailed
			err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		} else {
			updateResourceWF.Status = WorkflowStatusCompleted
			err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		}
	}()

	_, err = updateResourceWF.Run(ctx, params)
	if err != nil {
		log.Errorf("handleResourceEventOffStateWorkflow workflow completed with error: %v", err)
		return nil, err
	}
	log.Infof("handleResourceEventOffStateWorkflow workflow completed successfully")
	return nil, err
}

func (wf *updateResourceStateOFFWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	updateResourceStateParams := input.(*common.UpdateResourceStateParams)
	info := workflow.GetInfo(ctx)
	wf.CustomerID = updateResourceStateParams.ProjectNumber
	wf.ID = info.WorkflowExecution.ID
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

func (s updateResourceStateOFFWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	handleResourceEventParams := args[0].(*common.UpdateResourceStateParams)
	handleResourceEventActivity := &resource_events_activities.ResourceEventsActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	interval, err := time.ParseDuration(handleResourceVCPJobMaxRetryInterval)
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        interval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        handleResourceVCPJobMaxRetryAttempts,
			NonRetryableErrorTypes: []string{"NotFoundErr"},
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

	var isVCPResource bool
	if isPreAGA {
		err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventCheckForVCPActivity, handleResourceEventParams).Get(ctx, &isVCPResource)
		if err == nil {
			return nil, errors.NewNotImplementedYetErr()
		}
	} else {
		err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventsOFFForVCPActivity, handleResourceEventParams).Get(ctx, &isVCPResource)
		if err == nil {
			return nil, nil
		}
	}
	var applicationErr *temporal.ApplicationError
	if errorcore.As(err, &applicationErr) && applicationErr.NonRetryable() && applicationErr.Type() != resource_events_activities.ErrTypeResourceNotFound {
		return nil, err
	}

	var result *common.HandleResourceEventResult
	err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventsForSDEActivity, handleResourceEventParams).Get(ctx, &result)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx1, handleResourceEventActivity.PollHandleResourceEventSDEOperationActivity, handleResourceEventParams, &result).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
}

type updateResourceStateCommonResourceOFFWorkflow struct {
	BaseWorkflow
	ResourceID string
}

func UpdateResourceStateCommonResourceOFFWorkflow(ctx workflow.Context, params *common.UpdateResourceStateParams) (interface{}, error) {
	log := util.GetLogger(ctx)
	updateResourceWF := new(updateResourceStateCommonResourceOFFWorkflow)
	err := updateResourceWF.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	updateResourceWF.Status = WorkflowStatusRunning
	err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			updateResourceWF.Status = WorkflowStatusFailed
			err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		} else {
			updateResourceWF.Status = WorkflowStatusCompleted
			err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		}
	}()

	_, err = updateResourceWF.Run(ctx, params)
	if err != nil {
		log.Errorf("handleResourceEventOffStateWorkflow workflow completed with error: %v", err)
		return nil, err
	}
	log.Infof("handleResourceEventOffStateWorkflow workflow completed successfully")
	return nil, err
}

func (wf *updateResourceStateCommonResourceOFFWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	updateResourceStateParams := input.(*common.UpdateResourceStateParams)
	info := workflow.GetInfo(ctx)
	wf.CustomerID = updateResourceStateParams.ProjectNumber
	wf.ID = info.WorkflowExecution.ID
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

func (s updateResourceStateCommonResourceOFFWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	handleResourceEventParams := args[0].(*common.UpdateResourceStateParams)
	handleResourceEventActivity := &resource_events_activities.ResourceEventsActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	interval, err := time.ParseDuration(handleResourceVCPJobMaxRetryInterval)
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        interval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        handleResourceVCPJobMaxRetryAttempts,
			NonRetryableErrorTypes: []string{"NotFoundErr"},
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

	var isVCPResource bool
	if isPreAGA {
		err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventCheckForVCPActivity, handleResourceEventParams).Get(ctx, &isVCPResource)
	} else {
		err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventsOFFForVCPActivity, handleResourceEventParams).Get(ctx, &isVCPResource)
	}
	var applicationErr *temporal.ApplicationError
	if errorcore.As(err, &applicationErr) && applicationErr.NonRetryable() && applicationErr.Type() != resource_events_activities.ErrTypeResourceNotFound {
		return nil, err
	}

	var result *common.HandleResourceEventResult
	err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventsForSDEActivity, handleResourceEventParams).Get(ctx, &result)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx1, handleResourceEventActivity.PollHandleResourceEventSDEOperationActivity, handleResourceEventParams, &result).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
}

type updateResourceStateCommonResourceONWorkflow struct {
	BaseWorkflow
	ResourceID string
}

func UpdateResourceStateCommonResourceONWorkflow(ctx workflow.Context, params *common.UpdateResourceStateParams) (interface{}, error) {
	log := util.GetLogger(ctx)
	updateResourceWF := new(updateResourceStateCommonResourceONWorkflow)
	err := updateResourceWF.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	updateResourceWF.Status = WorkflowStatusRunning
	err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			updateResourceWF.Status = WorkflowStatusFailed
			err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		} else {
			updateResourceWF.Status = WorkflowStatusCompleted
			err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		}
	}()

	_, err = updateResourceWF.Run(ctx, params)
	if err != nil {
		log.Errorf("handleResourceEventOffStateWorkflow workflow completed with error: %v", err)
		return nil, err
	}
	log.Infof("handleResourceEventOffStateWorkflow workflow completed successfully")
	return nil, err
}

func (wf *updateResourceStateCommonResourceONWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	updateResourceStateParams := input.(*common.UpdateResourceStateParams)
	info := workflow.GetInfo(ctx)
	wf.CustomerID = updateResourceStateParams.ProjectNumber
	wf.ID = info.WorkflowExecution.ID
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

func (s updateResourceStateCommonResourceONWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	handleResourceEventParams := args[0].(*common.UpdateResourceStateParams)
	handleResourceEventActivity := &resource_events_activities.ResourceEventsActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	interval, err := time.ParseDuration(handleResourceVCPJobMaxRetryInterval)
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        interval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        handleResourceVCPJobMaxRetryAttempts,
			NonRetryableErrorTypes: []string{"NotFoundErr"},
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

	var isVCPResource bool
	if isPreAGA {
		err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventCheckForVCPActivity, handleResourceEventParams).Get(ctx, &isVCPResource)
	} else {
		err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventsONForVCPActivity, handleResourceEventParams).Get(ctx, &isVCPResource)
	}
	var applicationErr *temporal.ApplicationError
	if errorcore.As(err, &applicationErr) && applicationErr.NonRetryable() && applicationErr.Type() != resource_events_activities.ErrTypeResourceNotFound {
		return nil, err
	}

	var result *common.HandleResourceEventResult
	err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventsForSDEActivity, handleResourceEventParams).Get(ctx, &result)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx1, handleResourceEventActivity.PollHandleResourceEventSDEOperationActivity, handleResourceEventParams, &result).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
}
