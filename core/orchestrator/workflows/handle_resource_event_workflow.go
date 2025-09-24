package workflows

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	errorcore "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/resource_events_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
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
		return nil, ConvertToVSAError(err)
	}
	updateResourceWF.Status = WorkflowStatusRunning
	err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	var customErr *vsaerrors.CustomError
	defer func() {
		if customErr != nil {
			updateResourceWF.Status = WorkflowStatusFailed
			err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		} else {
			updateResourceWF.Status = WorkflowStatusCompleted
			err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		}
	}()

	_, customErr = updateResourceWF.Run(ctx, params)
	if customErr != nil {
		log.Errorf("updateResourceStateONWorkflow workflow completed with error: %v", customErr)
		return nil, customErr
	}
	log.Infof("updateResourceStateONWorkflow workflow completed successfully")
	return nil, nil
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

func (s updateResourceStateONWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	handleResourceEventParams := args[0].(*common.UpdateResourceStateParams)
	handleResourceEventActivity := &resource_events_activities.ResourceEventsActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	interval, err := time.ParseDuration(handleResourceVCPJobMaxRetryInterval)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        interval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        handleResourceVCPJobMaxRetryAttempts,
			NonRetryableErrorTypes: []string{"NonRetryableError", "PanicError"},
		},
	}

	ao1 := ao
	ao1.RetryPolicy.MaximumAttempts = int32(CVPJobRetryMaxAttempts)
	ao1.RetryPolicy.InitialInterval, err = time.ParseDuration(InitialRetryIntervalForCVPClient)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	ctx = workflow.WithActivityOptions(ctx, ao)
	ctx1 := workflow.WithActivityOptions(ctx, ao1)

	var isVCPResource bool
	err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventsONForVCPActivity, handleResourceEventParams).Get(ctx, &isVCPResource)
	if err != nil {
		var applicationErr *temporal.ApplicationError
		if errorcore.As(err, &applicationErr) && applicationErr.NonRetryable() {
			// For NotFoundErr, we should continue to SDE path instead of failing
			if applicationErr.Type() == resource_events_activities.ErrTypeResourceNotFound {
				isVCPResource = false
			} else {
				return nil, ConvertToVSAError(err)
			}
		} else {
			// For other retryable errors, return and let Temporal retry
			return nil, ConvertToVSAError(err)
		}
	} else if isVCPResource {
		if handleResourceEventParams.ResourceType == common.ResourceStateV1ResourceTypeStoragePool {
			return nil, ConvertToVSAError(errors.NewNotImplementedYetErr())
		}
		return nil, nil
	}

	if cvp.CVP_HOST == "" {
		return nil, nil
	}

	var result *common.HandleResourceEventResult
	err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventsForSDEActivity, handleResourceEventParams).Get(ctx, &result)
	if err != nil {
		var applicationErr *temporal.ApplicationError
		if errorcore.As(err, &applicationErr) && applicationErr.NonRetryable() {
			// For NotFoundErr from SDE (404 responses), treat as non-retryable and continue
			if applicationErr.Type() == resource_events_activities.ErrTypeResourceNotFound {
				logger := util.GetLogger(ctx)
				logger.Infof("Resource %s not found in SDE", handleResourceEventParams.ResourceId)
				return nil, ConvertToVSAError(err)
			} else {
				return nil, ConvertToVSAError(err)
			}
		} else {
			// For other retryable errors, return and let Temporal retry
			return nil, ConvertToVSAError(err)
		}
	}

	err = workflow.ExecuteActivity(ctx1, handleResourceEventActivity.PollHandleResourceEventSDEOperationActivity, handleResourceEventParams, &result).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	return nil, nil
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
		return nil, ConvertToVSAError(err)
	}
	updateResourceWF.Status = WorkflowStatusRunning
	err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	var customErr *vsaerrors.CustomError
	defer func() {
		if customErr != nil {
			updateResourceWF.Status = WorkflowStatusFailed
			err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		} else {
			updateResourceWF.Status = WorkflowStatusCompleted
			err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		}
	}()

	_, customErr = updateResourceWF.Run(ctx, params)
	if customErr != nil {
		log.Errorf("updateResourceStateOFFWorkflow workflow completed with error: %v", customErr)
		return nil, customErr
	}
	log.Infof("updateResourceStateOFFWorkflow workflow completed successfully")
	return nil, nil
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

func (s updateResourceStateOFFWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	handleResourceEventParams := args[0].(*common.UpdateResourceStateParams)
	handleResourceEventActivity := &resource_events_activities.ResourceEventsActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	interval, err := time.ParseDuration(handleResourceVCPJobMaxRetryInterval)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        interval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        handleResourceVCPJobMaxRetryAttempts,
			NonRetryableErrorTypes: []string{"NonRetryableError", "PanicError"},
		},
	}

	ao1 := ao
	ao1.RetryPolicy.MaximumAttempts = int32(CVPJobRetryMaxAttempts)
	ao1.RetryPolicy.InitialInterval, err = time.ParseDuration(InitialRetryIntervalForCVPClient)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	ctx = workflow.WithActivityOptions(ctx, ao)
	ctx1 := workflow.WithActivityOptions(ctx, ao1)

	var isVCPResource bool
	err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventsOFFForVCPActivity, handleResourceEventParams).Get(ctx, &isVCPResource)
	if err != nil {
		var applicationErr *temporal.ApplicationError
		if errorcore.As(err, &applicationErr) && applicationErr.NonRetryable() {
			// For NotFoundErr, we should continue to SDE path instead of failing
			if applicationErr.Type() == resource_events_activities.ErrTypeResourceNotFound {
				isVCPResource = false
			} else {
				return nil, ConvertToVSAError(err)
			}
		} else {
			// For other retryable errors, return and let Temporal retry
			return nil, ConvertToVSAError(err)
		}
	} else if isVCPResource {
		if handleResourceEventParams.ResourceType == common.ResourceStateV1ResourceTypeStoragePool {
			return nil, ConvertToVSAError(errors.NewNotImplementedYetErr())
		}
		return nil, nil
	}

	if cvp.CVP_HOST == "" {
		return nil, nil
	}

	var result *common.HandleResourceEventResult
	err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventsForSDEActivity, handleResourceEventParams).Get(ctx, &result)
	if err != nil {
		var applicationErr *temporal.ApplicationError
		if errorcore.As(err, &applicationErr) && applicationErr.NonRetryable() {
			// For NotFoundErr from SDE (404 responses), treat as non-retryable and continue
			if applicationErr.Type() == resource_events_activities.ErrTypeResourceNotFound {
				logger := util.GetLogger(ctx)
				logger.Infof("Resource %s not found in SDE", handleResourceEventParams.ResourceId)
				return nil, ConvertToVSAError(err)
			} else {
				return nil, ConvertToVSAError(err)
			}
		} else {
			// For other retryable errors, return and let Temporal retry
			return nil, ConvertToVSAError(err)
		}
	}

	err = workflow.ExecuteActivity(ctx1, handleResourceEventActivity.PollHandleResourceEventSDEOperationActivity, handleResourceEventParams, &result).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	return nil, nil
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
		return nil, ConvertToVSAError(err)
	}
	updateResourceWF.Status = WorkflowStatusRunning
	err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	var customErr *vsaerrors.CustomError
	defer func() {
		if customErr != nil {
			updateResourceWF.Status = WorkflowStatusFailed
			err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		} else {
			updateResourceWF.Status = WorkflowStatusCompleted
			err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		}
	}()

	_, customErr = updateResourceWF.Run(ctx, params)
	if customErr != nil {
		log.Errorf("updateResourceStateCommonResourceOFFWorkflow workflow completed with error: %v", customErr)
		return nil, customErr
	}
	log.Infof("updateResourceStateCommonResourceOFFWorkflow workflow completed successfully")
	return nil, nil
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

func (s updateResourceStateCommonResourceOFFWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	handleResourceEventParams := args[0].(*common.UpdateResourceStateParams)
	handleResourceEventActivity := &resource_events_activities.ResourceEventsActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	interval, err := time.ParseDuration(handleResourceVCPJobMaxRetryInterval)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        interval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        handleResourceVCPJobMaxRetryAttempts,
			NonRetryableErrorTypes: []string{"NonRetryableError", "PanicError"},
		},
	}

	ao1 := ao
	ao1.RetryPolicy.MaximumAttempts = int32(CVPJobRetryMaxAttempts)
	ao1.RetryPolicy.InitialInterval, err = time.ParseDuration(InitialRetryIntervalForCVPClient)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	ctx = workflow.WithActivityOptions(ctx, ao)
	ctx1 := workflow.WithActivityOptions(ctx, ao1)

	var isVCPResource bool
	err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventsOFFForVCPActivity, handleResourceEventParams).Get(ctx, &isVCPResource)
	if err != nil {
		var applicationErr *temporal.ApplicationError
		if errorcore.As(err, &applicationErr) && applicationErr.NonRetryable() {
			// For NotFoundErr, we should continue to SDE path instead of failing
			if applicationErr.Type() == resource_events_activities.ErrTypeResourceNotFound {
				isVCPResource = false
			} else {
				return nil, ConvertToVSAError(err)
			}
		} else {
			// For other retryable errors, return and let Temporal retry
			return nil, ConvertToVSAError(err)
		}
	}
	// For common resources, we always proceed to SDE regardless of VCP resource status
	// Log VCP resource status for observability
	logger := util.GetLogger(ctx)
	if isVCPResource {
		logger.Infof("Common resource OFF operation completed successfully in VCP for resource %s", handleResourceEventParams.ResourceId)
	} else {
		logger.Infof("Common resource not found in VCP, proceeding with SDE operation for resource %s", handleResourceEventParams.ResourceId)
	}

	if cvp.CVP_HOST == "" {
		return nil, nil
	}

	var result *common.HandleResourceEventResult
	err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventsForSDEActivity, handleResourceEventParams).Get(ctx, &result)
	if err != nil {
		var applicationErr *temporal.ApplicationError
		if errorcore.As(err, &applicationErr) && applicationErr.NonRetryable() {
			// For NotFoundErr from SDE (404 responses), treat as non-retryable and continue
			if applicationErr.Type() == resource_events_activities.ErrTypeResourceNotFound {
				logger.Infof("Resource %s not found in SDE", handleResourceEventParams.ResourceId)
				return nil, ConvertToVSAError(err)
			} else {
				return nil, ConvertToVSAError(err)
			}
		} else {
			// For other retryable errors, return and let Temporal retry
			return nil, ConvertToVSAError(err)
		}
	}

	err = workflow.ExecuteActivity(ctx1, handleResourceEventActivity.PollHandleResourceEventSDEOperationActivity, handleResourceEventParams, &result).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	return nil, nil
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
		return nil, ConvertToVSAError(err)
	}
	updateResourceWF.Status = WorkflowStatusRunning
	err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	var customErr *vsaerrors.CustomError
	defer func() {
		if customErr != nil {
			updateResourceWF.Status = WorkflowStatusFailed
			err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		} else {
			updateResourceWF.Status = WorkflowStatusCompleted
			err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		}
	}()

	_, customErr = updateResourceWF.Run(ctx, params)
	if customErr != nil {
		log.Errorf("updateResourceStateCommonResourceONWorkflow workflow completed with error: %v", customErr)
		return nil, customErr
	}
	log.Infof("updateResourceStateCommonResourceONWorkflow workflow completed successfully")
	return nil, nil
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

func (s updateResourceStateCommonResourceONWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	handleResourceEventParams := args[0].(*common.UpdateResourceStateParams)
	handleResourceEventActivity := &resource_events_activities.ResourceEventsActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	interval, err := time.ParseDuration(handleResourceVCPJobMaxRetryInterval)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        interval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        handleResourceVCPJobMaxRetryAttempts,
			NonRetryableErrorTypes: []string{"NonRetryableError", "PanicError"},
		},
	}

	ao1 := ao
	ao1.RetryPolicy.MaximumAttempts = int32(CVPJobRetryMaxAttempts)
	ao1.RetryPolicy.InitialInterval, err = time.ParseDuration(InitialRetryIntervalForCVPClient)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	ctx = workflow.WithActivityOptions(ctx, ao)
	ctx1 := workflow.WithActivityOptions(ctx, ao1)

	var isVCPResource bool
	err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventsONForVCPActivity, handleResourceEventParams).Get(ctx, &isVCPResource)
	if err != nil {
		var applicationErr *temporal.ApplicationError
		if errorcore.As(err, &applicationErr) && applicationErr.NonRetryable() {
			// For NotFoundErr, we should continue to SDE path instead of failing
			if applicationErr.Type() == resource_events_activities.ErrTypeResourceNotFound {
				isVCPResource = false
			} else {
				return nil, ConvertToVSAError(err)
			}
		} else {
			// For other retryable errors, return and let Temporal retry
			return nil, ConvertToVSAError(err)
		}
	}
	// For common resources, we always proceed to SDE regardless of VCP resource status
	// Log VCP resource status for observability
	logger := util.GetLogger(ctx)
	if isVCPResource {
		logger.Infof("Common resource ON operation completed successfully in VCP for resource %s", handleResourceEventParams.ResourceId)
	} else {
		logger.Infof("Common resource not found in VCP, proceeding with SDE operation for resource %s", handleResourceEventParams.ResourceId)
	}

	if cvp.CVP_HOST == "" {
		return nil, nil
	}

	var result *common.HandleResourceEventResult
	err = workflow.ExecuteActivity(ctx, handleResourceEventActivity.HandleResourceEventsForSDEActivity, handleResourceEventParams).Get(ctx, &result)
	if err != nil {
		var applicationErr *temporal.ApplicationError
		if errorcore.As(err, &applicationErr) && applicationErr.NonRetryable() {
			// For NotFoundErr from SDE (404 responses), treat as non-retryable and continue
			if applicationErr.Type() == resource_events_activities.ErrTypeResourceNotFound {
				logger.Infof("Resource %s not found in SDE", handleResourceEventParams.ResourceId)
				return nil, ConvertToVSAError(err)
			} else {
				return nil, ConvertToVSAError(err)
			}
		} else {
			// For other retryable errors, return and let Temporal retry
			return nil, ConvertToVSAError(err)
		}
	}

	err = workflow.ExecuteActivity(ctx1, handleResourceEventActivity.PollHandleResourceEventSDEOperationActivity, handleResourceEventParams, &result).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	return nil, nil
}

type updateResourceStateDELETEWorkflow struct {
	BaseWorkflow
	ResourceID string
}

func UpdateResourceStateDELETEWorkflow(ctx workflow.Context, params *common.UpdateResourceStateParams) (interface{}, error) {
	log := util.GetLogger(ctx)
	updateResourceWF := new(updateResourceStateDELETEWorkflow)
	err := updateResourceWF.Setup(ctx, params)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	updateResourceWF.Status = WorkflowStatusRunning
	err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	var customErr *vsaerrors.CustomError
	defer func() {
		if customErr != nil {
			updateResourceWF.Status = WorkflowStatusFailed
			err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		} else {
			updateResourceWF.Status = WorkflowStatusCompleted
			err = updateResourceWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		}
	}()

	_, customErr = updateResourceWF.Run(ctx, params)
	if customErr != nil {
		log.Errorf("updateResourceStateDELETEWorkflow workflow completed with error: %v", customErr)
		return nil, customErr
	}
	log.Infof("updateResourceStateDELETEWorkflow workflow completed successfully")
	return nil, nil
}

func (wf *updateResourceStateDELETEWorkflow) Setup(ctx workflow.Context, input interface{}) error {
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

func (s updateResourceStateDELETEWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	updateResourceStateParams := args[0].(*common.UpdateResourceStateParams)

	// Validate this is a storage pool or volume delete operation
	if updateResourceStateParams.State != models.StateDelete ||
		(updateResourceStateParams.ResourceType != common.ResourceStateV1ResourceTypeStoragePool &&
			updateResourceStateParams.ResourceType != common.ResourceStateV1ResourceTypeVolume) {
		return nil, ConvertToVSAError(errors.NewBadRequestErr("DELETE workflow only supports storage pool and volume deletion"))
	}

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
			NonRetryableErrorTypes: []string{"NonRetryableError", "PanicError"},
		},
	}
	aoCVP := ao
	aoCVP.RetryPolicy.MaximumAttempts = int32(CVPJobRetryMaxAttempts)
	aoCVP.RetryPolicy.BackoffCoefficient = finishProjectBackoffCoefficientForCVPClient
	aoCVP.RetryPolicy.InitialInterval, err = time.ParseDuration(InitialRetryIntervalForCVPClient)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	ctx = workflow.WithActivityOptions(ctx, ao)
	ctx1 := workflow.WithActivityOptions(ctx, aoCVP)

	poolActivity := &activities.PoolActivity{}
	volumeActivity := &activities.VolumeCreateActivity{}
	resourceEventsActivity := &resource_events_activities.ResourceEventsActivity{}
	logger := util.GetLogger(ctx)

	// Check for resource location in VCP
	var isVCPResource bool
	err = workflow.ExecuteActivity(ctx, resourceEventsActivity.HandleResourceEventCheckForVCPActivity, updateResourceStateParams).Get(ctx, &isVCPResource)
	if err != nil {
		var applicationErr *temporal.ApplicationError
		if errorcore.As(err, &applicationErr) && applicationErr.NonRetryable() && applicationErr.Type() != resource_events_activities.ErrTypeResourceNotFound {
			return nil, ConvertToVSAError(err)
		}
		// If NotFoundErr, treat as SDE resource
		isVCPResource = false
	}

	// Log resource location for observability
	if isVCPResource {
		logger.Infof("Resource %s found in VCP for deletion", updateResourceStateParams.ResourceId)
	} else {
		logger.Infof("Resource %s not found in VCP, proceeding with SDE deletion", updateResourceStateParams.ResourceId)
	}

	// If resource is not in VCP, use SDE deletion path
	if !isVCPResource {
		if cvp.CVP_HOST == "" {
			return nil, nil
		}

		var result *common.HandleResourceEventResult
		err = workflow.ExecuteActivity(ctx, resourceEventsActivity.HandleResourceEventsForSDEActivity, updateResourceStateParams).Get(ctx, &result)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		err = workflow.ExecuteActivity(ctx1, resourceEventsActivity.PollHandleResourceEventSDEOperationActivity, updateResourceStateParams, &result).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		return nil, nil
	}

	// If resource is a pool and is present in VCP, proceed with the deletion logic
	if updateResourceStateParams.ResourceType == common.ResourceStateV1ResourceTypeStoragePool {
		dbPool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: updateResourceStateParams.ResourceId},
		}
		// Get pool details
		poolView := &datamodel.PoolView{}
		err = workflow.ExecuteActivity(ctx, poolActivity.GetPoolView, dbPool).Get(ctx, &poolView)

		var applicationErr *temporal.ApplicationError
		if errorcore.As(err, &applicationErr) && applicationErr.NonRetryable() && applicationErr.Type() != resource_events_activities.ErrTypeResourceNotFound {
			return nil, ConvertToVSAError(err)
		} else {
			// If pool is not found, it may have already been deleted, so we can consider this a success
			if errorcore.As(err, &applicationErr) && applicationErr.Type() == resource_events_activities.ErrTypeResourceNotFound {
				return nil, nil
			}
		}

		// Check if there are volumes in the pool that need to be disabled/deleted
		if poolView.VolumeCount > 0 {
			var volumes []*datamodel.Volume
			errGetVolume := workflow.ExecuteActivity(ctx, volumeActivity.GetVolumesByPoolID, poolView.Pool.ID).Get(ctx, &volumes)
			if errGetVolume != nil {
				return nil, ConvertToVSAError(errGetVolume)
			}

			for _, volume := range volumes {
				err = workflow.ExecuteActivity(ctx, resourceEventsActivity.DeleteReplicationsForVolume, volume).Get(ctx, nil)
				if err != nil {
					return nil, ConvertToVSAError(err)
				}
				err = workflow.ExecuteActivity(ctx, resourceEventsActivity.DeleteVolumeForPool, volume).Get(ctx, nil)
				if err != nil {
					return nil, ConvertToVSAError(err)
				}
			}
		}

		deletePoolParams := &common.DeletePoolParams{
			AccountName: updateResourceStateParams.ProjectNumber,
			PoolID:      updateResourceStateParams.ResourceId,
		}

		err = workflow.ExecuteActivity(ctx, poolActivity.GetPool, dbPool).Get(ctx, &dbPool)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		// Execute DeletePoolWorkflow logic as a child workflow
		childWorkflowOptions := workflow.ChildWorkflowOptions{
			TaskQueue:          workflowengine.CustomerTaskQueue,
			WorkflowRunTimeout: workflowengine.GetWorkflowGlobalTimeout(),
		}
		childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

		err = workflow.ExecuteChildWorkflow(childCtx, DeletePoolWorkflowInternal, deletePoolParams, dbPool).Get(childCtx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	if updateResourceStateParams.ResourceType == common.ResourceStateV1ResourceTypeVolume && isVCPResource {
		return nil, ConvertToVSAError(errors.NewNotImplementedYetErr())
	}

	return nil, nil
}
