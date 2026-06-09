package common

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// WorkflowCancellationParams holds parameters for handling cancellation in delete workflows
type WorkflowCancellationParams struct {
	ResourceUUID  string
	CorrelationID string
	CreateJobType datamodel.JobType
	SignalName    string
	// CancellationAckTimeout is the duration to wait for graceful cancellation acknowledgment after sending
	// a cancellation signal to the create workflow. If the workflow doesn't acknowledge within
	// this time, it will be forcefully terminated.
	CancellationAckTimeout time.Duration
	// ForceTerminationAckTimeout is the duration to wait for force termination acknowledgment
	// after forcefully terminating a workflow that didn't respond to the cancellation signal.
	// This is typically a shorter timeout since force termination should be immediate.
	ForceTerminationAckTimeout time.Duration
}

const (
	// DefaultCancelSignalName is the default signal name for cancellation
	DefaultCancelSignalName = "cancel-resource-creation"
	// DefaultCancellationTimeout is the default timeout (5 minutes) for waiting for graceful cancellation acknowledgment
	DefaultCancellationTimeout = 5 * time.Minute
	// DefaultForceCancelWaitTimeout is the default timeout (30 seconds) for waiting for force termination acknowledgment
	DefaultForceCancelWaitTimeout = 30 * time.Second
)

// IsWorkflowRunning checks if a workflow is still running
func IsWorkflowRunning(ctx context.Context, temporalClient client.Client, workflowID string) (bool, error) {
	desc, err := temporalClient.DescribeWorkflowExecution(ctx, workflowID, "")
	if err != nil {
		return false, err
	}

	status := desc.WorkflowExecutionInfo.Status
	// Workflow is running if status is RUNNING
	return status == enums.WORKFLOW_EXECUTION_STATUS_RUNNING, nil
}

// WaitForWorkflowCancellationAck waits for workflow to complete/cancel with timeout
// Returns true if workflow completed/cancelled, false if timeout
func WaitForWorkflowCancellationAck(ctx context.Context, temporalClient client.Client, workflowID string, timeout time.Duration) (bool, error) {
	workflowRun := temporalClient.GetWorkflow(ctx, workflowID, "")

	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := workflowRun.Get(ctxWithTimeout, nil)
	if err != nil {
		// Check if the error is due to context timeout
		if err == context.DeadlineExceeded || ctxWithTimeout.Err() == context.DeadlineExceeded {
			return false, nil // Timeout
		}
		// Check if workflow was cancelled or completed
		if temporal.IsCanceledError(err) {
			return true, nil // Cancelled successfully
		}
		// Check if workflow completed (even with error)
		var appErr *temporal.ApplicationError
		if errors.As(err, &appErr) {
			return true, nil // Completed (with error)
		}
		return false, err
	}

	return true, nil // Completed successfully
}

// WorkflowCancellationHandler handles cancellation signals in workflows
type WorkflowCancellationHandler struct {
	cancelled        bool
	cancelSignalChan workflow.ReceiveChannel
	logger           log.Logger
	resourceUUID     string
	resourceName     string
}

// NewWorkflowCancellationHandler creates a new cancellation handler for a workflow
func NewWorkflowCancellationHandler(ctx workflow.Context, signalName string, resourceUUID string, resourceName string) *WorkflowCancellationHandler {
	if signalName == "" {
		signalName = DefaultCancelSignalName
	}

	logger := util.GetLogger(ctx)
	return &WorkflowCancellationHandler{
		cancelSignalChan: workflow.GetSignalChannel(ctx, signalName),
		logger:           logger,
		resourceUUID:     resourceUUID,
		resourceName:     resourceName,
	}
}

// CheckCancellation checks for cancellation signal (non-blocking)
// Should be called before starting new activities
// Returns an error if cancellation was detected, nil otherwise
func (h *WorkflowCancellationHandler) CheckCancellation(ctx workflow.Context) error {
	// Non-blocking check using selector
	selector := workflow.NewSelector(ctx)
	selector.AddReceive(h.cancelSignalChan, func(c workflow.ReceiveChannel, more bool) {
		if more {
			var cancelData string
			c.Receive(ctx, &cancelData)
			h.logger.Infof("Received cancellation signal for %s %s: %s", h.resourceName, h.resourceUUID, cancelData)
			h.cancelled = true
		}
	})
	selector.AddDefault(func() {
		// No signal available, continue
	})
	selector.Select(ctx)

	if h.cancelled {
		h.logger.Infof("%s creation cancelled, will stop after current activity completes for %s: %s",
			h.resourceName, h.resourceName, h.resourceUUID)
		return fmt.Errorf("%s creation cancelled by delete request", h.resourceName)
	}
	return nil
}

// IsCancelled returns true if cancellation was detected
func (h *WorkflowCancellationHandler) IsCancelled() bool {
	return h.cancelled
}

// CheckCancellationSignal checks for cancellation and converts the error to CustomError if cancellation was detected.
func (h *WorkflowCancellationHandler) CheckCancellationSignal(ctx workflow.Context) *vsaerrors.CustomError {
	if err := h.CheckCancellation(ctx); err != nil {
		return vsaerrors.ExtractCustomError(vsaerrors.New(err.Error()))
	}
	return nil
}

// CreateJobResult holds the result of getting a create job
type CreateJobResult struct {
	JobUUID    string
	WorkflowID string
}

// CancellationActivityMethods defines the methods needed from a cancellation activity
// This interface allows the generic handler to work with any activity type that implements these methods
type CancellationActivityMethods interface {
	IsWorkflowRunningActivity(ctx context.Context, workflowID string) (bool, error)
	SendCancelSignalActivity(ctx context.Context, workflowID string, signalName string, signalData string) error
	WaitForWorkflowCancellationAckActivity(ctx context.Context, workflowID string, timeout time.Duration) (bool, error)
	ForceCancelWorkflowActivity(ctx context.Context, workflowID string) error
}

// CommonActivityMethods defines the methods needed from a common activity
type CommonActivityMethods interface {
	UpdateJobStatus(ctx context.Context, job *datamodel.Job) error
}

// HandleCancellationInDeleteWorkflow handles cancellation of ongoing create workflow from within a delete workflow.
func HandleCancellationInDeleteWorkflow(
	ctx workflow.Context,
	params WorkflowCancellationParams,
	getCreateJobActivity interface{}, // Activity function: func(ctx, resourceUUID, correlationID, jobType string) (*CreateJobResult, error)
	cancellationActivity CancellationActivityMethods,
	commonActivity CommonActivityMethods,
) error {
	logger := util.GetLogger(ctx)
	logger.Infof("Resource %s is in CREATING state, handling cancellation of create workflow", params.ResourceUUID)

	if params.SignalName == "" {
		params.SignalName = DefaultCancelSignalName
	}
	if params.CancellationAckTimeout == 0 {
		params.CancellationAckTimeout = DefaultCancellationTimeout
	}
	if params.ForceTerminationAckTimeout == 0 {
		params.ForceTerminationAckTimeout = DefaultForceCancelWaitTimeout
	}

	if params.CreateJobType == "" {
		logger.Warnf("CreateJobType not specified for resource %s, skipping cancellation handling", params.ResourceUUID)
		return nil
	}

	// Get the create job using the provided activity function
	var createJobResult *CreateJobResult
	err := workflow.ExecuteActivity(ctx, getCreateJobActivity,
		params.ResourceUUID, params.CorrelationID, string(params.CreateJobType)).Get(ctx, &createJobResult)
	if err != nil {
		logger.Warnf("Could not find create job for resource %s: %v, proceeding with normal delete", params.ResourceUUID, err)
		return nil
	}

	if createJobResult == nil || createJobResult.WorkflowID == "" {
		logger.Warnf("Create job not found or workflow ID missing for resource %s, proceeding with normal delete", params.ResourceUUID)
		return nil
	}

	logger.Infof("Found matching create job %s with workflow ID %s", createJobResult.JobUUID, createJobResult.WorkflowID)

	// Checking if workflow is still running
	var isRunning bool
	err = workflow.ExecuteActivity(ctx, cancellationActivity.IsWorkflowRunningActivity,
		createJobResult.WorkflowID).Get(ctx, &isRunning)
	if err != nil {
		logger.Warnf("Failed to check workflow status: %v, proceeding with normal delete", err)
		return nil
	}

	if !isRunning {
		// Workflow already completed - update job state and proceed
		logger.Infof("Create workflow %s is already completed, updating job state", createJobResult.WorkflowID)
		createJob := &datamodel.Job{
			BaseModel:    datamodel.BaseModel{UUID: createJobResult.JobUUID},
			State:        string(datamodel.JobsStateERROR),
			TrackingID:   vsaerrors.ErrInternalServerError,
			ErrorDetails: "Resource creation cancelled due to delete request",
		}
		if err := workflow.ExecuteActivity(ctx, commonActivity.UpdateJobStatus, createJob).Get(ctx, nil); err != nil {
			logger.Warnf("Failed to update create job with error details: %v", err)
		}
		return nil
	}

	// Send cancellation signal to create workflow
	logger.Infof("Sending cancellation signal to create workflow %s", createJobResult.WorkflowID)
	signalData := fmt.Sprintf("Delete requested for resource %s", params.ResourceUUID)
	err = workflow.ExecuteActivity(ctx, cancellationActivity.SendCancelSignalActivity,
		createJobResult.WorkflowID, params.SignalName, signalData).Get(ctx, nil)
	if err != nil {
		logger.Warnf("Failed to send cancel signal: %v, will force cancel if needed", err)
	}

	// Wait for cancellation acknowledgment with timeout
	logger.Infof("Waiting for create workflow cancellation acknowledgment (timeout: %v)", params.CancellationAckTimeout)
	var acknowledged bool
	err = workflow.ExecuteActivity(ctx, cancellationActivity.WaitForWorkflowCancellationAckActivity,
		createJobResult.WorkflowID, params.CancellationAckTimeout).Get(ctx, &acknowledged)

	if err != nil {
		logger.Warnf("Error waiting for cancellation: %v", err)
	}

	// Handle timeout or success
	var errorMessage string
	if !acknowledged {
		// Terminating the parent workflow
		logger.Warnf("Timeout waiting for cancellation acknowledgment (> %v), forcefully terminating workflow %s (this will also stop all child workflows)",
			params.CancellationAckTimeout, createJobResult.WorkflowID)
		err = workflow.ExecuteActivity(ctx, cancellationActivity.ForceCancelWorkflowActivity,
			createJobResult.WorkflowID).Get(ctx, nil)
		if err != nil {
			logger.Warnf("Failed to force cancel workflow %s: %v, proceeding anyway", createJobResult.WorkflowID, err)
		} else {
			// Wait for the workflow to actually be cancelled
			logger.Infof("Waiting for force cancellation to complete for workflow %s (timeout: %v)", createJobResult.WorkflowID, params.ForceTerminationAckTimeout)
			var forceCancelAcknowledged bool
			waitErr := workflow.ExecuteActivity(ctx, cancellationActivity.WaitForWorkflowCancellationAckActivity,
				createJobResult.WorkflowID, params.ForceTerminationAckTimeout).Get(ctx, &forceCancelAcknowledged)
			if waitErr != nil {
				logger.Warnf("Error waiting for force cancellation to complete: %v, proceeding anyway", waitErr)
			} else if forceCancelAcknowledged {
				logger.Infof("Force cancellation completed for workflow %s", createJobResult.WorkflowID)
			} else {
				logger.Warnf("Force cancellation wait timeout for workflow %s, proceeding anyway", createJobResult.WorkflowID)
			}
		}
		errorMessage = "Resource creation forcefully terminated due to delete request (timeout exceeded)"
	} else {
		logger.Infof("Create workflow %s cancelled successfully", createJobResult.WorkflowID)
		errorMessage = "Resource creation cancelled due to delete request"
	}

	// Updating create job with error details
	createJob := &datamodel.Job{
		BaseModel:    datamodel.BaseModel{UUID: createJobResult.JobUUID},
		State:        string(datamodel.JobsStateERROR),
		TrackingID:   vsaerrors.ErrInternalServerError,
		ErrorDetails: errorMessage,
	}
	updateErr := workflow.ExecuteActivity(ctx, commonActivity.UpdateJobStatus, createJob).Get(ctx, nil)
	if updateErr != nil {
		logger.Warnf("Failed to update create job with error details: %v", updateErr)
	} else {
		logger.Infof("Updated create job %s with error details", createJobResult.JobUUID)
	}

	return nil
}

// HandleCancellationForCreatingResourceParams holds parameters for the HandleCancellationForCreatingResource helper
type HandleCancellationForCreatingResourceParams struct {
	ResourceUUID               string
	ResourceState              string
	CreateJobType              datamodel.JobType
	SignalName                 string
	CancellationAckTimeout     time.Duration
	ForceTerminationAckTimeout time.Duration
}

// HandleCancellationForCreatingResource is a helper function that handles cancellation logic for resources in CREATING state within delete workflows.
func HandleCancellationForCreatingResource(ctx workflow.Context, logger log.Logger, params HandleCancellationForCreatingResourceParams, getCreateJobActivity interface{}, cancellationActivity CancellationActivityMethods, commonActivity CommonActivityMethods) error {
	// Only handle cancellation if resource is in CREATING state
	if params.ResourceState != datamodel.LifeCycleStateCreating {
		return nil
	}
	// Get correlation ID from workflow context
	correlationID, errCorr := utils.GetCorrelationIDFromWorkflowContextLoggerFields(ctx)
	if errCorr != nil {
		logger.Warnf("Could not get correlation ID from workflow context: %v", errCorr)
		correlationID = ""
	}
	// Set up cancellation parameters
	cancellationParams := WorkflowCancellationParams{
		ResourceUUID:               params.ResourceUUID,
		CorrelationID:              correlationID,
		CreateJobType:              params.CreateJobType,
		SignalName:                 params.SignalName,
		CancellationAckTimeout:     params.CancellationAckTimeout,
		ForceTerminationAckTimeout: params.ForceTerminationAckTimeout,
	}
	// Handle cancellation
	if cancelErr := HandleCancellationInDeleteWorkflow(ctx, cancellationParams, getCreateJobActivity, cancellationActivity, commonActivity); cancelErr != nil {
		logger.Warnf("Error handling cancellation: %v, proceeding with normal delete", cancelErr)
		return cancelErr
	}
	return nil
}
