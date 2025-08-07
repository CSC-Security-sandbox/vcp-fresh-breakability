package workflows

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	WorkflowStatusCreated   = "CREATED"
	WorkflowStatusRunning   = "RUNNING"
	WorkflowStatusCompleted = "COMPLETED"
	WorkflowStatusFailed    = "FAILED"
	WorkflowStatusCancelled = "CANCELLED"
	WorkflowStatusTimeout   = "TIMEOUT"
	WorkflowStatusRetry     = "RETRY"
	WorkflowStatusPaused    = "PAUSED"
	WorkflowStatusResumed   = "RESUMED"
	WorkflowStatusAborted   = "ABORTED"
	WorkflowStatusPending   = "PENDING"

	StatusQueryName = "status"

	pollDBJobWaitTimeSecond = 30
)

var (
	StartToCloseTimeout = env.GetString("START_TO_CLOSE_WORKFLOW_TIMEOUT", "25m")
	RetryInterval       = env.GetString("RETRY_INTERVAL", "5s")
	RetryMaxAttempts    = env.GetInt("RETRY_MAX_ATTEMPTS", 3)
	RetryMaxInterval    = env.GetString("RETRY_MAX_INTERVAL", "5m")
	RetryBackoff        = env.GetString("RETRY_BACKOFF_COEFFICIENT", "2.0")

	// Service Account specific retry policy configurations
	SARetryStartToCloseTimeout = env.GetString("SA_RETRY_START_TO_CLOSE_TIMEOUT", "25m")
	SARetryInitialInterval     = env.GetString("SA_RETRY_INITIAL_INTERVAL", "10s")
	SARetryMaximumAttempts     = env.GetInt("SA_RETRY_MAXIMUM_ATTEMPTS", 12)
	SARetryMaximumInterval     = env.GetString("SA_RETRY_MAXIMUM_INTERVAL", "60s")
	SARetryBackoffCoefficient  = env.GetString("SA_RETRY_BACKOFF_COEFFICIENT", "2.0")

	executeActivity = workflow.ExecuteActivity
)

type WorkflowRetryPolicy struct {
	InitialInterval     time.Duration
	BackoffCoefficient  float64
	MaximumInterval     time.Duration
	MaximumAttempts     int
	StartToCloseTimeout time.Duration
}

// WorkflowInterface defines the common methods for all workflows.
type WorkflowInterface interface {
	Setup(ctx workflow.Context, input interface{}) error
	Run(ctx workflow.Context, args ...interface{}) (interface{}, error)
	UpdateJobStatus(ctx workflow.Context, status string, err error) error
}

// BaseWorkflow provides common functionalities for all workflows.
type BaseWorkflow struct {
	ID         string
	Status     string
	CustomerID string
	Logger     log.Logger
}

type WorkflowStatus struct {
	ID         string `json:"id"`
	Status     string `json:"status"`
	CustomerID string `json:"customer_id"`
}

func (bw *BaseWorkflow) GetDefaultActivityOptions(ctx workflow.Context) workflow.ActivityOptions {
	// Set default activity options
	return workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	}
}

func PopulateRetryPolicyParams() (*WorkflowRetryPolicy, error) {
	activityStartToCloseTimeout, err := time.ParseDuration(StartToCloseTimeout)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrWorkflowConfigurationError, err)
	}
	activityRetryInterval, err := time.ParseDuration(RetryInterval)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrWorkflowConfigurationError, err)
	}
	activityRetryMaxAttempts := RetryMaxAttempts
	activityRetryMaxInterval, err := time.ParseDuration(RetryMaxInterval)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrWorkflowConfigurationError, err)
	}
	activityRetryBackoff, err := strconv.ParseFloat(RetryBackoff, 64)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrWorkflowConfigurationError, err)
	}
	return &WorkflowRetryPolicy{
		InitialInterval:     activityRetryInterval,
		StartToCloseTimeout: activityStartToCloseTimeout,
		BackoffCoefficient:  activityRetryBackoff,
		MaximumInterval:     activityRetryMaxInterval,
		MaximumAttempts:     activityRetryMaxAttempts,
	}, nil
}

// populateServiceAccountRetryPolicyParams returns retry policy specific to service account operations
// with custom values: backoff coefficient 2.0, max attempts 12, max interval 60 seconds
func populateServiceAccountRetryPolicyParams() (*WorkflowRetryPolicy, error) {
	activityStartToCloseTimeout, err := time.ParseDuration(SARetryStartToCloseTimeout)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrWorkflowConfigurationError, err)
	}
	activityRetryInterval, err := time.ParseDuration(SARetryInitialInterval)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrWorkflowConfigurationError, err)
	}
	activityRetryMaxAttempts := SARetryMaximumAttempts
	activityRetryMaxInterval, err := time.ParseDuration(SARetryMaximumInterval)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrWorkflowConfigurationError, err)
	}
	activityRetryBackoff, err := strconv.ParseFloat(SARetryBackoffCoefficient, 64)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrWorkflowConfigurationError, err)
	}
	return &WorkflowRetryPolicy{
		InitialInterval:     activityRetryInterval,
		StartToCloseTimeout: activityStartToCloseTimeout,
		BackoffCoefficient:  activityRetryBackoff,
		MaximumInterval:     activityRetryMaxInterval,
		MaximumAttempts:     activityRetryMaxAttempts,
	}, nil
}

func (bw *BaseWorkflow) UpdateJobStatus(ctx workflow.Context, status string, err error) error {
	if bw.ID == "" {
		return vsaerrors.NewVCPError(vsaerrors.ErrWorkflowConfigurationError,
			errors.New("job uuid cannot be empty"))
	}

	updatedJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: bw.ID},
		State:     status,
	}
	if err != nil {
		var applicationError *temporal.ApplicationError
		if vsaerrors.As(err, &applicationError) {
			if applicationError.Type() == "CustomError" {
				var (
					trackingID   int
					errorDetails string
				)

				err = applicationError.Details(&trackingID, &errorDetails)
				if err != nil {
					bw.Logger.Warn("Couldn't find tracking ID/original error details in the application error", err)
					updatedJob.TrackingID = -1
					updatedJob.ErrorDetails = err.Error()
				}

				updatedJob.TrackingID = trackingID
				updatedJob.ErrorDetails = errorDetails
			} else {
				updatedJob.TrackingID = 0
				updatedJob.ErrorDetails = err.Error()
			}
		} else {
			// If the error is not an application error, we set the tracking ID to 0 and the error details to the error message.
			// This is required so that generic errors that are not of type ApplicationError do not get lost.
			updatedJob.TrackingID = 0
			updatedJob.ErrorDetails = err.Error()
		}
	}

	commonActivity := activities.CommonActivities{}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})
	return executeActivity(ctx, commonActivity.UpdateJobStatus, updatedJob).Get(ctx, nil)
}

// QueryWorkflowStatus queries the status of a workflow using its ID and run ID.
func QueryWorkflowStatus(ctx context.Context, tempClient client.Client, workflowID, runID string) (*WorkflowStatus, error) {
	var status WorkflowStatus
	encVal, err := tempClient.QueryWorkflow(ctx, workflowID, runID, StatusQueryName)
	if err != nil {
		return nil, err
	}
	err = encVal.Get(&status)
	if err != nil {
		return nil, err
	}

	return &status, nil
}

func getSnapshotPolicyName(volume *datamodel.Volume) string {
	if volume != nil && volume.SnapshotPolicy != nil && volume.SnapshotPolicy.Name != "" {
		return volume.SnapshotPolicy.Name
	}
	return ""
}

// WaitForONTAPJob waits for an ONTAP job to complete, taking a workflow context, job UUID, node, and timeout as input.
func WaitForONTAPJob(ctx workflow.Context, jobResponse *vsa.OntapAsyncResponse, node *models.Node, timeout time.Duration) error {
	if jobResponse == nil || jobResponse.JobUUID == "" {
		return nil
	}
	var job *vsa.OntapJob
	startTime := time.Now()

	for {
		// Check if the timeout has been reached
		if time.Since(startTime) > timeout {
			return fmt.Errorf("ontap job %s timed out after %v", jobResponse.JobUUID, timeout)
		}

		// Execute the activity to get the ONTAP job status
		err := workflow.ExecuteActivity(ctx, activities.CommonActivities.GetOntapJob, jobResponse.JobUUID, node).Get(ctx, &job)
		if err != nil {
			return err
		}

		// Check the job state
		switch job.State {
		case "failure":
			if job.Error != nil {
				return errors.New(job.Error.Message)
			}
			return fmt.Errorf("ontap job %s failed with no error message", job.UUID)
		case "success":
			return nil
		}

		// Sleep for a short duration before checking again
		err = workflow.Sleep(ctx, 5*time.Second)
		if err != nil {
			return fmt.Errorf("failed to sleep while waiting for ONTAP job %s: %w", jobResponse.JobUUID, err)
		}
	}
}

// PollOnDBJob waits for a db job to complete, taking a workflow context, job UUID, and timeout as input.
func PollOnDBJob(ctx workflow.Context, jobUUID string, timeout time.Duration) error {
	startTime := workflow.Now(ctx)
	commonActivity := activities.CommonActivities{}

	for {
		// Check if the timeout has been reached.
		if workflow.Now(ctx).Sub(startTime) > timeout {
			return fmt.Errorf("db job %s timed out after %v", jobUUID, timeout)
		}

		// Execute GetJob as an activity to fetch the latest job state.
		var job *datamodel.Job
		err := executeActivity(ctx, commonActivity.GetJob, jobUUID).Get(ctx, &job)
		if err != nil {
			return fmt.Errorf("failed to get db job status for %s: %w", jobUUID, err)
		}

		// Check the job state.
		if job.State == string(models.JobsStateDONE) {
			if job.ErrorDetails != "" {
				return errors.New("job completed with error: " + job.ErrorDetails)
			}
			return nil
		}

		// Sleep for a some duration before checking again.
		err = workflow.Sleep(ctx, pollDBJobWaitTimeSecond*time.Second)
		if err != nil {
			return fmt.Errorf("failed to sleep while waiting for db job %s: %w", jobUUID, err)
		}
	}
}
