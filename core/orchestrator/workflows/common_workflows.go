package workflows

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
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
)

var (
	StartToCloseTimeout = env.GetString("START_TO_CLOSE_WORKFLOW_TIMEOUT", "25m")
	RetryInterval       = env.GetString("RETRY_INTERVAL", "5s")
	RetryMaxAttempts    = env.GetInt("RETRY_MAX_ATTEMPTS", 3)
	RetryMaxInterval    = env.GetString("RETRY_MAX_INTERVAL", "5m")
	RetryBackoff        = env.GetString("RETRY_BACKOFF_COEFFICIENT", "2.0")

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
					updatedJob.ErrorDetails = []byte(err.Error())
				}

				updatedJob.TrackingID = trackingID
				updatedJob.ErrorDetails = []byte(errorDetails)
			} else {
				updatedJob.TrackingID = 0
				updatedJob.ErrorDetails = []byte(err.Error())
			}
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

func CreateNodeForProviderWithPool(dbNodes []*datamodel.Node, pool *datamodel.Pool) *models.Node {
	ipAddrs := make([]string, 0)
	for _, node := range dbNodes {
		if node.EndpointAddress != "" {
			ipAddrs = append(ipAddrs, node.EndpointAddress)
		}
	}

	node := &models.Node{
		EndpointAddresses: ipAddrs,
		Username:          pool.Username,
		Password:          pool.Password,
	}
	return node
}
