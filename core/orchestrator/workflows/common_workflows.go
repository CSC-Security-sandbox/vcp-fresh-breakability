package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"strconv"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

const (
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
	UpdateStatus(ctx workflow.Context, status string, error string) error
	Revert(ctx workflow.Context) error
}

// BaseWorkflow provides common functionalities for all workflows.
type BaseWorkflow struct {
	ID         string
	Status     string
	CustomerID string
	Logger     log.Logger
}

// Setup sets up the workflow with a Logger and initial values.
func (bw *BaseWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	bw.ID = input.(struct{ ID string }).ID
	bw.CustomerID = input.(struct{ CustomerID string }).CustomerID
	bw.Logger = workflow.GetLogger(ctx)

	return nil
}

// UpdateStatus updates the workflow's status.
func (bw *BaseWorkflow) UpdateStatus(ctx workflow.Context, status string, error string) error {
	bw.Status = status
	bw.Logger.Info("Workflow status updated", "status", status)
	return nil
}

// Run is a placeholder implementation for the workflow's main logic.
func (bw *BaseWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	bw.Logger.Info("Running workflow", "ID", bw.ID)
	// Add workflow logic here
	return nil, nil
}

// Revert is a placeholder implementation for the workflow's revert logic.
func (bw *BaseWorkflow) Revert(ctx workflow.Context) error {
	bw.Logger.Info("Reverting workflow", "ID", bw.ID)
	// Add workflow logic here
	return nil
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
		return nil, err
	}
	activityRetryInterval, err := time.ParseDuration(RetryInterval)
	if err != nil {
		return nil, err
	}
	activityRetryMaxAttempts := RetryMaxAttempts
	activityRetryMaxInterval, err := time.ParseDuration(RetryMaxInterval)
	if err != nil {
		return nil, err
	}
	activityRetryBackoff, err := strconv.ParseFloat(RetryBackoff, 64)
	if err != nil {
		return nil, err
	}
	return &WorkflowRetryPolicy{
		InitialInterval:     activityRetryInterval,
		StartToCloseTimeout: activityStartToCloseTimeout,
		BackoffCoefficient:  activityRetryBackoff,
		MaximumInterval:     activityRetryMaxInterval,
		MaximumAttempts:     activityRetryMaxAttempts,
	}, nil
}
