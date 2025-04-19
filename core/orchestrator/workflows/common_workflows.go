package workflows

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
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

// WorkflowInterface defines the common methods for all workflows.
type WorkflowInterface interface {
	Setup(ctx workflow.Context, input interface{}) error
	Run(ctx workflow.Context, input interface{}) (interface{}, error)
	UpdateStatus(ctx workflow.Context) error
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

	return workflow.SetQueryHandler(ctx, StatusQueryName, func() (*datamodel.Job, error) {
		return &datamodel.Job{
			ID:         bw.ID,
			Status:     bw.Status,
			CustomerID: bw.CustomerID,
		}, nil
	})

}

// UpdateStatus updates the workflow's status.
func (bw *BaseWorkflow) UpdateStatus(ctx workflow.Context, status string) error {
	bw.Status = status
	bw.Logger.Info("Workflow status updated", "status", status)
	return nil
}

// Run is a placeholder implementation for the workflow's main logic.
func (bw *BaseWorkflow) Run(ctx workflow.Context, input interface{}) (interface{}, error) {
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
