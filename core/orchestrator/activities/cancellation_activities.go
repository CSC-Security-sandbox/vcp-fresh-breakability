package activities

import (
	"context"
	"time"

	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
)

// CancellationActivity provides activities for handling workflow cancellation
type CancellationActivity struct {
	TemporalClient client.Client
}

// NewCancellationActivity creates a new CancellationActivity with TemporalClient
func NewCancellationActivity(temporalClient client.Client) *CancellationActivity {
	return &CancellationActivity{
		TemporalClient: temporalClient,
	}
}

// getTemporalClient gets the Temporal client, either from the activity instance or from the activity context
func (a *CancellationActivity) getTemporalClient(ctx context.Context) client.Client {
	if a.TemporalClient != nil {
		return a.TemporalClient
	}
	return activity.GetClient(ctx)
}

// IsWorkflowRunningActivity checks if a workflow is still running
func (a *CancellationActivity) IsWorkflowRunningActivity(ctx context.Context, workflowID string) (bool, error) {
	return commonparams.IsWorkflowRunning(ctx, a.getTemporalClient(ctx), workflowID)
}

// SendCancelSignalActivity sends a cancellation signal to a workflow
func (a *CancellationActivity) SendCancelSignalActivity(ctx context.Context, workflowID string, signalName string, signalData string) error {
	logger := util.GetLogger(ctx)
	logger.Infof("Sending cancellation signal %s to workflow %s", signalName, workflowID)

	err := a.getTemporalClient(ctx).SignalWorkflow(ctx, workflowID, "", signalName, signalData)
	if err != nil {
		logger.Warnf("Failed to send cancel signal to workflow %s: %v", workflowID, err)
		return err
	}

	logger.Infof("Successfully sent cancellation signal to workflow %s", workflowID)
	return nil
}

// WaitForWorkflowCancellationAckActivity waits for a workflow to be cancelled/completed
// This activity can be called from within a workflow to wait for cancellation acknowledgment
func (a *CancellationActivity) WaitForWorkflowCancellationAckActivity(ctx context.Context, workflowID string, timeout time.Duration) (bool, error) {
	logger := util.GetLogger(ctx)
	logger.Infof("Waiting for workflow %s cancellation acknowledgment (timeout: %v)", workflowID, timeout)

	return commonparams.WaitForWorkflowCancellationAck(ctx, a.getTemporalClient(ctx), workflowID, timeout)
}

// ForceCancelWorkflowActivity forcefully terminates the workflow and its child workflows
func (a *CancellationActivity) ForceCancelWorkflowActivity(ctx context.Context, workflowID string) error {
	logger := util.GetLogger(ctx)
	temporalClient := a.getTemporalClient(ctx)
	logger.Infof("Force terminating workflow %s and its child workflows", workflowID)
	err := temporalClient.TerminateWorkflow(ctx, workflowID, "", "Force cancelled due to delete request", nil)
	if err != nil {
		logger.Warnf("Failed to terminate workflow %s: %v", workflowID, err)
		return err
	}

	logger.Infof("Successfully force terminated workflow %s (child workflows will be automatically terminated)", workflowID)
	return nil
}
