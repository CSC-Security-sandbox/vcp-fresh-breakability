package gcp

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	expertModeWorkflows "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/expertMode"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// UpdateRbacForPools triggers the workflow to update RBAC hash for all active ONTAP mode pools
func (o *GCPOrchestrator) UpdateRbacForPools(ctx context.Context) (string, error) {
	return _updateRbacForPools(ctx, o.storage, o.temporal)
}

// _updateRbacForPools creates a job and triggers the RBAC update workflow
func _updateRbacForPools(ctx context.Context, se database.Storage, temporal client.Client) (string, error) {
	logger := util.GetLogger(ctx)

	// Create a job for the RBAC update workflow
	// Since this is a system-level operation, we don't need an account
	job := &datamodel.Job{
		Type:          string(models.JobTypeExpertModeRbacRefresh),
		State:         string(models.JobsStateNEW),
		AccountID:     sql.NullInt64{Valid: false}, // No specific account for system-level operation
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job for RBAC update workflow", "error", err)
		return "", err
	}

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.BackgroundTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		expertModeWorkflows.UpdateRbacForPoolsWorkflow,
	)

	if err != nil {
		logger.Error("Failed to start RBAC update workflow", "workflowID", createdJob.WorkflowID, "error", err)
		if updateErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), createdJob.TrackingID, err.Error()); updateErr != nil {
			logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", updateErr)
		}
		return "", fmt.Errorf("failed to start RBAC update workflow: %w", err)
	}

	logger.Infof("Successfully triggered RBAC update workflow with job ID: %s", createdJob.UUID)
	return createdJob.UUID, nil
}
