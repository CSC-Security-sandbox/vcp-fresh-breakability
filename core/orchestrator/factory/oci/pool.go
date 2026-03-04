package oci

import (
	"context"
	"errors"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	ociworkflows "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/oci"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// CreatePool creates the specified pool and adds it to the list of pools belonging to the specified owner
func (o *OCIOrchestrator) CreatePool(ctx context.Context, params *commonparams.CreatePoolParams) (*models.Pool, string, error) {
	logger := util.GetLogger(ctx)
	se := o.storage
	temporal := o.temporal

	account, err := common.GetOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return nil, "", err
	}

	dbPool, err2 := common.CreatePoolInDB(ctx, se, params, account, logger, nil, 50)
	if err2 != nil {
		logger.Error("Failed to create pool in database", "error", err2)
		// Check if it's a specific error that should be passed through
		if customerrors.IsConflictErr(err2) {
			return nil, "", err2
		}
		return nil, "", errors.New("unable to process request, please try again later")
	}

	defer func() {
		if err != nil {
			common.CleanupPoolOnError(ctx, se, dbPool, err)
		}
	}()

	job := common.CreatePoolJob(ctx, params, account, dbPool)
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", errors.New("unable to process request, please try again later")
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			common.HandleCreatePoolError(ctx, se, createdJob, err)
		}
	}()

	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)
	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		createdJob.WorkflowID,
		workflowengine.CustomerTaskQueue,
		ociworkflows.OCICreatePoolWorkflow,
		workflowengine.GetCreatePoolWorkflowRunTimeout(params.LargeCapacity),
		params,
		dbPool,
	)
	if err != nil {
		logger.Error("Failed to start pool create workflow: ", "error", err)
		return nil, "", err
	}

	poolView := database.ConvertPoolToPoolView(dbPool)
	return common.ConvertDatastorePoolToModel(poolView, account.Name), createdJob.UUID, nil
}
