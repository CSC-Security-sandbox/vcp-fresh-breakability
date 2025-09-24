package orchestrator

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/replicationWorkflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

var (
	getMultipleReplicationsInternal = _getMultipleReplicationsInternal
	performMountCheck               = _performMountCheck
)

func (o *Orchestrator) GetMultipleReplicationsInternal(ctx context.Context, accountName string, replicationUUIDs []string) ([]*datamodel.VolumeReplication, error) {
	return getMultipleReplicationsInternal(ctx, o.storage, o.temporal, accountName, replicationUUIDs)
}

func _getMultipleReplicationsInternal(ctx context.Context, se database.Storage, temporal client.Client, accountName string, replicationUUIDs []string) ([]*datamodel.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	account, err := getAccountWithName(ctx, se, accountName)
	if err != nil {
		return nil, err
	}

	filter := utils.CreateFilterWithConditions(
		utils.NewFilterCondition("account_id", "=", account.ID),
		utils.NewFilterCondition("uuid", "in", replicationUUIDs))

	replications, err := se.ListVolumeReplications(ctx, *filter)
	if err != nil {
		logger.Errorf("Failed to list replications for account %s: %v", accountName, err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	if len(replications) == 0 {
		logger.Warnf("No replications found for account %s with UUIDs %v", accountName, replicationUUIDs)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, errors.NewNotFoundErr("replication", nil))
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeRefreshVolumeReplicationInternal),
		State:        string(models.JobsStateNEW),
		ResourceName: "Replication Sync",
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, err
	}

	params := &common.ReplicationInternalGetMultipleParams{
		ReplicationUUIDs: replicationUUIDs,
		AccountName:      accountName,
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		replicationWorkflows.GetMultipleReplicationsInternalWorkflow,
		params,
	)

	if err != nil {
		logger.Error("Failed to execute workflow for volume replication creation", "error", err)
		return nil, err
	}

	return replications, nil
}

func (o *Orchestrator) PerformMountCheck(ctx context.Context, replicationUUID string, accountName string) (*models.Job, error) {
	return performMountCheck(ctx, o.storage, o.temporal, replicationUUID, accountName)
}

func _performMountCheck(ctx context.Context, se database.Storage, temporal client.Client, replicationUUID string, accountName string) (*models.Job, error) {
	logger := util.GetLogger(ctx)
	account, err := getAccountWithName(ctx, se, accountName)
	if err != nil {
		return nil, err
	}
	job := &datamodel.Job{
		Type:         string(models.JobTypeMountCheck),
		State:        string(models.JobsStateNEW),
		ResourceName: replicationUUID,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, err
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		replicationWorkflows.PerformMountCheckWorkflow,
		replicationUUID,
		accountName,
	)
	if err != nil {
		logger.Error("Failed to start MountJob Workflow: ", "error", err)
		return nil, err
	}
	return convertDatastoreOperationToModel(createdJob), nil
}

// UpdateVolumeReplicationAttributes updates volume replication attributes in the database
func (o *Orchestrator) UpdateVolumeReplicationAttributes(ctx context.Context, params models.UpdateVolumeReplicationAttributesParams) (*models.Job, error) {
	return updateVolumeReplicationAttributes(ctx, o.storage, o.temporal, params)
}

func updateVolumeReplicationAttributes(ctx context.Context, se database.Storage, temporal client.Client, params models.UpdateVolumeReplicationAttributesParams) (*models.Job, error) {
	logger := util.GetLogger(ctx)

	// Get account for job creation
	account, err := se.GetAccount(ctx, params.ProjectNumber)
	if err != nil {
		logger.Error("Failed to get account", "error", err)
		return nil, err
	}

	// Parse region and zone from location
	region, zone, err := utilParseRegionAndZone(params.LocationId)
	if err != nil {
		logger.Error("Failed to parse region and zone", "locationId", params.LocationId, "error", err)
		return nil, err
	}

	// Create workflow parameters
	updateParams := &commonparams.UpdateVolumeReplicationAttributesParams{
		AccountName:            account.Name,
		Region:                 region,
		Zone:                   zone,
		UpdateAttributesParams: &params,
	}

	// Create event for the workflow
	event := &replication.UpdateVolumeReplicationAttributesEvent{
		UpdateVolumeReplicationAttributesParams: &params,
	}

	// Create a job for this operation
	job := &datamodel.Job{
		Type:         string(models.JobTypeUpdateVolumeReplicationAttributes),
		State:        string(models.JobsStatePROCESSING),
		ResourceName: params.VolumeReplicationId,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
		WorkflowID:   "UpdateVolumeReplicationAttributes-" + params.VolumeReplicationId + "-" + uuid.New().String(),
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, err
	}

	// Defer statement to mark job as errored if workflow fails to start
	defer func() {
		if err != nil {
			if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
				logger.Error("Failed to update job status to error", "jobID", createdJob.UUID, "error", jobErr)
			}
		}
	}()
	
	// Configure workflow options
	workflowOptions := client.StartWorkflowOptions{
		TaskQueue:             workflowengine.CustomerTaskQueue,
		ID:                    createdJob.WorkflowID,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
	}

	// Start the workflow
	_, err = temporal.ExecuteWorkflow(ctx, workflowOptions, replicationWorkflows.UpdateVolumeReplicationAttributesWorkflow, updateParams, event)
	if err != nil {
		logger.Error("Failed to start update replication attributes Workflow: ", "error", err)
		return nil, err
	}

	logger.Info("Successfully started UpdateVolumeReplicationAttributes workflow",
		"volumeReplicationId", params.VolumeReplicationId,
		"workflowId", createdJob.WorkflowID)

	return convertDatastoreOperationToModel(createdJob), nil
}
