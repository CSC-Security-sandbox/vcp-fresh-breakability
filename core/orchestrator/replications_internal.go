package orchestrator

import (
	"context"
	"database/sql"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/replicationWorkflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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
	}

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
