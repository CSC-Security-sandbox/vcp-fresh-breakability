package orchestrator

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

const (
	backupTypeMANUAL = "MANUAL"
)

var (
	maxBackupsToKeep                    = env.GetInt("MAX_BACKUPS_TO_KEEP", 1000)
	convertDatastoreBackupPolicyToModel = _convertDatastoreBackupPolicyToModel
	getBackupPolicyByNameAndOwnerID     = _getBackupPolicyByNameAndOwnerID
	listBackupPolicyVolumeCount         = _listBackupPolicyVolumeCount
	listBackupPolicies                  = _listBackupPolicies
)

func (o *Orchestrator) GetBackupPolicyByNameAndOwnerID(ctx context.Context, backupPolicyName, ownerID string) (*models.BackupPolicy, error) {
	se := o.storage
	backupPolicyDetails, err := getBackupPolicyByNameAndOwnerID(ctx, se, backupPolicyName, ownerID)
	if err != nil {
		return nil, err
	}
	return convertDatastoreBackupPolicyToModel(backupPolicyDetails), nil
}

func (o *Orchestrator) ListBackupPoliciesAndVolumeCount(ctx context.Context, ownerID string, backupPolicyUUIDs []string) (map[string]int64, map[string]*models.BackupPolicy, error) {
	se := o.storage
	account, err := getAccountWithName(ctx, se, ownerID)
	if err != nil {
		return nil, nil, err
	}

	backupPolicyVolumeCount, err := listBackupPolicyVolumeCount(ctx, se, account.ID, backupPolicyUUIDs)
	if err != nil {
		return nil, nil, err
	}

	backupPolicies, err := listBackupPolicies(ctx, se, account.ID, backupPolicyUUIDs)
	if err != nil {
		return nil, nil, err
	}

	backupPolicyMap := make(map[string]*models.BackupPolicy)
	for _, backupPolicy := range backupPolicies {
		backupPolicyMap[backupPolicy.UUID] = convertDatastoreBackupPolicyToModel(backupPolicy)
	}
	return backupPolicyVolumeCount, backupPolicyMap, nil
}

func (o *Orchestrator) GetBackupPolicyByUUIDAndOwnerID(ctx context.Context, uuid string, ownerID string) (*models.BackupPolicy, error) {
	se := o.storage
	account, err := se.GetAccount(ctx, ownerID)
	if err != nil {
		return nil, err
	}
	backupPolicy, err := se.GetBackupPolicyByUUIDAndOwnerID(ctx, uuid, account.ID)
	if err != nil {
		return nil, err
	}
	return convertDatastoreBackupPolicyToModel(backupPolicy), nil
}

func (o *Orchestrator) UpdateBackupPolicy(ctx context.Context, params *commonparams.UpdateBackupPolicyParams) (*models.BackupPolicy, string, error) {
	se := o.storage
	logger := util.GetLogger(ctx)
	account, err := se.GetAccount(ctx, params.AccountName)
	if err != nil {
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeUpdateBackupPolicy),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}
	dbBackupPolicy, err := se.GetBackupPolicyByUUIDAndOwnerID(ctx, params.BackupPolicyID, account.ID)
	if err != nil {
		return nil, "", err
	}
	if dbBackupPolicy.LifeCycleState != models.LifeCycleStateREADY {
		return nil, "", customerrors.NewUserInputValidationErr("backup policy is not in a valid state for update")
	}

	err = validateBackupLimits(ctx, se, params)
	if err != nil {
		return nil, "", err
	}

	updates := map[string]interface{}{
		"life_cycle_state":         models.LifeCycleStateUpdating,
		"life_cycle_state_details": models.LifeCycleStateUpdatingDetails,
	}
	updatingBackupPolicy, err := se.UpdateBackupPolicy(ctx, dbBackupPolicy.UUID, updates)
	if err != nil {
		return nil, "", err
	}
	_, err = o.temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		workflows.UpdateBackupPolicyWorkflow,
		params,
		dbBackupPolicy,
	)
	if err != nil {
		logger.Error("Failed to start backup policy update workflow: ", "error", err)
		return nil, "", err
	}
	return convertDatastoreBackupPolicyToModel(updatingBackupPolicy), createdJob.UUID, nil
}

func validateBackupLimits(ctx context.Context, se database.Storage, params *commonparams.UpdateBackupPolicyParams) error {
	// Fetch all the volumes associated with the backup policy
	conditions := [][]interface{}{{"data_protection->>'backup_policy_id' = ?", params.BackupPolicyID}}
	volumes, err := se.GetMultipleVolumes(ctx, conditions)
	if err != nil {
		return err
	}

	// Fetch the count of existing manual backups for each volume associated with the backup policy
	volumeUUIDs := make([]string, 0, len(volumes))
	for _, volume := range volumes {
		volumeUUIDs = append(volumeUUIDs, volume.UUID)
	}
	conditions = [][]interface{}{
		{"type = ?", backupTypeMANUAL},
		{"state != ?", models.LifeCycleStateError},
	}
	backupsCountByVolume, err := se.GetBackupCountByVolumeUUIDs(ctx, volumeUUIDs, conditions)
	if err != nil {
		return err
	}

	// Validate that the total number of backups per volume does not exceed the defined limits with the new params
	scheduledBackupsToKeep := int64(0)
	if params.DailyBackupLimit != nil {
		scheduledBackupsToKeep += *params.DailyBackupLimit
	}
	if params.WeeklyBackupLimit != nil {
		scheduledBackupsToKeep += *params.WeeklyBackupLimit
	}
	if params.MonthlyBackupLimit != nil {
		scheduledBackupsToKeep += *params.MonthlyBackupLimit
	}
	for volumeUUID, backupCount := range backupsCountByVolume {
		totalBackupsToKeep := scheduledBackupsToKeep + backupCount
		if totalBackupsToKeep > int64(maxBackupsToKeep) {
			return customerrors.NewUserInputValidationErr(
				fmt.Sprintf("the total number of backups exceeds the limit of %d for volume %s", maxBackupsToKeep, volumeUUID))
		}
	}
	return nil
}

func _getBackupPolicyByNameAndOwnerID(ctx context.Context, se database.Storage, backupPolicyName, ownerID string) (*datamodel.BackupPolicy, error) {
	account, err := getAccountWithName(ctx, se, ownerID)
	if err != nil {
		return nil, err
	}

	backupPolicy, err := se.GetBackupPolicyByNameAndOwnerID(ctx, backupPolicyName, account.ID)
	if err != nil {
		return nil, err
	}
	return backupPolicy, nil
}

func _listBackupPolicyVolumeCount(ctx context.Context, se database.Storage, accountID int64, backupPolicyUUIDs []string) (map[string]int64, error) {
	conditions := [][]interface{}{{"account_id = ?", accountID}}
	if len(backupPolicyUUIDs) > 0 {
		conditions = append(conditions, []interface{}{"data_protection->>'backup_policy_id' IN ?", backupPolicyUUIDs})
	}

	backupPolicies, err := se.ListBackupPolicyVolumeCount(ctx, conditions)
	if err != nil {
		return nil, err
	}
	return backupPolicies, nil
}

func _listBackupPolicies(ctx context.Context, se database.Storage, accountID int64, backupPolicyUUIDs []string) ([]*datamodel.BackupPolicy, error) {
	conditions := [][]interface{}{{"account_id = ?", accountID}}
	if len(backupPolicyUUIDs) > 0 {
		conditions = append(conditions, []interface{}{"uuid IN ?", backupPolicyUUIDs})
	}

	backupPolicies, err := se.ListBackupPolicies(ctx, conditions)
	if err != nil {
		return nil, err
	}
	return backupPolicies, nil
}

func _convertDatastoreBackupPolicyToModel(backupPolicy *datamodel.BackupPolicy) *models.BackupPolicy {
	return &models.BackupPolicy{
		ResourceID:         backupPolicy.Name,
		BackupPolicyUUID:   backupPolicy.UUID,
		DailyBackupLimit:   backupPolicy.DailyBackupsToKeep,
		WeeklyBackupLimit:  backupPolicy.WeeklyBackupsToKeep,
		MonthlyBackupLimit: backupPolicy.MonthlyBackupsToKeep,
		Enabled:            backupPolicy.PolicyEnabled,
		Description:        backupPolicy.Description,
		State:              backupPolicy.LifeCycleState,
		CreatedAt:          backupPolicy.CreatedAt,
	}
}
