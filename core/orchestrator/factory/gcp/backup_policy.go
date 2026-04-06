package gcp

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
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

func (o *GCPOrchestrator) GetBackupPolicyByNameAndOwnerID(ctx context.Context, backupPolicyName, ownerID string) (*models.BackupPolicy, error) {
	se := o.storage
	backupPolicyDetails, err := getBackupPolicyByNameAndOwnerID(ctx, se, backupPolicyName, ownerID)
	if err != nil {
		return nil, err
	}
	return convertDatastoreBackupPolicyToModel(backupPolicyDetails), nil
}

// GetBackupPolicyByUUIDAndOwnerID retrieves a backup policy by its UUID and owner ID
func (o *GCPOrchestrator) GetBackupPolicyByUUIDAndOwnerID(ctx context.Context, backupPolicyUUID string, ownerId string) (*models.BackupPolicy, error) {
	se := o.storage
	account, err := getAccountWithName(ctx, se, ownerId)
	if err != nil {
		return nil, err
	}
	backupPolicy, err := se.GetBackupPolicyByUUIDAndOwnerID(ctx, backupPolicyUUID, account.ID)
	if err != nil {
		return nil, err
	}
	// Convert datamodel.BackupPolicy to models.BackupPolicy
	return convertDatastoreBackupPolicyToModel(backupPolicy), nil
}

func (o *GCPOrchestrator) ListBackupPoliciesAndVolumeCount(ctx context.Context, ownerID string, backupPolicyUUIDs []string) (map[string]int64, map[string]*models.BackupPolicy, error) {
	se := o.storage
	account, err := getOrCreateAccount(ctx, se, ownerID)
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

func (o *GCPOrchestrator) UpdateBackupPolicy(ctx context.Context, params *commonparams.UpdateBackupPolicyParams) (*models.BackupPolicy, string, error) {
	se := o.storage
	logger := util.GetLogger(ctx)
	account, err := se.GetAccount(ctx, params.AccountName)
	if err != nil {
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

	var (
		createdJob                                               *datamodel.Job
		jobCreationErr, backupPolicyUpdateErr, workflowLaunchErr error
	)
	defer func() {
		if workflowLaunchErr != nil {
			// If workflowLaunchErr is not nil, the workflow launch attempt failed after updating the backup policy state,
			// so we must revert the backup policy to its previous state.
			updates := map[string]interface{}{
				"life_cycle_state":         models.LifeCycleStateREADY,
				"life_cycle_state_details": models.LifeCycleStateReadyDetails,
			}
			_, err2 := se.UpdateBackupPolicy(ctx, dbBackupPolicy.UUID, updates)
			if err2 != nil {
				logger.Errorf("Failed to rollback backup policy in database: %v", err2)
			}

			// Update the job state to ERROR
			err2 = se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), vsaerrors.ErrWorkflowNotLaunched, workflowLaunchErr.Error())
			if err2 != nil {
				logger.Errorf("Failed to update job state to ERROR in database: %v", err2)
			}
		}

		if backupPolicyUpdateErr != nil {
			// If backupPolicyUpdateErr is not nil, the update to the backup policy failed after creating the job,
			// so we must update the job state to ERROR.
			err2 := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), vsaerrors.ErrWorkflowNotLaunched, backupPolicyUpdateErr.Error())
			if err2 != nil {
				logger.Errorf("Failed to update job state to ERROR in database: %v", err2)
			}
		}
	}()

	// Create a job for backup policy update
	job := &datamodel.Job{
		Type:          string(models.JobTypeUpdateBackupPolicy),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         dbBackupPolicy.UUID,
			PreviousState:        dbBackupPolicy.LifeCycleState,
			PreviousStateDetails: dbBackupPolicy.LifeCycleStateDetails,
		},
	}
	createdJob, jobCreationErr = se.CreateJob(ctx, job)
	if jobCreationErr != nil {
		logger.Errorf("Failed to create job in database: %v", jobCreationErr)
		return nil, "", jobCreationErr
	}

	updates := map[string]interface{}{
		"life_cycle_state":         models.LifeCycleStateUpdating,
		"life_cycle_state_details": models.LifeCycleStateUpdatingDetails,
	}
	updatingBackupPolicy, backupPolicyUpdateErr := se.UpdateBackupPolicy(ctx, dbBackupPolicy.UUID, updates)
	if backupPolicyUpdateErr != nil {
		logger.Errorf("Failed to update backup policy in database: %v", backupPolicyUpdateErr)
		return nil, "", backupPolicyUpdateErr
	}

	workflowExecutor := workflows.NewWorkflowExecutor(o.temporal, logger)
	workflowLaunchErr = workflowExecutor.ExecuteWorkflow(
		ctx,
		createdJob.WorkflowID,
		workflowengine.CustomerTaskQueue,
		workflows.UpdateBackupPolicyWorkflow,
		nil,
		params,
		dbBackupPolicy,
	)
	if workflowLaunchErr != nil {
		logger.Errorf("Failed to launch workflow for backup policy update after retries: %v", workflowLaunchErr)
		return nil, "", workflowLaunchErr
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

func (o *GCPOrchestrator) DeleteBackupPolicy(ctx context.Context, params *commonparams.DeleteBackupPolicyParams) (*models.BackupPolicy, string, error) {
	se := o.storage
	logger := util.GetLogger(ctx)

	account, err := se.GetAccount(ctx, params.OwnerID)
	if err != nil {
		return nil, "", err
	}

	dbBackupPolicy, err := se.GetBackupPolicyByUUIDAndOwnerID(ctx, params.BackupPolicyID, account.ID)
	if err != nil {
		return nil, "", err
	}
	if dbBackupPolicy.LifeCycleState != models.LifeCycleStateREADY {
		return nil, "", customerrors.NewUserInputValidationErr("backup policy is not in ready state, please check the backup policy and try again")
	}

	volumes, err := se.GetVolumeCountByBackupPolicyID(ctx, params.BackupPolicyID)
	if err != nil {
		return nil, "", err
	}
	if volumes > 0 {
		return nil, "", customerrors.NewUserInputValidationErr("backup policy has volumes attached, please detach backup policy from volumes before deleting backup policy")
	}

	updates := map[string]interface{}{
		"life_cycle_state":         models.LifeCycleStateDeleting,
		"life_cycle_state_details": models.LifeCycleStateDeletingDetails,
	}
	updatedBackupPolicy, err := se.UpdateBackupPolicy(ctx, dbBackupPolicy.UUID, updates)
	if err != nil {
		return nil, "", err
	}
	var createdJob *datamodel.Job
	defer func() {
		if err != nil {
			// If there is an error, revert the state to READY
			_, revertErr := se.UpdateBackupPolicy(ctx, dbBackupPolicy.UUID, map[string]interface{}{
				"life_cycle_state":         models.LifeCycleStateREADY,
				"life_cycle_state_details": models.LifeCycleStateAvailableDetails,
			})
			if revertErr != nil {
				logger.Error("Failed to revert backup policy state after delete failure", "error", revertErr)
			}
		}
		if createdJob != nil && err != nil {
			// If a job was created, update its state to ERROR if there was an error
			updateErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), createdJob.TrackingID, err.Error())
			if updateErr != nil {
				logger.Error("Failed to update job state", "error", updateErr, "jobUUID", createdJob.UUID)
			}
		}
	}()

	job := &datamodel.Job{
		Type:          string(models.JobTypeDeleteBackupPolicy),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         dbBackupPolicy.UUID,
			PreviousState:        dbBackupPolicy.LifeCycleState,
			PreviousStateDetails: dbBackupPolicy.LifeCycleStateDetails,
		},
	}

	createdJob, err = se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	workflowExecutor := workflows.NewWorkflowExecutor(o.temporal, logger)
	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		createdJob.WorkflowID,
		workflowengine.CustomerTaskQueue,
		workflows.DeleteBackupPolicyWorkflow,
		nil,
		params,
		updatedBackupPolicy,
	)
	if err != nil {
		logger.Error("Failed to start delete backup policy workflow after retries", "error", err)
		return nil, "", err
	}
	return convertDatastoreBackupPolicyToModel(updatedBackupPolicy), createdJob.UUID, nil
}

// GetBackupPolicyUUIDsFromBackupVaultUUID retrieves all backup policy UUIDs associated with volumes that have the given backup vault ID
func (o *GCPOrchestrator) GetBackupPolicyUUIDsFromBackupVaultUUID(ctx context.Context, backupVaultUUID string, ownerId string) ([]string, error) {
	se := o.storage
	account, err := getAccountWithName(ctx, se, ownerId)
	if err != nil {
		return nil, err
	}
	backupPolicyUUIDs, err := se.GetBackupPolicyUUIDsFromBackupVaultUUID(ctx, backupVaultUUID, account.ID)
	if err != nil {
		return nil, err
	}
	return backupPolicyUUIDs, nil
}

func _getBackupPolicyByNameAndOwnerID(ctx context.Context, se database.Storage, backupPolicyName, ownerID string) (*datamodel.BackupPolicy, error) {
	account, err := getOrCreateAccount(ctx, se, ownerID)
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

// CreateBackupPolicy creates a backup policy synchronously for VCP region.
func (o *GCPOrchestrator) CreateBackupPolicy(ctx context.Context, params *commonparams.CreateBackupPolicyParams) (*models.BackupPolicy, error) {
	se := o.storage
	logger := util.GetLogger(ctx)
	account, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return nil, err
	}

	// Check if backup policy already exists
	existingBackupPolicy, err := se.GetBackupPolicyByNameAndOwnerID(ctx, params.Name, account.ID)
	if err == nil && existingBackupPolicy != nil {
		return nil, customerrors.NewConflictErr(fmt.Sprintf("backup policy with name %s already exists", params.Name))
	} else if err != nil && !customerrors.IsNotFoundErr(err) {
		return nil, err
	}

	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			UUID: utils.RandomUUID(),
		},
		Name:                  params.Name,
		AccountID:             account.ID,
		Description:           params.Description,
		DailyBackupsToKeep:    int64(0),
		WeeklyBackupsToKeep:   int64(0),
		MonthlyBackupsToKeep:  int64(0),
		PolicyEnabled:         false,
		LifeCycleState:        models.LifeCycleStateREADY,
		LifeCycleStateDetails: models.LifeCycleStateReadyDetails,
	}
	if params.DailyBackupLimit != nil {
		dbBackupPolicy.DailyBackupsToKeep = *params.DailyBackupLimit
	}
	if params.WeeklyBackupLimit != nil {
		dbBackupPolicy.WeeklyBackupsToKeep = *params.WeeklyBackupLimit
	}
	if params.MonthlyBackupLimit != nil {
		dbBackupPolicy.MonthlyBackupsToKeep = *params.MonthlyBackupLimit
	}
	if params.PolicyEnabled != nil {
		dbBackupPolicy.PolicyEnabled = *params.PolicyEnabled
	}

	readyBackupPolicy, err := se.CreateBackupPolicyEntryInVCP(ctx, dbBackupPolicy)
	if err != nil {
		logger.Errorf("Failed to create backup policy in database: %v", err)
		return nil, err
	}

	temporalScheduler := scheduler.NewTemporalScheduler(o.temporal.ScheduleClient())
	var scheduleCreateErr, pauseScheduleErr error
	defer func() {
		if scheduleCreateErr != nil {
			logger.Errorf("Failed to create backup policy schedule for policy %s: %v", readyBackupPolicy.UUID, scheduleCreateErr)
			if _, rollbackErr := se.DeleteBackupPolicy(ctx, readyBackupPolicy.UUID); rollbackErr != nil {
				logger.Errorf("Failed to rollback backup policy entry %s after schedule creation failure: %v", readyBackupPolicy.UUID, rollbackErr)
			}
		}
		if pauseScheduleErr != nil {
			logger.Errorf("Failed to pause backup policy schedule for policy %s: %v", readyBackupPolicy.UUID, pauseScheduleErr)
			if _, deleteScheduleErr := temporalScheduler.Delete(ctx, scheduler.DeleteScheduleParams{ScheduleParams: scheduler.ScheduleParams{ScheduleID: readyBackupPolicy.UUID}}); deleteScheduleErr != nil {
				logger.Errorf("Failed to delete backup policy schedule for policy %s after pause failure: %v", readyBackupPolicy.UUID, deleteScheduleErr)
			}
			if _, rollbackErr := se.DeleteBackupPolicy(ctx, readyBackupPolicy.UUID); rollbackErr != nil {
				logger.Errorf("Failed to rollback backup policy entry %s after pause failure: %v", readyBackupPolicy.UUID, rollbackErr)
			}
		}
	}()

	scheduleCreateErr = activities.CreateBackupPolicySchedule(ctx, temporalScheduler, readyBackupPolicy, "")
	if scheduleCreateErr != nil {
		return nil, scheduleCreateErr
	}

	if !readyBackupPolicy.PolicyEnabled {
		_, pauseScheduleErr = temporalScheduler.Pause(ctx, scheduler.PauseScheduleParams{ScheduleParams: scheduler.ScheduleParams{ScheduleID: readyBackupPolicy.UUID}})
		if pauseScheduleErr != nil {
			return nil, pauseScheduleErr
		}
	}

	return convertDatastoreBackupPolicyToModel(readyBackupPolicy), nil
}
