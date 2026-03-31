package activities

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_policy"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	logger "golang.org/x/exp/slog"
)

var (
	CvpCreateClient                = cvp.CreateClient
	ConvertToBackupPolicyDataModel = _convertToBackupPolicyDataModel
)

type BackupPolicyActivity struct {
	SE        database.Storage
	Scheduler scheduler.Scheduler
}

func (j *BackupPolicyActivity) UpdateBackupPolicyInSDE(ctx context.Context, params *common.UpdateBackupPolicyParams) (*cvpmodels.BackupPolicyV1beta, error) {
	return updateBackupPolicyInSDE(ctx, params)
}

func (j *BackupPolicyActivity) RevertBackupPolicyUpdateInSDE(ctx context.Context, params *common.UpdateBackupPolicyParams, dbBackupPolicy *datamodel.BackupPolicy) (*cvpmodels.BackupPolicyV1beta, error) {
	params.Description = dbBackupPolicy.Description
	params.PolicyEnabled = &dbBackupPolicy.PolicyEnabled
	params.DailyBackupLimit = &dbBackupPolicy.DailyBackupsToKeep
	params.WeeklyBackupLimit = &dbBackupPolicy.WeeklyBackupsToKeep
	params.MonthlyBackupLimit = &dbBackupPolicy.MonthlyBackupsToKeep
	return updateBackupPolicyInSDE(ctx, params)
}

func (j *BackupPolicyActivity) UpdateBackupPolicyInVCP(ctx context.Context, params *common.UpdateBackupPolicyParams, backupPolicy *datamodel.BackupPolicy) (*datamodel.BackupPolicy, error) {
	se := j.SE

	updates := map[string]interface{}{
		"life_cycle_state":         models.LifeCycleStateREADY,
		"life_cycle_state_details": models.LifeCycleStateReadyDetails,
	}
	if params.Description != nil {
		updates["description"] = *params.Description
	}
	if params.PolicyEnabled != nil {
		updates["policy_enabled"] = *params.PolicyEnabled
	}
	if params.DailyBackupLimit != nil {
		updates["daily_backups_to_keep"] = *params.DailyBackupLimit
	}
	if params.WeeklyBackupLimit != nil {
		updates["weekly_backups_to_keep"] = *params.WeeklyBackupLimit
	}
	if params.MonthlyBackupLimit != nil {
		updates["monthly_backups_to_keep"] = *params.MonthlyBackupLimit
	}
	updated, err := se.UpdateBackupPolicy(ctx, backupPolicy.UUID, updates)
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func (j *BackupPolicyActivity) RevertBackupPolicyUpdateInVCP(ctx context.Context, dbBackupPolicy *datamodel.BackupPolicy) (*datamodel.BackupPolicy, error) {
	se := j.SE
	updates := map[string]interface{}{
		"description":              dbBackupPolicy.Description,
		"policy_enabled":           dbBackupPolicy.PolicyEnabled,
		"daily_backups_to_keep":    dbBackupPolicy.DailyBackupsToKeep,
		"weekly_backups_to_keep":   dbBackupPolicy.WeeklyBackupsToKeep,
		"monthly_backups_to_keep":  dbBackupPolicy.MonthlyBackupsToKeep,
		"life_cycle_state":         models.LifeCycleStateREADY,
		"life_cycle_state_details": models.LifeCycleStateReadyDetails,
	}
	updated, err := se.UpdateBackupPolicy(ctx, dbBackupPolicy.UUID, updates)
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func (j *BackupPolicyActivity) PauseBackupPolicySchedule(ctx context.Context, dbBackupPolicy *datamodel.BackupPolicy) error {
	temporalScheduler := j.Scheduler

	// Check current scheduler state to avoid pausing an already paused schedule
	description, err := temporalScheduler.Describe(ctx, scheduler.DescribeScheduleParams{
		ScheduleParams: scheduler.ScheduleParams{ScheduleID: dbBackupPolicy.UUID},
	})
	if err != nil {
		return err
	}

	// If already paused, no need to pause again
	if description.Paused {
		logger.Info("Backup policy schedule is already paused")
		return nil
	}

	_, err = temporalScheduler.Pause(ctx, scheduler.PauseScheduleParams{ScheduleParams: scheduler.ScheduleParams{ScheduleID: dbBackupPolicy.UUID}})
	if err != nil {
		return err
	}
	return nil
}

func (j *BackupPolicyActivity) UnpauseBackupPolicySchedule(ctx context.Context, dbBackupPolicy *datamodel.BackupPolicy) error {
	temporalScheduler := j.Scheduler

	// Check current scheduler state to avoid unpausing an already active schedule
	description, err := temporalScheduler.Describe(ctx, scheduler.DescribeScheduleParams{
		ScheduleParams: scheduler.ScheduleParams{ScheduleID: dbBackupPolicy.UUID},
	})
	if err != nil {
		return err
	}

	// If already active (not paused), no need to unpause again
	if !description.Paused {
		logger.Info("Backup policy schedule is already un-paused")
		return nil
	}

	_, err = temporalScheduler.Unpause(ctx, scheduler.UnpauseScheduleParams{ScheduleParams: scheduler.ScheduleParams{ScheduleID: dbBackupPolicy.UUID}})
	if err != nil {
		return err
	}
	return nil
}

func updateBackupPolicyInSDE(ctx context.Context, params *common.UpdateBackupPolicyParams) (*cvpmodels.BackupPolicyV1beta, error) {
	logger := util.GetLogger(ctx)
	token := utils.GetAuthTokenFromContext(ctx)
	cvpClient := CvpCreateClient(logger, token)
	xCorrelationID := utils.GetCoRelationIDFromContext(ctx)

	op, _, err := cvpClient.BackupPolicy.V1betaUpdateBackupPolicy(&backup_policy.V1betaUpdateBackupPolicyParams{
		LocationID:     params.LocationID,
		ProjectNumber:  params.AccountName,
		XCorrelationID: &xCorrelationID,
		BackupPolicyID: params.BackupPolicyID,
		Body: &cvpmodels.BackupPolicyUpdateV1beta{
			Description: params.Description,
			Enabled:     params.PolicyEnabled,
			BackupPolicyScheduleV1beta: cvpmodels.BackupPolicyScheduleV1beta{
				DailyBackupLimit:   params.DailyBackupLimit,
				WeeklyBackupLimit:  params.WeeklyBackupLimit,
				MonthlyBackupLimit: params.MonthlyBackupLimit,
			},
		},
	})
	if err != nil {
		logger.Error("Error Updating BackupPolicy : ", err)
		return nil, err
	}

	responseBytes, err := json.MarshalIndent(op.Payload.Response, "", "  ")
	if err != nil {
		return nil, errors.New("failed to marshal response from SDE BackupPolicy Update")
	}
	data := cvpmodels.BackupPolicyV1beta{}
	err = utils.ConvertJsonToModel(responseBytes, &data)
	if err != nil {
		return nil, err
	}
	return &data, nil
}

func (j *BackupPolicyActivity) DeleteBackupPolicyInSDE(ctx context.Context, params *common.DeleteBackupPolicyParams) error {
	logger := util.GetLogger(ctx)
	GetSignedJwtToken := utils.GetAuthTokenFromContext(ctx)
	cvpClient := CvpCreateClient(logger, GetSignedJwtToken)
	xCorrelationID := utils.GetCoRelationIDFromContext(ctx)

	res, _, err := cvpClient.BackupPolicy.V1betaDeleteBackupPolicy(&backup_policy.V1betaDeleteBackupPolicyParams{
		LocationID:     params.LocationID,
		ProjectNumber:  params.OwnerID,
		BackupPolicyID: params.BackupPolicyID,
		XCorrelationID: &xCorrelationID,
	})
	if err != nil {
		logger.Errorf("Error deleting backup policy : %v", err)
		switch e := err.(type) {
		case *backup_policy.V1betaDeleteBackupPolicyBadRequest:
			return temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Bad request deleting backup policy %s: %s", params.BackupPolicyID, e.Error()),
				"V1betaDeleteBackupPolicyBadRequest",
				err,
			)

		case *backup_policy.V1betaDeleteBackupPolicyUnauthorized:
			return temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Unauthorized to delete backup policy %s: %s", params.BackupPolicyID, e.Error()),
				"V1betaDeleteBackupPolicyUnauthorized",
				err,
			)

		case *backup_policy.V1betaDeleteBackupPolicyForbidden:
			return temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Forbidden to delete backup policy %s: %s", params.BackupPolicyID, e.Error()),
				"V1betaDeleteBackupPolicyForbidden",
				err,
			)

		case *backup_policy.V1betaDeleteBackupPolicyNotFound:
			return temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Backup policy %s not found: %s", params.BackupPolicyID, e.Error()),
				"V1betaDeleteBackupPolicyNotFound",
				err,
			)

		case *backup_policy.V1betaDeleteBackupPolicyConflict:
			return temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Conflict deleting backup policy %s: %s", params.BackupPolicyID, e.Error()),
				"V1betaDeleteBackupPolicyConflict",
				err,
			)

		case *backup_policy.V1betaDeleteBackupPolicyInternalServerError:
			return temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Internal server error deleting backup policy %s: %s", params.BackupPolicyID, e.Error()),
				"V1betaDeleteBackupPolicyInternalServerError",
				err,
			)

		case *backup_policy.V1betaDeleteBackupPolicyNotImplemented:
			return temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Not implemented delete backup policy %s: %s", params.BackupPolicyID, e.Error()),
				"V1betaDeleteBackupPolicyNotImplemented",
				err,
			)

		default:
			logger.Warnf("Unknown error type for backup policy deletion %s: %T - %s", params.BackupPolicyID, err, err.Error())
			return err
		}
	}
	if res == nil || res.Payload == nil || res.Payload.Done == nil || !*res.Payload.Done {
		return errors.New("unknown error during delete backup policy in SDE")
	}
	return nil
}

func (j *BackupPolicyActivity) DeleteBackupPolicySchedule(ctx context.Context, backupPolicyID string) error {
	temporalScheduler := j.Scheduler
	_, err := temporalScheduler.Delete(ctx, scheduler.DeleteScheduleParams{ScheduleParams: scheduler.ScheduleParams{ScheduleID: backupPolicyID}})
	if err != nil {
		return err
	}
	return nil
}

// CheckIfBackupPolicyScheduleExists checks if a schedule exists for the backup policy.
func CheckIfBackupPolicyScheduleExists(ctx context.Context, temporalScheduler *scheduler.TemporalScheduler, backupPolicyUUID string) (bool, error) {
	_, err := temporalScheduler.Describe(ctx, scheduler.DescribeScheduleParams{
		ScheduleParams: scheduler.ScheduleParams{ScheduleID: backupPolicyUUID},
	})
	if err != nil {
		// Check if the error is a NotFound error (schedule doesn't exist)
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		// For other errors, return the error
		return false, err
	}
	return true, nil
}

// GetBackupPolicyByUUID gets a backup policy from VCP by UUID and owner account.
func (j *BackupPolicyActivity) GetBackupPolicyByUUID(ctx context.Context, backupPolicyUUID string, accountID int64) (*datamodel.BackupPolicy, error) {
	return j.SE.GetBackupPolicyByUUIDAndOwnerID(ctx, backupPolicyUUID, accountID)
}

// CheckIfBackupPolicyScheduleExists checks if a schedule exists for the backup policy.
func (j *BackupPolicyActivity) CheckIfBackupPolicyScheduleExists(ctx context.Context, backupPolicyUUID string) (bool, error) {
	temporalScheduler, ok := j.Scheduler.(*scheduler.TemporalScheduler)
	if !ok {
		return false, fmt.Errorf("invalid scheduler type %T for CheckIfBackupPolicyScheduleExists", j.Scheduler)
	}
	return CheckIfBackupPolicyScheduleExists(ctx, temporalScheduler, backupPolicyUUID)
}

func (j *BackupPolicyActivity) DeleteBackupPolicyInVCP(ctx context.Context, backupPolicyID string) (*datamodel.BackupPolicy, error) {
	se := j.SE
	backupPolicy, err := se.DeleteBackupPolicy(ctx, backupPolicyID)
	if err != nil {
		return nil, err
	}
	return backupPolicy, nil
}

func (j *BackupPolicyActivity) UpdateBackupPolicyStateInCaseOfError(ctx context.Context, backupPolicy *datamodel.BackupPolicy, state, stateDetails string) error {
	se := j.SE

	// Update the state of the BackupPolicy in the database
	updates := map[string]interface{}{
		"life_cycle_state":         state,
		"life_cycle_state_details": stateDetails,
	}
	_, err := se.UpdateBackupPolicy(ctx, backupPolicy.UUID, updates)
	if err != nil {
		return err
	}
	return nil
}

func _convertToBackupPolicyDataModel(backupPolicy *cvpmodels.BackupPolicyDetailsV1beta) *datamodel.BackupPolicy {
	var createdTime strfmt.DateTime
	if backupPolicy.CreatedAt != nil {
		createdTime = *backupPolicy.CreatedAt
	}
	var resourceID string
	if backupPolicy.ResourceID != nil {
		resourceID = *backupPolicy.ResourceID
	}
	var policyEnabled bool
	if backupPolicy.Enabled != nil {
		policyEnabled = *backupPolicy.Enabled
	}
	var dailyLimit, monthlyLimit, weeklyLimit int64
	if backupPolicy.DailyBackupLimit != nil {
		dailyLimit = *backupPolicy.DailyBackupLimit
	}
	if backupPolicy.WeeklyBackupLimit != nil {
		weeklyLimit = *backupPolicy.WeeklyBackupLimit
	}
	if backupPolicy.MonthlyBackupLimit != nil {
		monthlyLimit = *backupPolicy.MonthlyBackupLimit
	}
	var lifeCycleStateDetails string
	if backupPolicy.State == models.LifeCycleStateREADY {
		lifeCycleStateDetails = models.LifeCycleStateAvailableDetails
	}
	return &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			UUID:      backupPolicy.BackupPolicyID,
			CreatedAt: time.Time(createdTime),
		},
		Name:                  resourceID,
		Description:           backupPolicy.Description,
		DailyBackupsToKeep:    dailyLimit,
		WeeklyBackupsToKeep:   weeklyLimit,
		MonthlyBackupsToKeep:  monthlyLimit,
		PolicyEnabled:         policyEnabled,
		LifeCycleState:        backupPolicy.State,
		LifeCycleStateDetails: lifeCycleStateDetails,
	}
}

// CleanupBackupPoliciesForAccount fetches all backup policies for an account and cleans them up
func (a *BackupPolicyActivity) CleanupBackupPoliciesForAccount(ctx context.Context, projectNumber string) error {
	logger := util.GetLogger(ctx)

	// Get account ID from project number
	account, err := a.SE.GetAccount(ctx, projectNumber)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Fetch all backup policies for the account
	conditions := [][]interface{}{
		{"account_id = ?", account.ID},
	}
	backupPolicies, err := a.SE.ListBackupPolicies(ctx, conditions)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if len(backupPolicies) > 0 {
		logger.Infof("Cleaning up %d backup policies for project %s", len(backupPolicies), projectNumber)
	}

	// Cleanup each backup policy
	for _, policy := range backupPolicies {
		err = a.cleanupBackupPolicy(ctx, policy)
		if err != nil {
			logger.Errorf("Failed to cleanup backup policy %s: %v", policy.UUID, err)
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
	}

	return nil
}

// cleanupBackupPolicy handles the cleanup of a single backup policy
func (a *BackupPolicyActivity) cleanupBackupPolicy(ctx context.Context, policy *datamodel.BackupPolicy) error {
	logger := util.GetLogger(ctx)

	// 1. Delete temporal scheduler for this policy
	_, err := a.Scheduler.Delete(ctx, scheduler.DeleteScheduleParams{
		ScheduleParams: scheduler.ScheduleParams{
			ScheduleID: policy.UUID,
		},
	})
	if err != nil {
		logger.Errorf("Failed to delete temporal scheduler for policy %s: %v", policy.UUID, err)
		// Continue with database deletion even if scheduler deletion fails
	}

	// 2. Soft delete the backup policy in database
	_, err = a.SE.DeleteBackupPolicy(ctx, policy.UUID)
	if err != nil {
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Failed to soft delete backup policy %s: %v", policy.UUID, err),
			"DeleteBackupPolicyError",
			err,
		)
	}

	return nil
}
