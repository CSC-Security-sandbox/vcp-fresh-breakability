package backgroundactivities

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"github.com/xyproto/randomstring"
)

const (
	backupTypeSCHEDULED       = "SCHEDULED"
	scheduledBackupNameFormat = "%s-scheduled-backup-%s-%s"
)

// ScheduledBackupActivity represents activities related to scheduled backups.
type ScheduledBackupActivity struct {
	SE database.Storage
}

// CreateScheduledBackup creates a scheduled backup for the given volume and backup vault.
// Returns the created Backup object or an error.
func (j *ScheduledBackupActivity) CreateScheduledBackup(ctx context.Context, volume *datamodel.Volume, backupVault *datamodel.BackupVault, timestamp, scheduleTag string) (*datamodel.Backup, error) {
	se := j.SE

	name := fmt.Sprintf(scheduledBackupNameFormat, scheduleTag, RandomString(8), timestamp)
	backup, err := se.CreateBackup(ctx, &datamodel.Backup{
		BaseModel: datamodel.BaseModel{
			UUID: utils.RandomUUID(),
		},
		Name:          name,
		State:         models.LifeCycleStateCreating,
		StateDetails:  models.LifeCycleStateCreatingDetails,
		Type:          backupTypeSCHEDULED,
		ScheduleTag:   &scheduleTag,
		VolumeUUID:    volume.UUID,
		BackupVaultID: backupVault.ID,
		BackupVault:   backupVault,
	})
	if err != nil {
		return nil, err
	}

	return backup, nil
}

// GenerateScheduledSnapshotName generates a name for a scheduled snapshot using a random string and timestamp.
// Returns the generated name or an error.
func (j *ScheduledBackupActivity) GenerateScheduledSnapshotName(ctx context.Context, timestamp string) (string, error) {
	return fmt.Sprintf("scheduled-snapshot-%s-%s", RandomString(8), timestamp), nil
}

// HydrateCreatedBackupsToCCFE sends information about created scheduled backups to CCFE.
// Returns an error if the operation fails.
func (j *ScheduledBackupActivity) HydrateCreatedBackupsToCCFE(ctx context.Context, volume *datamodel.Volume, backups []*datamodel.Backup, backupVaultName string) error {
	logger := util.GetLogger(ctx)

	if !hydrationEnabled {
		logger.Info("Hydration is disabled, skipping created backups hydration to CCFE")
		return nil
	}

	if len(backups) == 0 {
		logger.Warnf("HydrateCreatedBackupsToCCFE called with no backups for volume %s", volume.Name)
		return nil
	}

	token, err := auth.GenerateCallbackToken(ctx)
	if err != nil {
		return err
	}
	region, err := utils.GetBackupRegion(volume)
	if err != nil {
		return err
	}
	projectId := volume.Account.Name
	requests := common.ConvertToGCPHydrateBackupCreateRequests(backups)
	err = common.HydrateCreatedBackups(ctx, logger, requests, backupVaultName, region, projectId, token)
	if err != nil {
		return err
	}
	return nil
}

// HydrateDeletedBackupsToCCFE sends information about deleted scheduled backups to CCFE.
// Returns an error if the operation fails.
func (j *ScheduledBackupActivity) HydrateDeletedBackupsToCCFE(ctx context.Context, volume *datamodel.Volume, backups []*datamodel.Backup, backupVaultName string) error {
	logger := util.GetLogger(ctx)

	if !hydrationEnabled {
		logger.Info("Hydration is disabled, skipping deleted backups hydration to CCFE")
		return nil
	}

	if len(backups) == 0 {
		logger.Warnf("HydrateDeletedBackupsToCCFE called with no backups for volume %s", volume.Name)
		return nil
	}

	token, err := auth.GenerateCallbackToken(ctx)
	if err != nil {
		return err
	}
	region, err := utils.GetBackupRegion(volume)
	if err != nil {
		return err
	}
	projectId := volume.Account.Name
	names := common.ConvertToGCPHydrateBackupDeleteRequests(backups)
	err = common.HydrateDeletedBackups(ctx, logger, names, backupVaultName, region, projectId, token)
	if err != nil {
		return err
	}
	return nil
}

// GetBackupPolicyByUUID retrieves a backup policy from the database by UUID and account ID.
// Returns the BackupPolicy object or an error.
func (j *ScheduledBackupActivity) GetBackupPolicyByUUID(ctx context.Context, backupPolicyUUID string, accountID int64) (*datamodel.BackupPolicy, error) {
	se := j.SE
	backupPolicy, err := se.GetBackupPolicyByUUIDAndOwnerID(ctx, backupPolicyUUID, accountID)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			return nil, vsaerrors.WrapAsTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, err),
			)
		}
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return backupPolicy, nil
}

// GetVolumesByBackupPolicyUUID retrieves volumes that have the specified backup policy enabled for a given account.
// Returns a slice of Volume objects or an error.
func (j *ScheduledBackupActivity) GetVolumesByBackupPolicyUUID(ctx context.Context, backupPolicyUUID string, accountID int64, limit, offset int) ([]*datamodel.Volume, error) {
	se := j.SE
	// Get the list of all volumes which have the specified backup policy enabled
	conditions := [][]interface{}{
		{"account_id = ?", accountID},
		{"state = ?", models.LifeCycleStateREADY},
		{"data_protection->>'backup_policy_id' = ?", backupPolicyUUID},
		{"data_protection->>'scheduled_backup_enabled' = 'true'"},
	}
	pagination := &dbutils.Pagination{
		Limit:  limit,
		Offset: offset,
	}
	volumes, err := se.ListVolumesWithPagination(ctx, conditions, pagination)
	if err != nil {
		return nil, err
	}
	return volumes, nil
}

// FetchScheduledBackupForDeletion fetches scheduled backups for a volume and backup policy that are eligible for deletion.
// Returns a slice of Backup objects or an error.
func (j *ScheduledBackupActivity) FetchScheduledBackupForDeletion(ctx context.Context, volume *datamodel.Volume, backupPolicy *datamodel.BackupPolicy) ([]*datamodel.Backup, error) {
	se := j.SE
	return se.FetchScheduledBackupsForDeletion(ctx, volume, backupPolicy)
}

func (j *ScheduledBackupActivity) CreateBackupSnapshotInDB(ctx context.Context, volume *datamodel.Volume, snapshotName string) (*datamodel.Snapshot, error) {
	se := j.SE
	logger := util.GetLogger(ctx)

	snapshot := &datamodel.Snapshot{
		Name:               snapshotName,
		Description:        activities.BackupComment,
		VolumeID:           volume.ID,
		AccountID:          volume.AccountID,
		Volume:             volume,
		Account:            volume.Account,
		IsAppConsistent:    false,
		Type:               SnapshotTypeBackup,
		SnapshotAttributes: &datamodel.SnapshotAttributes{},
	}
	dbSnapshot, err := se.CreatingSnapshot(ctx, snapshot)
	if err != nil {
		logger.Errorf("Failed to create snapshot in database. Error: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return dbSnapshot, nil
}

func (j *ScheduledBackupActivity) UpdateBackupSnapshotInDB(ctx context.Context, dbSnapshot *datamodel.Snapshot, ontapSnapshot *vsa.SnapshotProviderResponse) (*datamodel.Snapshot, error) {
	se := j.SE
	logger := util.GetLogger(ctx)

	// Update the snapshot in the database
	dbSnapshot.State = models.LifeCycleStateREADY
	dbSnapshot.StateDetails = models.LifeCycleStateAvailableDetails
	dbSnapshot.SnapshotAttributes = &datamodel.SnapshotAttributes{
		SizeInBytes:            ontapSnapshot.SizeInBytes,
		ExternalUUID:           ontapSnapshot.ExternalUUID,
		LogicalSizeUsedInBytes: ontapSnapshot.LogicalSizeInBytes,
	}

	_, err := se.UpdateSnapshot(ctx, dbSnapshot)
	if err != nil {
		logger.Errorf("Failed to update snapshot details in database. Error: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return dbSnapshot, nil
}

func (j *ScheduledBackupActivity) DeleteBackupSnapshotInDB(ctx context.Context, snapshotUUID string) error {
	se := j.SE
	logger := util.GetLogger(ctx)

	_, err := se.DeleteSnapshot(ctx, snapshotUUID)
	if err != nil {
		logger.Errorf("Failed to delete snapshot in database. Error: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return nil
}

func (j *ScheduledBackupActivity) UpdateBackupState(ctx context.Context, backup *datamodel.Backup) (*datamodel.Backup, error) {
	se := j.SE
	updated, err := se.UpdateBackupState(ctx, backup)
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func (j *ScheduledBackupActivity) UpdateBackupSize(ctx context.Context, backup *datamodel.Backup, volume *datamodel.Volume) error {
	logger := util.GetLogger(ctx)
	se := j.SE

	_, err := se.FinishBackup(ctx, backup)
	if err != nil {
		logger.Errorf("Failed to update backup %s with size information: %v", backup.UUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Set LatestLogicalBackupSize to 0 for all previous backups of the same volume in a single query
	// This ensures that only the latest backup has the correct size
	// Update only if the latest logical backup size is not zero for the current backup
	if backup.LatestLogicalBackupSize != 0 {
		err = se.UpdateBackupLatestLogicalBackupSizeByVolume(ctx, volume.UUID, backup.UUID)
		if err != nil {
			logger.Errorf("Failed to reset LatestLogicalBackupSize for previous backups of volume %s: %v", volume.UUID, err)
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
	}

	// Update the volume's LatestLogicalBackupSize field
	volume.DataProtection.BackupChainBytes = &backup.LatestLogicalBackupSize
	updates := map[string]interface{}{
		"data_protection": volume.DataProtection,
	}
	err = se.UpdateVolumeFields(ctx, volume.UUID, updates)
	if err != nil {
		logger.Errorf("Failed to update volume %s with latest logical backup size: %v", volume.UUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Debugf("Successfully updated backup size fields for backup %s and volume %s", backup.UUID, volume.UUID)
	return nil
}

func (j *ScheduledBackupActivity) GetSnapshotByNameAndVolumeID(ctx context.Context, snapshotName string, accountID, volumeID int64) (*datamodel.Snapshot, error) {
	logger := util.GetLogger(ctx)

	se := j.SE
	snapshot, err := se.GetSnapshotByNameAndVolumeId(ctx, snapshotName, accountID, volumeID)
	if err != nil {
		logger.Errorf("Failed to get snapshot by name and volumeID: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return snapshot, nil
}

// RandomString generates a human-friendly random string of the specified length.
// Returns the generated string.
func RandomString(n int) string {
	return randomstring.HumanFriendlyEnglishString(n)
}

// CreateRemoteScheduledBackupsFromVCPActivity creates remote backups in the remote region for scheduled backups
func (j *ScheduledBackupActivity) CreateRemoteScheduledBackupsFromVCPActivity(ctx context.Context, backupVault *datamodel.BackupVault, backups []*datamodel.Backup, volume *datamodel.Volume, projectNumber string) error {
	logger := util.GetLogger(ctx)

	// Check if this is a cross-region backup vault
	if backupVault.BackupVaultType != activities.CrossRegionBackupType || backupVault.BackupRegionName == nil {
		// Not a cross-region backup, skip
		return nil
	}

	// Create remote backup for each backup
	backupActivity := &activities.BackupActivity{SE: j.SE}
	for _, backup := range backups {
		// Create context with backup vault and volume information
		backupActivitiesContext := &activities.BackupActivitiesContext{
			BackupWorkflowInit: &activities.BackupWorkflowInput{
				Backup:      backup,
				BackupVault: backupVault,
				Volume:      volume,
			},
		}

		err := backupActivity.CreateRemoteBackupFromVCPActivity(ctx, backupActivitiesContext)
		if err != nil {
			logger.Errorf("Failed to create remote backup from VCP for scheduled backup %s: %v", backup.UUID, err)
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
	}

	logger.Infof("Successfully created %d remote backups for scheduled backups", len(backups))
	return nil
}

// DeleteRemoteScheduledBackupFromVCPActivity deletes a remote backup in the remote region for scheduled backup
func (j *ScheduledBackupActivity) DeleteRemoteScheduledBackupFromVCPActivity(ctx context.Context, backupUUID, backupVaultUUID, projectNumber, region string) error {
	logger := util.GetLogger(ctx)
	backupActivity := &activities.BackupActivity{SE: j.SE}

	err := backupActivity.DeleteRemoteBackupFromVCPActivity(ctx, backupUUID, backupVaultUUID, projectNumber, region)
	if err != nil {
		logger.Errorf("Failed to delete remote backup from VCP for scheduled backup %s: %v", backupUUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Successfully deleted remote backup for scheduled backup %s", backupUUID)
	return nil
}

// CheckBackupsInProgressByVolume checks if any backup for the volume is in CREATING or DELETING state.
// Excludes the specified backup UUIDs from the check (typically the backups being created in the current workflow).
// Returns an error if a backup is found in these states to prevent parallel transfers on the same Snapmirror instance.
func (j *ScheduledBackupActivity) CheckBackupsInProgressByVolume(ctx context.Context, volumeUUID string, excludedBackupUUIDs []string) error {
	se := j.SE
	logger := util.GetLogger(ctx)

	backupInTransition, err := se.AreBackupsInProgressForVolume(ctx, volumeUUID, excludedBackupUUIDs)
	if err != nil {
		logger.Errorf("Failed to check backup state for volume %s: %v", volumeUUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if backupInTransition {
		logger.Warnf("Another backup operation is already in progress for volume %s. Skipping to prevent parallel transfers on the same Snapmirror instance", volumeUUID)
		return vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("another backup operation is already in progress for volume %s. Please wait for it to complete before starting a new backup", volumeUUID))
	}

	return nil
}
