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
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
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
	if len(backups) == 0 {
		logger.Warnf("HydrateCreatedBackupsToCCFE called with no backups for volume %s", volume.Name)
		return nil
	}

	token, err := auth.GenerateCallbackToken(ctx)
	if err != nil {
		return err
	}
	// TODO: Consider validating the "GetBackupRegion" function for CRB
	region, err := utils.GetBackupRegion(volume)
	if err != nil {
		return err
	}
	projectId := volume.Account.Name
	requests := convertToGCPHydrateCreateRequests(backups)
	err = common.HydrateCreatedScheduledBackups(ctx, logger, requests, backupVaultName, region, projectId, token)
	if err != nil {
		return err
	}
	return nil
}

// HydrateDeletedBackupsToCCFE sends information about deleted scheduled backups to CCFE.
// Returns an error if the operation fails.
func (j *ScheduledBackupActivity) HydrateDeletedBackupsToCCFE(ctx context.Context, volume *datamodel.Volume, backups []*datamodel.Backup, backupVaultName string) error {
	logger := util.GetLogger(ctx)
	if len(backups) == 0 {
		logger.Warnf("HydrateDeletedBackupsToCCFE called with no backups for volume %s", volume.Name)
		return nil
	}

	token, err := auth.GenerateCallbackToken(ctx)
	if err != nil {
		return err
	}
	// TODO: Consider validating the "GetBackupRegion" function for CRB
	region, err := utils.GetBackupRegion(volume)
	if err != nil {
		return err
	}
	projectId := volume.Account.Name
	names := convertToGCPHydrateDeleteRequests(backups)
	err = common.HydrateDeletedScheduledBackups(ctx, logger, names, backupVaultName, region, projectId, token)
	if err != nil {
		return err
	}
	return nil
}

// GetVolumesByBackupPolicyUUID retrieves volumes that have the specified backup policy enabled for a given account.
// Returns a slice of Volume objects or an error.
func (j *ScheduledBackupActivity) GetVolumesByBackupPolicyUUID(ctx context.Context, backupPolicyUUID string, accountID int64) ([]*datamodel.Volume, error) {
	se := j.SE
	// Get the list of all volumes which have the specified backup policy enabled
	conditions := [][]interface{}{
		{"account_id = ?", accountID},
		{"data_protection->>'backup_policy_id' = ?", backupPolicyUUID},
		{"data_protection->>'scheduled_backup_enabled' = 'true'"},
	}
	volumes, err := se.ListVolumes(ctx, conditions)
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
		Type:               SnapshotTypeBackupScheduled,
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

	_, err := se.UpdateBackup(ctx, backup)
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

// convertToGCPHydrateCreateRequests converts a slice of Backup objects to GCP hydrate create requests.
// Returns a slice of Request objects.
func convertToGCPHydrateCreateRequests(backups []*datamodel.Backup) []models.Request {
	var requests []models.Request
	for _, backup := range backups {
		volumeUsageInBytes := uint64(backup.SizeInBytes)
		request := models.Request{Backup: &models.HydrateBackup{
			ResourceId:       backup.Name,
			BackupId:         backup.UUID,
			VolumeUsageBytes: &volumeUsageInBytes,
		}}
		requests = append(requests, request)
	}
	return requests
}

// convertToGCPHydrateDeleteRequests converts a slice of Backup objects to a slice of backup names for deletion.
// Returns a slice of strings.
func convertToGCPHydrateDeleteRequests(backups []*datamodel.Backup) []string {
	var names []string
	for _, backup := range backups {
		names = append(names, fmt.Sprintf("backups/%s", backup.Name))
	}
	return names
}

// RandomString generates a human-friendly random string of the specified length.
// Returns the generated string.
func RandomString(n int) string {
	return randomstring.HumanFriendlyEnglishString(n)
}
