package backgroundactivities

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
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
		ScheduleTag:   scheduleTag,
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
	region := backups[0].BackupVault.RegionName
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
	region := backups[0].BackupVault.RegionName
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
		{"data_protection->>'scheduled_backup_enabled' = true"},
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
		names = append(names, backup.Name)
	}
	return names
}

// RandomString generates a human-friendly random string of the specified length.
// Returns the generated string.
func RandomString(n int) string {
	return randomstring.HumanFriendlyEnglishString(n)
}
