package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	metricsdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	vcpdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type CrossRegionRestoreBillingResult struct {
	HydratedMetricsDataModel []datamodel2.HydratedMetrics
}

// ProcessRestoreBillingMetrics generates billing metrics for cross-region backup restores.
//
// Background:
// When customers restore a backup from a different region, we bill them for the data transfer.
// We use a checkpoint timestamp to track which restores we've already billed, preventing double-charging.
//
// Algorithm (aligned with SDE/cloud-volumes-telemetry):
//  1. Read the last checkpoint timestamp from metrics DB (defaults to current time on first run — no lookback)
//  2. Eagerly advance the checkpoint to current time BEFORE processing, so the query window
//     always moves forward regardless of whether processing succeeds or jobs are found
//  3. Query for completed RESTORE_BACKUP jobs (and RESTORE_FILES_BACKUP if SFR billing is enabled)
//     in the bounded window (lastCheckpoint, currentTime]
//  4. For each completed restore job:
//     a. Look up the restored volume (including soft-deleted volumes, since we bill regardless)
//     b. Resolve the source backup using volume.RestoredBackupID or volume.RestoredBackupPath
//     c. Check if the backup came from a cross-region vault
//     d. If cross-region, emit a billing metric — using backup.SizeInBytes for full restores
//     or sfr_metadata.FilesSize for SFR restores
//
// Note: We intentionally include deleted volumes because the data transfer already occurred.
func ProcessRestoreBillingMetrics(
	ctx context.Context,
	vcpDB vcpdb.Storage,
	metricsDB metricsdb.Storage,
	config *common.TelemetryConfig,
	timestamp time.Time,
) (*CrossRegionRestoreBillingResult, error) {
	logger := util.GetLogger(ctx)

	lastProcessedAt, err := getLastRestoreTimestamp(ctx, metricsDB, timestamp)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to get timestamp info, error %v", err))
		return &CrossRegionRestoreBillingResult{}, err
	}

	logger.Debug(fmt.Sprintf("Timestamp got from LastRestoreTimestamp table is %v and Update restore timestamp to %v", lastProcessedAt, timestamp))

	if err := metricsDB.UpdateRestoreTimestamp(ctx, timestamp); err != nil {
		logger.Error(fmt.Sprintf("Could not update timestamp %v, Error: %v", timestamp, err))
		return &CrossRegionRestoreBillingResult{}, err
	}
	logger.Debug(fmt.Sprintf("stored updated timestamp %v", timestamp))

	jobs, err := fetchRestoreJobs(ctx, vcpDB, lastProcessedAt, timestamp, config.EnableSFRCrossRegionRestoreBilling)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to fetch restore jobs, error %v", err))
		return &CrossRegionRestoreBillingResult{}, err
	}

	if len(jobs) == 0 {
		logger.Debug("No completed restore jobs found since last timestamp", "lastProcessedAt", lastProcessedAt)
		return &CrossRegionRestoreBillingResult{}, nil
	}

	logger.Info(fmt.Sprintf("Processing %d completed restore jobs for cross-region restore billing", len(jobs)),
		"sinceTimestamp", lastProcessedAt)

	var hydratedMetrics []datamodel2.HydratedMetrics

	for _, job := range jobs {
		metric := processRestoreJob(ctx, vcpDB, config, job, timestamp)
		if metric != nil {
			hydratedMetrics = append(hydratedMetrics, *metric)
		}
	}

	logger.Debug("Exit ProcessRestoreBillingMetrics")

	return &CrossRegionRestoreBillingResult{
		HydratedMetricsDataModel: hydratedMetrics,
	}, nil
}

// getLastRestoreTimestamp reads the last processed restore timestamp from metricsDB.
// If no previous timestamp exists (first run), defaults to current time (no lookback)
func getLastRestoreTimestamp(ctx context.Context, metricsDB metricsdb.Storage, now time.Time) (time.Time, error) {
	logger := util.GetLogger(ctx)

	restoreTimestamp, err := metricsDB.GetRestoreTimestamp(ctx)
	if err != nil {
		return time.Time{}, err
	}

	if restoreTimestamp == nil {
		logger.Debug(fmt.Sprintf("set last restore time to %v", now))
		return now, nil
	}

	return restoreTimestamp.LastProcessedAt, nil
}

// fetchRestoreJobs queries vcpDB for restore jobs in the bounded window [since, until].
// The bounded window ensures each billing run processes a distinct time range,
// matching the SDE query: updated_at >= $1 AND updated_at <= $2.
// When includeSFR is true, both RESTORE_BACKUP and RESTORE_FILES_BACKUP jobs are returned,
// and SFR jobs in ERROR state are also included (partial SFR failures may still have
// transferred data that should be billed).
func fetchRestoreJobs(ctx context.Context, vcpDB vcpdb.Storage, since time.Time, until time.Time, includeSFR bool) ([]*datamodel.Job, error) {
	if !includeSFR {
		filter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("type", "=", string(models.JobTypeRestoreBackup)),
			dbutils.NewFilterCondition("state", "=", string(models.JobsStateDONE)),
			dbutils.NewFilterCondition("updated_at", ">=", since.Format(time.RFC3339Nano)),
			dbutils.NewFilterCondition("updated_at", "<=", until.Format(time.RFC3339Nano)),
		)
		return vcpDB.GetJobsWithCondition(ctx, *filter)
	}

	// When SFR is enabled, include DONE jobs for both types and also ERROR SFR jobs
	// (partial SFR failures may have restored some files before failing).
	filter := dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("type", "IN", []string{
			string(models.JobTypeRestoreBackup),
			string(models.JobTypeRestoreFilesBackup),
		}),
		dbutils.NewFilterCondition("state", "IN", []string{
			string(models.JobsStateDONE),
			string(models.JobsStateERROR),
		}),
		dbutils.NewFilterCondition("updated_at", ">=", since.Format(time.RFC3339Nano)),
		dbutils.NewFilterCondition("updated_at", "<=", until.Format(time.RFC3339Nano)),
	)

	return vcpDB.GetJobsWithCondition(ctx, *filter)
}

// processRestoreJob checks if the restore is cross-region, and returns a hydrated metric if so.
// For SFR jobs (RESTORE_FILES_BACKUP), the backup is resolved via sfr_metadata and the
// transferred size is the actual restored file size rather than the full backup size.
func processRestoreJob(
	ctx context.Context,
	vcpDB vcpdb.Storage,
	config *common.TelemetryConfig,
	job *datamodel.Job,
	timestamp time.Time,
) *datamodel2.HydratedMetrics {
	logger := util.GetLogger(ctx)

	if !job.AccountID.Valid {
		logger.Warn("Skipping restore job with invalid account ID", "jobUUID", job.UUID)
		return nil
	}

	isSFR := job.Type == string(models.JobTypeRestoreFilesBackup)

	// Failed full volume restores are not billable; failed SFR jobs may have
	// partially transferred data and are handled via sfr_metadata.
	if !isSFR && job.State == string(models.JobsStateERROR) {
		logger.Debug("Skipping failed full volume restore", "jobUUID", job.UUID)
		return nil
	}

	if isSFR {
		return createSfrCrossRegionRestoreMetrics(ctx, vcpDB, config, job, timestamp)
	}

	return createCrossRegionRestoreMetrics(ctx, vcpDB, config, job, timestamp)
}

// createCrossRegionRestoreMetrics handles full volume restore billing (RESTORE_BACKUP).
func createCrossRegionRestoreMetrics(
	ctx context.Context,
	vcpDB vcpdb.Storage,
	config *common.TelemetryConfig,
	job *datamodel.Job,
	timestamp time.Time,
) *datamodel2.HydratedMetrics {
	logger := util.GetLogger(ctx)

	volume, err := getRestoredVolume(ctx, vcpDB, job)
	if err != nil || volume == nil {
		logger.Warn("Volume not found for restore job", "jobUUID", job.UUID, "resourceName", job.ResourceName, "error", err)
		return nil
	}

	backup, err := FetchSourceBackupForRestoredVolumeUsingIDOrBackupPath(ctx, vcpDB, volume, job)
	if err != nil || backup == nil {
		return nil
	}

	if backup.BackupVault == nil {
		logger.Warn("Backup vault not loaded for backup", "jobUUID", job.UUID, "backupUUID", backup.UUID)
		return nil
	}

	if backup.BackupVault.BackupVaultType != activities.CrossRegionBackupType {
		logger.Debug("Skipping non-cross-region restore", "jobUUID", job.UUID, "backupUUID", backup.UUID, "vaultType", backup.BackupVault.BackupVaultType)
		return nil
	}

	if backup.BackupVault.BackupRegionName == nil {
		logger.Warn("Skipping CbsCrossRegionVolumeRestoreTransferBytes for restore job: BackupRegionName is nil", "jobUUID", job.UUID, "backupUUID", backup.UUID)
		return nil
	}

	if *backup.BackupVault.BackupRegionName == config.RegionName {
		logger.Warn("Skipping CbsCrossRegionVolumeRestoreTransferBytes for restore job: BackupRegionName matches current region", "jobUUID", job.UUID, "backupUUID", backup.UUID, "backupRegionName", *backup.BackupVault.BackupRegionName)
		return nil
	}

	if backup.SizeInBytes <= 0 {
		logger.Debug("Skipping restore with zero backup size", "jobUUID", job.UUID, "backupUUID", backup.UUID)
		return nil
	}

	restoreMetadata := assembleMetadata(volume, backup, config)
	accountName := getVolumeAccountName(volume)

	logger.Debug("Generating cross-region restore billing metric",
		"jobUUID", job.UUID, "volumeUUID", volume.UUID, "backupUUID", backup.UUID,
		"sizeInBytes", backup.SizeInBytes, "accountName", accountName)

	hm := setupHydratedMetricsDataModel(
		metadata.CbsCrossRegionVolumeRestoreTransferBytes,
		metadata.Volume,
		accountName,
		restoreMetadata,
		timestamp,
		float64(backup.SizeInBytes),
	)
	setRegionMetadataForBilling(hm, restoreMetadata)
	return hm
}

// createSfrCrossRegionRestoreMetrics handles SFR (selective file restore) billing (RESTORE_FILES_BACKUP).
// For SFR jobs, the backup is resolved via sfr_metadata.BackupUUID and the quantity is
// sfr_metadata.FilesSize (actual restored file size) instead of the full backup size.
func createSfrCrossRegionRestoreMetrics(
	ctx context.Context,
	vcpDB vcpdb.Storage,
	config *common.TelemetryConfig,
	job *datamodel.Job,
	timestamp time.Time,
) *datamodel2.HydratedMetrics {
	logger := util.GetLogger(ctx)

	sfrMetadata, err := vcpDB.GetSfrMetadataByJobID(ctx, job.ID)
	if err != nil || sfrMetadata == nil {
		logger.Warn("SFR metadata not found for restore-files job", "jobUUID", job.UUID, "jobID", job.ID, "error", err)
		return nil
	}

	if sfrMetadata.FilesSize <= 0 {
		logger.Debug("Skipping SFR restore with zero file size", "jobUUID", job.UUID, "jobID", job.ID)
		return nil
	}

	backup, err := vcpDB.GetBackupWithVaultByUUID(ctx, sfrMetadata.BackupUUID)
	if err != nil || backup == nil {
		logger.Warn("Backup not found for SFR restore", "jobUUID", job.UUID, "backupUUID", sfrMetadata.BackupUUID, "error", err)
		return nil
	}

	if backup.BackupVault == nil {
		logger.Warn("Backup vault not loaded for SFR backup", "jobUUID", job.UUID, "backupUUID", backup.UUID)
		return nil
	}

	if backup.BackupVault.BackupVaultType != activities.CrossRegionBackupType {
		logger.Debug("Skipping non-cross-region SFR restore", "jobUUID", job.UUID, "backupUUID", backup.UUID, "vaultType", backup.BackupVault.BackupVaultType)
		return nil
	}

	if backup.BackupVault.BackupRegionName == nil {
		logger.Warn("Skipping CbsCrossRegionVolumeRestoreTransferBytes for SFR restore job: BackupRegionName is nil", "jobUUID", job.UUID, "backupUUID", backup.UUID)
		return nil
	}

	if *backup.BackupVault.BackupRegionName == config.RegionName {
		logger.Warn("Skipping CbsCrossRegionVolumeRestoreTransferBytes for SFR restore job: BackupRegionName matches current region", "jobUUID", job.UUID, "backupUUID", backup.UUID, "backupRegionName", *backup.BackupVault.BackupRegionName)
		return nil
	}

	// For SFR, job.ResourceName is the volume UUID
	volume, err := getSfrRestoredVolume(ctx, vcpDB, sfrMetadata.VolumeUUID)
	if err != nil || volume == nil {
		logger.Warn("Volume not found for SFR restore", "jobUUID", job.UUID, "volumeUUID", sfrMetadata.VolumeUUID, "error", err)
		return nil
	}

	restoreMetadata := assembleMetadata(volume, backup, config)
	accountName := getVolumeAccountName(volume)

	logger.Debug("Generating SFR cross-region restore billing metric",
		"jobUUID", job.UUID, "volumeUUID", volume.UUID, "backupUUID", backup.UUID,
		"sfrFilesSize", sfrMetadata.FilesSize, "sfrFileCount", sfrMetadata.FileCount,
		"accountName", accountName, "measuredType", metadata.CbsCrossRegionVolumeRestoreTransferBytes)

	hm := setupHydratedMetricsDataModel(
		metadata.CbsCrossRegionVolumeRestoreTransferBytes,
		metadata.Volume,
		accountName,
		restoreMetadata,
		timestamp,
		float64(sfrMetadata.FilesSize),
	)
	setRegionMetadataForBilling(hm, restoreMetadata)
	return hm
}

// FetchSourceBackupForRestoredVolumeUsingIDOrBackupPath finds the backup associated with a restore volume by trying
// RestoredBackupID first, then falling back to parsing RestoredBackupPath.
func FetchSourceBackupForRestoredVolumeUsingIDOrBackupPath(ctx context.Context, vcpDB vcpdb.Storage, volume *datamodel.Volume, job *datamodel.Job) (*datamodel.Backup, error) {
	logger := util.GetLogger(ctx)

	if volume.VolumeAttributes == nil {
		logger.Debug("Volume has no attributes, cannot find source backup for volumeUUID", "volumeUUID", volume.UUID)
		return nil, nil
	}

	if volume.VolumeAttributes.RestoredBackupID != "" {
		backup, err := vcpDB.GetBackupWithVaultByUUID(ctx, volume.VolumeAttributes.RestoredBackupID)
		if err != nil {
			logger.Warn("Failed to find backup by RestoredBackupID",
				"jobUUID", job.UUID, "volumeUUID", volume.UUID,
				"restoredBackupID", volume.VolumeAttributes.RestoredBackupID, "error", err)
			return nil, err
		}
		return backup, nil
	}

	if volume.VolumeAttributes.RestoredBackupPath != "" {
		logger.Debug("RestoredBackupID empty, finding via RestoredBackupPath",
			"jobUUID", job.UUID, "volumeUUID", volume.UUID,
			"restoredBackupPath", volume.VolumeAttributes.RestoredBackupPath)
		return FetchSourceBackupByResourcePath(ctx, vcpDB, volume, job)
	}

	logger.Debug("No RestoredBackupID or RestoredBackupPath on volume, skipping",
		"jobUUID", job.UUID, "volumeUUID", volume.UUID)
	return nil, nil
}

// FetchSourceBackupByResourcePath parses backupPath from RestoredBackupPath and looks up the backup and vault by name.
// Expected path format: projects/{project}/locations/{location}/backupVaults/{vault}/backups/{backup}
func FetchSourceBackupByResourcePath(ctx context.Context, vcpDB vcpdb.Storage, volume *datamodel.Volume, job *datamodel.Job) (*datamodel.Backup, error) {
	logger := util.GetLogger(ctx)
	path := volume.VolumeAttributes.RestoredBackupPath

	vaultName, backupName, err := parseRestoredBackupPath(path)
	if err != nil {
		logger.Warn("Failed to parse RestoredBackupPath", "jobUUID", job.UUID, "path", path, "error", err)
		return nil, err
	}

	accountID := fmt.Sprintf("%d", job.AccountID.Int64)
	vault, err := vcpDB.GetBackupVaultByNameAndOwnerID(ctx, vaultName, accountID)
	if err != nil || vault == nil {
		logger.Warn("Backup vault not found from resource path",
			"jobUUID", job.UUID, "vaultName", vaultName, "accountID", accountID, "error", err)
		return nil, err
	}

	backup, err := vcpDB.GetBackupByNameAndBackupVaultID(ctx, backupName, vault.ID)
	if err != nil || backup == nil {
		logger.Warn("Backup not found from resource path",
			"jobUUID", job.UUID, "backupName", backupName, "vaultID", vault.ID, "error", err)
		return nil, err
	}

	logger.Debug("Resolved backup from resource path",
		"jobUUID", job.UUID, "backupUUID", backup.UUID, "vaultName", vaultName, "backupName", backupName)

	return backup, nil
}

// parseRestoredBackupPath extracts vault name and backup name from a resource path.
// Expected format: projects/{project}/locations/{location}/backupVaults/{vault}/backups/{backup}
func parseRestoredBackupPath(path string) (vaultName string, backupName string, err error) {
	parts := strings.Split(path, "/")
	var vaultIdx, backupIdx int
	for i, p := range parts {
		if p == "backupVaults" && i+1 < len(parts) {
			vaultIdx = i + 1
		}
		if p == "backups" && i+1 < len(parts) {
			backupIdx = i + 1
		}
	}
	if vaultIdx == 0 || backupIdx == 0 {
		return "", "", fmt.Errorf("cannot parse backup path: %s", path)
	}
	return parts[vaultIdx], parts[backupIdx], nil
}

// getRestoredVolume looks up the volume by name and account ID from the restore job.
// Uses ListVolumesWithPagination (Unscoped) so deleted volumes are included —
// the restore transfer is billable regardless of whether the volume was later deleted.
func getRestoredVolume(ctx context.Context, vcpDB vcpdb.Storage, job *datamodel.Job) (*datamodel.Volume, error) {
	conditions := [][]interface{}{
		{"name = ? AND account_id = ?", job.ResourceName, job.AccountID.Int64},
	}
	pagination := &dbutils.Pagination{Offset: 0, Limit: 1}

	volumes, err := vcpDB.ListVolumesWithPagination(ctx, conditions, pagination)
	if err != nil {
		return nil, err
	}
	if len(volumes) == 0 {
		return nil, nil
	}
	return volumes[0], nil
}

// getSfrRestoredVolume looks up the volume by UUID for SFR billing.
// Uses ListVolumesWithPagination (Unscoped) so deleted volumes are included.
func getSfrRestoredVolume(ctx context.Context, vcpDB vcpdb.Storage, volumeUUID string) (*datamodel.Volume, error) {
	conditions := [][]interface{}{
		{"uuid = ?", volumeUUID},
	}
	pagination := &dbutils.Pagination{Offset: 0, Limit: 1}

	volumes, err := vcpDB.ListVolumesWithPagination(ctx, conditions, pagination)
	if err != nil {
		return nil, err
	}
	if len(volumes) == 0 {
		return nil, nil
	}
	return volumes[0], nil
}

// assembleMetadata builds ResourceMetadata for a cross-region restore billing metric.
//
// Region mapping:
//   - RegionName = config.RegionName (local VCP region where the volume lives)
//   - BackupRegionName = backup vault's BackupRegionName (region where backup data is stored / transferred FROM)
//   - SourceRegionName = config.RegionName (volume's region, where data is transferred TO)
func assembleMetadata(volume *datamodel.Volume, backup *datamodel.Backup, config *common.TelemetryConfig) metadata.ResourceMetadata {
	met := metadata.ResourceMetadata{}
	met.SetResourceUUID(volume.UUID)
	met.SetResourceType(metadata.Volume)
	met.SetResourceName(volume.Name)
	met.SetResourceDisplayName(volume.Name)
	met.SetRegionName(config.RegionName)
	met.SetSizeInBytes(backup.SizeInBytes)
	met.SetAccountName(getVolumeAccountName(volume))

	if volume.Pool != nil {
		met.SetDeploymentName(volume.Pool.DeploymentName)
	} else if volume.VolumeAttributes != nil && volume.VolumeAttributes.DeploymentName != "" {
		met.SetDeploymentName(volume.VolumeAttributes.DeploymentName)
	} else {
		met.SetDeploymentName(EmptyDeploymentName)
	}

	if backup.BackupVault != nil && backup.BackupVault.BackupRegionName != nil {
		met.SetBackupRegionName(*backup.BackupVault.BackupRegionName)
	}

	met.SetSourceRegionName(config.RegionName)

	return met
}

func getVolumeAccountName(volume *datamodel.Volume) string {
	if volume.Account != nil && volume.Account.Name != "" {
		return volume.Account.Name
	}
	if volume.VolumeAttributes != nil && volume.VolumeAttributes.AccountName != "" {
		return volume.VolumeAttributes.AccountName
	}
	return ""
}

// setRegionMetadataForBilling stores BackupRegionName and SourceRegionName into the
// HydratedMetrics.Metadata JSONB column so the aggregator can set distinct
// source/destination regions on the AggregatedUsage record.
func setRegionMetadataForBilling(hm *datamodel2.HydratedMetrics, rm metadata.ResourceMetadata) {
	if hm == nil || (rm.BackupRegionName == nil && rm.SourceRegionName == nil) {
		return
	}
	extra := make(map[string]string)
	if rm.BackupRegionName != nil {
		extra["backup_region_name"] = *rm.BackupRegionName
	}
	if rm.SourceRegionName != nil {
		extra["source_region_name"] = *rm.SourceRegionName
	}
	if b, err := json.Marshal(extra); err == nil {
		hm.Metadata = b
	}
}
