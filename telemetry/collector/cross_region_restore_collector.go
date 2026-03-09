package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"strings"
	"time"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	orchcommon "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	metricsdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	vcpdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	common "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

var (
	getGProxyClient       = googleproxyclient.GetGProxyClient
	getRemoteRegionConfig = orchcommon.GetRemoteRegionConfig
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

	backup, err := FetchSourceBackupForRestoredVolumeUsingIDOrBackupPath(ctx, vcpDB, volume, job, config)
	if err != nil || backup == nil {
		if err != nil {
			logger.Warn("Failed to fetch source backup for restore job",
				"jobUUID", job.UUID, "volumeUUID", volume.UUID, "error", err)
		} else {
			logger.Debug("No source backup found for restore job",
				"jobUUID", job.UUID, "volumeUUID", volume.UUID)
		}
		return nil
	}

	if !isCrossRegionRestore(backup, config) {
		logger.Debug("Skipping non-cross-region restore", "jobUUID", job.UUID, "backupUUID", backup.UUID)
		return nil
	}

	if volume.VolumeAttributes == nil || volume.VolumeAttributes.Protocols == nil {
		logger.Debug("Skipping CRB restore billing metric with missing volume protocols", "jobUUID", job.UUID, "volumeUUID", volume.UUID)
		return nil
	}

	isSANProtocol := utils.IsSanProtocols(volume.VolumeAttributes.Protocols)
	if !isSANProtocol && !config.EnableFilesBackupBilling {
		logger.Debug("Skipping CRB restore billing metric as file billing is disabled", "jobUUID", job.UUID, "volumeUUID", volume.UUID)
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

	// For SFR, job.ResourceName is the volume UUID
	volume, err := getSfrRestoredVolume(ctx, vcpDB, sfrMetadata.VolumeUUID)
	if err != nil || volume == nil {
		logger.Warn("Volume not found for SFR restore", "jobUUID", job.UUID, "volumeUUID", sfrMetadata.VolumeUUID, "error", err)
		return nil
	}

	backup, err := vcpDB.GetBackupWithVaultByUUID(ctx, sfrMetadata.BackupUUID)
	if err != nil || backup == nil {
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			logger.Warn("Backup lookup failed for SFR restore", "jobUUID", job.UUID, "backupUUID", sfrMetadata.BackupUUID, "error", err)
			return nil
		}
		backup, err = FetchSourceBackupForRestoredVolumeUsingIDOrBackupPath(ctx, vcpDB, volume, job, config)
		if err != nil || backup == nil {
			if err != nil {
				logger.Warn("Failed to fetch source backup for SFR restore",
					"jobUUID", job.UUID, "volumeUUID", volume.UUID, "error", err)
			} else {
				logger.Debug("No source backup found for SFR restore",
					"jobUUID", job.UUID, "volumeUUID", volume.UUID)
			}
			return nil
		}
	}

	if !isCrossRegionRestore(backup, config) {
		logger.Debug("Skipping non-cross-region SFR restore", "jobUUID", job.UUID, "backupUUID", backup.UUID)
		return nil
	}

	if volume.VolumeAttributes == nil || volume.VolumeAttributes.Protocols == nil {
		logger.Debug("Skipping SFR restore billing metric with missing volume protocols", "jobUUID", job.UUID, "volumeUUID", volume.UUID)
		return nil
	}

	isSANProtocol := utils.IsSanProtocols(volume.VolumeAttributes.Protocols)
	if !isSANProtocol && !config.EnableFilesBackupBilling {
		logger.Debug("Skipping SFR restore billing metric as file billing is disabled", "jobUUID", job.UUID, "volumeUUID", volume.UUID)
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

// isCrossRegionRestore checks whether a backup is CROSS_REGION type
func isCrossRegionRestore(backup *datamodel.Backup, config *common.TelemetryConfig) bool {
	if backup.BackupVault == nil {
		return false
	}
	if backup.BackupVault.BackupVaultType != activities.CrossRegionBackupType {
		return false
	}
	if backup.BackupVault.BackupRegionName == nil {
		return false
	}
	return *backup.BackupVault.BackupRegionName != config.RegionName
}

// FetchSourceBackupForRestoredVolumeUsingIDOrBackupPath finds the backup associated with a restored volume.
func FetchSourceBackupForRestoredVolumeUsingIDOrBackupPath(ctx context.Context, vcpDB vcpdb.Storage, volume *datamodel.Volume, job *datamodel.Job, config *common.TelemetryConfig) (*datamodel.Backup, error) {
	logger := util.GetLogger(ctx)

	if volume.VolumeAttributes == nil {
		logger.Debug("Volume has no attributes, cannot find source backup for volumeUUID", "volumeUUID", volume.UUID)
		return nil, nil
	}

	if volume.VolumeAttributes.RestoredBackupID != "" {
		backup, err := vcpDB.GetBackupWithVaultByUUID(ctx, volume.VolumeAttributes.RestoredBackupID)
		if err == nil && backup != nil {
			return backup, nil
		}
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) && !strings.Contains(err.Error(), "not found") {
			logger.Warn("Failed to find backup by RestoredBackupID",
				"jobUUID", job.UUID, "volumeUUID", volume.UUID,
				"restoredBackupID", volume.VolumeAttributes.RestoredBackupID, "error", err)
			return nil, err
		}
		logger.Debug("Backup not found by RestoredBackupID, falling back to RestoredBackupPath",
			"jobUUID", job.UUID, "volumeUUID", volume.UUID,
			"restoredBackupID", volume.VolumeAttributes.RestoredBackupID)
	}

	if volume.VolumeAttributes.RestoredBackupPath != "" {
		logger.Debug("Finding backup via RestoredBackupPath",
			"jobUUID", job.UUID, "volumeUUID", volume.UUID,
			"restoredBackupPath", volume.VolumeAttributes.RestoredBackupPath)
		return FetchSourceBackupByResourcePath(ctx, volume, job, config)
	}

	logger.Debug("No RestoredBackupID or RestoredBackupPath on volume, skipping",
		"jobUUID", job.UUID, "volumeUUID", volume.UUID)
	return nil, nil
}

// FetchSourceBackupByResourcePath parses backupPath from RestoredBackupPath and looks up the backup and vault by name.
// Expected path format: projects/{project}/locations/{location}/backupVaults/{vault}/backups/{backup}
func FetchSourceBackupByResourcePath(ctx context.Context, volume *datamodel.Volume, job *datamodel.Job, config *common.TelemetryConfig) (*datamodel.Backup, error) {
	logger := util.GetLogger(ctx)
	path := volume.VolumeAttributes.RestoredBackupPath

	vaultName, vaultResourcePath, backupName, err := parseRestoredBackupPath(path)
	if err != nil {
		logger.Warn("Failed to parse RestoredBackupPath", "jobUUID", job.UUID, "path", path, "error", err)
		return nil, err
	}

	projectNumber := volume.VolumeAttributes.AccountName
	if projectNumber == "" {
		projectNumber = fmt.Sprintf("%d", job.AccountID.Int64)
	}

	backup, err := getBackupFromBackupVaultResourcePath(ctx, config, vaultName, vaultResourcePath, backupName, projectNumber, logger)
	if err != nil {
		logger.Warn("Backup fetch failed",
			"jobUUID", job.UUID, "vaultName", vaultName, "backupName", backupName, "error", err)
	}
	return backup, err
}

// getBackupFromBackupVaultResourcePath calls the local google-proxy to resolve a backup vault and backup.
// The google-proxy V1betaListBackupVaults endpoint returns merged results from both VCP DB and
// CVP/SDE, so this single call covers all vault sources. The vault is matched by its resource name
// or its destination backup vault name/path for cross-region vault references.
func getBackupFromBackupVaultResourcePath(ctx context.Context, config *common.TelemetryConfig, vaultName, vaultResourcePath, backupName, projectNumber string, logger log.Logger) (*datamodel.Backup, error) {
	basePath, jwtToken, err := getRemoteRegionConfig(config.RegionName, projectNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to get region config for google-proxy: %w", err)
	}

	proxyClient := getGProxyClient(basePath, jwtToken, logger)

	vaultsRes, err := proxyClient.Invoker.V1betaListBackupVaults(ctx, googleproxyclient.V1betaListBackupVaultsParams{
		ProjectNumber: projectNumber,
		LocationId:    config.RegionName,
	})
	if err != nil {
		return nil, fmt.Errorf("google-proxy ListBackupVaults failed: %w", err)
	}

	var backupVaults []googleproxyclient.BackupVaultV1beta
	switch r := vaultsRes.(type) {
	case *googleproxyclient.V1betaListBackupVaultsOK:
		backupVaults = r.BackupVaults
	default:
		return nil, fmt.Errorf("google-proxy ListBackupVaults unexpected response: %T", vaultsRes)
	}

	bv, err := findVaultByNameOrCrossRegionVaultName(backupVaults, vaultName, vaultResourcePath, config.RegionName)
	if err != nil {
		logger.Warn("Failed to find backup vault", "vaultName", vaultName, "backupName", backupName, "error", err)
		return nil, err
	}

	// List backups in the backup vault, filtered by backup name
	backupsRes, err := proxyClient.Invoker.V1betaListBackups(ctx, googleproxyclient.V1betaListBackupsParams{
		ProjectNumber: projectNumber,
		LocationId:    config.RegionName,
		BackupVaultId: bv.UUID,
		BackupName:    googleproxyclient.NewOptString(backupName),
	})
	if err != nil {
		return nil, fmt.Errorf("google-proxy ListBackups failed for vault %s: %w", bv.UUID, err)
	}

	var backupsList []googleproxyclient.BackupV1beta
	switch r := backupsRes.(type) {
	case *googleproxyclient.V1betaListBackupsOK:
		backupsList = r.Backups
	default:
		return nil, fmt.Errorf("google-proxy ListBackups unexpected response: %T", backupsRes)
	}

	for _, b := range backupsList {
		if b.ResourceId.Value == backupName {
			return convertGProxyBackupToDataModel(b, bv), nil
		}
	}

	return nil, fmt.Errorf("backup %q not found in vault %s", backupName, bv.UUID)
}

// findVaultByNameOrCrossRegionVaultName searches backup vaults for one matching by resource name
// or by destination backup vault name/path. The cross_region_backup_vault_name field stores
// the full resource path, so we match against both the short vault name and the full path.
func findVaultByNameOrCrossRegionVaultName(vaults []googleproxyclient.BackupVaultV1beta, vaultName, vaultResourcePath, region string) (*datamodel.BackupVault, error) {
	for _, bv := range vaults {
		if matchesVaultName(bv, vaultName, vaultResourcePath) {
			return convertGProxyVaultToDataModel(bv, region), nil
		}
	}
	return nil, fmt.Errorf("backup vault %q not found via google-proxy", vaultName)
}

func matchesVaultName(bv googleproxyclient.BackupVaultV1beta, vaultName, vaultResourcePath string) bool {
	if bv.ResourceId == vaultName {
		return true
	}
	if bv.DestinationBackupVault.IsSet() {
		dest := bv.DestinationBackupVault.Value
		if dest == vaultName || (vaultResourcePath != "" && dest == vaultResourcePath) {
			return true
		}
	}
	return false
}

// convertGProxyBackupToDataModel converts a google-proxy BackupV1beta to data model.Backup.
func convertGProxyBackupToDataModel(b googleproxyclient.BackupV1beta, vault *datamodel.BackupVault) *datamodel.Backup {
	var sizeInBytes int64
	if b.VolumeUsageBytes.IsSet() {
		sizeInBytes = b.VolumeUsageBytes.Value
	} else if b.BackupChainBytes.IsSet() {
		sizeInBytes = b.BackupChainBytes.Value
	}

	var latestLogicalBackupSize int64
	if b.BackupChainBytes.IsSet() {
		latestLogicalBackupSize = b.BackupChainBytes.Value
	}

	var createdAt time.Time
	if b.Created.IsSet() {
		createdAt = b.Created.Value
	}

	var attributes *datamodel.BackupAttributes
	if b.BucketName.IsSet() || b.SnapshotName.IsSet() {
		attributes = &datamodel.BackupAttributes{
			BucketName:   b.BucketName.Value,
			SnapshotName: b.SnapshotName.Value,
		}
		if b.SourceVolume.IsSet() {
			attributes.VolumeName = b.SourceVolume.Value
		}
	}

	return &datamodel.Backup{
		BaseModel: datamodel.BaseModel{
			UUID:      b.BackupId.Value,
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
		},
		Name:                    b.ResourceId.Value,
		Description:             b.Description.Value,
		State:                   string(b.State.Value),
		Type:                    string(b.BackupType.Value),
		VolumeUUID:              b.VolumeId.Value,
		SizeInBytes:             sizeInBytes,
		LatestLogicalBackupSize: latestLogicalBackupSize,
		Attributes:              attributes,
		BackupVault:             vault,
		BackupVaultID:           vault.ID,
	}
}

// convertGProxyVaultToDataModel converts a google-proxy BackupVaultV1beta to datamodel.BackupVault.
func convertGProxyVaultToDataModel(bv googleproxyclient.BackupVaultV1beta, locationID string) *datamodel.BackupVault {
	var backupRegion, sourceRegion, crossRegionBackupVaultName *string
	var backupVaultType string

	if bv.BackupRegion.IsSet() {
		backupRegion = nillable.ToPointer(bv.BackupRegion.Value)
	}
	if bv.BackupVaultType.IsSet() {
		backupVaultType = string(bv.BackupVaultType.Value)
	}

	// SourceRegion is nil in SDE for IN_REGION vaults; set only for CROSS_REGION vaults.
	if bv.SourceRegion.IsSet() {
		sourceRegion = nillable.ToPointer(bv.SourceRegion.Value)
	} else {
		sourceRegion = nillable.ToPointer(locationID)
	}

	if bv.DestinationBackupVault.IsSet() {
		crossRegionBackupVaultName = nillable.ToPointer(bv.DestinationBackupVault.Value)
	}

	var createdAt time.Time
	if bv.CreatedAt.IsSet() {
		createdAt = bv.CreatedAt.Value
	}

	vault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{
			UUID:      bv.BackupVaultId.Value,
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
		},
		Name:                       bv.ResourceId,
		BackupRegionName:           backupRegion,
		SourceRegionName:           sourceRegion,
		LifeCycleState:             string(bv.State.Value),
		LifeCycleStateDetails:      bv.StateDetails.Value,
		BackupVaultType:            backupVaultType,
		CrossRegionBackupVaultName: crossRegionBackupVaultName,
		ServiceType:                models.ServiceTypeGCNV,
	}

	return vault
}

// parseRestoredBackupPath extracts vault name, vault resource path, and backup name from a resource path.
// Expected format: projects/{project}/locations/{location}/backupVaults/{vault}/backups/{backup}
// vaultResourcePath is the full path up to the vault: projects/{project}/locations/{location}/backupVaults/{vault}
func parseRestoredBackupPath(path string) (vaultName, vaultResourcePath, backupName string, err error) {
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
		return "", "", "", fmt.Errorf("cannot parse backup path: %s", path)
	}
	return parts[vaultIdx], strings.Join(parts[:vaultIdx+1], "/"), parts[backupIdx], nil
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
