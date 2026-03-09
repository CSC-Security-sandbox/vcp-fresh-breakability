package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	pageSize = env.GetInt64("PAGE_SIZE", 1000)
)

const EmptyDeploymentName = ""

// BackupMetricsResult holds the results from GetBackupMetrics operation
type BackupMetricsResult struct {
	// HydratedMetrics contains the traditional hydrated metrics
	HydratedMetrics []entity.HydratedMetric
	// HydratedMetricsDataModel contains the data model hydrated metrics
	HydratedMetricsDataModel []datamodel2.HydratedMetrics
}

// GetBackupMetrics retrieves backup logical size metrics from the database and returns them in a structured result.
func GetBackupMetrics(ctx context.Context, vcpDB database.Storage, config *common.TelemetryConfig, timestamp time.Time) (*BackupMetricsResult, error) {
	logger := util.GetLogger(ctx)

	// Fetch all backup metrics using pagination
	var allBackups []*datamodel.Backup
	offset := int64(0)
	limit := pageSize // Use a reasonable page size for collector

	// Create conditions for backup metrics (available state is already handled in the query)
	conditions := [][]interface{}{}

	for {
		// Create pagination with offset and limit
		pagination := &dbutils.Pagination{
			Offset: int(offset),
			Limit:  int(limit),
		}

		// Fetch paginated backup metrics
		backups, err := vcpDB.GetBackupMetrics(ctx, conditions, pagination)
		if err != nil {
			logger.Error("Failed to get backup logical size metrics", "error", err.Error())
			return &BackupMetricsResult{}, err
		}

		// Break if no records returned
		if len(backups) == 0 {
			break
		}

		// Append to all backups
		allBackups = append(allBackups, backups...)

		// Update offset for next iteration
		offset += limit
	}

	logger.Info(fmt.Sprintf("Found %d backup metrics", len(allBackups)))

	if len(allBackups) == 0 {
		return &BackupMetricsResult{}, nil
	}

	// Initialize a slice to hold the hydrated metrics
	var metrics []entity.HydratedMetric
	var hydratedMetrics []datamodel2.HydratedMetrics

	// Iterate over all backups and generate metrics
	for _, backup := range allBackups {
		// Assemble metadata for the backup (billing on volume)
		if backup.Attributes == nil {
			logger.Error(fmt.Sprintf("Backup attributes is missing found for volume %s", backup.VolumeUUID))
			continue
		}
		backupMetadata := assembleBackupMetadata(backup, config)

		// Create a metric for the backup logical size
		metric := setupHydratedMetric(timestamp, backupMetadata, metadata.BackupLogicalSize, float64(backup.LatestLogicalBackupSize))
		metrics = append(metrics, metric)

		// Determine if we should skip billing for this backup.
		skipBilling := false

		// Skip billing for cross region backups if the feature flag is disabled.
		if !config.EnableCrossRegionBackupBillingMetrics {
			if backup.BackupVault != nil && backup.BackupVault.BackupVaultType == activities.CrossRegionBackupType {
				logger.Debug("Skipping BackupLogicalSize billing metric for cross-region backup", "backupUUID", backup.UUID)
				skipBilling = true
			}
		}

		// When cross-region billing is enabled, skip BackupLogicalSize for cross-region
		// backups where the backup region is nil or matches the current region.
		if !skipBilling && config.EnableCrossRegionBackupBillingMetrics &&
			backup.BackupVault != nil &&
			backup.BackupVault.BackupVaultType == activities.CrossRegionBackupType {
			if backup.BackupVault.BackupRegionName == nil {
				logger.Warnf("Skipping BackupLogicalSize billing for cross-region backup %s (volume %s): BackupRegionName is nil", backup.UUID, backup.VolumeUUID)
				skipBilling = true
			} else if *backup.BackupVault.BackupRegionName == config.RegionName {
				logger.Warnf("Skipping BackupLogicalSize billing for cross-region backup %s (volume %s): BackupRegionName %s matches current region", backup.UUID, backup.VolumeUUID, *backup.BackupVault.BackupRegionName)
				skipBilling = true
			}
		}

		// Skip billing for backups in CMEK backup vaults when CMEK backup billing is disabled.
		if !skipBilling && !config.EnableCmekBackupBilling {
			if backup.BackupVault != nil &&
				backup.BackupVault.CmekAttributes != nil &&
				backup.BackupVault.CmekAttributes.KmsConfigResourcePath != nil &&
				*backup.BackupVault.CmekAttributes.KmsConfigResourcePath != "" {
				logger.Debug("Skipping BackupLogicalSize billing metric for CMEK backup", "backupUUID", backup.UUID, "backupVaultID", backup.BackupVault.UUID)
				skipBilling = true
			}
		}

		// Skip billing for backups in GCBDR backup vaults when GCBDR backup billing is disabled.
		if !skipBilling && !config.EnableGcbdrBackupBilling {
			if backup.BackupVault != nil && backup.BackupVault.ServiceType == models.ServiceTypeGCBDR {
				logger.Debug("Skipping BackupLogicalSize billing metric for GCBDR backup", "backupUUID", backup.UUID, "backupVaultID", backup.BackupVault.UUID)
				skipBilling = true
			}
		}

		// Get account identifier from backup attributes
		accountName := ""
		if backup.Attributes != nil {
			accountName = backup.Attributes.AccountIdentifier
		}
		// Execute only if SAN protocol or files backup billing enabled and billing is not skipped.
		isSANProtocol := utils.IsSanProtocols(backup.Attributes.Protocols)
		if !skipBilling && (config.EnableFilesBackupBilling || isSANProtocol) {
			if hydratedMetric := setupHydratedMetricsDataModel(metric.MeasuredType, metric.Metadata.ResourceType, accountName, backupMetadata, timestamp, float64(backup.LatestLogicalBackupSize)); hydratedMetric != nil {
				hydratedMetrics = append(hydratedMetrics, *hydratedMetric)
			}
			// cross region backup network transfer billing metric
			if config.EnableCrossRegionBackupBillingMetrics &&
				backup.BackupVault != nil &&
				backup.BackupVault.BackupVaultType == activities.CrossRegionBackupType &&
				backup.Attributes.GetTotalTransferBytes() > 0 {
				totalTransferBytes := float64(backup.Attributes.GetTotalTransferBytes())
				if hm := setupHydratedMetricsDataModel(
					metadata.CbsCrossRegionVolumeBackupTransferBytes,
					metadata.Backup,
					accountName,
					backupMetadata,
					timestamp,
					totalTransferBytes,
				); hm != nil {
					setCrossRegionRegionMetadata(logger, hm, backupMetadata)
					hydratedMetrics = append(hydratedMetrics, *hm)
				}
			}
		}
	}

	// Return the structured result
	return &BackupMetricsResult{
		HydratedMetrics:          metrics,
		HydratedMetricsDataModel: hydratedMetrics,
	}, nil
}

func assembleBackupMetadata(backup *datamodel.Backup, config *common.TelemetryConfig) metadata.ResourceMetadata {
	met := metadata.ResourceMetadata{}
	met.SetResourceUUID(backup.VolumeUUID)
	met.SetResourceType(metadata.Backup)
	met.SetSizeInBytes(backup.LatestLogicalBackupSize)
	met.SetRegionName(config.RegionName)
	met.SetResourceName(backup.VolumeUUID)
	met.SetResourceDisplayName(backup.Attributes.VolumeName)
	met.SetAccountName(backup.Attributes.AccountIdentifier)

	// Check if BackupVault is not nil before accessing its Name
	if backup.BackupVault != nil {
		met.SetDeploymentName(backup.BackupVault.Name)
		if backup.BackupVault.BackupRegionName != nil && *backup.BackupVault.BackupRegionName != "" &&
			backup.BackupVault.BackupVaultType == activities.CrossRegionBackupType {
			met.SetBackupRegionName(*backup.BackupVault.BackupRegionName)
		}
	} else {
		met.SetDeploymentName(EmptyDeploymentName)
	}
	return met
}

// setCrossRegionRegionMetadata stores BackupRegionName into the
// HydratedMetrics.Metadata JSONB column so the aggregator can set the
// destination region on the AggregatedUsage record.
func setCrossRegionRegionMetadata(logger log.Logger, hm *datamodel2.HydratedMetrics, rm metadata.ResourceMetadata) {
	if hm == nil || (rm.BackupRegionName == nil) {
		return
	}
	extra := make(map[string]string)
	if rm.BackupRegionName != nil {
		extra["backup_region_name"] = *rm.BackupRegionName
	}
	b, err := json.Marshal(extra)
	if err != nil {
		logger.Warnf("Failed to marshal cross-region metadata: %v", err)
		return
	}
	hm.Metadata = b
}
