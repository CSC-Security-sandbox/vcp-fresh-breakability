package collector

import (
	"context"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
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

		// skip billing for cross region backups if the feature flag is disabled
		if !config.EnableCrossRegionBackupBillingMetrics {
			if backup.BackupVault != nil && backup.BackupVault.BackupVaultType == activities.CrossRegionBackupType {
				logger.Debug("Skipping BackupLogicalSize billing metric for cross-region backup", "backupUUID", backup.UUID)
				continue
			}
		}

		// Get account identifier from backup attributes
		accountName := ""
		if backup.Attributes != nil {
			accountName = backup.Attributes.AccountIdentifier
		}
		// Execute only if SAN protocol or files backup billing enabled
		isSANProtocol := utils.IsSanProtocols(backup.Attributes.Protocols)
		if config.EnableFilesBackupBilling || isSANProtocol {
			if hydratedMetric := setupHydratedMetricsDataModel(metric.MeasuredType, metric.Metadata.ResourceType, accountName, backupMetadata, timestamp, float64(backup.LatestLogicalBackupSize)); hydratedMetric != nil {
				hydratedMetrics = append(hydratedMetrics, *hydratedMetric)
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
	met.SetResourceName(backup.Attributes.VolumeName)
	met.SetResourceDisplayName(backup.Attributes.VolumeName)
	met.SetAccountName(backup.Attributes.AccountIdentifier)

	// Check if BackupVault is not nil before accessing its Name
	if backup.BackupVault != nil {
		met.SetDeploymentName(backup.BackupVault.Name)
	} else {
		met.SetDeploymentName(EmptyDeploymentName)
	}
	return met
}
