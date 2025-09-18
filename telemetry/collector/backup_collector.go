package collector

import (
	"context"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// BackupMetricsResult holds the results from GetBackupMetrics operation
type BackupMetricsResult struct {
	// HydratedMetrics contains the traditional hydrated metrics
	HydratedMetrics []entity.HydratedMetric
	// HydratedMetricsDataModel contains the data model hydrated metrics
	HydratedMetricsDataModel []datamodel2.HydratedMetrics
}

// GetBackupMetrics retrieves backup logical size metrics from the database and returns them in a structured result.
func GetBackupMetrics(ctx context.Context, vcpDB database.Storage, config *common.TelemetryConfig) (*BackupMetricsResult, error) {
	logger := util.GetLogger(ctx)
	backups, err := vcpDB.GetBackupLogicalSizeMetrics(ctx)
	if err != nil {
		logger.Error("Failed to get backup logical size metrics", "error", err.Error())
		return &BackupMetricsResult{}, err
	}
	logger.Info(fmt.Sprintf("Found %d backup metrics", len(backups)))

	if len(backups) == 0 {
		return &BackupMetricsResult{}, nil
	}

	// Initialize a slice to hold the hydrated metrics
	var metrics []entity.HydratedMetric
	var hydratedMetrics []datamodel2.HydratedMetrics

	// Iterate over all backups and generate metrics
	for _, backup := range backups {
		// Assemble metadata for the backup (billing on volume)
		if backup.Attributes == nil {
			logger.Error(fmt.Sprintf("Backup attributes is missing found for volume %s", backup.VolumeUUID))
			continue
		}
		backupMetadata := assembleBackupMetadata(backup, config)

		// Create a metric for the backup logical size
		now := time.Now()
		metric := setupHydratedMetric(now, backupMetadata, metadata.BackupLogicalSize, float64(backup.LatestLogicalBackupSize))
		metrics = append(metrics, metric)
		// Get account identifier from backup attributes
		accountName := ""
		if backup.Attributes != nil {
			accountName = backup.Attributes.AccountIdentifier
		}
		hydratedMetrics = append(hydratedMetrics, setupHydratedMetricsDataModel(metric.MeasuredType, metric.Metadata.ResourceType, accountName, backupMetadata, now, float64(backup.LatestLogicalBackupSize)))
	}

	// Return the structured result
	return &BackupMetricsResult{
		HydratedMetrics:          metrics,
		HydratedMetricsDataModel: hydratedMetrics,
	}, nil
}

func assembleBackupMetadata(backup *datamodel.Backup, config *common.TelemetryConfig) metadata.ResourceMetadata {
	met := metadata.ResourceMetadata{}
	// Billing on volume, so use volume UUID as resource UUID
	met.SetResourceUUID(backup.VolumeUUID)
	met.SetResourceType(metadata.Volume) // Billing on volume
	met.SetSizeInBytes(backup.LatestLogicalBackupSize)
	met.SetRegionName(config.RegionName)
	met.SetResourceName(backup.Attributes.VolumeName)
	met.SetResourceDisplayName(backup.Attributes.VolumeName)
	met.SetAccountName(backup.Attributes.AccountIdentifier)
	return met
}
