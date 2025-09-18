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

// VolumeMetricsResult holds the results from GetVolumeMetrics operation
type VolumeMetricsResult struct {
	// HydratedMetrics contains the traditional hydrated metrics
	HydratedMetrics []entity.HydratedMetric
	// HydratedMetricsDataModel contains the data model hydrated metrics
	HydratedMetricsDataModel []datamodel2.HydratedMetrics
}

// GetVolumeMetrics retrieves volume allocated size metrics for volumes with backup data from the database and returns them in a structured result.
func GetVolumeMetrics(ctx context.Context, vcpDB database.Storage, config *common.TelemetryConfig) (*VolumeMetricsResult, error) {
	logger := util.GetLogger(ctx)
	volumes, err := vcpDB.ListVolumesWithAccounts(ctx)
	if err != nil {
		logger.Error("Failed to get volume metrics", "error", err.Error())
		return &VolumeMetricsResult{}, err
	}
	logger.Info(fmt.Sprintf("Found %d volume metrics", len(volumes)))

	if len(volumes) == 0 {
		return &VolumeMetricsResult{}, nil
	}

	// Initialize a slice to hold the hydrated metrics
	var metrics []entity.HydratedMetric
	var hydratedMetrics []datamodel2.HydratedMetrics

	// Iterate over all volumes and generate metrics
	for _, volume := range volumes {
		// Filter volumes that have backup logical size > 0
		if volume.DataProtection != nil && volume.DataProtection.BackupChainBytes != nil && *volume.DataProtection.BackupChainBytes <= 0 {
			continue
		}

		// Validate volume attributes before processing
		if volume.UUID == "" {
			logger.Error(fmt.Sprintf("Volume UUID is missing for volume %s", volume.Name))
			continue
		}
		if volume.Name == "" {
			logger.Error(fmt.Sprintf("Volume name is missing for volume %s", volume.UUID))
			continue
		}
		if volume.Account == nil {
			logger.Error(fmt.Sprintf("Volume account is missing for volume %s", volume.UUID))
			continue
		}
		if volume.Account.Name == "" {
			logger.Error(fmt.Sprintf("Volume account name is missing for volume %s", volume.UUID))
			continue
		}

		// Assemble metadata for the volume
		volumeMetadata := assembleVolumeMetadata(volume, config)

		// Create a metric for the volume allocated size
		now := time.Now()
		// Use the allocated size (size_in_bytes) as the quantity for volume allocated size
		allocatedSize := volume.SizeInBytes

		metric := setupHydratedMetric(now, volumeMetadata, metadata.BackupVolumeAllocatedSize, float64(allocatedSize))
		metrics = append(metrics, metric)
		// Use actual account name from the preloaded account
		accountName := ""
		if volume.Account != nil {
			accountName = volume.Account.Name
		}
		hydratedMetrics = append(hydratedMetrics, setupHydratedMetricsDataModel(metric.MeasuredType, metric.Metadata.ResourceType, accountName, volumeMetadata, now, float64(allocatedSize)))
	}

	// Return the structured result
	return &VolumeMetricsResult{
		HydratedMetrics:          metrics,
		HydratedMetricsDataModel: hydratedMetrics,
	}, nil
}

func assembleVolumeMetadata(volume *datamodel.Volume, config *common.TelemetryConfig) metadata.ResourceMetadata {
	met := metadata.ResourceMetadata{}
	met.SetResourceUUID(volume.UUID)
	met.SetResourceName(volume.Name)
	met.SetResourceDisplayName(volume.Name)
	met.SetResourceType(metadata.Volume)
	// Use the allocated size (size_in_bytes) for billing
	met.SetSizeInBytes(volume.SizeInBytes)
	met.SetRegionName(config.RegionName)
	met.SetAccountName(volume.Account.Name)
	return met
}
