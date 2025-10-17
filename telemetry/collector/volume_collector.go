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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// VolumeMetricsResult holds the results from GetVolumeMetrics operation
type VolumeMetricsResult struct {
	// HydratedMetrics contains the traditional hydrated metrics
	HydratedMetrics []entity.HydratedMetric
	// HydratedMetricsDataModel contains the data model hydrated metrics
	HydratedMetricsDataModel []datamodel2.HydratedMetrics
	// VolumeAllocatedThroughputHydratedMetrics contains volume allocated throughput metrics
	VolumeAllocatedThroughputHydratedMetrics []entity.HydratedMetric
}

// GetVolumeMetrics retrieves volume allocated size metrics for volumes with backup data from the database and returns them in a structured result.
func GetVolumeMetrics(ctx context.Context, vcpDB database.Storage, config *common.TelemetryConfig, poolMetadataMap map[int64]metadata.ResourceMetadata, timestamp time.Time) (*VolumeMetricsResult, error) {
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
	var volumeAllocatedThroughputMetrics []entity.HydratedMetric
	var hydratedMetrics []datamodel2.HydratedMetrics

	// Iterate over all volumes and generate metrics
	for _, volume := range volumes {
		var volumeAllocatedThroughputMetric entity.HydratedMetric
		// Assemble metadata for the volume
		volumeMetadata := assembleVolumeMetadata(volume, config)

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

		// Handle case for zonal and regional volumes for VolumeAllocatedThroughput metric
		var resourceType metadata.ResourceType
		if poolMeta, ok := poolMetadataMap[volume.PoolID]; ok {
			if poolMeta.ResourceType == metadata.VolumePoolRegionalHA {
				resourceType = metadata.VolumeRegionalHA
			} else {
				resourceType = metadata.Volume
			}
		}

		if volume.Throughput != 0 {
			volumeAllocatedThroughputMetric = setupHydratedMetric(timestamp, volumeMetadata, metadata.VolumeAllocatedThroughput, float64(volume.Throughput))
			volumeAllocatedThroughputMetric.Metadata.ResourceType = resourceType
			volumeAllocatedThroughputMetrics = append(volumeAllocatedThroughputMetrics, volumeAllocatedThroughputMetric)
		} else {
			var poolThroughput *float64
			// Lookup pool metadata using PoolID
			if poolMeta, ok := poolMetadataMap[volume.PoolID]; ok {
				poolThroughput = poolMeta.Throughput
				if poolThroughput == nil {
					poolThroughput = nillable.ToPointer(0.0)
				}

				volumeAllocatedThroughputMetric = setupHydratedMetric(timestamp, volumeMetadata, metadata.VolumeAllocatedThroughput, *poolThroughput)
				volumeAllocatedThroughputMetric.Metadata.ResourceType = resourceType
				volumeAllocatedThroughputMetrics = append(volumeAllocatedThroughputMetrics, volumeAllocatedThroughputMetric)
			} else {
				logger.Warnf("Pool metadata missing for PoolID %d (volume %s)", volume.PoolID, volume.UUID)
			}
		}

		// Filter volumes that have backup logical size > 0
		if volume.DataProtection != nil && volume.DataProtection.BackupChainBytes != nil && *volume.DataProtection.BackupChainBytes <= 0 {
			continue
		}
		// Create a metric for the volume allocated size
		// Use the allocated size (size_in_bytes) as the quantity for volume allocated size
		allocatedSize := volume.SizeInBytes

		if config.EnableBackupBillingMetrics {
			metric := setupHydratedMetric(timestamp, volumeMetadata, metadata.BackupEnabledVolumeAllocatedSize, float64(allocatedSize))
			metrics = append(metrics, metric)

			// Use actual account name from the preloaded account
			accountName := ""
			if volume.Account != nil {
				accountName = volume.Account.Name
			}
			metric.Metadata.ResourceType = resourceType
			hydratedMetrics = append(hydratedMetrics, setupHydratedMetricsDataModel(metric.MeasuredType, metric.Metadata.ResourceType, accountName, volumeMetadata, timestamp, float64(allocatedSize)))
		}
	}

	// Return the structured result
	return &VolumeMetricsResult{
		HydratedMetrics:                          metrics,
		HydratedMetricsDataModel:                 hydratedMetrics,
		VolumeAllocatedThroughputHydratedMetrics: volumeAllocatedThroughputMetrics,
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
	if volume.Account != nil {
		met.SetAccountName(volume.Account.Name)
	}
	if volume.Pool != nil {
		met.SetDeploymentName(volume.Pool.DeploymentName)
	}
	return met
}
