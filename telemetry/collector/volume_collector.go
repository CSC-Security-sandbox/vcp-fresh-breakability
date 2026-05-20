package collector

import (
	"context"
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
	// SFRHydratedMetrics contains sfr metrics
	SFRHydratedMetrics []entity.HydratedMetric
}

// GetVolumeMetrics retrieves volume allocated size metrics for volumes with backup data from the database and returns them in a structured result.
func GetVolumeMetrics(ctx context.Context, vcpDB database.Storage, config *common.TelemetryConfig, poolMetadataMap map[int64]metadata.ResourceMetadata, timestamp time.Time) (*VolumeMetricsResult, error) {
	logger := util.GetLogger(ctx)
	// Use optimized query that fetches only required fields without JOINs
	volumes, err := vcpDB.ListVolumesForTelemetryMetrics(ctx)
	if err != nil {
		logger.Error("Failed to get volume metrics", "error", err.Error())
		return &VolumeMetricsResult{}, err
	}
	logger.Info(fmt.Sprintf("Found %d volume metrics", len(volumes)))

	// Early return only when there are no regular volumes AND expert mode backup billing
	// is not enabled (expert mode volumes are processed separately below).
	if len(volumes) == 0 && !(config.EnableBackupBillingMetrics && config.EnableExpertModeBackupBilling) {
		return &VolumeMetricsResult{}, nil
	}

	// Fetch all accounts and create a map of account name -> account state for efficient lookup
	accountStateMap := buildAccountStateMap(ctx, vcpDB)

	// Initialize a slice to hold the hydrated metrics
	var metrics []entity.HydratedMetric
	var volumeAllocatedThroughputMetrics []entity.HydratedMetric
	var sfrMetrics []entity.HydratedMetric
	var hydratedMetrics []datamodel2.HydratedMetrics

	// Pre-fetch backup vaults only when needed for billing filters (CRB, CMEK, or GCBDR).
	backupVaultMap := make(map[string]*datamodel.BackupVault)
	if config.EnableBackupBillingMetrics && (!config.EnableCrossRegionBackupBillingMetrics || !config.EnableCmekBackupBilling || !config.EnableGcbdrBackupBilling) {
		backupVaults, err := vcpDB.GetMultipleBackupVaults(ctx, nil)
		if err != nil {
			logger.Error("Failed to fetch backup vaults for billing filters", "error", err.Error())
		} else {
			for _, bv := range backupVaults {
				backupVaultMap[bv.UUID] = bv
			}
			logger.Info(fmt.Sprintf("Fetched %d backup vaults for billing filters", len(backupVaults)))
		}
	}

	var sfrMetricsMap map[string]datamodel.SfrMetricsAggregate
	if config.SFRMetricsEnabled {
		startTime := timestamp.Add(-5 * time.Minute)
		endTime := timestamp
		logger.Infof("fetching sfr metrics from start time %v and end time %v", startTime, endTime)
		var err error
		sfrMetricsMap, err = vcpDB.GetSfrMetricsByTimeRange(ctx, startTime, endTime)
		if err != nil {
			logger.Error("Failed to get SFR metrics", "error", err.Error())
			// Continue processing even if SFR metrics fetch fails
			sfrMetricsMap = make(map[string]datamodel.SfrMetricsAggregate)
		}
		logger.Info(fmt.Sprintf("Found %d SFR metrics", len(sfrMetricsMap)))
	}

	// Iterate over all volumes and generate metrics
	for _, volume := range volumes {
		var volumeAllocatedThroughputMetric entity.HydratedMetric
		// Assemble metadata for the volume using optimized struct
		volumeMetadata := assembleVolumeMetadata(volume, config)

		// Get account name from VolumeAttributes
		accountName := volume.GetAccountName()

		// Validate volume attributes before processing
		if volume.UUID == "" {
			logger.Error(fmt.Sprintf("Volume UUID is missing for volume %s", volume.Name))
			continue
		}
		if volume.Name == "" {
			logger.Error(fmt.Sprintf("Volume name is missing for volume %s", volume.UUID))
			continue
		}
		if accountName == "" {
			logger.Error(fmt.Sprintf("Volume account name is missing for volume %s", volume.UUID))
			continue
		}
		// Skip metrics collection if account state is HYPERSCALERDISABLED
		if accountState, exists := accountStateMap[accountName]; exists && accountState == models.AccountStateHyperscalerDisabled {
			logger.Debugf("Skipping volume %s (UUID: %s) metrics collection as account %s is in HYPERSCALERDISABLED state", volume.Name, volume.UUID, accountName)
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

		// Create a metric for the volume allocated size
		// Use the allocated size (size_in_bytes) as the quantity for volume allocated size
		allocatedSize := volume.SizeInBytes
		if config.EnableBackupBillingMetrics {
			// Include if files backup billing is enabled or if volume is SAN protocol
			isSANProtocol := volume.VolumeAttributes != nil && utils.IsSanProtocols(volume.VolumeAttributes.Protocols)
			if config.EnableFilesBackupBilling || isSANProtocol {
				if volume.DataProtection != nil && volume.DataProtection.BackupChainBytes != nil && *volume.DataProtection.BackupChainBytes > 0 {
					metric := setupHydratedMetric(timestamp, volumeMetadata, metadata.BackupEnabledVolumeAllocatedSize, float64(allocatedSize))
					metric.Metadata.ResourceType = resourceType

					// Skip BMF billing metrics for volume with cross-region backupVaults
					// TODO: CRB billing is temporary disabled for preview. Will enable cross-region backup billing metrics as per VSCP-3455.
					skipBilling := false
					if !config.EnableCrossRegionBackupBillingMetrics && volume.DataProtection.BackupVaultID != "" {
						if bv, exists := backupVaultMap[volume.DataProtection.BackupVaultID]; exists {
							if bv.SourceRegionName != nil && bv.BackupRegionName != nil && *bv.SourceRegionName != *bv.BackupRegionName {
								logger.Debug("Skipping BackupEnabledVolumeAllocatedSize billing metric for volume with cross-region backup vault", "volumeUUID", volume.UUID)
								skipBilling = true
							}
						} else {
							logger.Error("Backup vault not found in map", "backupVaultID", volume.DataProtection.BackupVaultID, "for volumeUUID", volume.UUID)
							skipBilling = true
						}
					}

					// Additionally skip billing for CMEK backup vaults when CMEK backup billing is disabled.
					if !skipBilling && !config.EnableCmekBackupBilling && volume.DataProtection.BackupVaultID != "" {
						if bv, exists := backupVaultMap[volume.DataProtection.BackupVaultID]; exists {
							if bv.CmekAttributes != nil && bv.CmekAttributes.KmsConfigResourcePath != nil && *bv.CmekAttributes.KmsConfigResourcePath != "" {
								logger.Debug("Skipping BackupEnabledVolumeAllocatedSize billing metric for CMEK backup vault", "volumeUUID", volume.UUID, "backupVaultID", volume.DataProtection.BackupVaultID)
								skipBilling = true
							}
						} else {
							// Conservative: if we can't look up the vault while CMEK billing is disabled,
							// treat it as non-billable to avoid incorrectly billing CMEK backups.
							logger.Error("Backup vault not found in map for CMEK billing check", "backupVaultID", volume.DataProtection.BackupVaultID, "for volumeUUID", volume.UUID)
							skipBilling = true
						}
					}

					// Skip billing for cross-project backup vaults when GCBDR backup billing is disabled.
					if !skipBilling && !config.EnableGcbdrBackupBilling && volume.DataProtection.BackupVaultID != "" {
						if bv, exists := backupVaultMap[volume.DataProtection.BackupVaultID]; exists {
							if bv.ServiceType == models.ServiceTypeCrossProject {
								logger.Debug("Skipping BackupEnabledVolumeAllocatedSize billing metric for cross-project backup vault", "volumeUUID", volume.UUID, "backupVaultID", volume.DataProtection.BackupVaultID)
								skipBilling = true
							}
						} else {
							logger.Error("Backup vault not found in map for cross-project billing check", "backupVaultID", volume.DataProtection.BackupVaultID, "for volumeUUID", volume.UUID)
							skipBilling = true
						}
					}

				if !skipBilling {
					if hydratedMetric := setupHydratedMetricsDataModel(metric.MeasuredType, metric.Metadata.ResourceType, accountName, volumeMetadata, timestamp, float64(allocatedSize)); hydratedMetric != nil {
						if volume.DataProtection.BackupVaultID != "" {
							if bv, exists := backupVaultMap[volume.DataProtection.BackupVaultID]; exists &&
								bv.BackupVaultType == activities.CrossRegionBackupType &&
								bv.BackupRegionName != nil && *bv.BackupRegionName != "" {
								volumeMetadata.SetBackupRegionName(*bv.BackupRegionName)
								setCrossRegionRegionMetadata(logger, hydratedMetric, volumeMetadata)
							}
						}
						// Standard volumes always use DEFAULT mode.
						setBackupModeMetadata(hydratedMetric, BackupModeDefault)
						hydratedMetrics = append(hydratedMetrics, *hydratedMetric)
					}
				}
				}
			}
		}

		// Process SFR metrics if enabled and volume exists in map
		if config.SFRMetricsEnabled && len(sfrMetricsMap) > 0 {
			if sfrData, exists := sfrMetricsMap[volume.UUID]; exists {
				// Create SFR Total Size Restored Bytes metric
				sizeMetric := setupHydratedMetric(timestamp, volumeMetadata, metadata.SFRTotalSizeRestoredBytes, float64(sfrData.TotalSize))
				sizeMetric.Metadata.ResourceType = resourceType
				sfrMetrics = append(sfrMetrics, sizeMetric)

				// Create SFR Total Files Restored Count metric
				countMetric := setupHydratedMetric(timestamp, volumeMetadata, metadata.SFRTotalFilesRestoredCount, float64(sfrData.TotalCount))
				countMetric.Metadata.ResourceType = resourceType
				sfrMetrics = append(sfrMetrics, countMetric)
			} else {
				logger.Infof("No SFR metrics found for volume UUID: %s", volume.UUID)
			}
		}
	}

	// Process expert mode volumes for backup management fee billing.
	if config.EnableBackupBillingMetrics && config.EnableExpertModeBackupBilling {
		expertModeMetrics, err := getExpertModeVolumeBackupMetrics(ctx, vcpDB, config, poolMetadataMap, backupVaultMap, accountStateMap, timestamp)
		if err != nil {
			logger.Error("Failed to get expert mode volume backup metrics", "error", err.Error())
		} else {
			hydratedMetrics = append(hydratedMetrics, expertModeMetrics...)
		}
	}

	// Return the structured result
	return &VolumeMetricsResult{
		HydratedMetrics:                          metrics,
		HydratedMetricsDataModel:                 hydratedMetrics,
		VolumeAllocatedThroughputHydratedMetrics: volumeAllocatedThroughputMetrics,
		SFRHydratedMetrics:                       sfrMetrics,
	}, nil
}

// getExpertModeVolumeBackupMetrics generates BackupEnabledVolumeAllocatedSize hydrated metrics
// for expert mode volumes that have active backup chains, emitting the
// /BackupManagementFeeGbBillable SKU when the volume qualifies for billing.
func getExpertModeVolumeBackupMetrics(
	ctx context.Context,
	vcpDB database.Storage,
	config *common.TelemetryConfig,
	poolMetadataMap map[int64]metadata.ResourceMetadata,
	backupVaultMap map[string]*datamodel.BackupVault,
	accountStateMap map[string]string,
	timestamp time.Time,
) ([]datamodel2.HydratedMetrics, error) {
	logger := util.GetLogger(ctx)

	var hydratedMetrics []datamodel2.HydratedMetrics

	offset := 0
	limit := config.PoolVolumeLabelPageSize
	totalProcessed := 0

	for {
		pagination := &dbutils.Pagination{
			Offset: offset,
			Limit:  limit,
		}

		expertVolumes, err := vcpDB.ListExpertModeVolumesForTelemetryMetrics(ctx, pagination)
		if err != nil {
			return nil, fmt.Errorf("failed to list expert mode volumes for backup billing metrics (offset %d): %w", offset, err)
		}

		if len(expertVolumes) == 0 {
			break
		}

		logger.Info(fmt.Sprintf("Processing %d expert mode volumes for backup billing metrics (offset %d)", len(expertVolumes), offset))

		for _, volume := range expertVolumes {
			if volume.AccountName == "" {
				logger.Error(fmt.Sprintf("Expert mode volume account name is missing for volume %s", volume.UUID))
				continue
			}
			if accountState, exists := accountStateMap[volume.AccountName]; exists && accountState == models.AccountStateHyperscalerDisabled {
				logger.Debugf("Skipping expert mode volume %s (UUID: %s) metrics collection as account %s is in HYPERSCALERDISABLED state", volume.Name, volume.UUID, volume.AccountName)
				continue
			}

			if volume.BackupConfig == nil || volume.BackupConfig.BackupChainBytes == nil || *volume.BackupConfig.BackupChainBytes <= 0 {
				continue
			}

			isSANProtocol := utils.IsSanProtocols(volume.GetProtocols())
			if !config.EnableFilesBackupBilling && !isSANProtocol {
				continue
			}

			// Determine resource type based on pool metadata (regional HA vs standard).
			resourceType := metadata.Volume
			if poolMeta, ok := poolMetadataMap[volume.PoolID]; ok {
				if poolMeta.ResourceType == metadata.VolumePoolRegionalHA {
					resourceType = metadata.VolumeRegionalHA
				}
			}

			skipBilling := false

			// Skip for cross-region backup vaults when CRB billing is disabled.
			if !config.EnableCrossRegionBackupBillingMetrics && volume.BackupConfig.BackupVaultID != "" {
				if bv, exists := backupVaultMap[volume.BackupConfig.BackupVaultID]; exists {
					if bv.SourceRegionName != nil && bv.BackupRegionName != nil && *bv.SourceRegionName != *bv.BackupRegionName {
						logger.Debug("Skipping BackupEnabledVolumeAllocatedSize billing for expert mode volume with cross-region backup vault", "volumeUUID", volume.UUID)
						skipBilling = true
					}
				} else {
					logger.Error("Backup vault not found in map for expert mode volume", "backupVaultID", volume.BackupConfig.BackupVaultID, "volumeUUID", volume.UUID)
					skipBilling = true
				}
			}

			// Skip for CMEK backup vaults when CMEK billing is disabled.
			if !skipBilling && !config.EnableCmekBackupBilling && volume.BackupConfig.BackupVaultID != "" {
				if bv, exists := backupVaultMap[volume.BackupConfig.BackupVaultID]; exists {
					if bv.CmekAttributes != nil && bv.CmekAttributes.KmsConfigResourcePath != nil && *bv.CmekAttributes.KmsConfigResourcePath != "" {
						logger.Debug("Skipping BackupEnabledVolumeAllocatedSize billing for expert mode volume with CMEK backup vault", "volumeUUID", volume.UUID, "backupVaultID", volume.BackupConfig.BackupVaultID)
						skipBilling = true
					}
				} else {
					logger.Error("Backup vault not found in map for CMEK billing check (expert mode volume)", "backupVaultID", volume.BackupConfig.BackupVaultID, "volumeUUID", volume.UUID)
					skipBilling = true
				}
			}

			// Skip for cross-project backup vaults when GCBDR billing is disabled.
			if !skipBilling && !config.EnableGcbdrBackupBilling && volume.BackupConfig.BackupVaultID != "" {
				if bv, exists := backupVaultMap[volume.BackupConfig.BackupVaultID]; exists {
					if bv.ServiceType == models.ServiceTypeCrossProject {
						logger.Debug("Skipping BackupEnabledVolumeAllocatedSize billing for expert mode volume with cross-project backup vault", "volumeUUID", volume.UUID, "backupVaultID", volume.BackupConfig.BackupVaultID)
						skipBilling = true
					}
				} else {
					logger.Error("Backup vault not found in map for cross-project billing check (expert mode volume)", "backupVaultID", volume.BackupConfig.BackupVaultID, "volumeUUID", volume.UUID)
					skipBilling = true
				}
			}

			if skipBilling {
				continue
			}

			volumeMetadata := assembleExpertModeVolumeMetadata(volume, config)
			if hm := setupHydratedMetricsDataModel(
				metadata.BackupEnabledVolumeAllocatedSize,
				resourceType,
				volume.AccountName,
				volumeMetadata,
				timestamp,
				float64(volume.SizeInBytes),
			); hm != nil {
				if volume.BackupConfig.BackupVaultID != "" {
					if bv, exists := backupVaultMap[volume.BackupConfig.BackupVaultID]; exists &&
						bv.BackupVaultType == activities.CrossRegionBackupType &&
						bv.BackupRegionName != nil && *bv.BackupRegionName != "" {
						volumeMetadata.SetBackupRegionName(*bv.BackupRegionName)
						setCrossRegionRegionMetadata(logger, hm, volumeMetadata)
					}
				}
				// Expert mode volumes always use ONTAP mode.
				setBackupModeMetadata(hm, BackupModeOntap)
				hydratedMetrics = append(hydratedMetrics, *hm)
			}
		}

		totalProcessed += len(expertVolumes)
		offset += len(expertVolumes)
	}

	logger.Info(fmt.Sprintf("Processed %d expert mode volumes for backup billing metrics", totalProcessed))

	return hydratedMetrics, nil
}

// assembleExpertModeVolumeMetadata creates ResourceMetadata for an expert mode volume.
func assembleExpertModeVolumeMetadata(volume *database.ExpertModeVolumeMetricsData, config *common.TelemetryConfig) metadata.ResourceMetadata {
	met := metadata.ResourceMetadata{}
	met.SetResourceUUID(volume.UUID)
	met.SetResourceName(volume.Name)
	met.SetResourceDisplayName(volume.Name)
	met.SetResourceType(metadata.Volume)
	met.SetSizeInBytes(volume.SizeInBytes)
	met.SetRegionName(config.RegionName)
	met.SetAccountName(volume.AccountName)
	met.SetDeploymentName(volume.DeploymentName)
	return met
}

// assembleVolumeMetadata creates metadata from optimized VolumeMetricsData struct
func assembleVolumeMetadata(volume *database.VolumeMetricsData, config *common.TelemetryConfig) metadata.ResourceMetadata {
	met := metadata.ResourceMetadata{}
	met.SetResourceUUID(volume.UUID)
	met.SetResourceName(volume.Name)
	met.SetResourceDisplayName(volume.Name)
	met.SetResourceType(metadata.Volume)
	// Use the allocated size (size_in_bytes) for billing
	met.SetSizeInBytes(volume.SizeInBytes)
	met.SetRegionName(config.RegionName)
	// Get account name and deployment name from VolumeAttributes JSONB
	met.SetAccountName(volume.GetAccountName())
	met.SetDeploymentName(volume.GetDeploymentName())
	return met
}
