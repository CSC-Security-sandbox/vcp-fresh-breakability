package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	unifiedServiceType = "unified"
)

type ResourceKey struct {
	ResourceType   metadata.ResourceType
	ResourceName   string
	DeploymentName string
	ConsumerID     string
}

type Labels map[string]interface{}

type ResourceData struct {
	UUID                  string
	AccountID             int64
	Labels                Labels
	VolumeReplicationInfo *VolumeReplicationInfo
}

type VolumeReplicationInfo struct {
	ReplicationName       *string
	ReplicationType       string
	ReplicationSchedule   string
	SourceLocation        *string
	DestinationVolumeUUID *string
	DestinationLocation   *string
}

type ResourceCollection struct {
	PoolData              map[ResourceKey]ResourceData
	VolumeData            map[ResourceKey]ResourceData
	BackupData            map[ResourceKey]ResourceData
	VolumeReplicationData map[ResourceKey]ResourceData
}

type BillingProvider struct {
	metricsDB    database2.Storage
	vcpDataStore database.Storage
	config       *common.TelemetryConfig
	usageSink    common.UsageSink
}

func NewBillingProvider(db database2.Storage, vcpDB database.Storage, config *common.TelemetryConfig, usageSink common.UsageSink) *BillingProvider {
	return &BillingProvider{
		metricsDB:    db,
		vcpDataStore: vcpDB,
		config:       config,
		usageSink:    usageSink,
	}
}

// ProcessBillingMetrics processes raw metrics from cvt_metrics table and aggregates them
func (p *BillingProvider) ProcessBillingMetrics(ctx context.Context, aggregationEndTime time.Time) error {
	logger := util.GetLogger(ctx)
	var aggregatedRecords []datamodel2.AggregatedUsage
	aggregationStartTime := aggregationEndTime.Add(-1 * time.Hour)
	logger.Infof("Processing metrics from %v to %v", aggregationStartTime, aggregationEndTime)

	// Fetch label values from VCP database at the start of each aggregator cycle
	resourceCollection, err := p.fetchResourceData(ctx, aggregationStartTime, aggregationEndTime)
	if err != nil {
		logger.Errorf("Failed to fetch resource data: %v", err)
	}

	// Get unsent/retry records from database
	aggregatedRecordsToRetry, err := p.getUnsentGoogleUsages(ctx, p.config.MaxGoogleBillingPushRetry)
	if err != nil {
		logger.Errorf("error getting unsent google usages", "error", err)
	} else {
		aggregatedRecords = append(aggregatedRecords, aggregatedRecordsToRetry...)
	}

	// Process each job definition
	for key, jobDef := range common.DefaultAggregationJobDefinitions {
		var metrics []datamodel2.HydratedMetrics
		var err error

		if jobDef.AggregationType == common.IntegralAggregation || jobDef.AggregationType == common.CounterAggregation {
			// For counter aggregation and integral aggregation, we need:
			// 1. All records from current aggregation window
			// 2. Only the latest record from previous period (closest to aggregation start)
			metrics, err = p.fetchMetricsForCounterAndIntegralAggregation(ctx, aggregationStartTime, aggregationEndTime, key.ResourceType.String(), key.MeasuredType.String())
		} else {
			// For other aggregation types, fetch only current window
			filter := p.CreateFilterWithConditions(
				aggregationStartTime,
				aggregationEndTime,
				key.ResourceType.String(),
				key.MeasuredType.String(),
			)
			metrics, err = p.metricsDB.GetHydratedMetrics(ctx, filter)
		}

		if err != nil {
			logger.Error("Failed to list hydrated metrics", "error", err.Error())
			return err
		}
		logger.Debugf("Fetched %d metrics for aggregation - ResourceType: %s, MeasuredType: %s",
			len(metrics), key.ResourceType.String(), key.MeasuredType.String())
		// Group metrics by resource
		resourceGroups := p.groupMetricsByResource(metrics)

		// Process each resource group
		for resourceIdentifier, resourceMetrics := range resourceGroups {
			if err := p.processMetricsWithJobDef(ctx, resourceIdentifier, resourceMetrics, jobDef, aggregationStartTime, aggregationEndTime, resourceCollection, &aggregatedRecords); err != nil {
				logger.Errorf("Failed to process metrics for resource %s and customer id %s : %v", resourceIdentifier.ResourceName, resourceIdentifier.ConsumerID, err)
				continue
			}
		}
	}

	// Batch save all aggregated usage records
	if len(aggregatedRecords) > 0 {
		batchSize := p.config.PushBatchSize

		err := p.metricsDB.CreateAggregatedUsageBatch(ctx, aggregatedRecords, int(batchSize))
		if err != nil {
			logger.Errorf("Failed to batch save aggregated usage records: %v", err)
			return err
		}
		logger.Infof("Successfully saved %d aggregated usage records in batches of %d", len(aggregatedRecords), batchSize)
	}

	// Deliver all aggregated metrics at the end
	if len(aggregatedRecords) > 0 {
		if _, err := p.usageSink.DeliverMetrics(ctx, aggregatedRecords); err != nil {
			logger.Errorf("Failed to deliver aggregated metrics: %v", err)
		}
	}

	return nil
}

// fetchResourceData fetches label values from pool and volume tables in VCP database
func (p *BillingProvider) fetchResourceData(ctx context.Context, aggregationStartTime, aggregationEndTime time.Time) (*ResourceCollection, error) {
	logger := util.GetLogger(ctx)
	logger.Info("Fetching resource data from VCP database")

	// Create a new ResourceCollection for this aggregation cycle
	resourceCollection := &ResourceCollection{
		PoolData:              make(map[ResourceKey]ResourceData),
		VolumeData:            make(map[ResourceKey]ResourceData),
		VolumeReplicationData: make(map[ResourceKey]ResourceData),
		BackupData:            make(map[ResourceKey]ResourceData),
	}

	var poolsDataError, volumeDataError, backupDataError, volumeReplicationDataError error

	// Fetch pool labels
	if err := p.fetchPoolData(ctx, aggregationStartTime, aggregationEndTime, resourceCollection); err != nil {
		logger.Errorf("Failed to fetch pool resource data: %v", err)
		poolsDataError = err
	}

	// Fetch volume labels
	if err := p.fetchVolumeData(ctx, aggregationStartTime, aggregationEndTime, resourceCollection); err != nil {
		logger.Errorf("Failed to fetch volume labels: %v", err)
		volumeDataError = err
	}

	// Fetch backup data only if backup billing is enabled
	if p.config.EnableBackupBillingMetrics {
		if err := p.fetchBackupData(ctx, aggregationStartTime, aggregationEndTime, resourceCollection); err != nil {
			logger.Errorf("Failed to fetch backup data: %v", err)
			backupDataError = err
		}
	}

	if p.config.EnableReplicationBillingMetrics {
		if err := p.fetchVolumeReplicationData(ctx, aggregationStartTime, aggregationEndTime, resourceCollection); err != nil {
			logger.Errorf("Failed to fetch volume replication labels: %v", err)
			volumeReplicationDataError = err
		}
	}

	poolCount := len(resourceCollection.PoolData)
	volumeCount := len(resourceCollection.VolumeData)
	backupCount := len(resourceCollection.BackupData)
	volumeReplicationCount := len(resourceCollection.VolumeReplicationData)

	if poolsDataError != nil && volumeDataError != nil {
		logger.Errorf("Failed to fetch both pool and volume resource data. Pool error: %v, Volume error: %v, Backup error: %v", poolsDataError, volumeDataError, backupDataError)
		return nil, fmt.Errorf("failed to fetch any resource data")
	} else if poolsDataError != nil {
		logger.Warnf("Failed to fetch pool resource data: %v, but successfully fetched %d volume resource data ", poolsDataError, volumeCount)
	} else if volumeDataError != nil {
		logger.Warnf("Failed to fetch volume resource data: %v, but successfully fetched %d pool resource data", volumeDataError, poolCount)
	} else if backupDataError != nil {
		logger.Warnf("Failed to fetch backup resource data: %v", backupDataError)
	} else if volumeReplicationDataError != nil {
		logger.Warnf("Failed to fetch volume replication resource data: %v, but successfully fetched %d pool and %d volume resource data", volumeReplicationDataError, poolCount, volumeCount)
	} else {
		logger.Infof("Successfully fetched resource data for %d pools, %d volumes, %d backups, %d volume replication", poolCount, volumeCount, backupCount, volumeReplicationCount)
	}

	return resourceCollection, nil
}

// fetchPoolData fetches labels from pool table using pagination
func (p *BillingProvider) fetchPoolData(ctx context.Context, aggregationStartTime, aggregationEndTime time.Time, resourceCollection *ResourceCollection) error {
	logger := util.GetLogger(ctx)

	conditions := [][]interface{}{
		{"(deleted_at IS NULL OR (deleted_at >= ? AND deleted_at <= ?))", aggregationStartTime, aggregationEndTime},
	}

	offset := 0
	// Use configurable limit from config
	limit := p.config.PoolVolumeLabelPageSize
	totalProcessed := 0
	batchCount := 0

	for {
		// Create pagination with offset and limit
		pagination := &dbutils.Pagination{
			Offset: offset,
			Limit:  limit,
		}

		// Fetch paginated pools using ListPoolsWithPagination
		pools, err := p.vcpDataStore.ListPoolsWithPagination(ctx, conditions, pagination)
		if err != nil {
			return fmt.Errorf("failed to list pools (offset %d): %w", offset, err)
		}

		// Break if no records returned
		if len(pools) == 0 {
			break
		}

		// Process current batch
		for _, pool := range pools {
			// Skip pools with nil Account to prevent panic
			if pool.Account == nil {
				logger.Warnf("Skipping pool %s (%s) due to nil Account relationship", pool.Name, pool.UUID)
				continue
			}

			// Extract and limit labels (handle nil PoolAttributes)
			var limitedLabels Labels
			if pool.PoolAttributes != nil && pool.PoolAttributes.Labels != nil {
				limitedLabels = p.limitLabels(pool.PoolAttributes.Labels)
			} else {
				limitedLabels = make(Labels)
			}

			poolResourceData := ResourceData{
				UUID:      pool.UUID,
				AccountID: pool.AccountID,
				Labels:    limitedLabels,
			}
			resourceType := metadata.VolumePool
			if pool.PoolAttributes != nil && pool.PoolAttributes.IsRegionalHA {
				resourceType = metadata.VolumePoolRegionalHA
			}
			id := ResourceKey{
				ResourceType:   resourceType,
				ResourceName:   pool.Name,
				DeploymentName: pool.DeploymentName,
				ConsumerID:     pool.Account.Name,
			}
			resourceCollection.PoolData[id] = poolResourceData
		}

		totalProcessed += len(pools)
		batchCount++
		logger.Debugf("Processed %d pools in batch %d (offset: %d, total: %d)", len(pools), batchCount, offset, totalProcessed)

		// Update offset for next iteration
		offset += limit
	}

	logger.Infof("Fetched resource data for %d pools in %d batches", len(resourceCollection.PoolData), batchCount)
	return nil
}

// fetchVolumeData fetches labels from volume table using pagination
func (p *BillingProvider) fetchVolumeData(ctx context.Context, aggregationStartTime, aggregationEndTime time.Time, resourceCollection *ResourceCollection) error {
	logger := util.GetLogger(ctx)

	// Create conditions for volumes including deleted volumes where deleted_at is between aggregation times
	conditions := [][]interface{}{
		{"(deleted_at IS NULL OR (deleted_at >= ? AND deleted_at <= ?))", aggregationStartTime, aggregationEndTime},
	}

	offset := 0
	// Use configurable limit from config
	limit := p.config.PoolVolumeLabelPageSize
	totalProcessed := 0
	batchCount := 0

	for {
		// Create pagination with offset and limit
		pagination := &dbutils.Pagination{
			Offset: offset,
			Limit:  limit,
		}

		// Fetch paginated volumes using ListVolumesWithPagination
		volumes, err := p.vcpDataStore.ListVolumesWithPagination(ctx, conditions, pagination)
		if err != nil {
			return fmt.Errorf("failed to list volumes (offset %d): %w", offset, err)
		}

		// Break if no records returned
		if len(volumes) == 0 {
			break
		}

		// Process current batch
		for _, volume := range volumes {
			// Skip volumes with nil Account or Pool to prevent panic
			if volume.Account == nil {
				logger.Warnf("Skipping volume %s (%s) due to nil Account relationship", volume.Name, volume.UUID)
				continue
			}
			if volume.Pool == nil {
				logger.Warnf("Skipping volume %s (%s) due to nil Pool relationship", volume.Name, volume.UUID)
				continue
			}

			// Extract and limit labels (handle nil VolumeAttributes)
			var limitedLabels Labels
			if volume.VolumeAttributes != nil && volume.VolumeAttributes.Labels != nil {
				limitedLabels = p.limitLabels(volume.VolumeAttributes.Labels)
			} else {
				limitedLabels = make(Labels)
			}

			volumeResourceData := ResourceData{
				UUID:      volume.UUID,
				AccountID: volume.AccountID,
				Labels:    limitedLabels,
			}
			resourceType := metadata.Volume
			if volume.Pool.PoolAttributes != nil && volume.Pool.PoolAttributes.IsRegionalHA {
				resourceType = metadata.VolumeRegionalHA
			}
			id := ResourceKey{
				ResourceType:   resourceType,
				ResourceName:   volume.Name,
				DeploymentName: volume.Pool.DeploymentName,
				ConsumerID:     volume.Account.Name,
			}
			resourceCollection.VolumeData[id] = volumeResourceData
		}

		totalProcessed += len(volumes)
		batchCount++
		logger.Debugf("Processed %d volumes in batch %d (offset: %d, total: %d)", len(volumes), batchCount, offset, totalProcessed)

		// Update offset for next iteration
		offset += limit
	}

	logger.Infof("Fetched resource data for %d volumes in %d batches", len(resourceCollection.VolumeData), batchCount)
	return nil
}

// fetchBackupData fetches backup data and constructs ResourceData and ResourceKey
func (p *BillingProvider) fetchBackupData(ctx context.Context, aggregationStartTime, aggregationEndTime time.Time, resourceCollection *ResourceCollection) error {
	logger := util.GetLogger(ctx)

	// First, fetch all backup metadata entries to get volumeUUID -> labels mapping
	volumeLabelsMap, err := p.fetchBackupMetadata(ctx, aggregationStartTime, aggregationEndTime)
	if err != nil {
		logger.Warnf("Failed to fetch backup metadata (table may not exist yet): %v", err)
		// Continue with empty labels map if metadata fetch fails
		volumeLabelsMap = make(map[string]Labels)
	}

	// Create conditions for backups including deleted backups where deleted_at is between aggregation times
	conditions := [][]interface{}{
		{"(deleted_at IS NULL OR (deleted_at >= ? AND deleted_at <= ?))", aggregationStartTime, aggregationEndTime},
	}

	offset := int32(0)
	// Use configurable limit from config
	limit := p.config.PoolVolumeLabelPageSize
	totalProcessed := 0
	batchCount := 0

	for {
		// Create pagination with offset and limit
		pagination := &dbutils.Pagination{
			Offset: int(offset),
			Limit:  int(limit),
		}

		// Fetch paginated backup metrics
		backups, err := p.vcpDataStore.GetBackupMetrics(ctx, conditions, pagination)
		if err != nil {
			return fmt.Errorf("failed to get backup metrics (offset %d): %w", offset, err)
		}

		// Break if no records returned
		if len(backups) == 0 {
			break
		}

		// Process current batch
		for _, backup := range backups {
			// Skip backups with nil Attributes or BackupVault to prevent panic
			if backup.Attributes == nil {
				logger.Warnf("Skipping backup %s due to nil Attributes", backup.UUID)
				continue
			}
			if backup.BackupVault == nil {
				logger.Warnf("Skipping backup %s due to nil BackupVault relationship", backup.UUID)
				continue
			}

			// Get labels from the backup metadata map
			labels := volumeLabelsMap[backup.VolumeUUID]
			if labels == nil {
				labels = make(Labels)
			}

			backupResourceData := ResourceData{
				UUID:      backup.VolumeUUID, // Using volume UUID
				AccountID: backup.BackupVault.AccountID,
				Labels:    labels,
			}
			id := ResourceKey{
				ResourceType:   metadata.Backup,
				ResourceName:   backup.Attributes.VolumeName,
				DeploymentName: backup.BackupVault.Name,
				ConsumerID:     backup.Attributes.AccountIdentifier,
			}
			resourceCollection.BackupData[id] = backupResourceData
		}

		totalProcessed += len(backups)
		batchCount++
		logger.Debugf("Processed %d backup metrics in batch %d (offset: %d, total: %d)", len(backups), batchCount, offset, totalProcessed)

		// Update offset for next iteration
		offset += int32(limit)
	}

	logger.Infof("Fetched resource data for %d backups with %d volume labels in %d batches", len(resourceCollection.BackupData), len(volumeLabelsMap), batchCount)
	return nil
}

// fetchVolumeReplicationData fetches labels from volume replication table using pagination
func (p *BillingProvider) fetchVolumeReplicationData(ctx context.Context, aggregationStartTime, aggregationEndTime time.Time, resourceCollection *ResourceCollection) error {
	logger := util.GetLogger(ctx)

	offset := 0
	// Use configurable limit from config
	limit := p.config.PoolVolumeLabelPageSize
	totalProcessed := 0
	batchCount := 0

	// Create conditions for backups including deleted backups where deleted_at is between aggregation times
	conditions := [][]interface{}{
		{"(deleted_at IS NULL OR (deleted_at >= ? AND deleted_at <= ?))", aggregationStartTime, aggregationEndTime},
	}

	for {
		// Create pagination with offset and limit
		pagination := &dbutils.Pagination{
			Offset: offset,
			Limit:  limit,
		}

		// Fetch paginated volume replications using ListVolumeReplicationsWithPagination
		volumeReplications, err := p.vcpDataStore.ListVolumeReplicationsWithPagination(ctx, conditions, pagination)
		if err != nil {
			return fmt.Errorf("failed to list volume replications (offset %d): %w", offset, err)
		}

		// Break if no records returned
		if len(volumeReplications) == 0 {
			break
		}

		// Process current batch
		for _, volumeReplication := range volumeReplications {
			// Skip volume replications with nil Account to prevent panic
			if volumeReplication.Account == nil {
				logger.Warnf("Skipping volume replication %s (%s) due to nil Account relationship", volumeReplication.Name, volumeReplication.UUID)
				continue
			}

			var limitedLabels Labels
			var volRepInfo *VolumeReplicationInfo
			if volumeReplication.ReplicationAttributes != nil {
				if volumeReplication.ReplicationAttributes.Labels != nil {
					limitedLabels = p.limitLabels(volumeReplication.ReplicationAttributes.Labels)
				} else {
					limitedLabels = make(Labels)
				}
				volRepInfo = &VolumeReplicationInfo{
					ReplicationName:       &volumeReplication.Name,
					ReplicationSchedule:   volumeReplication.ReplicationAttributes.ReplicationSchedule,
					ReplicationType:       volumeReplication.ReplicationAttributes.ReplicationType,
					SourceLocation:        &volumeReplication.ReplicationAttributes.SourceLocation,
					DestinationVolumeUUID: &volumeReplication.ReplicationAttributes.DestinationVolumeUUID,
					DestinationLocation:   &volumeReplication.ReplicationAttributes.DestinationLocation,
				}
			}

			volumeReplicationResourceData := ResourceData{
				UUID:                  volumeReplication.UUID,
				AccountID:             volumeReplication.AccountID,
				Labels:                limitedLabels,
				VolumeReplicationInfo: volRepInfo,
			}
			id := ResourceKey{
				ResourceType:   metadata.VolumeReplicationRelationship,
				ResourceName:   volumeReplication.ReplicationAttributes.ExternalUUID,
				DeploymentName: volumeReplication.Volume.Pool.DeploymentName,
				ConsumerID:     volumeReplication.Account.Name,
			}
			logger.Infof("Volume Replication name %s", volumeReplication.Name)
			resourceCollection.VolumeReplicationData[id] = volumeReplicationResourceData
		}

		totalProcessed += len(volumeReplications)
		batchCount++
		logger.Debugf("Processed %d volume replications in batch %d (offset: %d, total: %d)", len(volumeReplications), batchCount, offset, totalProcessed)

		// Update offset for next iteration
		offset += limit
	}

	logger.Infof("Fetched resource data for %d volume replications in %d batches", len(resourceCollection.VolumeReplicationData), batchCount)
	return nil
}

// fetchBackupMetadata fetches all backup metadata entries and returns volumeUUID -> labels mapping
func (p *BillingProvider) fetchBackupMetadata(ctx context.Context, aggregationStartTime, aggregationEndTime time.Time) (map[string]Labels, error) {
	logger := util.GetLogger(ctx)

	// Create conditions for backup metadata with labels including deleted metadata where deleted_at is between aggregation times
	conditions := [][]interface{}{
		{"labels IS NOT NULL"},
		{"(deleted_at IS NULL OR (deleted_at >= ? AND deleted_at <= ?))", aggregationStartTime, aggregationEndTime},
	}

	offset := int32(0)
	// Use configurable limit from config
	limit := p.config.PoolVolumeLabelPageSize
	totalProcessed := 0
	batchCount := 0

	// Create volumeUUID -> labels mapping
	volumeLabelsMap := make(map[string]Labels)

	for {
		// Create pagination with offset and limit
		pagination := &dbutils.Pagination{
			Offset: int(offset),
			Limit:  int(limit),
		}

		// Fetch paginated backup metadata using interface method
		backupMetadataList, err := p.vcpDataStore.GetBackupMetadata(ctx, conditions, pagination)
		if err != nil {
			// Check if it's a table not found error
			if strings.Contains(err.Error(), "does not exist") {
				logger.Warnf("Backup metadata table does not exist yet, returning empty labels map")
				return make(map[string]Labels), nil
			}
			return nil, fmt.Errorf("failed to fetch backup metadata (offset %d): %w", offset, err)
		}

		// Break if no records returned
		if len(backupMetadataList) == 0 {
			break
		}

		// Process current batch
		for _, backupMetadata := range backupMetadataList {
			if backupMetadata.Labels != nil && backupMetadata.VolumeUUID != "" {
				// Convert JSONB to Labels map
				labels := p.limitLabels(backupMetadata.Labels)
				volumeLabelsMap[backupMetadata.VolumeUUID] = labels
			}
		}

		totalProcessed += len(backupMetadataList)
		batchCount++
		logger.Debugf("Processed %d backup metadata entries in batch %d (offset: %d, total: %d)", len(backupMetadataList), batchCount, offset, totalProcessed)

		// Update offset for next iteration
		offset += int32(limit)
	}

	logger.Infof("Fetched %d backup metadata entries with labels in %d batches", len(volumeLabelsMap), batchCount)
	return volumeLabelsMap, nil
}

// limitLabels limits the number of labels to the configured maximum
func (p *BillingProvider) limitLabels(labels *datamodel.JSONB) Labels {
	if labels == nil {
		return make(Labels)
	}

	limitedLabels := make(Labels)
	count := 0

	for key, value := range *labels {
		if count >= p.config.GoogleBillingLabelsMaxEntries {
			break
		}
		limitedLabels[key] = value
		count++
	}

	return limitedLabels
}

// getResourceDataForAggregationUsage returns the billing labels for a given resource
func (p *BillingProvider) getResourceDataForAggregationUsage(id ResourceKey, resourceType metadata.ResourceType, resourceCollection *ResourceCollection) *ResourceData {
	var resourceData ResourceData
	var found bool

	// Get labels based on resource type
	switch resourceType {
	case metadata.VolumePool:
		resourceData, found = resourceCollection.PoolData[id]
	case metadata.Volume:
		resourceData, found = resourceCollection.VolumeData[id]
	case metadata.Backup:
		resourceData, found = resourceCollection.BackupData[id]
	case metadata.VolumePoolRegionalHA:
		resourceData, found = resourceCollection.PoolData[id]
	case metadata.VolumeRegionalHA:
		resourceData, found = resourceCollection.VolumeData[id]
	case metadata.VolumeReplicationRelationship:
		resourceData, found = resourceCollection.VolumeReplicationData[id]
	default:
		return nil
	}
	if !found {
		return nil
	}
	return &resourceData
}

// CreateFilterWithConditions creates a filter map with conditions for metrics queries
func (p *BillingProvider) CreateFilterWithConditions(startTime time.Time, endTime time.Time, resourceType string, measuredType string) map[string]interface{} {
	conditions := [][]interface{}{
		{"metric_timestamp >= ?", startTime},
		{"metric_timestamp <= ?", endTime},
	}

	if resourceType != "" {
		conditions = append(conditions, []interface{}{"resource_type = ?", resourceType})
	}

	if measuredType != "" {
		conditions = append(conditions, []interface{}{"measured_type = ?", measuredType})
	}

	return map[string]interface{}{
		"conditions": conditions,
	}
}

// CreateComplexFilter creates a filter with multiple optional conditions
func (p *BillingProvider) CreateComplexFilter(options map[string]interface{}) map[string]interface{} {
	var conditions [][]interface{}

	// Add time range conditions if present
	if startTime, ok := options["startTime"].(time.Time); ok {
		conditions = append(conditions, []interface{}{"metric_timestamp >= ?", startTime})
	}

	if endTime, ok := options["endTime"].(time.Time); ok {
		conditions = append(conditions, []interface{}{"metric_timestamp <= ?", endTime})
	}

	// Add resource type condition if present
	if resourceType, ok := options["resourceType"].(string); ok && resourceType != "" {
		conditions = append(conditions, []interface{}{"resource_type = ?", resourceType})
	}

	// Add measured type condition if present
	if measuredType, ok := options["measuredType"].(string); ok && measuredType != "" {
		conditions = append(conditions, []interface{}{"measured_type = ?", measuredType})
	}

	// Add UUID filter if present
	if uuids, ok := options["uuids"].([]string); ok && len(uuids) > 0 {
		conditions = append(conditions, []interface{}{"uuid in ?", uuids})
	}

	filter := map[string]interface{}{
		"conditions": conditions,
	}

	// Add ordering if present
	if order, ok := options["order"].(string); ok && order != "" {
		filter["order"] = order
	}

	// Add limit if present
	if limit, ok := options["limit"].(int); ok && limit > 0 {
		filter["limit"] = limit
	}

	return filter
}

func (p *BillingProvider) groupMetricsByResource(metrics []datamodel2.HydratedMetrics) map[ResourceKey][]datamodel2.HydratedMetrics {
	groups := make(map[ResourceKey][]datamodel2.HydratedMetrics)
	for _, metric := range metrics {
		if metric.ResourceName != "" {
			identifier := ResourceKey{
				ResourceName:   metric.ResourceName,
				DeploymentName: metric.DeploymentName,
				ConsumerID:     metric.ConsumerID,
				ResourceType:   metric.ResourceType,
			}
			groups[identifier] = append(groups[identifier], metric)
		}
	}
	return groups
}

// fetchMetricsForCounterAndIntegralAggregation fetches metrics for counter aggregation using a single query
// This gets all records from the current aggregation window plus the latest record from the previous period
func (p *BillingProvider) fetchMetricsForCounterAndIntegralAggregation(ctx context.Context, aggregationStartTime, aggregationEndTime time.Time, resourceType, measuredType string) ([]datamodel2.HydratedMetrics, error) {
	// Create a complex filter that sorts by resource and timestamp
	filter := p.CreateComplexFilter(map[string]interface{}{
		"startTime":    aggregationStartTime.Add(-2 * time.Hour), // Look back 2 hours
		"endTime":      aggregationEndTime,
		"resourceType": resourceType,
		"measuredType": measuredType,
		"order":        "resource_name, deployment_name, consumer_id, metric_timestamp DESC", // Database sorts for us
	})

	// Fetch all metrics within the extended time range, already sorted by the database
	allMetrics, err := p.metricsDB.GetHydratedMetrics(ctx, filter)
	if err != nil {
		return nil, err
	}

	// Group by resource and filter to get only what we need
	resourceGroups := p.groupMetricsByResource(allMetrics)
	var finalMetrics []datamodel2.HydratedMetrics

	for _, resourceMetrics := range resourceGroups {
		// No need to sort - data is already sorted by the database
		// Filter each resource group to get the desired records
		filteredForResource := p.filterMetricsForCounterAndIntegralAggregationSorted(resourceMetrics, aggregationStartTime)
		finalMetrics = append(finalMetrics, filteredForResource...)
	}

	return finalMetrics, nil
}

// filterMetricsForCounterAndIntegralAggregationSorted filters metrics to include only the latest record before
// aggregationStartTime and all records within the aggregation window
// Assumes metrics are already sorted by timestamp DESC (latest first)
func (p *BillingProvider) filterMetricsForCounterAndIntegralAggregationSorted(metrics []datamodel2.HydratedMetrics, aggregationStartTime time.Time) []datamodel2.HydratedMetrics {
	var result []datamodel2.HydratedMetrics
	var foundPreviousRecord bool

	// Since metrics are already sorted by timestamp DESC, we can process them in order
	for _, metric := range metrics {
		if metric.MetricTimestamp.Before(aggregationStartTime) {
			// This is a record from before the aggregation window
			// Since data is sorted DESC, this is the latest record before the window
			if !foundPreviousRecord {
				result = append(result, metric)
				foundPreviousRecord = true
			}
			// Skip any other records from before the window (they're older)
		} else {
			// This record is within the aggregation window - include it
			result = append(result, metric)
		}
	}

	return result
}

func (p *BillingProvider) processMetricsWithJobDef(ctx context.Context, resourceKey ResourceKey, metrics []datamodel2.HydratedMetrics, jobDef common.AggregationJobDefinition, start, end time.Time, resourceCollection *ResourceCollection, aggregatedRecords *[]datamodel2.AggregatedUsage) error {
	logger := util.GetLogger(ctx)
	if len(metrics) == 0 {
		logger.Infof("No metrics found for resource key %s and customer id %s", resourceKey, resourceKey.ConsumerID)
		return nil
	}

	// Calculate aggregated value based on job type
	var quantity float64
	switch jobDef.AggregationType {
	case common.IntegralAggregation:
		quantity = common.Integral(metrics)
	case common.CounterAggregation:
		quantity = common.CounterDelta(metrics)
	case common.SumAggregation:
		quantity = common.Sum(metrics)
	case common.FirstAggregation:
		quantity = common.First(metrics)
	default:
		return fmt.Errorf("unsupported job type: %s", jobDef.AggregationType)
	}

	// Get last counter value for counter metrics
	var lastCounterValue *float64
	if jobDef.AggregationType == common.CounterAggregation && len(metrics) > 0 {
		val := metrics[len(metrics)-1].Quantity
		lastCounterValue = &val
	}

	// Get resource data for the resource
	resourceData := p.getResourceDataForAggregationUsage(resourceKey, metrics[0].ResourceType, resourceCollection)

	// Initialize with default values
	var billingLabelsJSON *string
	resourceUUID := ""
	accountID := ""

	// Process resource data if available
	if resourceData != nil {
		resourceUUID = resourceData.UUID
		accountID = strconv.FormatInt(resourceData.AccountID, 10)

		// Only marshal labels if they exist
		if len(resourceData.Labels) > 0 {
			labelsBytes, err := json.Marshal(resourceData.Labels)
			if err != nil {
				logger.Errorf("Failed to marshal billing labels for resource name %s, deployment name :%s : %v", resourceKey.ResourceName, resourceKey.DeploymentName, err)
			} else {
				labelsStr := string(labelsBytes)
				billingLabelsJSON = &labelsStr
			}
		}
	} else {
		return fmt.Errorf("skipping aggregation usage record as resource data not found for resource name : %s, deployment name : %s, customer ID : %s", resourceKey.ResourceName, resourceKey.DeploymentName, resourceKey.ConsumerID)
	}

	if metrics[0].MeasuredType != metadata.PoolTotalIops && metrics[0].MeasuredType != metadata.PoolTotalThroughputMibps {
		quantity = BytesToMiB(quantity)
	}

	// Create aggregated record with all available fields
	aggregated := &datamodel2.AggregatedUsage{
		ResourceUUID:           resourceUUID,
		AccountID:              accountID,
		VendorCustomerID:       &resourceKey.ConsumerID,
		AggregationStart:       start,
		AggregationEnd:         end,
		MeasuredType:           metrics[0].MeasuredType,
		ResourceType:           metrics[0].ResourceType,
		Quantity:               quantity,
		ResourceName:           &metrics[0].ResourceName,
		RegionName:             &metrics[0].Location,
		LastCounterValue:       lastCounterValue,
		SourceRegion:           &metrics[0].Location,
		DestinationRegion:      nil,
		BillingLabels:          billingLabelsJSON,
		ReplicationDstVolumeID: nil,
		DoubleEncryption:       nil,
		State:                  datamodel2.Unsubmitted,
		ErrorCount:             0,
		ErrorMessage:           nil,
		IsBillable:             common.IsBillableMetric(ctx, metrics[0].ResourceType, metrics[0].MeasuredType),
		AggregationType:        string(jobDef.AggregationType),
		ServiceLevel:           unifiedServiceType,
	}

	if aggregated.MeasuredType == metadata.BackupLogicalSize {
		aggregated.DestinationRegion = &metrics[0].Location
	}

	if aggregated.ResourceType == metadata.VolumeReplicationRelationship {
		if resourceData.VolumeReplicationInfo != nil {
			aggregated.ServiceLevel = setServiceLevelForCRR(resourceData.VolumeReplicationInfo.ReplicationSchedule)
			aggregated.ResourceName = resourceData.VolumeReplicationInfo.ReplicationName
			aggregated.SourceRegion = resourceData.VolumeReplicationInfo.SourceLocation
			aggregated.DestinationRegion = resourceData.VolumeReplicationInfo.DestinationLocation
			aggregated.ReplicationDstVolumeID = resourceData.VolumeReplicationInfo.DestinationVolumeUUID
			aggregated.ReplicationType = resourceData.VolumeReplicationInfo.ReplicationType
		} else {
			logger.Infof("No resourceData found for resource name %s, deployment name :%s", resourceKey.ResourceName, resourceKey.DeploymentName)
		}
	}

	logger.Debugf("Processing metrics for resource %s and customer id %s with aggregation type %s and %s", resourceKey.ResourceName, resourceKey.ConsumerID, jobDef.AggregationType, aggregated)
	// Format labels for better readability
	labelsInfo := "none"
	if len(resourceData.Labels) > 0 {
		// Create a formatted string of key-value pairs
		var labelPairs []string
		for key, value := range resourceData.Labels {
			labelPairs = append(labelPairs, fmt.Sprintf("%s=%v", key, value))
		}
		labelsInfo = fmt.Sprintf("[%s]", strings.Join(labelPairs, ", "))
	}

	logger.Debugf("Processing metrics for resource %s (customer: %s, type: %s, labels: %s)",
		resourceKey.ResourceName, resourceKey.ConsumerID, jobDef.AggregationType, labelsInfo)

	*aggregatedRecords = append(*aggregatedRecords, *aggregated)
	return nil
}

func (p *BillingProvider) getUnsentGoogleUsages(ctx context.Context, maxRetries int64) ([]datamodel2.AggregatedUsage, error) {
	var allRecords []datamodel2.AggregatedUsage

	// Get records with UNSUBMITTED state
	unsubmittedFilter := map[string]interface{}{
		"state":       datamodel2.Unsubmitted,
		"is_billable": true,
	}
	unsubmittedRecords, err := p.metricsDB.GetAggregatedUsage(ctx, unsubmittedFilter)
	if err != nil {
		return nil, err
	}
	allRecords = append(allRecords, unsubmittedRecords...)

	// Get records with ERROR state and error_count <= maxRetries
	// Since we can't do complex comparisons with current interface, we'll fetch all ERROR records
	// and filter in memory
	errorFilter := map[string]interface{}{
		"state": datamodel2.Error,
	}
	errorRecords, err := p.metricsDB.GetAggregatedUsage(ctx, errorFilter)
	if err != nil {
		return nil, err
	}

	// Filter error records by error_count in memory
	for _, record := range errorRecords {
		if int64(record.ErrorCount) < maxRetries {
			allRecords = append(allRecords, record)
		}
	}

	return allRecords, nil
}

// BytesToMiB converts bytes to MiB (Mebibytes)
func BytesToMiB(bytes float64) float64 {
	const bytesInMiB = 1024 * 1024 // 1024^2
	return bytes / float64(bytesInMiB)
}

func setServiceLevelForCRR(schedule string) string {
	switch schedule {
	case vsa.VolumeReplicationSchedule10Minutely:
		return "1"
	case vsa.VolumeReplicationScheduleHourly:
		return "2"
	case vsa.VolumeReplicationScheduleDaily:
		return "3"
	default:
		return ""
	}
}
