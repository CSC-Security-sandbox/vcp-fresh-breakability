package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/jobs"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
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

type CounterAggregationCacheResourceKey struct {
	ResourceUUID string
	MeasuredType metadata.MeasuredType
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
	jobQueue     *utils.JobQueue
}

func NewBillingProvider(db database2.Storage, vcpDB database.Storage, config *common.TelemetryConfig, usageSink common.UsageSink) *BillingProvider {
	return &BillingProvider{
		metricsDB:    db,
		vcpDataStore: vcpDB,
		config:       config,
		usageSink:    usageSink,
	}
}

// GetUnsentGoogleUsages retrieves unsent Google usage records within the aggregation time window
func (p *BillingProvider) GetUnsentGoogleUsages(ctx context.Context, maxRetries int64, aggregationEndTime time.Time) ([]datamodel2.AggregatedUsage, error) {
	return p.getUnsentGoogleUsages(ctx, maxRetries, aggregationEndTime)
}

// GetUsageSink returns the usage sink for delivering metrics
func (p *BillingProvider) GetUsageSink() common.UsageSink {
	return p.usageSink
}

func (p *BillingProvider) SetJobQueue(q *utils.JobQueue) {
	p.jobQueue = q
}

func (p *BillingProvider) GetJobQueue() *utils.JobQueue {
	return p.jobQueue
}

// ProcessBillingMetrics processes raw metrics from cvt_metrics table and aggregates them
func (p *BillingProvider) ProcessBillingMetrics(ctx context.Context, aggregationEndTime time.Time) error {
	startTime := time.Now()
	logger := util.GetLogger(ctx)
	var aggregatedRecords []datamodel2.AggregatedUsage
	aggregationStartTime := aggregationEndTime.Add(-1 * time.Hour)
	logger.Infof("Processing metrics from %v to %v", aggregationStartTime, aggregationEndTime)

	// Fetch label values from VCP database at the start of each aggregator cycle
	resourceCollection, err := p.fetchResourceData(ctx, aggregationStartTime, aggregationEndTime)
	if err != nil {
		logger.Errorf("Failed to fetch resource data: %v", err)
	}

	// Populate BackfillLimit for all DefaultAggregationJobDefinitions based on config
	p.populateBackfillLimit(logger)

	// Pre-fetch all counter values for optimization
	counterCache, cacheErr := p.preloadCounterValues(ctx, aggregationStartTime, aggregationEndTime, logger)
	if cacheErr != nil {
		logger.Warnf("Failed to preload counter values : %v", cacheErr)
		counterCache = make(map[CounterAggregationCacheResourceKey]*float64) // Initialize empty cache
	}
	logger.Debugf("Counter cache loaded with %d entries: %v", len(counterCache), counterCache)

	// Process each job definition
	for key, jobDef := range common.DefaultAggregationJobDefinitions {
		var metrics []datamodel2.HydratedMetrics
		var err error

		if jobDef.AggregationType == common.IntegralAggregation || jobDef.AggregationType == common.CounterAggregation {
			// For counter aggregation and integral aggregation, we need:
			// 1. All records from current aggregation window
			// 2. Only the latest record from previous period (closest to aggregation start)
			// 3. Only the earliest record from next period (closest to aggregation end)
			metrics, err = p.fetchMetricsForCounterAndIntegralAggregation(ctx, aggregationStartTime, aggregationEndTime, key.ResourceType.String(), key.MeasuredType.String(), jobDef.TimeSeriesFormatter.GetBackfillLimit())
			if err != nil {
				logger.Error("Failed to list hydrated metrics", "error", err.Error())
				continue
			}
		}
		logger.Debugf("Fetched %d metrics for aggregation - ResourceType: %s, MeasuredType: %s",
			len(metrics), key.ResourceType.String(), key.MeasuredType.String())
		// Group metrics by resource
		resourceGroups := p.groupMetricsByResource(metrics)

		// Process each resource group
		for resourceIdentifier, resourceMetrics := range resourceGroups {
			// Inject metricsDB into CounterMetricsFormatter if applicable
			if counterFormatter, ok := jobDef.TimeSeriesFormatter.(*common.CounterMetricsFormatter); ok {
				counterFormatter.MetricsDB = p.metricsDB
			}

			// Format the raw metrics into time series using the job definition's formatter.
			// The formatter groups metrics by metadata changes and applies trimming logic based on
			// aggregation type (Counter/Integral/etc). For counter metrics, it includes the last
			// datapoint from the previous period for delta calculation. For integral metrics, it
			// may include the first datapoint from the next period. Returns a slice of TimeSeries,
			// where each TimeSeries represents a continuous period with consistent metadata.
			series := jobDef.TimeSeriesFormatter.Format(ctx, logger, resourceMetrics, aggregationStartTime, aggregationEndTime)

			// loop through each series and process metrics
			for _, metricseries := range series {
				logger.Debugf("Collected timeseries %s, %s, %v for resource %s and customer id %s ", metricseries.AggregationStart, metricseries.AggregationEnd, metricseries.DataPoints, resourceIdentifier.ResourceName, resourceIdentifier.ConsumerID)
				if err := p.processMetricsWithJobDef(ctx, resourceIdentifier, metricseries, jobDef, metricseries.AggregationStart, metricseries.AggregationEnd, resourceCollection, &aggregatedRecords, counterCache, logger); err != nil {
					logger.Errorf("Failed to process metrics for resource %s and customer id %s : %v", resourceIdentifier.ResourceName, resourceIdentifier.ConsumerID, err)
					continue
				}
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
	if len(aggregatedRecords) > 0 && p.jobQueue != nil {
		j := jobs.NewDeliverBillingMetrics(aggregationEndTime)
		err = p.jobQueue.Enqueue(ctx, j, utils.BillingRetryQueue)
		if err != nil {
			logger.Errorf("Failed to enqueue BillingRetry jobs: %v", err)
			return err
		}
	}

	elapsed := time.Since(startTime)
	logger.Infof("ProcessBillingMetrics completed successfully in %v (%.2f seconds). Processed %d aggregated records",
		elapsed, elapsed.Seconds(), len(aggregatedRecords))

	return nil
}

// populateBackfillLimit sets the BackfillLimit for all job definitions based on config
func (p *BillingProvider) populateBackfillLimit(logger log.Logger) {
	for key, jobDef := range common.DefaultAggregationJobDefinitions {
		// Set BackfillLimit based on aggregation type
		if jobDef.AggregationType == common.IntegralAggregation {
			jobDef.TimeSeriesFormatter.SetBackfillLimit(time.Duration(p.config.IntervalBackfillLimitMinutes) * time.Minute)
			logger.Debugf("Set BackfillLimit to %v for IntegralAggregation: %s/%s",
				jobDef.TimeSeriesFormatter.GetBackfillLimit(), key.ResourceType, key.MeasuredType)
		} else if jobDef.AggregationType == common.CounterAggregation {
			jobDef.TimeSeriesFormatter.SetBackfillLimit(time.Duration(p.config.CounterBackfillLimitMinutes) * time.Minute)
			logger.Debugf("Set BackfillLimit to %v for CounterAggregation: %s/%s",
				jobDef.TimeSeriesFormatter.GetBackfillLimit(), key.ResourceType, key.MeasuredType)
		}
	}
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
				ResourceName:   backup.VolumeUUID,
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

			// Check if we need to skip files volumes
			if !p.config.EnableFilesReplicationBillingMetrics {
				if volumeReplication.Volume == nil || volumeReplication.Volume.VolumeAttributes == nil || volumeReplication.Volume.VolumeAttributes.Protocols == nil {
					logger.Warnf("Skipping volume replication %s (%s) due to missing volume or volume attributes", volumeReplication.Name, volumeReplication.UUID)
					continue
				}
				if !slices.Contains(volumeReplication.Volume.VolumeAttributes.Protocols, "ISCSI") {
					logger.Debugf("Skipping volume replication %s (%s) - volume protocol is not ISCSI (protocols: %v)", volumeReplication.Name, volumeReplication.UUID, volumeReplication.Volume.VolumeAttributes.Protocols)
					continue
				}
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

func (p *BillingProvider) groupMetricsByResource(metrics []datamodel2.HydratedMetrics) map[ResourceKey][]entity.HydratedMetric {
	groups := make(map[ResourceKey][]entity.HydratedMetric)
	for _, metric := range metrics {
		if metric.ResourceName != "" {
			identifier := ResourceKey{
				ResourceName:   metric.ResourceName,
				DeploymentName: metric.DeploymentName,
				ConsumerID:     metric.ConsumerID,
				ResourceType:   metric.ResourceType,
			}
			hydratedMetric := entity.HydratedMetric{
				Quantity:     metric.Quantity,
				MeasuredType: metric.MeasuredType,
				Timestamp:    entity.UnixNano(metric.MetricTimestamp.UnixNano()),
				Metadata: metadata.ResourceMetadata{
					ResourceType:   metric.ResourceType,
					ResourceName:   &metric.ResourceName,
					DeploymentName: &metric.DeploymentName,
					AccountName:    &metric.ConsumerID,
					RegionName:     &metric.Location,
				},
			}
			groups[identifier] = append(groups[identifier], hydratedMetric)
		}
	}
	return groups
}

// fetchMetricsForCounterAndIntegralAggregation fetches metrics for counter aggregation using pagination
// This gets all records from the current aggregation window plus the latest record from the previous period
func (p *BillingProvider) fetchMetricsForCounterAndIntegralAggregation(ctx context.Context, aggregationStartTime, aggregationEndTime time.Time, resourceType, measuredType string, backfillLimit time.Duration) ([]datamodel2.HydratedMetrics, error) {
	// Create a complex filter that sorts by resource and timestamp

	//	Look ahead 1 hour before and after the currenbt aggregation cycle. This is used in Forward and Backward aggregation scenarios. Forward aggregation
	//	is used for integral aggregation where we need the latest record after the aggregation end time. Backward aggregation is used for counter aggregation
	//	where we need the latest record before the aggregation start time.
	filter := p.CreateComplexFilter(map[string]interface{}{
		"startTime":    aggregationStartTime.Add(-(backfillLimit / 60) * time.Hour), // Look back 1 hour before aggregation start
		"endTime":      aggregationEndTime.Add((backfillLimit / 60) * time.Hour),    // Look ahead 1 hour after aggregation end
		"resourceType": resourceType,
		"measuredType": measuredType,
		"order":        "resource_name, deployment_name, consumer_id, metric_timestamp DESC", // Database sorts for us
	})

	// Fetch all metrics using pagination to handle large datasets efficiently
	allMetrics, err := p.fetchAllHydratedMetricsWithPagination(ctx, filter)
	if err != nil {
		return nil, err
	}
	slices.Reverse(allMetrics) // Reverse to have ASC order
	return allMetrics, nil
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

func (p *BillingProvider) processMetricsWithJobDef(ctx context.Context, resourceKey ResourceKey, metrics common.TimeSeries, jobDef common.AggregationJobDefinition, start, end time.Time, resourceCollection *ResourceCollection, aggregatedRecords *[]datamodel2.AggregatedUsage, counterCache map[CounterAggregationCacheResourceKey]*float64, logger log.Logger) error {
	if len(metrics.DataPoints) == 0 {
		logger.Infof("No metrics found for resource key %s and customer id %s", resourceKey, resourceKey.ConsumerID)
		return nil
	}

	// Get resource data for the resource
	resourceData := p.getResourceDataForAggregationUsage(resourceKey, metrics.Metadata.ResourceType, resourceCollection)
	if resourceData == nil {
		logger.Warnf("No resource data found for resource name %s, deployment name :%s, customer ID : %s", resourceKey.ResourceName, resourceKey.DeploymentName, resourceKey.ConsumerID)
		return nil
	}

	// Calculate aggregated value based on job type
	var quantity float64
	switch jobDef.AggregationType {
	case common.IntegralAggregation:
		quantity = common.Integral(metrics.DataPoints)
	case common.CounterAggregation:
		// Use the new method that considers previous aggregated counter values
		quantity = p.calculateCounterDeltaWithAggregatedHistory(ctx, resourceKey, metrics.DataPoints, metrics.MeasuredType, start, counterCache, resourceData.UUID, logger)
	case common.SumAggregation:
		quantity = common.Sum(metrics.DataPoints)
	case common.FirstAggregation:
		quantity = common.First(metrics.DataPoints)
	default:
		return fmt.Errorf("unsupported job type: %s", jobDef.AggregationType)
	}

	// Get last counter value for counter metrics TODO Rishabh: verify if this is needed
	var lastCounterValue *float64
	if jobDef.AggregationType == common.CounterAggregation && len(metrics.DataPoints) > 0 {
		val := metrics.DataPoints[len(metrics.DataPoints)-1].Quantity
		lastCounterValue = &val
	}

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

	if metrics.MeasuredType != metadata.PoolTotalIops && metrics.MeasuredType != metadata.PoolTotalThroughputMibps {
		quantity = BytesToMiB(quantity)
	}

	// Create aggregated record with all available fields
	aggregated := &datamodel2.AggregatedUsage{
		ResourceUUID:           resourceUUID,
		AccountID:              accountID,
		VendorCustomerID:       &resourceKey.ConsumerID,
		AggregationStart:       start,
		AggregationEnd:         end,
		MeasuredType:           metrics.MeasuredType,
		ResourceType:           resourceKey.ResourceType,
		Quantity:               quantity,
		ResourceName:           &resourceKey.ResourceName,
		RegionName:             metrics.Metadata.RegionName,
		LastCounterValue:       lastCounterValue,
		SourceRegion:           metrics.Metadata.RegionName,
		DestinationRegion:      nil,
		BillingLabels:          billingLabelsJSON,
		ReplicationDstVolumeID: nil,
		DoubleEncryption:       nil,
		State:                  datamodel2.Unsubmitted,
		ErrorCount:             0,
		ErrorMessage:           nil,
		IsBillable:             common.IsBillableMetric(ctx, metrics.Metadata.ResourceType, metrics.MeasuredType),
		AggregationType:        string(jobDef.AggregationType),
		ServiceLevel:           unifiedServiceType,
	}

	if aggregated.MeasuredType == metadata.BackupLogicalSize {
		aggregated.DestinationRegion = metrics.Metadata.RegionName
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

func (p *BillingProvider) getUnsentGoogleUsages(ctx context.Context, maxRetries int64, aggregationEndTime time.Time) ([]datamodel2.AggregatedUsage, error) {
	aggregationStartTime := aggregationEndTime.Add(-1 * time.Hour)
	var allRecords []datamodel2.AggregatedUsage

	// Get records with UNSUBMITTED state within the aggregation time window
	unsubmittedConditions := [][]interface{}{
		{"state = ?", datamodel2.Unsubmitted},
		{"is_billable = ?", true},
		{"aggregation_end <= ?", aggregationEndTime},
		{"aggregation_start >= ?", aggregationStartTime},
	}
	unsubmittedRecords, err := p.fetchAllRecordsWithPagination(ctx, unsubmittedConditions)
	if err != nil {
		return nil, err
	}
	allRecords = append(allRecords, unsubmittedRecords...)

	// Get records with ERROR state within the aggregation time window
	errorConditions := [][]interface{}{
		{"state = ?", datamodel2.Error},
		{"aggregation_end <= ?", aggregationEndTime},
		{"aggregation_start >= ?", aggregationStartTime},
	}
	errorRecords, err := p.fetchAllRecordsWithPagination(ctx, errorConditions)
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

// fetchAllRecordsWithPagination fetches all aggregated usage records with pagination to handle large datasets efficiently
func (p *BillingProvider) fetchAllRecordsWithPagination(ctx context.Context, conditions [][]interface{}) ([]datamodel2.AggregatedUsage, error) {
	var allRecords []datamodel2.AggregatedUsage
	pageSize := p.config.PoolVolumeLabelPageSize
	offset := 0

	for {
		pagination := &dbutils.Pagination{
			Limit:  pageSize,
			Offset: offset,
		}

		records, err := p.metricsDB.GetAggregatedUsageWithPagination(ctx, conditions, pagination)
		if err != nil {
			return nil, err
		}

		if len(records) == 0 {
			break // No more records
		}

		allRecords = append(allRecords, records...)
		offset += len(records)

		// Break if we get fewer records than page size (last page)
		if len(records) < pageSize {
			break
		}
	}

	return allRecords, nil
}

// fetchAllHydratedMetricsWithPagination fetches all hydrated metrics with pagination to handle large datasets efficiently
func (p *BillingProvider) fetchAllHydratedMetricsWithPagination(ctx context.Context, filter map[string]interface{}) ([]datamodel2.HydratedMetrics, error) {
	var allMetrics []datamodel2.HydratedMetrics
	pageSize := p.config.PoolVolumeLabelPageSize
	offset := 0

	// Extract conditions from filter if present
	var conditions [][]interface{}
	if conditionsFromFilter, ok := filter["conditions"]; ok {
		if condArr, ok := conditionsFromFilter.([][]interface{}); ok {
			conditions = condArr
		}
	}

	for {
		pagination := &dbutils.Pagination{
			Limit:  pageSize,
			Offset: offset,
		}

		metrics, err := p.metricsDB.GetHydratedMetricsWithPagination(ctx, conditions, pagination)
		if err != nil {
			return nil, err
		}

		if len(metrics) == 0 {
			break // No more records
		}

		allMetrics = append(allMetrics, metrics...)
		offset += len(metrics)

		// Break if we get fewer records than page size (last page)
		if len(metrics) < pageSize {
			break
		}
	}

	return allMetrics, nil
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

// fetchAndCacheCounterValues fetches all latest aggregated counter values using pagination and builds the cache
func (p *BillingProvider) fetchAndCacheCounterValues(ctx context.Context, aggregationType string, pageSize int, logger log.Logger) (map[CounterAggregationCacheResourceKey]*float64, error) {
	result := make(map[CounterAggregationCacheResourceKey]*float64)
	offset := 0
	totalProcessed := 0
	batchCount := 0
	cachedCount := 0

	// Use pagination to fetch all records and build cache
	for {
		// Fetch paginated records using the database method
		usageRecords, err := p.metricsDB.GetLatestAggregatedUsageForAllResources(ctx, aggregationType, pageSize, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch latest counter records from aggregated_usages (offset %d): %v", offset, err)
		}

		// Break if no records returned
		if len(usageRecords) == 0 {
			break
		}

		// Process current batch and populate cache
		for i := range usageRecords {
			record := &usageRecords[i]
			if record.LastCounterValue == nil {
				continue
			}

			// Use CounterAggregationCacheResourceKey as the cache key
			cacheKey := CounterAggregationCacheResourceKey{
				ResourceUUID: record.ResourceUUID,
				MeasuredType: record.MeasuredType,
			}
			result[cacheKey] = record.LastCounterValue
			cachedCount++
			logger.Debugf("Cached counter value %.2f for ResourceUUID %s, MeasuredType %s from DB query",
				*record.LastCounterValue, record.ResourceUUID, record.MeasuredType)
		}

		totalProcessed += len(usageRecords)
		batchCount++
		logger.Debugf("Processed %d records in batch %d (offset: %d, total processed: %d, cached: %d)",
			len(usageRecords), batchCount, offset, totalProcessed, cachedCount)

		// Update offset for next iteration
		offset += pageSize
	}

	logger.Infof("Preloaded %d counter values into cache from %d total records in %d batches", cachedCount, totalProcessed, batchCount)
	return result, nil
}

// preloadCounterValues fetches the latest counter values for all resources directly from the database
// using a single query with window functions and pagination
func (p *BillingProvider) preloadCounterValues(ctx context.Context, aggregationStartTime, aggregationEndTime time.Time, logger log.Logger) (map[CounterAggregationCacheResourceKey]*float64, error) {
	// Get page size from config
	pageSize := p.config.PoolVolumeLabelPageSize

	// Fetch and cache counter values with pagination
	return p.fetchAndCacheCounterValues(ctx, "CounterAggregation", pageSize, logger)
}

// calculateCounterDeltaWithAggregatedHistory adds the last aggregated counter value
// as first data point and uses the existing CounterDelta logic
func (p *BillingProvider) calculateCounterDeltaWithAggregatedHistory(ctx context.Context, resourceKey ResourceKey, dataPoints []common.DataPoint, measuredType metadata.MeasuredType, aggregationStartTime time.Time, counterCache map[CounterAggregationCacheResourceKey]*float64, resourceUUID string, logger log.Logger) float64 {
	if len(dataPoints) == 0 {
		return 0
	}

	// Create the cache key using ResourceUUID and MeasuredType
	cacheKey := CounterAggregationCacheResourceKey{
		ResourceUUID: resourceUUID,
		MeasuredType: measuredType,
	}
	lastAggregatedCounterValue := counterCache[cacheKey]

	// If we have a previous aggregated counter value, add it as the first data point
	if lastAggregatedCounterValue != nil {
		// Create a synthetic data point with the last aggregated counter value
		// Use aggregationStartTime - 1 minute to ensure it comes before current cycle data
		lastCounterDataPoint := common.DataPoint{
			Timestamp: aggregationStartTime.Add(-1 * time.Minute),
			Quantity:  *lastAggregatedCounterValue,
		}

		// Prepend the last counter value to the data points
		enhancedDataPoints := append([]common.DataPoint{lastCounterDataPoint}, dataPoints...)

		logger.Debugf("Added last counter value %.2f from cache as starting point for resource %s, measured type %s",
			*lastAggregatedCounterValue, resourceUUID, measuredType)

		// Use existing CounterDelta logic with enhanced data points
		return common.CounterDelta(enhancedDataPoints, logger)
	}

	// No previous aggregated value found, use standard counter delta calculation
	logger.Debugf("No previous aggregated counter value found for resource %s, measured type %s, using standard CounterDelta", resourceUUID, measuredType)
	return common.CounterDelta(dataPoints, logger)
}
