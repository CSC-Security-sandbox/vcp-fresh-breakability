package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	database2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type ResourceKey struct {
	ResourceType   metadata.ResourceType
	ResourceName   string
	DeploymentName string
	ConsumerID     string
}

type Labels map[string]interface{}

type ResourceData struct {
	UUID      string
	AccountID int64
	Labels    Labels
}

type ResourceCollection struct {
	PoolData   map[ResourceKey]ResourceData
	VolumeData map[ResourceKey]ResourceData
}

type BillingProvider struct {
	metricsdb          database2.Storage
	vcpDataStore       database.Storage
	config             *common.TelemetryConfig
	usageSink          common.UsageSink
	resourceCollection *ResourceCollection
	usedKeys           map[ResourceKey]bool
}

func NewBillingProvider(db database2.Storage, vcpDB database.Storage, config *common.TelemetryConfig, usageSink common.UsageSink) *BillingProvider {
	return &BillingProvider{
		metricsdb:    db,
		vcpDataStore: vcpDB,
		config:       config,
		usageSink:    usageSink,
		resourceCollection: &ResourceCollection{
			PoolData:   make(map[ResourceKey]ResourceData),
			VolumeData: make(map[ResourceKey]ResourceData),
		},
		usedKeys: make(map[ResourceKey]bool),
	}
}

// ProcessBillingMetrics processes raw metrics from cvt_metrics table and aggregates them
func (p *BillingProvider) ProcessBillingMetrics(ctx context.Context, aggregationEndTime time.Time) error {
	logger := util.GetLogger(ctx)
	var aggregatedRecords []datamodel2.AggregatedUsage
	var aggregatedUsageForDB []datamodel2.AggregatedUsage // Collect all records for batch saving
	aggregationStartTime := aggregationEndTime.Add(-1 * time.Hour)
	logger.Infof("Processing metrics from %v to %v", aggregationStartTime, aggregationEndTime)

	// Fetch label values from VCP database at the start of each aggregator cycle
	if err := p.fetchResourceData(ctx); err != nil {
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
			metrics, err = p.metricsdb.GetHydratedMetrics(ctx, filter)
		}

		if err != nil {
			logger.Error("Failed to list hydrated metrics", "error", err.Error())
			return err
		}

		logger.Debugf("Fetched metrics for aggregation", "total_metrics:", len(metrics), "aggregation_type:", jobDef.AggregationType)

		// Group metrics by resource
		resourceGroups := p.groupMetricsByResource(metrics)

		// Process each resource group
		for resourceKey, resourceMetrics := range resourceGroups {
			if err := p.processMetricsWithJobDef(ctx, resourceKey, resourceMetrics, jobDef, aggregationStartTime, aggregationEndTime, &aggregatedRecords, &aggregatedUsageForDB); err != nil {
				logger.Errorf("Failed to process metrics for resource key %s : %v", resourceKey, err)
				continue
			}
		}
	}

	// Batch save all aggregated usage records
	if len(aggregatedUsageForDB) > 0 {
		batchSize := p.config.PushBatchSize

		err := p.metricsdb.CreateAggregatedUsageBatch(ctx, aggregatedUsageForDB, int(batchSize))
		if err != nil {
			logger.Errorf("Failed to batch save aggregated usage records: %v", err)
			return err
		}
		logger.Infof("Successfully saved %d aggregated usage records in batches of %d", len(aggregatedUsageForDB), batchSize)
	}

	// Deliver all aggregated metrics at the end
	if len(aggregatedRecords) > 0 {
		if _, err := p.usageSink.DeliverMetrics(ctx, aggregatedRecords); err != nil {
			logger.Errorf("Failed to deliver aggregated metrics: %v", err)
		}
	}

	// Cleanup unused resource keys in ResourceCollection Map asynchronously
	p.cleanupUnusedResourceKeys(ctx)

	return nil
}

// fetchResourceData fetches label values from pool and volume tables in VCP database
func (p *BillingProvider) fetchResourceData(ctx context.Context) error {
	logger := util.GetLogger(ctx)
	logger.Info("Fetching resource data from VCP database")

	var poolsDataError, volumeDataError error

	// Fetch pool labels
	if err := p.fetchPoolData(ctx); err != nil {
		logger.Errorf("Failed to fetch pool resource data: %v", err)
		poolsDataError = err
	}

	// Fetch volume labels
	if err := p.fetchVolumeData(ctx); err != nil {
		logger.Errorf("Failed to fetch volume labels: %v", err)
		volumeDataError = err
	}

	// Log summary of what was successfully fetched
	poolCount := len(p.resourceCollection.PoolData)
	volumeCount := len(p.resourceCollection.VolumeData)

	if poolsDataError != nil && volumeDataError != nil {
		logger.Errorf("Failed to fetch both pool and volume resource data. Pool error: %v, Volume error: %v", poolsDataError, volumeDataError)
		return fmt.Errorf("failed to fetch any resource data")
	} else if poolsDataError != nil {
		logger.Warnf("Failed to fetch pool resource data: %v, but successfully fetched %d volume resource data", poolsDataError, volumeCount)
	} else if volumeDataError != nil {
		logger.Warnf("Failed to fetch volume resource data: %v, but successfully fetched %d pool resource data", volumeDataError, poolCount)
	} else {
		logger.Infof("Successfully fetched resource data for %d pools and %d volumes", poolCount, volumeCount)
	}

	return nil
}

// fetchPoolData fetches labels from pool table using pagination
func (p *BillingProvider) fetchPoolData(ctx context.Context) error {
	logger := util.GetLogger(ctx)

	// Create filter for pools with labels
	filter := dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("pool_attributes IS NOT NULL AND pool_attributes ? 'labels'", "", nil),
	)

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
		pools, err := p.vcpDataStore.ListPoolsWithPagination(ctx, filter, pagination)
		if err != nil {
			return fmt.Errorf("failed to list pools (offset %d): %w", offset, err)
		}

		// Break if no records returned
		if len(pools) == 0 {
			break
		}

		// Process current batch
		for _, pool := range pools {
			// Extract and limit labels
			limitedLabels := p.limitLabels(pool.PoolAttributes.Labels)

			poolResourceData := ResourceData{
				UUID:      pool.UUID,
				AccountID: pool.AccountID,
				Labels:    limitedLabels,
			}
			id := ResourceKey{
				ResourceType:   metadata.VolumePool,
				ResourceName:   pool.Name,
				DeploymentName: pool.DeploymentName,
				ConsumerID:     pool.Account.Name,
			}
			p.resourceCollection.PoolData[id] = poolResourceData
		}

		totalProcessed += len(pools)
		batchCount++
		logger.Debugf("Processed %d pools in batch %d (offset: %d, total: %d)", len(pools), batchCount, offset, totalProcessed)

		// Update offset for next iteration
		offset += limit
	}

	logger.Infof("Fetched resource data for %d pools in %d batches", len(p.resourceCollection.PoolData), batchCount)
	return nil
}

// fetchVolumeData fetches labels from volume table using pagination
func (p *BillingProvider) fetchVolumeData(ctx context.Context) error {
	logger := util.GetLogger(ctx)

	// Create conditions for volumes with labels
	conditions := [][]interface{}{
		{"volume_attributes IS NOT NULL"},
		{"volume_attributes ? 'labels'"},
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
			// Extract and limit labels
			limitedLabels := p.limitLabels(volume.VolumeAttributes.Labels)

			volumeResourceData := ResourceData{
				UUID:      volume.UUID,
				AccountID: volume.AccountID,
				Labels:    limitedLabels,
			}
			id := ResourceKey{
				ResourceType:   metadata.Volume,
				ResourceName:   volume.Name,
				DeploymentName: volume.Pool.DeploymentName,
				ConsumerID:     volume.Account.Name,
			}
			p.resourceCollection.VolumeData[id] = volumeResourceData
		}

		totalProcessed += len(volumes)
		batchCount++
		logger.Debugf("Processed %d volumes in batch %d (offset: %d, total: %d)", len(volumes), batchCount, offset, totalProcessed)

		// Update offset for next iteration
		offset += limit
	}

	logger.Infof("Fetched resource data for %d volumes in %d batches", len(p.resourceCollection.VolumeData), batchCount)
	return nil
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
// and tracks the key as used for cleanup purposes
func (p *BillingProvider) getResourceDataForAggregationUsage(id ResourceKey, resourceType metadata.ResourceType) *ResourceData {
	var resourceData ResourceData
	var found bool

	// Track this key as used
	if p.usedKeys == nil {
		p.usedKeys = make(map[ResourceKey]bool)
	}
	p.usedKeys[id] = true

	// Get labels based on resource type
	switch resourceType {
	case metadata.VolumePool:
		resourceData, found = p.resourceCollection.PoolData[id]
	case metadata.Volume:
		resourceData, found = p.resourceCollection.VolumeData[id]
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
	allMetrics, err := p.metricsdb.GetHydratedMetrics(ctx, filter)
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

func (p *BillingProvider) processMetricsWithJobDef(ctx context.Context, resourceKey ResourceKey, metrics []datamodel2.HydratedMetrics, jobDef common.AggregationJobDefinition, start, end time.Time, aggregatedRecords *[]datamodel2.AggregatedUsage, aggregatedUsageForDB *[]datamodel2.AggregatedUsage) error {
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
	resourceData := p.getResourceDataForAggregationUsage(resourceKey, metrics[0].ResourceType)

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
		logger.Infof("No resourceData found for resource name %s, deployment name :%s", resourceKey.ResourceName, resourceKey.DeploymentName)
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
	}

	// Format labels for better readability
	labelsInfo := "none"
	if resourceData != nil && len(resourceData.Labels) > 0 {
		// Create a formatted string of key-value pairs
		var labelPairs []string
		for key, value := range resourceData.Labels {
			labelPairs = append(labelPairs, fmt.Sprintf("%s=%v", key, value))
		}
		labelsInfo = fmt.Sprintf("[%s]", strings.Join(labelPairs, ", "))
	}

	logger.Debugf("Processing metrics for resource %s (customer: %s, type: %s, labels: %s)",
		resourceKey.ResourceName, resourceKey.ConsumerID, jobDef.AggregationType, labelsInfo)

	// Collect aggregated record for batch saving
	*aggregatedUsageForDB = append(*aggregatedUsageForDB, *aggregated)

	if aggregated.IsBillable {
		*aggregatedRecords = append(*aggregatedRecords, *aggregated)
	}
	return nil
}

func (p *BillingProvider) getUnsentGoogleUsages(ctx context.Context, maxRetries int64) ([]datamodel2.AggregatedUsage, error) {
	var allRecords []datamodel2.AggregatedUsage

	// Get records with UNSUBMITTED state
	unsubmittedFilter := map[string]interface{}{
		"state":       datamodel2.Unsubmitted,
		"is_billable": true,
	}
	unsubmittedRecords, err := p.metricsdb.GetAggregatedUsage(ctx, unsubmittedFilter)
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
	errorRecords, err := p.metricsdb.GetAggregatedUsage(ctx, errorFilter)
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

// cleanupUnusedResourceKeys asynchronously removes unused keys from ResourceCollection Map
func (p *BillingProvider) cleanupUnusedResourceKeys(ctx context.Context) {
	go func() {
		logger := util.GetLogger(ctx)

		if p.usedKeys == nil {
			logger.Debug("No used keys tracked, skipping cleanup")
			return
		}

		deletedPoolCount := 0
		deletedVolumeCount := 0

		// Clean up unused pool keys
		for key := range p.resourceCollection.PoolData {
			if !p.usedKeys[key] {
				delete(p.resourceCollection.PoolData, key)
				deletedPoolCount++
			}
		}

		// Clean up unused volume keys
		for key := range p.resourceCollection.VolumeData {
			if !p.usedKeys[key] {
				delete(p.resourceCollection.VolumeData, key)
				deletedVolumeCount++
			}
		}

		logger.Infof("ResourceCollection Map cleanup completed - removed %d unused pool keys and %d unused volume keys",
			deletedPoolCount, deletedVolumeCount)

		// Reset used keys for next aggregation cycle
		p.usedKeys = nil
	}()
}
