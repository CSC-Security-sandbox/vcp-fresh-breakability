package aggregator

import (
	"context"
	"fmt"
	"time"

	database2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type ResourceUniqueIdentifier struct {
	ResourceName string
	ConsumerID   string
	Location     string
}

type BillingProvider struct {
	metricsdb database2.Storage
	config    *common.TelemetryConfig
	usageSink common.UsageSink
}

func NewBillingProvider(db database2.Storage, config *common.TelemetryConfig, usageSink common.UsageSink) *BillingProvider {
	return &BillingProvider{
		metricsdb: db,
		config:    config,
		usageSink: usageSink,
	}
}

// ProcessBillingMetrics processes raw metrics from cvt_metrics table and aggregates them
func (p *BillingProvider) ProcessBillingMetrics(ctx context.Context, aggregationEndTime time.Time) error {
	logger := util.GetLogger(ctx)
	var aggregatedRecords []datamodel2.AggregatedUsage
	aggregationStartTime := aggregationEndTime.Add(-1 * time.Hour)
	logger.Infof("Processing metrics from %v to %v", aggregationStartTime, aggregationEndTime)

	// Get unsent/retry records from database
	aggregatedRecordsToRetry, err := p.getUnsentGoogleUsages(ctx, p.config.MaxGoogleBillingPushRetry)
	if err != nil {
		logger.Errorf("error getting unsent google usages", "error", err)
	} else {
		aggregatedRecords = append(aggregatedRecords, aggregatedRecordsToRetry...)
	}

	// Process each job definition
	for key, jobDef := range common.DefaultAggregationJobDefinitions {
		// Create filter with conditions using our helper method
		filter := p.CreateFilterWithConditions(
			aggregationStartTime,
			aggregationEndTime,
			key.ResourceType.String(),
			key.MeasuredType.String(),
		)

		// Fetch metrics with filter conditions
		metrics, err := p.metricsdb.GetHydratedMetrics(ctx, filter)
		if err != nil {
			logger.Error("Failed to list hydrated metrics", "error", err.Error())
			return err
		}
		logger.Debugf("Fetched metrics for aggregation", "metrics:", metrics)
		// Group metrics by resource
		resourceGroups := p.groupMetricsByResource(metrics)

		// Process each resource group
		for resourceIdentifier, resourceMetrics := range resourceGroups {
			if err := p.processMetricsWithJobDef(ctx, resourceIdentifier, resourceMetrics, jobDef, aggregationStartTime, aggregationEndTime, &aggregatedRecords); err != nil {
				logger.Errorf("Failed to process metrics for resource %s and customer id %s : %v", resourceIdentifier.ResourceName, resourceIdentifier.ConsumerID, err)
				continue
			}
		}
	}

	// Deliver all aggregated metrics at the end
	if len(aggregatedRecords) > 0 {
		if _, err := p.usageSink.DeliverMetrics(ctx, aggregatedRecords); err != nil {
			logger.Errorf("Failed to deliver aggregated metrics: %v", err)
		}
	}
	return nil
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

	return map[string]interface{}{
		"conditions": conditions,
	}
}

func (p *BillingProvider) groupMetricsByResource(metrics []datamodel2.HydratedMetrics) map[ResourceUniqueIdentifier][]datamodel2.HydratedMetrics {
	groups := make(map[ResourceUniqueIdentifier][]datamodel2.HydratedMetrics)
	for _, metric := range metrics {
		if metric.ResourceName != "" {
			identifier := ResourceUniqueIdentifier{
				ResourceName: metric.ResourceName,
				Location:     metric.Location,
				ConsumerID:   metric.ConsumerID,
			}
			groups[identifier] = append(groups[identifier], metric)
		}
	}
	return groups
}

func (p *BillingProvider) processMetricsWithJobDef(ctx context.Context, resourceUniqueIdentifier ResourceUniqueIdentifier, metrics []datamodel2.HydratedMetrics, jobDef common.AggregationJobDefinition, start, end time.Time, aggregatedRecords *[]datamodel2.AggregatedUsage) error {
	logger := util.GetLogger(ctx)
	if len(metrics) == 0 {
		logger.Infof("No metrics found for resource %s and customer id %s", resourceUniqueIdentifier.ResourceName, resourceUniqueIdentifier.ConsumerID)
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

	// Create aggregated record with all available fields
	aggregated := &datamodel2.AggregatedUsage{
		VendorCustomerID:       &resourceUniqueIdentifier.ConsumerID,
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
		BillingLabels:          nil,
		ReplicationDstVolumeID: nil,
		DoubleEncryption:       nil,
		State:                  datamodel2.Unsubmitted,
		ErrorCount:             0,
		ErrorMessage:           nil,
		IsBillable:             common.IsBillableMetric(ctx, metrics[0].ResourceType, metrics[0].MeasuredType),
		AggregationType:        string(jobDef.AggregationType),
	}

	logger.Infof("Processing metrics for resource %s and customer id %s with aggregation type %s and %s", resourceUniqueIdentifier.ResourceName, resourceUniqueIdentifier.ConsumerID, jobDef.AggregationType, aggregated)
	// Store aggregated metrics
	err := p.metricsdb.CreateAggregatedUsage(ctx, aggregated)
	if err != nil {
		logger.Errorf("Failed to create aggregated usage for resource %s and customer id %s: %v", resourceUniqueIdentifier.ResourceName, resourceUniqueIdentifier.ConsumerID, err)
		return err
	}
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
