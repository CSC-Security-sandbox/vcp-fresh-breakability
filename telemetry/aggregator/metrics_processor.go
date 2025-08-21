package aggregator

import (
	"context"
	"fmt"
	"time"

	database2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
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
}

func NewBillingProvider(db database2.Storage, config *common.TelemetryConfig) *BillingProvider {
	return &BillingProvider{
		metricsdb: db,
		config:    config,
	}
}

// ProcessBillingMetrics processes raw metrics from cvt_metrics table and aggregates them
func (p *BillingProvider) ProcessBillingMetrics(ctx context.Context, aggregationEndTime time.Time) error {
	logger := util.GetLogger(ctx)
	aggregationStartTime := aggregationEndTime.Add(-1 * time.Hour)
	logger.Infof("Processing metrics from %v to %v", aggregationStartTime, aggregationEndTime)

	// Process each job definition
	for _, jobDef := range defaultAggregationJobDefinitions {
		// Create filter with conditions using our helper method
		filter := p.CreateFilterWithConditions(
			aggregationStartTime,
			aggregationEndTime,
			string(jobDef.ResourceType),
			string(jobDef.MeasuredType),
		)

		// Fetch metrics with filter conditions
		metrics, err := p.metricsdb.GetHydratedMetrics(ctx, filter)
		if err != nil {
			logger.Error("Failed to list hydrated metrics", "error", err.Error())
			return err
		}
		logger.Debugf("Fetched metrics for aggregation", "metrics:", metrics)
		// Skip if no metrics found
		if len(metrics) == 0 {
			continue
		}

		// Group metrics by resource
		resourceGroups := p.groupMetricsByResource(metrics)

		// Process each resource group
		for resourceIdentifier, resourceMetrics := range resourceGroups {
			if err := p.processMetricsWithJobDef(ctx, resourceIdentifier, resourceMetrics, jobDef, aggregationStartTime, aggregationEndTime); err != nil {
				logger.Errorf("Failed to process metrics for resource %s and customer id %s : %v", resourceIdentifier.ResourceName, resourceIdentifier.ConsumerID, err)
				continue
			}
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

func (p *BillingProvider) processMetricsWithJobDef(ctx context.Context, resourceUniqueIdentifier ResourceUniqueIdentifier, metrics []datamodel2.HydratedMetrics, jobDef AggregationJobDefinition, start, end time.Time) error {
	logger := util.GetLogger(ctx)
	if len(metrics) == 0 {
		logger.Infof("No metrics found for resource %s and customer id %s", resourceUniqueIdentifier.ResourceName, resourceUniqueIdentifier.ConsumerID)
		return nil
	}

	// Calculate aggregated value based on job type
	var quantity float64
	switch jobDef.AggregationType {
	case IntegralAggregation:
		quantity = Integral(metrics)
	case CounterAggregation:
		quantity = CounterDelta(metrics)
	case SumAggregation:
		quantity = Sum(metrics)
	case FirstAggregation:
		quantity = First(metrics)
	default:
		return fmt.Errorf("unsupported job type: %s", jobDef.AggregationType)
	}

	// Get last counter value for counter metrics
	var lastCounterValue *float64
	if jobDef.AggregationType == CounterAggregation && len(metrics) > 0 {
		val := metrics[len(metrics)-1].Quantity
		lastCounterValue = &val
	}

	// Create aggregated record with all available fields
	aggregated := &datamodel2.AggregatedUsage{
		AccountUuid:            &resourceUniqueIdentifier.ConsumerID,
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
		State:                  common.Unsubmitted,
		ErrorCount:             0,
		ErrorMessage:           nil,
		IsBillable:             isBillableMetric(ctx, metrics[0].ResourceType, metrics[0].MeasuredType),
		AggregationType:        string(jobDef.AggregationType),
	}

	logger.Infof("Processing metrics for resource %s and customer id %s with aggregation type %s and %s", resourceUniqueIdentifier.ResourceName, resourceUniqueIdentifier.ConsumerID, jobDef.AggregationType, aggregated)
	// Store aggregated metrics
	return p.metricsdb.CreateAggregatedUsage(ctx, aggregated)
}

func isBillableMetric(ctx context.Context, resourceType metadata.ResourceType, measuredType metadata.MeasuredType) bool {
	logger := util.GetLogger(ctx)
	// Create the lookup key
	key := metadata.CombinedKeyResourceTypeMeasuredType{
		ResourceType: resourceType,
		MeasuredType: measuredType,
	}

	// Look up the key in the pre-computed map
	jobDef, exists := defaultAggregationJobDefinitions[key]
	if !exists {
		logger.Warnf("No job definition found for resource type %s and measured type %s", resourceType, measuredType)
		return false // If the key does not exist, return false
	}
	return jobDef.IsBillable // Return true if the key exists, false otherwise
}
