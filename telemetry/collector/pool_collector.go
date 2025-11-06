package collector

import (
	"context"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/utils"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	BaseThroughputMibps = 64.0
	IopsFactor          = 16.0
)

// PoolMetricsResult holds the results from GetPoolMetrics operation
type PoolMetricsResult struct {
	// HydratedMetrics contains the traditional hydrated metrics
	HydratedMetrics []entity.HydratedMetric
	// HydratedMetricsDataModel contains the data model hydrated metrics
	HydratedMetricsDataModel []datamodel2.HydratedMetrics
	// PoolMetadataMap contains a map of pool IDs to its metadata
	PoolMetadataMap map[int64]metadata.ResourceMetadata
}

// GetPoolMetrics retrieves metrics for all pools from the database and returns them in a structured result.
func GetPoolMetrics(ctx context.Context, vcpDB database.Storage, config *common.TelemetryConfig, timestamp time.Time) (*PoolMetricsResult, error) {
	logger := util.GetLogger(ctx)
	pools, err := vcpDB.ListPools(ctx, nil)
	if err != nil {
		logger.Error("Failed to list pools", "error", err.Error())
		return &PoolMetricsResult{}, err
	}
	logger.Info(fmt.Sprintf("Found %d pools", len(pools)))

	if len(pools) == 0 {
		return &PoolMetricsResult{}, fmt.Errorf("no pools found from DB")
	}

	// Initialize a slice to hold the hydrated metrics
	var metrics []entity.HydratedMetric
	var hydratedMetrics []datamodel2.HydratedMetrics

	// Create a pool metadata map for all pools
	poolMetadataMap := make(map[int64]metadata.ResourceMetadata)

	// Iterate over all pools and generate metrics
	for _, pool := range pools {
		if pool.Account == nil || pool.PoolAttributes == nil {
			logger.Warnf("Skipping pool %s (ID: %d) as it has no associated account or pool attributes", pool.Name, pool.ID)
			continue
		}
		// Assemble metadata for the pool
		poolMetadata := assemblePoolMetadata(pool, config)

		// Add to the pool metadata map
		poolMetadataMap[pool.ID] = poolMetadata

		// Create a metric for the pool
		metricsToCollect := []struct {
			measureType metadata.MeasuredType
			value       float64
		}{
			{metadata.PoolAllocatedSize, float64(pool.SizeInBytes)},
			{metadata.AllocatedUsed, float64(pool.QuotaInBytes)},
			{metadata.PoolTotalThroughputMibps, float64(pool.PoolAttributes.ThroughputMibps)},
			{metadata.PoolTotalIops, float64(pool.PoolAttributes.Iops)},
		}

		for _, m := range metricsToCollect {
			setupPoolMetric(&metrics, &hydratedMetrics, timestamp, poolMetadata, m.measureType, m.value, pool.Account.Name)
		}
	}

	// Return the structured result
	return &PoolMetricsResult{
		HydratedMetrics:          metrics,
		HydratedMetricsDataModel: hydratedMetrics,
		PoolMetadataMap:          poolMetadataMap,
	}, nil
}

func assemblePoolMetadata(pool *datamodel.PoolView, config *common.TelemetryConfig) metadata.ResourceMetadata {
	met := metadata.ResourceMetadata{}
	met.SetResourceUUID(pool.UUID)
	met.SetResourceName(pool.Name)
	met.SetResourceDisplayName(pool.Name)
	if pool.PoolAttributes.IsRegionalHA {
		met.SetResourceType(metadata.VolumePoolRegionalHA)
	} else {
		met.SetResourceType(metadata.VolumePool)
	}
	met.SetSizeInBytes(pool.SizeInBytes)
	met.SetRegionName(config.RegionName)
	if pool.Account != nil {
		met.SetAccountName(pool.Account.Name)
	}
	met.SetDeploymentName(pool.DeploymentName)
	met.SetThroughput(float64(pool.PoolAttributes.ThroughputMibps))
	met.SetResourceID(pool.ID)
	return met
}

func setupHydratedMetric(now time.Time, poolMetadata metadata.ResourceMetadata, metricType metadata.MeasuredType, value float64) entity.HydratedMetric {
	return entity.HydratedMetric{
		Timestamp:    entity.UnixNano(now.UnixNano()),
		Metadata:     poolMetadata,
		MeasuredType: metricType,
		Quantity:     value,
	}
}

func setupHydratedMetricsDataModel(measuredType metadata.MeasuredType, resourceType metadata.ResourceType, projectID string, resourceMetadata metadata.ResourceMetadata, timestamp time.Time, quantity float64) *datamodel2.HydratedMetrics {
	logger := util.GetLogger(context.Background())

	if err := utils.ValidateResourceMetadata(resourceMetadata); err != nil {
		logger.Warn("Skipping metric", "error", err.Error())
		return nil
	}

	switch measuredType {
	case metadata.PoolTotalThroughputMibps:
		quantity = max(quantity-BaseThroughputMibps, 0)
		// Billable Throughput = Total Throughput set by the user - 64 (Minimum throughput included in the base price)
	case metadata.PoolTotalIops:
		if resourceMetadata.Throughput == nil {
			logger.Warn("Setting IOPS quantity to 0 due to nil Throughput")
			quantity = 0
		} else {
			throughput := *resourceMetadata.Throughput
			quantity = quantity - IopsFactor*throughput
			// Billable IOPS = Total IOPS set by the user - (16 * total throughput set by the user for the pool)
		}
	}
	return &datamodel2.HydratedMetrics{
		MetricTimestamp: timestamp,
		MeasuredType:    measuredType,
		ConsumerID:      projectID,
		ResourceType:    resourceType,
		ResourceName:    *resourceMetadata.ResourceName,
		Location:        *resourceMetadata.RegionName,
		Quantity:        quantity,
		DeploymentName:  *resourceMetadata.DeploymentName,
	}
}

func setupPoolMetric(metrics *[]entity.HydratedMetric, hydratedMetrics *[]datamodel2.HydratedMetrics, timestamp time.Time, poolMetadata metadata.ResourceMetadata, measureType metadata.MeasuredType, value float64, accountName string) {
	metric := setupHydratedMetric(timestamp, poolMetadata, measureType, value)
	*metrics = append(*metrics, metric)
	if hydratedMetric := setupHydratedMetricsDataModel(metric.MeasuredType, metric.Metadata.ResourceType, accountName, poolMetadata, timestamp, value); hydratedMetric != nil {
		*hydratedMetrics = append(*hydratedMetrics, *hydratedMetric)
	}
}
