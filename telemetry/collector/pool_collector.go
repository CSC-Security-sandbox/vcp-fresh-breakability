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
		metric := setupHydratedMetric(timestamp, poolMetadata, metadata.PoolAllocatedSize, float64(pool.SizeInBytes))
		metrics = append(metrics, metric)
		hydratedMetrics = append(hydratedMetrics, setupHydratedMetricsDataModel(metric.MeasuredType, metric.Metadata.ResourceType, pool.Account.Name, poolMetadata, timestamp, float64(pool.SizeInBytes)))

		metric = setupHydratedMetric(timestamp, poolMetadata, metadata.AllocatedUsed, float64(pool.QuotaInBytes))
		metrics = append(metrics, metric)
		hydratedMetrics = append(hydratedMetrics, setupHydratedMetricsDataModel(metric.MeasuredType, metric.Metadata.ResourceType, pool.Account.Name, poolMetadata, timestamp, float64(pool.QuotaInBytes)))
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

func setupHydratedMetricsDataModel(measuredType metadata.MeasuredType, resourceType metadata.ResourceType, projectID string, resourceMetadata metadata.ResourceMetadata, timestamp time.Time, quantity float64) datamodel2.HydratedMetrics {
	return datamodel2.HydratedMetrics{
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
