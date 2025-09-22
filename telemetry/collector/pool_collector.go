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
}

// GetPoolMetrics retrieves metrics for all pools from the database and returns them in a structured result.
func GetPoolMetrics(ctx context.Context, vcpDB database.Storage, config *common.TelemetryConfig) (*PoolMetricsResult, error) {
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

	// Iterate over all pools and generate metrics
	for _, pool := range pools {
		// Assemble metadata for the pool
		poolMetadata := assemblePoolMetadata(pool, config)

		// Create a metric for the pool
		now := time.Now()
		metric := setupHydratedMetric(now, poolMetadata, metadata.PoolAllocatedSize, float64(pool.SizeInBytes))
		metrics = append(metrics, metric)
		hydratedMetrics = append(hydratedMetrics, setupHydratedMetricsDataModel(metric.MeasuredType, metric.Metadata.ResourceType, pool.Account.Name, poolMetadata, now, float64(pool.SizeInBytes)))

		metric = setupHydratedMetric(now, poolMetadata, metadata.AllocatedUsed, float64(pool.UsedBytes))
		metrics = append(metrics, metric)
		hydratedMetrics = append(hydratedMetrics, setupHydratedMetricsDataModel(metric.MeasuredType, metric.Metadata.ResourceType, pool.Account.Name, poolMetadata, now, float64(pool.UsedBytes)))
	}

	// Return the structured result
	return &PoolMetricsResult{
		HydratedMetrics:          metrics,
		HydratedMetricsDataModel: hydratedMetrics,
	}, nil
}

func assemblePoolMetadata(pool *datamodel.PoolView, config *common.TelemetryConfig) metadata.ResourceMetadata {
	met := metadata.ResourceMetadata{}
	met.SetResourceUUID(pool.UUID)
	met.SetResourceName(pool.Name)
	met.SetResourceDisplayName(pool.Name)
	met.SetResourceType(metadata.VolumePool)
	met.SetSizeInBytes(pool.SizeInBytes)
	met.SetRegionName(config.RegionName)
	met.SetAccountName(pool.Account.Name)
	met.SetDeploymentName(pool.DeploymentName)
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
