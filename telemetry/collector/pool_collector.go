package collector

import (
	"context"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"time"
)

var (
	ctx    = context.Background()
	logger = util.GetLogger(ctx)
)

func GetPoolMetrics(orchestrator orchestrator.OrchestratorFactory) ([]entity.HydratedMetric, error) {
	pools, err := orchestrator.ListAllPools(ctx)
	if err != nil {
		logger.Error("Failed to list pools", "error", err.Error())
		return []entity.HydratedMetric{}, err
	}
	logger.Info(fmt.Sprintf("Found %d pools", len(pools)))

	if pools == nil {
		return []entity.HydratedMetric{}, fmt.Errorf("no pools found from DB")
	}

	// Initialize a slice to hold the hydrated metrics
	var metrics []entity.HydratedMetric

	// Iterate over all pools and generate metrics
	for _, pool := range pools {
		// Assemble metadata for the pool
		poolMetadata := assemblePoolMetadata(pool)

		// Create a metric for the pool
		now := time.Now()
		metric := setupHydratedMetric(now, poolMetadata, metadata.AllocatedSize, float64(pool.SizeInBytes))
		metrics = append(metrics, metric)
	}

	// Return the list of metrics
	return metrics, nil
}

func assemblePoolMetadata(pool *models.Pool) metadata.ResourceMetadata {
	met := metadata.ResourceMetadata{}
	met.SetResourceUUID(pool.UUID)
	met.SetResourceName(pool.Name)
	met.SetResourceType(metadata.VolumePool)
	met.SetSizeInBytes(int64(pool.SizeInBytes))
	met.SetRegionName(pool.Region)
	met.SetAccountName(pool.AccountName)
	return met
}

func setupHydratedMetric(now time.Time, poolMetadata metadata.ResourceMetadata, metricType metadata.MetricType, value float64) entity.HydratedMetric {
	return entity.HydratedMetric{
		Timestamp: now,
		Metadata:  poolMetadata,
		Type:      metricType,
		Value:     value,
	}
}
