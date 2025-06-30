package performance

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/collector"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func ProcessPerformanceMetrics() {
	ctx := context.Background()
	logger := util.GetLogger(ctx)
	telemetryConfig := common.LoadConfig()
	sink := NewSink(ctx, telemetryConfig)
	logger.Infof("Process %s!\n", "Performance Metrics")
	orchestrator := database.InitializeDatabase()
	poolMetrics, err := collector.GetPoolMetrics(orchestrator, telemetryConfig)
	if err != nil {
		logger.Error("Failed to get pool metrics", "error", err.Error())
	}
	sink.DeliverMetrics(ctx, poolMetrics)
}
