package performance

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/collector"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func ProcessPerformanceMetrics() {
	ctx := context.Background()
	logger := util.GetLogger(ctx)
	logger.Infof("Process %s!\n", "Performance Metrics")
	orchestrator := database.InitializeDatabase()
	_, err := collector.GetPoolMetrics(orchestrator)
	if err != nil {
		logger.Error("Failed to get pool metrics", "error", err.Error())
	}
}
