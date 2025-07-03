package processor

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/collector"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/performance"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type Processor interface {
	ProcessPerformanceMetrics(ctx context.Context) error
}

type MetricsProcessor struct {
	vcpDatastore       database.Storage
	telemetryDatastore database.Storage
	sink               performance.Sink
}

func NewMetricsProcessor(vcpDatastore database.Storage, telemetryDatastore database.Storage, sink performance.Sink) *MetricsProcessor {
	return &MetricsProcessor{vcpDatastore: vcpDatastore, telemetryDatastore: telemetryDatastore, sink: sink}
}

func (mp *MetricsProcessor) ProcessPerformanceMetrics(ctx context.Context) error {
	logger := util.GetLogger(ctx)
	telemetryConfig := common.LoadConfig()
	logger.Infof("Process %s!\n", "Performance Metrics")

	poolMetrics, err := collector.GetPoolMetrics(ctx, mp.vcpDatastore, telemetryConfig)
	if err != nil {
		logger.Error("Failed to get pool metrics", "error", err.Error())
		return err
	}
	mp.sink.DeliverMetrics(ctx, poolMetrics)
	return nil
}
