package api

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/api/telemetry-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/processor"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type Handler struct {
	oasgenserver.UnimplementedHandler
	vcpDatastore       database.Storage
	telemetryDatastore database.Storage
	metricsProcessor   processor.Processor
}

func NewHandler(vcpDatastore database.Storage, telemetryDatastore database.Storage, metricsProcessor processor.Processor) Handler {
	return Handler{
		vcpDatastore:       vcpDatastore,
		telemetryDatastore: telemetryDatastore,
		metricsProcessor:   metricsProcessor,
	}
}

func (h Handler) V1Performance(ctx context.Context) (r oasgenserver.V1PerformanceRes, _ error) {
	logger := util.GetLogger(ctx)
	if h.metricsProcessor == nil {
		logger.Errorf("metricsProcessor is nil in V1Performance handler")
		return &oasgenserver.V1PerformanceBadRequest{}, nil
	}
	go func(parent context.Context) {
		backgroundContext := context.WithoutCancel(ctx)
		_ = h.metricsProcessor.ProcessPerformanceMetrics(backgroundContext)
	}(ctx)
	return &oasgenserver.V1PerformanceAccepted{}, nil
}
