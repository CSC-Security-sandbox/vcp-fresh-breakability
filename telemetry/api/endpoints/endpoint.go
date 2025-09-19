package api

import (
	"context"
	"fmt"
	metricsdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/api/telemetry-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/jobs"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/processor"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type Handler struct {
	oasgenserver.UnimplementedHandler
	vcpDatastore       database.Storage
	telemetryDatastore metricsdb.Storage
	metricsProcessor   processor.MetricsProcessor
	jobQueue           *utils.JobQueue
}

func NewHandler(vcpDatastore database.Storage, telemetryDatastore metricsdb.Storage, metricsProcessor processor.MetricsProcessor, jobQueue *utils.JobQueue) Handler {
	return Handler{
		vcpDatastore:       vcpDatastore,
		telemetryDatastore: telemetryDatastore,
		metricsProcessor:   metricsProcessor,
		jobQueue:           jobQueue,
	}
}

func (h Handler) V1Performance(ctx context.Context) (r oasgenserver.V1PerformanceRes, _ error) {
	backgroundContext := context.WithoutCancel(ctx)
	j := jobs.NewProcessPerformanceMetrics("{}")
	err := h.jobQueue.Enqueue(backgroundContext, j, "performance")
	if err != nil {
		return &oasgenserver.V1PerformanceInternalServerError{}, fmt.Errorf("failed to enqueue ProcessPerformanceMetrics job: %v", err)
	}
	return &oasgenserver.V1PerformanceAccepted{}, nil
}

func (h Handler) V1Usage(ctx context.Context) (r oasgenserver.V1UsageRes, _ error) {
	logger := util.GetLogger(ctx)
	backgroundContext := context.WithoutCancel(ctx)
	j := jobs.NewProcessUsageMetrics("{}")
	err := h.jobQueue.Enqueue(backgroundContext, j, "usage")
	if err != nil {
		logger.Errorf("Failed to enqueue ProcessUsageMetrics job: %v", err)
		return &oasgenserver.V1UsageInternalServerError{}, fmt.Errorf("failed to enqueue ProcessUsageMetrics job: %v", err)
	}
	return &oasgenserver.V1UsageAccepted{}, nil
}
