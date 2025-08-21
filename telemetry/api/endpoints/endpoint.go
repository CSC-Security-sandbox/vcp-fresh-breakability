package api

import (
	"context"
	"log"

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
}

func NewHandler(vcpDatastore database.Storage, telemetryDatastore metricsdb.Storage, metricsProcessor processor.MetricsProcessor) Handler {
	return Handler{
		vcpDatastore:       vcpDatastore,
		telemetryDatastore: telemetryDatastore,
		metricsProcessor:   metricsProcessor,
	}
}

func (h Handler) V1Performance(ctx context.Context) (r oasgenserver.V1PerformanceRes, _ error) {
	logger := util.GetLogger(ctx)
	go func(parent context.Context) {
		backgroundContext := context.WithoutCancel(ctx)

		db := h.telemetryDatastore.SQLDB()
		queue := utils.NewQueue(db, &h.metricsProcessor)
		j := jobs.NewProcessPerformanceMetrics("{}")
		err := queue.Enqueue(backgroundContext, j, "performance")
		if err != nil {
			logger.Errorf("Failed to enqueue ProcessPerformanceMetrics job: %v", err)
			return
		}

		queues := []string{"performance"}
		if err := queue.Worker(backgroundContext, queues, &jobs.ProcessPerformanceMetrics{}); err != nil {
			log.Println(err)
		}
	}(ctx)
	return &oasgenserver.V1PerformanceAccepted{}, nil
}

func (h Handler) V1Usage(ctx context.Context) (r oasgenserver.V1UsageRes, _ error) {
	logger := util.GetLogger(ctx)
	go func(parent context.Context) {
		backgroundContext := context.WithoutCancel(ctx)

		db, err := h.telemetryDatastore.DB().DB()
		if err != nil {
			logger.Errorf("Failed to get telemetry datastore: %v", err)
			return
		}
		queue := utils.NewQueue(db, &h.metricsProcessor)
		j := jobs.NewProcessUsageMetrics("{}")
		err = queue.Enqueue(backgroundContext, j, "performance")
		if err != nil {
			logger.Errorf("Failed to enqueue ProcessUsageMetrics job: %v", err)
			return
		}

		queues := []string{"performance"}
		if err := queue.Worker(backgroundContext, queues, &jobs.ProcessUsageMetrics{}); err != nil {
			log.Println(err)
		}
	}(ctx)
	return &oasgenserver.V1UsageAccepted{}, nil
}
