package api

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	metricsdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/api/telemetry-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/jobs"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/processor"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	parseReportParams = utils.ParseBizOpsReportParams
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

func (h Handler) V1Performance(ctx context.Context, params oasgenserver.V1PerformanceParams) (r oasgenserver.V1PerformanceRes, _ error) {
	logger := util.GetLogger(ctx)

	// Extract correlation ID from parameters and add to context
	correlationID := ""
	if params.XCorrelationID.IsSet() {
		correlationID = params.XCorrelationID.Value
		logger.Infof("Processing performance metrics request with correlation ID: %s", correlationID)
	} else {
		// Generate a new correlation ID if not provided
		correlationID = uuid.NewString()
		logger.Infof("Processing performance metrics request with generated correlation ID: %s", correlationID)
	}

	// Add correlation ID to context for propagation
	loggerFields := log.Fields{
		"requestCorrelationID": correlationID,
	}
	// Create a logger with the correlation ID fields
	loggerWithFields := log.NewLogger().WithFields("requestFields", loggerFields)

	// Set both the logger and the fields in context for maximum compatibility
	ctxWithCorrelation := context.WithValue(ctx, middleware.TemporalSLoggerKey, loggerFields)
	ctxWithCorrelation = context.WithValue(ctxWithCorrelation, middleware.ContextSLoggerKey, loggerWithFields)
	backgroundContext := context.WithoutCancel(ctxWithCorrelation)

	j := jobs.NewProcessPerformanceMetrics("{}")
	j.CorrelationID = correlationID
	err := h.jobQueue.Enqueue(backgroundContext, j, utils.PerformanceQueue)
	if err != nil {
		logger.Errorf("Failed to enqueue ProcessPerformanceMetrics job with correlation ID %s: %v", correlationID, err)
		return &oasgenserver.V1PerformanceInternalServerError{}, fmt.Errorf("failed to enqueue ProcessPerformanceMetrics job: %v", err)
	}

	logger.Infof("Successfully enqueued performance metrics job with correlation ID: %s", correlationID)
	return &oasgenserver.V1PerformanceAccepted{}, nil
}

func (h Handler) V1Usage(ctx context.Context, params oasgenserver.V1UsageParams) (r oasgenserver.V1UsageRes, _ error) {
	logger := util.GetLogger(ctx)

	// Extract correlation ID from parameters and add to context
	correlationID := ""
	if params.XCorrelationID.IsSet() {
		correlationID = params.XCorrelationID.Value
		logger.Infof("Processing usage metrics request with correlation ID: %s", correlationID)
	} else {
		// Generate a new correlation ID if not provided
		correlationID = uuid.NewString()
		logger.Infof("Processing usage metrics request with generated correlation ID: %s", correlationID)
	}

	// Add correlation ID to context for propagation
	loggerFields := log.Fields{
		"requestCorrelationID": correlationID,
	}
	// Create a logger with the correlation ID fields
	loggerWithFields := log.NewLogger().WithFields("requestFields", loggerFields)

	// Set both the logger and the fields in context for maximum compatibility
	ctxWithCorrelation := context.WithValue(ctx, middleware.TemporalSLoggerKey, loggerFields)
	ctxWithCorrelation = context.WithValue(ctxWithCorrelation, middleware.ContextSLoggerKey, loggerWithFields)
	backgroundContext := context.WithoutCancel(ctxWithCorrelation)

	j := jobs.NewProcessUsageMetrics("{}")
	j.CorrelationID = correlationID
	err := h.jobQueue.Enqueue(backgroundContext, j, utils.UsageQueue)
	if err != nil {
		logger.Errorf("Failed to enqueue ProcessUsageMetrics job with correlation ID %s: %v", correlationID, err)
		return &oasgenserver.V1UsageInternalServerError{}, fmt.Errorf("failed to enqueue ProcessUsageMetrics job: %v", err)
	}

	logger.Infof("Successfully enqueued usage metrics job with correlation ID: %s", correlationID)
	return &oasgenserver.V1UsageAccepted{}, nil
}

func (h Handler) V1GenerateReport(ctx context.Context, req oasgenserver.OptGenerateReportV1beta) (r oasgenserver.V1GenerateReportRes, _ error) {
	logger := util.GetLogger(ctx)
	backgroundContext := context.WithoutCancel(ctx)
	params, err := getGenerateReportParams(req)
	if err != nil {
		logger.Errorf("Failed to get generate report params: %v", err)
		return &oasgenserver.V1GenerateReportInternalServerError{}, fmt.Errorf("failed to get generate report params: %v", err)
	}
	j := jobs.NewBizOpsReport(params)
	logger.Infof("Handler Bizops with params: %s!\n", params)
	err = h.jobQueue.Enqueue(backgroundContext, j, utils.BizOpsReportQueue)
	if err != nil {
		logger.Errorf("Failed to enqueue BizOpsReport job: %v", err)
		return &oasgenserver.V1GenerateReportInternalServerError{}, fmt.Errorf("failed to enqueue GenerateReport job: %v", err)
	}
	return &oasgenserver.V1GenerateReportAccepted{}, nil
}

func getGenerateReportParams(req oasgenserver.OptGenerateReportV1beta) (*utils.BizOpsReportParams, error) {
	reportParams := &utils.BizOpsReportParams{
		TimeZone: "UTC",      // Need to set from env or config
		SinkType: "terminal", // Need to set from env or config
	}
	if req.Value.TimeZone.Value != "" {
		reportParams.TimeZone = fmt.Sprint(req.Value.TimeZone.Value)
	}
	if req.Value.SinkType.Value != "" {
		reportParams.SinkType = fmt.Sprint(req.Value.SinkType.Value)
	}
	if !req.Value.StartDate.Value.IsZero() {
		reportParams.StartDate = req.Value.StartDate.Value
	}
	err := parseReportParams(reportParams)
	if err != nil {
		return nil, fmt.Errorf("failed to parse BizOps report params: %v", err)
	}
	return reportParams, nil
}
