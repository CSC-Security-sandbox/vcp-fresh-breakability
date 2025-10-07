package jobs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type ProcessUsageMetrics struct {
	Data          string `json:"data"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

func NewProcessUsageMetrics(data string) *ProcessUsageMetrics {
	return &ProcessUsageMetrics{
		Data: data,
	}
}

func (e ProcessUsageMetrics) Perform(p interface{}, attempt int32) error {
	proc, ok := p.(common.VCPProcessor)
	if !ok {
		return fmt.Errorf("invalid processor type: %T", p)
	}

	// Create context with correlation ID for better logging
	ctx := context.Background()
	if e.CorrelationID != "" {
		// Set up logger fields with correlation ID
		loggerFields := log.Fields{
			"requestCorrelationID": e.CorrelationID,
		}
		// Create a logger with the correlation ID fields
		logger := log.NewLogger().WithFields("requestFields", loggerFields)

		// Set both the logger and the fields in context for maximum compatibility
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, loggerFields)
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, logger)
	}
	logger := util.GetLogger(ctx)
	logger.Infof("Processing usage metrics job with correlation ID: %s, attempt: %d", e.CorrelationID, attempt)

	err := proc.ProcessUsageMetrics(ctx)
	if err != nil {
		if e.CorrelationID != "" {
			logger := util.GetLogger(ctx)
			logger.Errorf("Failed to process usage metrics with correlation ID %s: %v", e.CorrelationID, err)
		}
		return err
	}

	if e.CorrelationID != "" {
		logger := util.GetLogger(ctx)
		logger.Infof("Successfully processed usage metrics with correlation ID: %s", e.CorrelationID)
	}

	return nil
}

func (e ProcessUsageMetrics) Load(data string) (utils.Job, error) {
	var job ProcessUsageMetrics
	if err := json.Unmarshal([]byte(data), &job); err != nil {
		return ProcessUsageMetrics{}, err
	}
	return job, nil
}
