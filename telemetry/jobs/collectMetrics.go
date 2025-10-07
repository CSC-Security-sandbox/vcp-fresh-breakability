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

type CollectMetrics struct {
	Data          string `json:"data"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

func NewCollectMetrics(data string) *CollectMetrics {
	return &CollectMetrics{
		Data: data,
	}
}

func (e CollectMetrics) Perform(p interface{}, attempt int32) error {
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
	logger.Infof("Processing collect metrics job with correlation ID: %s, attempt: %d, project: %s", e.CorrelationID, attempt, e.Data)

	// e.Data contains the project ID directly, no need to unmarshal
	err := proc.CollectMetrics(ctx, e.Data)
	if err != nil {
		if e.CorrelationID != "" {
			logger := util.GetLogger(ctx)
			logger.Errorf("Failed to collect metrics with correlation ID %s for project %s: %v", e.CorrelationID, e.Data, err)
		}
		return err
	}

	if e.CorrelationID != "" {
		logger := util.GetLogger(ctx)
		logger.Infof("Successfully collected metrics with correlation ID: %s for project: %s", e.CorrelationID, e.Data)
	}

	return nil
}

func (e CollectMetrics) Load(data string) (utils.Job, error) {
	var job CollectMetrics
	if err := json.Unmarshal([]byte(data), &job); err != nil {
		return CollectMetrics{}, err
	}
	return job, nil
}
