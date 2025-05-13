package util

import (
	"context"
	"errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"go.temporal.io/sdk/workflow"
)

type contextHandler interface {
	extractLogger() (log.Logger, error)
}

type apiContext struct {
	ctx context.Context
}

func (c apiContext) extractLogger() (log.Logger, error) {
	if loggerFields, ok := c.ctx.Value(middleware.TemporalSLoggerKey).(log.Fields); ok {
		// Constructing back the logger with the fields extracted from the api context
		logger := log.NewLogger().WithFields("requestFields", loggerFields)
		return logger, nil
	}
	return nil, errors.New("no logger found in api context")
}

type workflowContext struct {
	ctx workflow.Context
}

// extractLogger extracts the logger from the workflow context
func (c workflowContext) extractLogger() (log.Logger, error) {
	if loggerFields, ok := c.ctx.Value(middleware.TemporalSLoggerKey).(log.Fields); ok {
		// Constructing back the logger with the fields extracted from the workflow context
		logger := log.NewLogger().WithFields("requestFields", loggerFields)
		return logger, nil
	}
	return nil, errors.New("no logger found in workflow context")
}

func GetLogger(ctx interface{}) (log.Logger, error) {
	var ctxHandler contextHandler
	switch ctxReceived := ctx.(type) {
	case context.Context:
		ctxHandler = apiContext{
			ctx: ctxReceived,
		}
	case workflow.Context:
		ctxHandler = workflowContext{
			ctx: ctxReceived,
		}
	}

	logger, err := ctxHandler.extractLogger()
	if err != nil {
		log.NewLogger().Error("failed to extract logger from context")
		return nil, err
	}
	return logger, nil
}
