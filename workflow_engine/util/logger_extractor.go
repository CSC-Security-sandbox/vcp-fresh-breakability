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
	if logger, ok := c.ctx.Value(middleware.ContextSLoggerKey).(log.Logger); ok {
		return logger, nil
	}

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
	if logger, ok := c.ctx.Value(middleware.ContextSLoggerKey).(log.Logger); ok {
		return logger, nil
	}

	if loggerFields, ok := c.ctx.Value(middleware.TemporalSLoggerKey).(log.Fields); ok {
		// Constructing back the logger with the fields extracted from the workflow context
		logger := log.NewLogger().WithFields("requestFields", loggerFields)
		return logger, nil
	}
	return nil, errors.New("no logger found in workflow context")
}

func GetLogger(ctx interface{}) log.Logger {
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
		newLogger := log.NewLogger()
		newLogger.Error("failed to extract logger from context")
		return newLogger
	}
	return logger
}

func AddExtraLoggerFields(ctx workflow.Context, keyValMap map[string]interface{}) workflow.Context {
	for key, val := range keyValMap {
		if loggerFields, ok := ctx.Value(middleware.TemporalSLoggerKey).(log.Fields); ok {
			loggerFields[key] = val
			ctx = workflow.WithValue(ctx, middleware.TemporalSLoggerKey, loggerFields)
		}
	}
	return ctx
}
