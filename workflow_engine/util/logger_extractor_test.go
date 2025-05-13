package util

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type LoggerExtractorTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestLoggerExtractorSuite(t *testing.T) {
	suite.Run(t, new(LoggerExtractorTestSuite))
}

func (s *LoggerExtractorTestSuite) TestGetLoggerFromAPIContext() {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

	logger, err := GetLogger(ctx)
	s.NoError(err)
	s.NotNil(logger)
}

func (s *LoggerExtractorTestSuite) TestGetLoggerFromAPIContextNoLogger() {
	ctx := context.Background()

	logger, err := GetLogger(ctx)
	s.Error(err)
	s.Nil(logger)
	s.EqualError(err, "no logger found in api context")
}

func (s *LoggerExtractorTestSuite) TestGetLoggerFromWorkflowContext() {
	env := s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(s.CheckNoLoggerInWorkflowContext)

	s.True(env.IsWorkflowCompleted())
	s.Error(env.GetWorkflowError())
}

func (s *LoggerExtractorTestSuite) CheckNoLoggerInWorkflowContext(ctx workflow.Context) error {
	logger, err := GetLogger(ctx)
	if err != nil {
		return err
	}
	if logger != nil {
		return errors.New("logger found but was supposed to be not present")
	}
	return nil
}
