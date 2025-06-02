package util

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type stubHeaderReaderWriter struct {
	storage map[string]*commonpb.Payload
}

func makeStubHeaderReaderWriter() stubHeaderReaderWriter {
	return stubHeaderReaderWriter{map[string]*commonpb.Payload{}}
}

func (s stubHeaderReaderWriter) Set(key string, value *commonpb.Payload) {
	s.storage[key] = value
}

func (s stubHeaderReaderWriter) Get(key string) (*commonpb.Payload, bool) {
	val, ok := s.storage[key]
	return val, ok
}

func (s stubHeaderReaderWriter) GetValue(key string, decodedValuePtr interface{}) error {
	return converter.GetDefaultDataConverter().FromPayload(s.storage[key], decodedValuePtr)
}

func (s stubHeaderReaderWriter) ForEachKey(handler func(string, *commonpb.Payload) error) error {
	for key, value := range s.storage {
		if err := handler(key, value); err != nil {
			return err
		}
	}
	return nil
}

type StringMapPropagatorTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	propagator workflow.ContextPropagator
}

func TestStringMapPropagatorSuite(t *testing.T) {
	s := StringMapPropagatorTestSuite{propagator: NewContextMapPropagator()}
	suite.Run(t, &s)
}

func (s *StringMapPropagatorTestSuite) TestOrdinaryContextPassedFields() {
	headersRWStub := makeStubHeaderReaderWriter()
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, log.Fields{"aaa": "bbb", "ccc": "ddd"})
	err := s.propagator.Inject(ctx, headersRWStub)
	s.NoError(err)
	ctxOut, err := s.propagator.Extract(context.Background(), headersRWStub)
	s.NoError(err)

	fieldsReceived := ctxOut.Value(middleware.TemporalSLoggerKey).(log.Fields)
	if fieldsReceived == nil {
		s.Fail("propagator does not propagate required fields")
	}
}

func (s *StringMapPropagatorTestSuite) TestOrdinaryContextNOTPassedFields() {
	headersRWStub := makeStubHeaderReaderWriter()

	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, log.Fields{})

	err := s.propagator.Inject(ctx, headersRWStub)
	s.NoError(err)
	ctxOut, err := s.propagator.Extract(context.Background(), headersRWStub)
	s.NoError(err)

	ccc := ctxOut.Value("ccc")
	s.Nil(ccc, "propagated field that was not required for the propagator")
}

func (s *StringMapPropagatorTestSuite) TestStringMapPropagatorWorkflowCtx() {
	env := s.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(s.CheckContextWorkflow)

	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *StringMapPropagatorTestSuite) CheckContextWorkflow(ctx workflow.Context) error {
	propagator := NewContextMapPropagator()
	headersRWStub := makeStubHeaderReaderWriter()

	ctx = workflow.WithValue(ctx, middleware.TemporalSLoggerKey, log.Fields{})

	err := propagator.InjectFromWorkflow(ctx, headersRWStub)
	if err != nil {
		return err
	}
	// clear context in order to take the values from the headers
	ctx = workflow.WithValue(ctx, middleware.TemporalSLoggerKey, "")

	ctxOut, err := propagator.ExtractToWorkflow(ctx, headersRWStub)
	if err != nil {
		return err
	}

	fields := ctxOut.Value(middleware.TemporalSLoggerKey)
	if fields == nil {
		return errors.New("propagator does not propagate required fields")
	}
	return nil
}
