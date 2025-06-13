package workflows

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type mockEncodedValue struct {
	err bool
}

type mockWorkflowContext struct {
	workflow.Context
	base context.Context
}

func (m *mockWorkflowContext) Value(key interface{}) interface{} {
	return m.base.Value(key)
}

type mockFuture struct{ mock.Mock }

func (m *mockFuture) Get(ctx workflow.Context, valuePtr interface{}) error {
	args := m.Called(ctx, valuePtr)
	return args.Error(0)
}
func (m *mockFuture) IsReady() bool { return true }

func (m mockEncodedValue) Get(valuePtr interface{}) error {
	if m.err {
		return fmt.Errorf("encoding error for value: %+v", valuePtr)
	}
	return nil
}

func (m mockEncodedValue) HasValue() bool {
	return true
}

func TestQueryWorkflowStatus_Success(t *testing.T) {
	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	expectedStatus := &WorkflowStatus{}

	mockClient.On("QueryWorkflow", mock.Anything, "wf-1", "run-1", StatusQueryName).
		Return(mockEncodedValue{err: false}, nil)

	status, err := QueryWorkflowStatus(context.Background(), mockClient, "wf-1", "run-1")
	assert.NoError(t, err)
	assert.Equal(t, expectedStatus, status)
}

func TestQueryWorkflowStatus_EncodeError(t *testing.T) {
	mockClient := workflow_engine.NewMockTemporalTestClient(t)

	mockClient.On("QueryWorkflow", mock.Anything, "wf-1", "run-1", StatusQueryName).
		Return(mockEncodedValue{err: true}, nil)

	status, err := QueryWorkflowStatus(context.Background(), mockClient, "wf-1", "run-1")
	assert.Error(t, err)
	assert.Nil(t, status)
}

func TestQueryWorkflowStatus_QueryError(t *testing.T) {
	mockClient := workflow_engine.NewMockTemporalTestClient(t)
	mockClient.On("QueryWorkflow", mock.Anything, "wf-2", "run-2", StatusQueryName).
		Return(nil, errors.New("query error"))

	status, err := QueryWorkflowStatus(context.Background(), mockClient, "wf-2", "run-2")
	assert.Error(t, err)
	assert.Nil(t, status)
}

func TestPopulateRetryPolicyParams(t *testing.T) {
	origStartToCloseTimeout := StartToCloseTimeout
	origRetryInterval := RetryInterval
	origRetryMaxAttempts := RetryMaxAttempts
	origRetryMaxInterval := RetryMaxInterval
	origRetryBackoff := RetryBackoff

	defer func() {
		StartToCloseTimeout = origStartToCloseTimeout
		RetryInterval = origRetryInterval
		RetryMaxAttempts = origRetryMaxAttempts
		RetryMaxInterval = origRetryMaxInterval
		RetryBackoff = origRetryBackoff
	}()

	t.Run("success", func(t *testing.T) {
		StartToCloseTimeout = "10m"
		RetryInterval = "1s"
		RetryMaxAttempts = 2
		RetryMaxInterval = "2m"
		RetryBackoff = "1.5"
		policy, err := PopulateRetryPolicyParams()
		assert.NoError(t, err)
		assert.NotNil(t, policy)
	})

	t.Run("invalid StartToCloseTimeout", func(t *testing.T) {
		StartToCloseTimeout = "invalid"
		_, err := PopulateRetryPolicyParams()
		assert.Error(t, err)
	})

	t.Run("invalid RetryInterval", func(t *testing.T) {
		StartToCloseTimeout = "10m"
		RetryInterval = "invalid"
		_, err := PopulateRetryPolicyParams()
		assert.Error(t, err)
	})

	t.Run("invalid RetryMaxInterval", func(t *testing.T) {
		StartToCloseTimeout = "10m"
		RetryInterval = "1s"
		RetryMaxInterval = "invalid"
		_, err := PopulateRetryPolicyParams()
		assert.Error(t, err)
	})

	t.Run("invalid RetryBackoff", func(t *testing.T) {
		StartToCloseTimeout = "10m"
		RetryInterval = "1s"
		RetryMaxInterval = "2m"
		RetryBackoff = "invalid"
		_, err := PopulateRetryPolicyParams()
		assert.Error(t, err)
	})
}

func TestUpdateJobStatusWithCustomError(t *testing.T) {
	ctx := context.TODO()
	wfCtx := &mockWorkflowContext{base: ctx}
	origExecuteActivity := executeActivity

	defer func() { executeActivity = origExecuteActivity }()
	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		return f
	}

	bw := &BaseWorkflow{ID: "test-id"}
	jobError := temporal.NewApplicationError("test error", "CustomError", 123, "original error details")
	err := bw.UpdateJobStatus(wfCtx, models.JobStateFailure, jobError)

	assert.NoError(t, err)
}

func TestUpdateJobStatusWithCustomErrorMissingDetails(t *testing.T) {
	ctx := context.TODO()
	wfCtx := &mockWorkflowContext{base: ctx}
	origExecuteActivity := executeActivity

	defer func() { executeActivity = origExecuteActivity }()
	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		return f
	}

	bw := &BaseWorkflow{Logger: util.GetLogger(ctx), ID: "test-id"}
	jobError := temporal.NewApplicationError("test error", "CustomError", nil)
	err := bw.UpdateJobStatus(wfCtx, models.JobStateFailure, jobError)

	assert.NoError(t, err)
}

func TestUpdateJobStatusWithGenericError(t *testing.T) {
	ctx := context.TODO()
	wfCtx := &mockWorkflowContext{base: ctx}
	origExecuteActivity := executeActivity

	defer func() { executeActivity = origExecuteActivity }()
	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		return f
	}

	bw := &BaseWorkflow{Logger: util.GetLogger(ctx), ID: "test-id"}
	jobError := errors.New("test error")
	err := bw.UpdateJobStatus(wfCtx, models.JobStateFailure, jobError)

	assert.NoError(t, err)
}

func TestUpdateJobStatusWithEmptyID(t *testing.T) {
	ctx := context.TODO()
	wfCtx := &mockWorkflowContext{base: ctx}
	origExecuteActivity := executeActivity

	defer func() { executeActivity = origExecuteActivity }()
	executeActivity = func(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
		f := new(mockFuture)
		f.On("Get", ctx, mock.Anything).Return(nil)
		return f
	}

	bw := &BaseWorkflow{ID: ""}
	err := bw.UpdateJobStatus(wfCtx, models.JobStateFailure, errors.New("test error"))

	assert.Error(t, err)
	assert.ErrorContains(t, err, "job uuid cannot be empty")
}
