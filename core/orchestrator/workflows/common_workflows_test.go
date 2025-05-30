package workflows

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
)

type mockEncodedValue struct {
	err bool
}

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
