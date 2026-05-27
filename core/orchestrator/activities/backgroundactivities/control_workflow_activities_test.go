package backgroundactivities

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	workflowEngine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

func TestControlWorkflowActivity_ExecutePoolCertificateRotationSequentially(t *testing.T) {
	// Save original function
	origFetchTemporalClient := workflowengine.FetchTemporalClient
	defer func() { workflowengine.FetchTemporalClient = origFetchTemporalClient }()

	// Save original ExecuteWorkflowSequentially
	origExecuteWorkflowSequentially := ExecuteWorkflowSequentially
	defer func() { ExecuteWorkflowSequentially = origExecuteWorkflowSequentially }()

	t.Run("Success", func(t *testing.T) {
		poolUUID := "test-pool-uuid"
		workflowTimeout := 5 * time.Minute

		// Mock temporal client
		mockClient := workflowEngine.NewMockTemporalTestClient(t)
		workflowengine.FetchTemporalClient = func() (client.Client, error) {
			return mockClient, nil
		}

		// Mock ExecuteWorkflowSequentially
		executeCalled := false
		ExecuteWorkflowSequentially = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			executeCalled = true
			// Verify control workflow ID format
			expectedControlWfID := "Pool_test-pool-uuid_Ops_All"
			assert.Equal(t, expectedControlWfID, sequenceWfOptions.ID)
			assert.Equal(t, workflowengine.CustomerTaskQueue, sequenceWfOptions.TaskQueue)

			// Verify child workflow options
			assert.Equal(t, workflowengine.CustomerTaskQueue, wfOptions.TaskQueue)
			assert.Equal(t, workflowTimeout, wfOptions.WorkflowRunTimeout)

			// Verify workflow args
			assert.Len(t, wfArgs, 1)
			assert.Equal(t, poolUUID, wfArgs[0])

			return nil
		}

		activity := &ControlWorkflowActivity{}
		err := activity.ExecutePoolCertificateRotationSequentially(context.Background(), poolUUID, workflowTimeout)

		assert.NoError(t, err)
		assert.True(t, executeCalled, "ExecuteWorkflowSequentially should have been called")
	})

	t.Run("TemporalClientError", func(t *testing.T) {
		poolUUID := "test-pool-uuid"
		workflowTimeout := 5 * time.Minute

		// Mock temporal client to return error
		workflowengine.FetchTemporalClient = func() (client.Client, error) {
			return nil, assert.AnError
		}

		activity := &ControlWorkflowActivity{}
		err := activity.ExecutePoolCertificateRotationSequentially(context.Background(), poolUUID, workflowTimeout)

		assert.Error(t, err)
	})
}

func TestControlWorkflowActivity_ExecutePoolPasswordRotationSequentially(t *testing.T) {
	// Save original function
	origFetchTemporalClient := workflowengine.FetchTemporalClient
	defer func() { workflowengine.FetchTemporalClient = origFetchTemporalClient }()

	// Save original ExecuteWorkflowSequentially
	origExecuteWorkflowSequentially := ExecuteWorkflowSequentially
	defer func() { ExecuteWorkflowSequentially = origExecuteWorkflowSequentially }()

	t.Run("Success", func(t *testing.T) {
		poolUUID := "test-pool-uuid"
		workflowTimeout := 5 * time.Minute

		// Mock temporal client
		mockClient := workflowEngine.NewMockTemporalTestClient(t)
		workflowengine.FetchTemporalClient = func() (client.Client, error) {
			return mockClient, nil
		}

		// Mock ExecuteWorkflowSequentially
		executeCalled := false
		ExecuteWorkflowSequentially = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			executeCalled = true
			// Verify control workflow ID format
			expectedControlWfID := "Pool_test-pool-uuid_Ops_All"
			assert.Equal(t, expectedControlWfID, sequenceWfOptions.ID)
			assert.Equal(t, workflowengine.CustomerTaskQueue, sequenceWfOptions.TaskQueue)

			// Verify child workflow options
			assert.Equal(t, workflowengine.CustomerTaskQueue, wfOptions.TaskQueue)
			assert.Equal(t, workflowTimeout, wfOptions.WorkflowRunTimeout)

			// Verify workflow args
			assert.Len(t, wfArgs, 1)
			assert.Equal(t, poolUUID, wfArgs[0])

			return nil
		}

		activity := &ControlWorkflowActivity{}
		err := activity.ExecutePoolPasswordRotationSequentially(context.Background(), poolUUID, workflowTimeout)

		assert.NoError(t, err)
		assert.True(t, executeCalled, "ExecuteWorkflowSequentially should have been called")
	})

	t.Run("TemporalClientError", func(t *testing.T) {
		poolUUID := "test-pool-uuid"
		workflowTimeout := 5 * time.Minute

		// Mock temporal client to return error
		workflowengine.FetchTemporalClient = func() (client.Client, error) {
			return nil, assert.AnError
		}

		activity := &ControlWorkflowActivity{}
		err := activity.ExecutePoolPasswordRotationSequentially(context.Background(), poolUUID, workflowTimeout)

		assert.Error(t, err)
	})
}
