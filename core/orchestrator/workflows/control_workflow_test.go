package workflows

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	workflowEngineMock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func WorkflowTest() {}

func TestExecuteWorkflowSequentially_Success(t *testing.T) {
	temporal := workflowEngineMock.NewMockTemporalTestClient(t)
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	ctx := context.Background()

	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&activities.PoolActivity{})

	temporal.EXPECT().SignalWithStartWorkflow(
		ctx,
		"test-sequence-workflow-id",
		Signal,
		SignalWorkflowParams{
			Function: "WorkflowTest",
			Options: workflow.ChildWorkflowOptions{
				TaskQueue:             workflowengine.CustomerTaskQueue,
				WorkflowID:            "test-workflow-id",
				WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
				WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
			},
		},
		client.StartWorkflowOptions{
			ID:        "test-sequence-workflow-id",
			TaskQueue: workflowengine.CustomerTaskQueue,
		},
		mock.Anything,
	).Return(nil, nil)

	err := ExecuteWorkflowSequentially(
		temporal,
		ctx,
		client.StartWorkflowOptions{
			ID: "test-sequence-workflow-id",
		},
		WorkflowTest,
		workflow.ChildWorkflowOptions{
			WorkflowID:            "test-workflow-id",
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
	)

	assert.NoError(t, err)

	// Assert workflow execution
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestExecuteWorkflowSequentially_SignalError(t *testing.T) {
	temporal := workflowEngineMock.NewMockTemporalTestClient(t)
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	ctx := context.Background()

	temporal.EXPECT().SignalWithStartWorkflow(
		ctx,
		"test-sequence-workflow-id",
		Signal,
		SignalWorkflowParams{
			Function: "WorkflowTest",
			Options: workflow.ChildWorkflowOptions{
				TaskQueue:             workflowengine.CustomerTaskQueue,
				WorkflowID:            "test-workflow-id",
				WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
				WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
			},
		},
		client.StartWorkflowOptions{
			ID:        "test-sequence-workflow-id",
			TaskQueue: workflowengine.CustomerTaskQueue,
		},
		mock.Anything,
	).Return(nil, errors.New("signal error"))

	err := ExecuteWorkflowSequentially(
		temporal,
		ctx,
		client.StartWorkflowOptions{
			ID:        "test-sequence-workflow-id",
			TaskQueue: workflowengine.CustomerTaskQueue,
		},
		WorkflowTest,
		workflow.ChildWorkflowOptions{
			WorkflowID:            "test-workflow-id",
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
	)

	assert.Error(t, err)

	// Assert workflow execution
	env.AssertExpectations(t)
}

func TestExecuteWorkflowSequentially_ValidationError_sequenceWorkflowIDNil(t *testing.T) {
	temporal := workflowEngineMock.NewMockTemporalTestClient(t)
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	ctx := context.Background()

	err := ExecuteWorkflowSequentially(
		temporal,
		ctx,
		client.StartWorkflowOptions{
			ID:        "",
			TaskQueue: workflowengine.CustomerTaskQueue,
		},
		WorkflowTest,
		workflow.ChildWorkflowOptions{
			WorkflowID:            "test-workflow-id",
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
	)

	assert.Error(t, err)
	assert.Equal(t,
		"Invalid parameters for sequence workflow execution, error: sequence workflow ID cannot be empty",
		err.Error(),
	)

	// Assert workflow execution
	env.AssertExpectations(t)
}

func TestExecuteWorkflowSequentially_ValidationError_ExecutionWorkflowIDNil(t *testing.T) {
	temporal := workflowEngineMock.NewMockTemporalTestClient(t)
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	ctx := context.Background()

	err := ExecuteWorkflowSequentially(
		temporal,
		ctx,
		client.StartWorkflowOptions{
			ID:        "test-sequence-workflow-id",
			TaskQueue: workflowengine.CustomerTaskQueue,
		},
		WorkflowTest,
		workflow.ChildWorkflowOptions{
			WorkflowID:            "",
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
	)

	assert.Error(t, err)
	assert.Equal(t,
		"Invalid parameters for sequence workflow execution, error: execution workflow ID cannot be empty",
		err.Error(),
	)

	// Assert workflow execution
	env.AssertExpectations(t)
}

func TestSequenceWorkflow_ExecutesChildWorkflowOnSignal(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	// Register a dummy child workflow
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(Signal, SignalWorkflowParams{
			Function: "DummyChildWorkflow",
			Args:     []interface{}{"arg1"},
			Options:  workflow.ChildWorkflowOptions{},
		})
	}, time.Second)

	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, arg string) error {
		return nil
	}, workflow.RegisterOptions{Name: "DummyChildWorkflow"})

	env.ExecuteWorkflow(SequenceWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSequenceWorkflow_ChildWorkflowError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(Signal, SignalWorkflowParams{
			Function: "ErrorChildWorkflow",
			Args:     []interface{}{"arg1"},
			Options:  workflow.ChildWorkflowOptions{},
		})
	}, time.Second)

	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, arg string) error {
		return errors.New("child error")
	}, workflow.RegisterOptions{Name: "ErrorChildWorkflow"})

	env.ExecuteWorkflow(SequenceWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSequenceWorkflow_ExitsOnTimeout(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(SequenceWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}
