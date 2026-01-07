package backgroundworkflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestCleanupHydratedMetricsTableWorkflow_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	// Mock the activity
	metricsCleanupActivity := &backgroundactivities.MetricsCleanupActivity{}
	env.OnActivity(metricsCleanupActivity.CleanupHydratedMetricsTableActivity, mock.Anything).Return(nil).Once()

	// Execute the workflow
	env.ExecuteWorkflow(CleanupHydratedMetricsTableWorkflow)

	// Verify
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestCleanupHydratedMetricsTableWorkflow_ActivityFails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	// Mock the activity to fail
	metricsCleanupActivity := &backgroundactivities.MetricsCleanupActivity{}
	env.OnActivity(metricsCleanupActivity.CleanupHydratedMetricsTableActivity, mock.Anything).Return(assert.AnError).Once()

	// Execute the workflow
	env.ExecuteWorkflow(CleanupHydratedMetricsTableWorkflow)

	// Verify
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestCleanupAggregatedUsageTableWorkflow_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	// Mock the activity
	metricsCleanupActivity := &backgroundactivities.MetricsCleanupActivity{}
	env.OnActivity(metricsCleanupActivity.CleanupAggregatedUsageTableActivity, mock.Anything).Return(nil).Once()

	// Execute the workflow
	env.ExecuteWorkflow(CleanupAggregatedUsageTableWorkflow)

	// Verify
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestCleanupAggregatedUsageTableWorkflow_ActivityFails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	// Mock the activity to fail
	metricsCleanupActivity := &backgroundactivities.MetricsCleanupActivity{}
	env.OnActivity(metricsCleanupActivity.CleanupAggregatedUsageTableActivity, mock.Anything).Return(assert.AnError).Once()

	// Execute the workflow
	env.ExecuteWorkflow(CleanupAggregatedUsageTableWorkflow)

	// Verify
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestCleanupJobsTableWorkflow_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	// Mock the activity
	metricsCleanupActivity := &backgroundactivities.MetricsCleanupActivity{}
	env.OnActivity(metricsCleanupActivity.CleanupJobsTableActivity, mock.Anything).Return(nil).Once()

	// Execute the workflow
	env.ExecuteWorkflow(CleanupJobsTableWorkflow)

	// Verify
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestCleanupJobsTableWorkflow_ActivityFails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	// Mock the activity to fail
	metricsCleanupActivity := &backgroundactivities.MetricsCleanupActivity{}
	env.OnActivity(metricsCleanupActivity.CleanupJobsTableActivity, mock.Anything).Return(assert.AnError).Once()

	// Execute the workflow
	env.ExecuteWorkflow(CleanupJobsTableWorkflow)

	// Verify
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}
