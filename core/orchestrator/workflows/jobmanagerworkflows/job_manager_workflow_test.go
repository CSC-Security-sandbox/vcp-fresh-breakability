package jobmanagerworkflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/jobmanageractivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestJobManagerWorkflow_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	// Register activities
	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&jobmanageractivities.JobManagerActivity{})

	// Mock CreateJob activity
	env.OnActivity("CreateJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "test-job"},
		Type:       string(models.JobTypeRefreshAdminJobSpecs),
		State:      string(models.JobsStatePROCESSING),
		IsAdminJob: true,
		WorkflowID: "test-workflow",
	}, nil)

	// Mock JobManager activities
	env.OnActivity("CreateScheduleActivity", mock.Anything).Return(nil)
	env.OnActivity("UpdateScheduleActivity", mock.Anything).Return(nil)
	env.OnActivity("DeleteScheduleActivity", mock.Anything).Return(nil)

	// Execute workflow
	env.ExecuteWorkflow(JobManagerWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestJobManagerWorkflow_ActivitiesFail(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	// Register activities
	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&jobmanageractivities.JobManagerActivity{})

	// Mock CreateJob activity
	env.OnActivity("CreateJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "test-job"},
		Type:       string(models.JobTypeRefreshAdminJobSpecs),
		State:      string(models.JobsStatePROCESSING),
		IsAdminJob: true,
		WorkflowID: "test-workflow",
	}, nil)

	// Mock JobManager activities to fail
	env.OnActivity("CreateScheduleActivity", mock.Anything).Return(assert.AnError)
	env.OnActivity("UpdateScheduleActivity", mock.Anything).Return(assert.AnError)
	env.OnActivity("DeleteScheduleActivity", mock.Anything).Return(assert.AnError)

	// Execute workflow
	env.ExecuteWorkflow(JobManagerWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}
