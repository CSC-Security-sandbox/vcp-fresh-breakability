package workflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestHostGroupUpdateWorkflowWorkflow(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)
	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&activities.HostGroupUpdateActivity{})

	hg := &datamodel.HostGroup{
		Name:        "test-hostgroup",
		Description: "Test Host Group",
		OSType:      "linux",
		Hosts: datamodel.Hosts{
			Hosts: []string{"host1", "host2"},
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("UpdateIGroups", mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	env.ExecuteWorkflow(UpdateHostGroupWorkflow, hg)

	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestHostGroupUpdateWorkflowWorkflow_UpdateJobStatusFails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)
	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&activities.HostGroupUpdateActivity{})

	hg := &datamodel.HostGroup{
		Name:        "test-hostgroup",
		Description: "Test Host Group",
		OSType:      "linux",
		Hosts: datamodel.Hosts{
			Hosts: []string{"host1", "host2"},
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(errors.New("some error"))

	// Execute workflow
	env.ExecuteWorkflow(UpdateHostGroupWorkflow, hg)

	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestHostGroupUpdateWorkflowWorkflow_UpdateIGroupsFails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)
	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&activities.HostGroupUpdateActivity{})

	hg := &datamodel.HostGroup{
		Name:        "test-hostgroup",
		Description: "Test Host Group",
		OSType:      "linux",
		Hosts: datamodel.Hosts{
			Hosts: []string{"host1", "host2"},
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("UpdateIGroups", mock.Anything, mock.Anything).Return(errors.New("some error"))

	// Execute workflow
	env.ExecuteWorkflow(UpdateHostGroupWorkflow, hg)

	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}
