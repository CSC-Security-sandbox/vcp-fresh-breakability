package workflows

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestWorkflowFailsIfLicenseIsEmpty(t *testing.T) {
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
	env.RegisterActivity(&activities.SmcTokenRotationActivity{})

	env.OnActivity("GetSMCLicenseFromCloud", mock.Anything).Return("", nil)
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

	params := &common.CreateSMCTokenRotationParams{AccountName: "test-account"}
	env.ExecuteWorkflow(CreateSMCTokenRotationWorkflow, params)
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "SMC license is empty")
	env.AssertExpectations(t)
}

func TestWorkflowFailsIfActivityReturnsError(t *testing.T) {
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
	env.RegisterActivity(&activities.SmcTokenRotationActivity{})

	env.OnActivity("GetSMCLicenseFromCloud", mock.Anything).Return("", fmt.Errorf("activity error"))
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

	params := &common.CreateSMCTokenRotationParams{AccountName: "test-account"}
	env.ExecuteWorkflow(CreateSMCTokenRotationWorkflow, params)
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "failed to get SMC license")
	env.AssertExpectations(t)
}

func TestWorkflowCompletesSuccessfully(t *testing.T) {
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
	env.RegisterActivity(&activities.SmcTokenRotationActivity{})

	env.OnActivity("GetSMCLicenseFromCloud", mock.Anything).Return("valid-license", nil)
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

	params := &common.CreateSMCTokenRotationParams{AccountName: "test-account"}
	env.ExecuteWorkflow(CreateSMCTokenRotationWorkflow, params)
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}
