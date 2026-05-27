package backgroundworkflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/vmscan"
	"go.temporal.io/sdk/testsuite"
)

func TestScanGCEInstancesWorkflow_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.ScanGCEInstancesActivity{}
	env.RegisterActivity(activity)

	in := vmscan.ScanInput{ProjectIDs: []string{"proj-1"}}
	expected := &vmscan.ScanOutput{
		Items: []vmscan.GCEInstanceItem{
			{Project: "proj-1", Name: "vm-1", SelfLink: "sl-1", Labels: map[string]string{"pool_uuid": "u1"}},
		},
	}
	env.OnActivity(activity.ScanGCEInstances, mock.Anything, in).Return(expected, nil)

	env.ExecuteWorkflow(ScanGCEInstancesWorkflow, in)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got vmscan.ScanOutput
	require.NoError(t, env.GetWorkflowResult(&got))
	require.Len(t, got.Items, 1)
	assert.Equal(t, "vm-1", got.Items[0].Name)
	assert.Equal(t, "u1", got.Items[0].Labels["pool_uuid"])
}

func TestScanGCEInstancesWorkflow_ActivityError_Propagates(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.ScanGCEInstancesActivity{}
	env.RegisterActivity(activity)

	in := vmscan.ScanInput{ProjectIDs: []string{"proj-1"}}
	env.OnActivity(activity.ScanGCEInstances, mock.Anything, in).Return(nil, errors.New("compute api down"))

	env.ExecuteWorkflow(ScanGCEInstancesWorkflow, in)

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "compute api down")
}

func TestScanGCEInstancesWorkflow_EmptyInput_StillRunsActivityOnce(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.ScanGCEInstancesActivity{}
	env.RegisterActivity(activity)

	in := vmscan.ScanInput{ProjectIDs: nil}
	env.OnActivity(activity.ScanGCEInstances, mock.Anything, in).Return(&vmscan.ScanOutput{}, nil)

	env.ExecuteWorkflow(ScanGCEInstancesWorkflow, in)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got vmscan.ScanOutput
	require.NoError(t, env.GetWorkflowResult(&got))
	assert.Empty(t, got.Items)
	assert.Empty(t, got.PartialFailures)
}
