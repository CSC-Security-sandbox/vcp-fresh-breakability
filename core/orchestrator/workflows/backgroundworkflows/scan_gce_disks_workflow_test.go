package backgroundworkflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/diskscan"
)

func TestScanGCEDisksWorkflow_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.ScanGCEDisksActivity{}
	env.RegisterActivity(activity)

	in := diskscan.ScanInput{ProjectIDs: []string{"proj-1"}}
	expected := &diskscan.ScanOutput{
		Items: []diskscan.GCEDiskItem{
			{Project: "proj-1", Name: "disk-1", SelfLink: "sl-1", Labels: map[string]string{"pool_uuid": "u1"}},
		},
	}
	env.OnActivity(activity.ScanGCEDisks, mock.Anything, in).Return(expected, nil)

	env.ExecuteWorkflow(ScanGCEDisksWorkflow, in)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got diskscan.ScanOutput
	require.NoError(t, env.GetWorkflowResult(&got))
	require.Len(t, got.Items, 1)
	assert.Equal(t, "disk-1", got.Items[0].Name)
	assert.Equal(t, "u1", got.Items[0].Labels["pool_uuid"])
}

func TestScanGCEDisksWorkflow_ActivityError_Propagates(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.ScanGCEDisksActivity{}
	env.RegisterActivity(activity)

	in := diskscan.ScanInput{ProjectIDs: []string{"proj-1"}}
	env.OnActivity(activity.ScanGCEDisks, mock.Anything, in).Return(nil, errors.New("compute api down"))

	env.ExecuteWorkflow(ScanGCEDisksWorkflow, in)

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "compute api down")
}

func TestScanGCEDisksWorkflow_EmptyInput_StillRunsActivityOnce(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.ScanGCEDisksActivity{}
	env.RegisterActivity(activity)

	in := diskscan.ScanInput{ProjectIDs: nil}
	env.OnActivity(activity.ScanGCEDisks, mock.Anything, in).Return(&diskscan.ScanOutput{}, nil)

	env.ExecuteWorkflow(ScanGCEDisksWorkflow, in)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got diskscan.ScanOutput
	require.NoError(t, env.GetWorkflowResult(&got))
	assert.Empty(t, got.Items)
	assert.Empty(t, got.PartialFailures)
}
