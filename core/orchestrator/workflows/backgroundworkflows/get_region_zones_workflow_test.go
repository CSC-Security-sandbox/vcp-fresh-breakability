package backgroundworkflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"go.temporal.io/sdk/testsuite"
)

func TestGetRegionZonesWorkflow_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	want := []string{"australia-southeast1-a", "australia-southeast1-b", "australia-southeast1-c"}

	act := &backgroundactivities.GetRegionZonesActivity{}
	env.RegisterActivity(act.GetRegionZones)
	env.OnActivity(act.GetRegionZones, mock.Anything, "australia-southeast1").Return(want, nil)

	env.ExecuteWorkflow(GetRegionZonesWorkflow, "australia-southeast1")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got []string
	require.NoError(t, env.GetWorkflowResult(&got))
	assert.Equal(t, want, got)
}

func TestGetRegionZonesWorkflow_ActivityNilSlice(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	act := &backgroundactivities.GetRegionZonesActivity{}
	env.RegisterActivity(act.GetRegionZones)
	// Activity returning (nil, nil) is the "SECRET_MANAGER_PROJECT_ID is empty"
	// short-circuit. Workflow propagates a nil slice with no error so the
	// caller falls back to region-only enumeration.
	env.OnActivity(act.GetRegionZones, mock.Anything, "us-central1").Return(nil, nil)

	env.ExecuteWorkflow(GetRegionZonesWorkflow, "us-central1")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got []string
	require.NoError(t, env.GetWorkflowResult(&got))
	assert.Nil(t, got)
}

func TestGetRegionZonesWorkflow_ActivityError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	act := &backgroundactivities.GetRegionZonesActivity{}
	env.RegisterActivity(act.GetRegionZones)
	env.OnActivity(act.GetRegionZones, mock.Anything, "us-central1").Return(nil, errors.New("compute api boom"))

	env.ExecuteWorkflow(GetRegionZonesWorkflow, "us-central1")

	require.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}
