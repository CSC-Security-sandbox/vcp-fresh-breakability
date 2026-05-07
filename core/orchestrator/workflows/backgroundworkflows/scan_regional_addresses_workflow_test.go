package backgroundworkflows

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/ipscan"
	hyperscalerleakedresources "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/leakedresources"
	"go.temporal.io/sdk/testsuite"
	"testing"
)

func TestScanRegionalAddressesWorkflow_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.ScanRegionalAddressesActivity{}
	env.RegisterActivity(activity)

	in := ipscan.ScanInput{
		Targets: []ipscan.ProjectRegion{{Project: "proj-1", Region: "us-central1"}},
	}
	expected := &ipscan.ScanOutput{
		Results: []ipscan.ScanResult{
			{
				Project: "proj-1",
				Region:  "us-central1",
				Addresses: []hyperscalerleakedresources.RegionalAddress{
					{Name: "addr-1", AddressType: "INTERNAL", Status: "RESERVED"},
				},
			},
		},
	}
	env.OnActivity(activity.ScanRegionalAddresses, mock.Anything, in).Return(expected, nil)

	env.ExecuteWorkflow(ScanRegionalAddressesWorkflow, in)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got ipscan.ScanOutput
	require.NoError(t, env.GetWorkflowResult(&got))
	require.Len(t, got.Results, 1)
	require.Len(t, got.Results[0].Addresses, 1)
	assert.Equal(t, "addr-1", got.Results[0].Addresses[0].Name)
}

func TestScanRegionalAddressesWorkflow_ActivityError_Propagates(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.ScanRegionalAddressesActivity{}
	env.RegisterActivity(activity)

	in := ipscan.ScanInput{
		Targets: []ipscan.ProjectRegion{{Project: "proj-1", Region: "us-central1"}},
	}
	env.OnActivity(activity.ScanRegionalAddresses, mock.Anything, in).Return(nil, errors.New("compute api down"))

	env.ExecuteWorkflow(ScanRegionalAddressesWorkflow, in)

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "compute api down")
}

func TestScanRegionalAddressesWorkflow_EmptyInput_StillRunsActivityOnce(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.ScanRegionalAddressesActivity{}
	env.RegisterActivity(activity)

	in := ipscan.ScanInput{Targets: nil}
	env.OnActivity(activity.ScanRegionalAddresses, mock.Anything, in).Return(&ipscan.ScanOutput{}, nil)

	env.ExecuteWorkflow(ScanRegionalAddressesWorkflow, in)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got ipscan.ScanOutput
	require.NoError(t, env.GetWorkflowResult(&got))
	assert.Empty(t, got.Results)
	assert.Empty(t, got.PartialFailures)
}
