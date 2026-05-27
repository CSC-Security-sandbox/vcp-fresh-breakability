package backgroundactivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/vmscan"
	hyperscalerleakedresources "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/leakedresources"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
)

type fakeInstanceLister struct {
	byProject map[string][]hyperscalermodels.GCEInstance
	errors    map[string]error
	calls     []string
}

func (f *fakeInstanceLister) ListInstances(ctx context.Context, projectID string) ([]hyperscalermodels.GCEInstance, error) {
	f.calls = append(f.calls, projectID)
	if err, ok := f.errors[projectID]; ok {
		return f.byProject[projectID], err
	}
	return f.byProject[projectID], nil
}

func swapInstanceLister(t *testing.T, l hyperscalerleakedresources.InstanceLister, initErr error) {
	t.Helper()
	orig := newInstanceListerForActivity
	t.Cleanup(func() { newInstanceListerForActivity = orig })
	newInstanceListerForActivity = func(ctx context.Context) (hyperscalerleakedresources.InstanceLister, error) {
		if initErr != nil {
			return nil, initErr
		}
		return l, nil
	}
}

func TestScanGCEInstancesActivity_NoProjects_ReturnsEmpty(t *testing.T) {
	a := &ScanGCEInstancesActivity{}
	out, err := a.ScanGCEInstances(context.Background(), vmscan.ScanInput{})
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Empty(t, out.Items)
	assert.Empty(t, out.PartialFailures)
}

func TestScanGCEInstancesActivity_ListerInitFails(t *testing.T) {
	swapInstanceLister(t, nil, errors.New("init failed"))

	a := &ScanGCEInstancesActivity{}
	out, err := a.ScanGCEInstances(context.Background(), vmscan.ScanInput{ProjectIDs: []string{"p1"}})
	require.Error(t, err)
	assert.Nil(t, out)
}

func TestScanGCEInstancesActivity_AggregatesAcrossProjects(t *testing.T) {
	lister := &fakeInstanceLister{
		byProject: map[string][]hyperscalermodels.GCEInstance{
			"p1": {
				{Project: "p1", Name: "vm-a", SelfLink: "sl-a", Labels: map[string]string{"pool_uuid": "u1"}},
			},
			"p2": {
				{Project: "p2", Name: "vm-b", SelfLink: "sl-b", Labels: map[string]string{"pool_uuid": "u2"}},
				{Project: "p2", Name: "vm-c", SelfLink: "sl-c", Labels: map[string]string{}},
			},
		},
	}
	swapInstanceLister(t, lister, nil)

	a := &ScanGCEInstancesActivity{}
	out, err := a.ScanGCEInstances(context.Background(), vmscan.ScanInput{ProjectIDs: []string{"p1", "p2"}})
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Len(t, out.Items, 3)
	assert.Empty(t, out.PartialFailures)
	assert.Equal(t, []string{"p1", "p2"}, lister.calls)
}

func TestScanGCEInstancesActivity_PartialFailure_IsRecorded_NotFatal(t *testing.T) {
	lister := &fakeInstanceLister{
		byProject: map[string][]hyperscalermodels.GCEInstance{
			"good": {{Project: "good", Name: "vm-good", SelfLink: "sl-good", Labels: map[string]string{"pool_uuid": "u"}}},
			"bad":  nil,
		},
		errors: map[string]error{"bad": errors.New("permission denied")},
	}
	swapInstanceLister(t, lister, nil)

	a := &ScanGCEInstancesActivity{}
	out, err := a.ScanGCEInstances(context.Background(), vmscan.ScanInput{ProjectIDs: []string{"good", "bad"}})
	require.NoError(t, err, "single project failure must not fail the whole activity")
	require.Len(t, out.Items, 1)
	require.Len(t, out.PartialFailures, 1)
	assert.Equal(t, "bad", out.PartialFailures[0].Project)
	assert.Contains(t, out.PartialFailures[0].Error, "permission denied")
}

func TestScanGCEInstancesActivity_ContextCancelled(t *testing.T) {
	lister := &fakeInstanceLister{
		byProject: map[string][]hyperscalermodels.GCEInstance{
			"p1": {{Project: "p1", Name: "vm-a", SelfLink: "sl-a"}},
		},
	}
	swapInstanceLister(t, lister, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	a := &ScanGCEInstancesActivity{}
	out, err := a.ScanGCEInstances(ctx, vmscan.ScanInput{ProjectIDs: []string{"p1"}})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, out)
	assert.Empty(t, out.Items, "no project should be processed when context is already cancelled")
}
