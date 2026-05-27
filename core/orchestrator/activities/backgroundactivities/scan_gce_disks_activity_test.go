package backgroundactivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/diskscan"
	hyperscalerleakedresources "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/leakedresources"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
)

type fakeDiskLister struct {
	byProject map[string][]hyperscalermodels.GCEDisk
	errors    map[string]error
	calls     []string
}

func (f *fakeDiskLister) ListDisks(ctx context.Context, projectID string) ([]hyperscalermodels.GCEDisk, error) {
	f.calls = append(f.calls, projectID)
	if err, ok := f.errors[projectID]; ok {
		return f.byProject[projectID], err
	}
	return f.byProject[projectID], nil
}

func swapDiskLister(t *testing.T, l hyperscalerleakedresources.DiskLister, initErr error) {
	t.Helper()
	orig := newDiskListerForActivity
	t.Cleanup(func() { newDiskListerForActivity = orig })
	newDiskListerForActivity = func(ctx context.Context) (hyperscalerleakedresources.DiskLister, error) {
		if initErr != nil {
			return nil, initErr
		}
		return l, nil
	}
}

func TestScanGCEDisksActivity_NoProjects_ReturnsEmpty(t *testing.T) {
	a := &ScanGCEDisksActivity{}
	out, err := a.ScanGCEDisks(context.Background(), diskscan.ScanInput{})
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Empty(t, out.Items)
	assert.Empty(t, out.PartialFailures)
}

func TestScanGCEDisksActivity_ListerInitFails(t *testing.T) {
	swapDiskLister(t, nil, errors.New("init failed"))

	a := &ScanGCEDisksActivity{}
	out, err := a.ScanGCEDisks(context.Background(), diskscan.ScanInput{ProjectIDs: []string{"p1"}})
	require.Error(t, err)
	assert.Nil(t, out)
}

func TestScanGCEDisksActivity_AggregatesAcrossProjects(t *testing.T) {
	lister := &fakeDiskLister{
		byProject: map[string][]hyperscalermodels.GCEDisk{
			"p1": {
				{Project: "p1", Name: "disk-a", SelfLink: "sl-a", Labels: map[string]string{"pool_uuid": "u1"}},
			},
			"p2": {
				{Project: "p2", Name: "disk-b", SelfLink: "sl-b", Labels: map[string]string{"pool_uuid": "u2"}},
				{Project: "p2", Name: "disk-c", SelfLink: "sl-c", Labels: map[string]string{}},
			},
		},
	}
	swapDiskLister(t, lister, nil)

	a := &ScanGCEDisksActivity{}
	out, err := a.ScanGCEDisks(context.Background(), diskscan.ScanInput{
		ProjectIDs: []string{"p1", "p2"},
	})
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Len(t, out.Items, 3)
	assert.Empty(t, out.PartialFailures)
	assert.Equal(t, []string{"p1", "p2"}, lister.calls)
}

func TestScanGCEDisksActivity_PartialFailure_IsRecorded_NotFatal(t *testing.T) {
	lister := &fakeDiskLister{
		byProject: map[string][]hyperscalermodels.GCEDisk{
			"good": {{Project: "good", Name: "disk-good", SelfLink: "sl-good", Labels: map[string]string{"pool_uuid": "u"}}},
			"bad":  nil,
		},
		errors: map[string]error{"bad": errors.New("permission denied")},
	}
	swapDiskLister(t, lister, nil)

	a := &ScanGCEDisksActivity{}
	out, err := a.ScanGCEDisks(context.Background(), diskscan.ScanInput{
		ProjectIDs: []string{"good", "bad"},
	})
	require.NoError(t, err, "single project failure must not fail the whole activity")
	require.Len(t, out.Items, 1)
	require.Len(t, out.PartialFailures, 1)
	assert.Equal(t, "bad", out.PartialFailures[0].Project)
	assert.Contains(t, out.PartialFailures[0].Error, "permission denied")
}

func TestScanGCEDisksActivity_ContextCancelled(t *testing.T) {
	lister := &fakeDiskLister{
		byProject: map[string][]hyperscalermodels.GCEDisk{
			"p1": {{Project: "p1", Name: "disk-a", SelfLink: "sl-a"}},
		},
	}
	swapDiskLister(t, lister, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	a := &ScanGCEDisksActivity{}
	out, err := a.ScanGCEDisks(ctx, diskscan.ScanInput{
		ProjectIDs: []string{"p1"},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, out)
	assert.Empty(t, out.Items, "no project should be processed when context is already cancelled")
}
