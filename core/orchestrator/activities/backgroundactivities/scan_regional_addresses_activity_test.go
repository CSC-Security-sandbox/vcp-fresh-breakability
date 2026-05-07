package backgroundactivities

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/ipscan"
	hyperscalerleakedresources "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/leakedresources"
	"testing"
)

type fakeRegionalAddressLister struct {
	byKey  map[string][]hyperscalerleakedresources.RegionalAddress
	errors map[string]error
	calls  []string
}

func (f *fakeRegionalAddressLister) ListRegionalAddresses(ctx context.Context, projectID, region string) ([]hyperscalerleakedresources.RegionalAddress, error) {
	key := projectID + "/" + region
	f.calls = append(f.calls, key)
	if err, ok := f.errors[key]; ok {
		return f.byKey[key], err
	}
	return f.byKey[key], nil
}

func swapRegionalAddressLister(t *testing.T, l hyperscalerleakedresources.RegionalAddressLister, initErr error) {
	t.Helper()
	orig := newRegionalAddressListerForActivity
	t.Cleanup(func() { newRegionalAddressListerForActivity = orig })
	newRegionalAddressListerForActivity = func(ctx context.Context) (hyperscalerleakedresources.RegionalAddressLister, error) {
		if initErr != nil {
			return nil, initErr
		}
		return l, nil
	}
}

func TestScanRegionalAddressesActivity_NoTargets_ReturnsEmpty(t *testing.T) {
	a := &ScanRegionalAddressesActivity{}
	out, err := a.ScanRegionalAddresses(context.Background(), ipscan.ScanInput{})
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Empty(t, out.Results)
	assert.Empty(t, out.PartialFailures)
}

func TestScanRegionalAddressesActivity_ListerInitFails(t *testing.T) {
	swapRegionalAddressLister(t, nil, errors.New("init failed"))

	a := &ScanRegionalAddressesActivity{}
	out, err := a.ScanRegionalAddresses(context.Background(), ipscan.ScanInput{
		Targets: []ipscan.ProjectRegion{{Project: "p1", Region: "us-central1"}},
	})
	require.Error(t, err)
	assert.Nil(t, out)
}

func TestScanRegionalAddressesActivity_AggregatesAcrossPairs(t *testing.T) {
	lister := &fakeRegionalAddressLister{
		byKey: map[string][]hyperscalerleakedresources.RegionalAddress{
			"p1/us-central1": {
				{Name: "a", AddressType: "INTERNAL", Status: "RESERVED"},
			},
			"p2/us-east4": {
				{Name: "b", AddressType: "INTERNAL", Status: "RESERVED"},
				{Name: "c", AddressType: "EXTERNAL", Status: "RESERVED"},
			},
		},
	}
	swapRegionalAddressLister(t, lister, nil)

	a := &ScanRegionalAddressesActivity{}
	out, err := a.ScanRegionalAddresses(context.Background(), ipscan.ScanInput{
		Targets: []ipscan.ProjectRegion{
			{Project: "p1", Region: "us-central1"},
			{Project: "p2", Region: "us-east4"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out)
	require.Len(t, out.Results, 2)
	assert.Empty(t, out.PartialFailures)
	assert.Equal(t, []string{"p1/us-central1", "p2/us-east4"}, lister.calls)

	byPair := map[string]ipscan.ScanResult{}
	for _, r := range out.Results {
		byPair[r.Project+"/"+r.Region] = r
	}
	assert.Len(t, byPair["p1/us-central1"].Addresses, 1)
	assert.Len(t, byPair["p2/us-east4"].Addresses, 2)
}

func TestScanRegionalAddressesActivity_PartialFailure_IsRecorded_NotFatal(t *testing.T) {
	lister := &fakeRegionalAddressLister{
		byKey: map[string][]hyperscalerleakedresources.RegionalAddress{
			"good/us-central1": {{Name: "ok-addr", AddressType: "INTERNAL", Status: "RESERVED"}},
			"bad/us-east4":     nil,
		},
		errors: map[string]error{"bad/us-east4": errors.New("permission denied")},
	}
	swapRegionalAddressLister(t, lister, nil)

	a := &ScanRegionalAddressesActivity{}
	out, err := a.ScanRegionalAddresses(context.Background(), ipscan.ScanInput{
		Targets: []ipscan.ProjectRegion{
			{Project: "good", Region: "us-central1"},
			{Project: "bad", Region: "us-east4"},
		},
	})
	require.NoError(t, err, "single pair failure must not fail the whole activity")
	require.Len(t, out.Results, 1)
	require.Len(t, out.PartialFailures, 1)
	assert.Equal(t, "bad", out.PartialFailures[0].Project)
	assert.Equal(t, "us-east4", out.PartialFailures[0].Region)
	assert.Contains(t, out.PartialFailures[0].Error, "permission denied")
}

func TestScanRegionalAddressesActivity_ContextCancelled(t *testing.T) {
	lister := &fakeRegionalAddressLister{
		byKey: map[string][]hyperscalerleakedresources.RegionalAddress{
			"p1/us-central1": {{Name: "a"}},
		},
	}
	swapRegionalAddressLister(t, lister, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	a := &ScanRegionalAddressesActivity{}
	out, err := a.ScanRegionalAddresses(ctx, ipscan.ScanInput{
		Targets: []ipscan.ProjectRegion{{Project: "p1", Region: "us-central1"}},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, out)
	assert.Empty(t, out.Results, "no pair should be processed when context is already cancelled")
}
