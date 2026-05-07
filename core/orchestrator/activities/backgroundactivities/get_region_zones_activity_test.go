package backgroundactivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

type fakeRegionZoneLister struct {
	mock.Mock
}

func (f *fakeRegionZoneLister) GetZones(project, region string) ([]string, error) {
	args := f.Called(project, region)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func withSecretManagerProject(t *testing.T, value string) {
	t.Helper()
	orig := env.SecretManagerProjectID
	env.SecretManagerProjectID = value
	t.Cleanup(func() { env.SecretManagerProjectID = orig })
}

func TestGetRegionZones_HappyPath(t *testing.T) {
	withSecretManagerProject(t, "vcp-host-project")
	gcp := &fakeRegionZoneLister{}
	gcp.On("GetZones", "vcp-host-project", "australia-southeast1").
		Return([]string{"australia-southeast1-a", "australia-southeast1-b", "australia-southeast1-c"}, nil)

	a := &GetRegionZonesActivity{GCP: gcp}
	got, err := a.GetRegionZones(context.Background(), "australia-southeast1")
	require.NoError(t, err)
	assert.Equal(t,
		[]string{"australia-southeast1-a", "australia-southeast1-b", "australia-southeast1-c"},
		got,
	)
	gcp.AssertExpectations(t)
}

func TestGetRegionZones_FiltersEmptyEntries(t *testing.T) {
	withSecretManagerProject(t, "vcp-host-project")
	gcp := &fakeRegionZoneLister{}
	gcp.On("GetZones", "vcp-host-project", "us-central1").
		Return([]string{"us-central1-a", "", "us-central1-b"}, nil)

	a := &GetRegionZonesActivity{GCP: gcp}
	got, err := a.GetRegionZones(context.Background(), "us-central1")
	require.NoError(t, err)
	assert.Equal(t, []string{"us-central1-a", "us-central1-b"}, got)
}

func TestGetRegionZones_EmptyProjectShortCircuits(t *testing.T) {
	withSecretManagerProject(t, "")
	gcp := &fakeRegionZoneLister{}

	a := &GetRegionZonesActivity{GCP: gcp}
	got, err := a.GetRegionZones(context.Background(), "us-central1")
	require.NoError(t, err)
	assert.Nil(t, got)
	gcp.AssertNotCalled(t, "GetZones", mock.Anything, mock.Anything)
}

func TestGetRegionZones_GetZonesError(t *testing.T) {
	withSecretManagerProject(t, "vcp-host-project")
	gcp := &fakeRegionZoneLister{}
	gcp.On("GetZones", "vcp-host-project", "us-central1").
		Return(nil, errors.New("compute api boom"))

	a := &GetRegionZonesActivity{GCP: gcp}
	got, err := a.GetRegionZones(context.Background(), "us-central1")
	assert.Error(t, err)
	assert.Nil(t, got)
}

func TestGetRegionZones_EmptyRegion(t *testing.T) {
	withSecretManagerProject(t, "vcp-host-project")
	a := &GetRegionZonesActivity{GCP: &fakeRegionZoneLister{}}

	got, err := a.GetRegionZones(context.Background(), "")
	assert.Error(t, err)
	assert.Nil(t, got)
}

// TestGetRegionZones_NilGCP_FallsBackToLazyInit covers the production
// path: when no GCP field is set on the activity (worker registers
// `&GetRegionZonesActivity{}`), the activity must lazily build a
// RegionZoneLister via gcpServiceForZones at call time.
func TestGetRegionZones_NilGCP_FallsBackToLazyInit(t *testing.T) {
	withSecretManagerProject(t, "vcp-host-project")

	gcp := &fakeRegionZoneLister{}
	gcp.On("GetZones", "vcp-host-project", "us-central1").
		Return([]string{"us-central1-a"}, nil)

	orig := gcpServiceForZones
	gcpServiceForZones = func(_ context.Context) (RegionZoneLister, error) {
		return gcp, nil
	}
	t.Cleanup(func() { gcpServiceForZones = orig })

	a := &GetRegionZonesActivity{}
	got, err := a.GetRegionZones(context.Background(), "us-central1")
	require.NoError(t, err)
	assert.Equal(t, []string{"us-central1-a"}, got)
	gcp.AssertExpectations(t)
}

func TestGetRegionZones_LazyInitFails_PropagatesError(t *testing.T) {
	withSecretManagerProject(t, "vcp-host-project")

	orig := gcpServiceForZones
	gcpServiceForZones = func(_ context.Context) (RegionZoneLister, error) {
		return nil, errors.New("init failed")
	}
	t.Cleanup(func() { gcpServiceForZones = orig })

	a := &GetRegionZonesActivity{}
	got, err := a.GetRegionZones(context.Background(), "us-central1")
	assert.Error(t, err)
	assert.Nil(t, got)
}
