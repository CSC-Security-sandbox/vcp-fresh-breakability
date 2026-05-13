package backgroundactivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/resourcescope"
)

// fakeCCFE is a tiny stand-in for ccfe.Client so we can drive the activity
// through its three observable outcomes (success / disabled / error) without
// spinning up the real httptest fixture used by the leaked-resources
// integration tests.
type fakeCCFE struct {
	mock.Mock
}

func (f *fakeCCFE) ListStoragePools(ctx context.Context, projectID, location string) ([]resourcescope.CachedPool, error) {
	args := f.Called(ctx, projectID, location)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]resourcescope.CachedPool), args.Error(1)
}

func TestFetchStoragePools_ReturnsCCFEResult(t *testing.T) {
	ctx := context.Background()
	want := []resourcescope.CachedPool{
		{UUID: "uuid-x", Name: "pool-x"},
		{UUID: "uuid-y", Name: "pool-y"},
	}
	ccfe := &fakeCCFE{}
	ccfe.On("ListStoragePools", ctx, "proj-a", "us-central1").Return(want, nil)

	a := &FetchStoragePoolsActivity{CCFE: ccfe}
	got, err := a.FetchStoragePools(ctx, "proj-a", "us-central1")
	require.NoError(t, err)
	assert.Equal(t, want, got)
	ccfe.AssertExpectations(t)
}

func TestFetchStoragePools_EmptyResultPropagated(t *testing.T) {
	ctx := context.Background()
	ccfe := &fakeCCFE{}
	// Empty (but non-nil) is a real answer: CCFE has zero pools for this pair.
	// The detector relies on this distinction to skip the pair on a nil result
	// (CCFE disabled) but still produce in_vcp_not_in_ccfe leaks on [].
	ccfe.On("ListStoragePools", ctx, "proj-a", "us-central1").Return([]resourcescope.CachedPool{}, nil)

	a := &FetchStoragePoolsActivity{CCFE: ccfe}
	got, err := a.FetchStoragePools(ctx, "proj-a", "us-central1")
	require.NoError(t, err)
	assert.NotNil(t, got)
	assert.Empty(t, got)
	ccfe.AssertExpectations(t)
}

func TestFetchStoragePools_CCFEDisabled_ReturnsNil(t *testing.T) {
	ctx := context.Background()
	ccfe := &fakeCCFE{}
	ccfe.On("ListStoragePools", ctx, "proj-a", "us-central1").Return(nil, nil)

	a := &FetchStoragePoolsActivity{CCFE: ccfe}
	got, err := a.FetchStoragePools(ctx, "proj-a", "us-central1")
	require.NoError(t, err)
	assert.Nil(t, got)
	ccfe.AssertExpectations(t)
}

func TestFetchStoragePools_CCFEError_PropagatesError(t *testing.T) {
	ctx := context.Background()
	ccfe := &fakeCCFE{}
	ccfe.On("ListStoragePools", ctx, "proj-a", "us-central1").Return(nil, errors.New("ccfe boom"))

	a := &FetchStoragePoolsActivity{CCFE: ccfe}
	got, err := a.FetchStoragePools(ctx, "proj-a", "us-central1")
	assert.Error(t, err)
	assert.Nil(t, got)
	ccfe.AssertExpectations(t)
}
