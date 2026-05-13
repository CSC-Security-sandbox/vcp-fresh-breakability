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

type fakeCCFEBackupVaults struct {
	mock.Mock
}

func (f *fakeCCFEBackupVaults) ListBackupVaults(ctx context.Context, projectID, location string) ([]resourcescope.CachedBackupVault, error) {
	args := f.Called(ctx, projectID, location)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]resourcescope.CachedBackupVault), args.Error(1)
}

func TestFetchBackupVaults_ReturnsCCFEResult(t *testing.T) {
	ctx := context.Background()
	want := []resourcescope.CachedBackupVault{
		{UUID: "uuid-x", Name: "vault-x"},
		{UUID: "uuid-y", Name: "vault-y"},
	}
	ccfe := &fakeCCFEBackupVaults{}
	ccfe.On("ListBackupVaults", ctx, "proj-a", "us-central1").Return(want, nil)

	a := &FetchBackupVaultsActivity{CCFE: ccfe}
	got, err := a.FetchBackupVaults(ctx, "proj-a", "us-central1")
	require.NoError(t, err)
	assert.Equal(t, want, got)
	ccfe.AssertExpectations(t)
}

func TestFetchBackupVaults_EmptyResultPropagated(t *testing.T) {
	ctx := context.Background()
	ccfe := &fakeCCFEBackupVaults{}
	ccfe.On("ListBackupVaults", ctx, "proj-a", "us-central1").Return([]resourcescope.CachedBackupVault{}, nil)

	a := &FetchBackupVaultsActivity{CCFE: ccfe}
	got, err := a.FetchBackupVaults(ctx, "proj-a", "us-central1")
	require.NoError(t, err)
	assert.NotNil(t, got)
	assert.Empty(t, got)
	ccfe.AssertExpectations(t)
}

func TestFetchBackupVaults_CCFEDisabled_ReturnsNil(t *testing.T) {
	ctx := context.Background()
	ccfe := &fakeCCFEBackupVaults{}
	ccfe.On("ListBackupVaults", ctx, "proj-a", "us-central1").Return(nil, nil)

	a := &FetchBackupVaultsActivity{CCFE: ccfe}
	got, err := a.FetchBackupVaults(ctx, "proj-a", "us-central1")
	require.NoError(t, err)
	assert.Nil(t, got)
	ccfe.AssertExpectations(t)
}

func TestFetchBackupVaults_CCFEError_PropagatesError(t *testing.T) {
	ctx := context.Background()
	ccfe := &fakeCCFEBackupVaults{}
	ccfe.On("ListBackupVaults", ctx, "proj-a", "us-central1").Return(nil, errors.New("ccfe boom"))

	a := &FetchBackupVaultsActivity{CCFE: ccfe}
	got, err := a.FetchBackupVaults(ctx, "proj-a", "us-central1")
	assert.Error(t, err)
	assert.Nil(t, got)
	ccfe.AssertExpectations(t)
}
