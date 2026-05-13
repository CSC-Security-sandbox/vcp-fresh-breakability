package detectors

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/resourcescope"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

type mockCCFEBackupVaultFetcher struct {
	mock.Mock
}

func (m *mockCCFEBackupVaultFetcher) FetchCCFEBackupVaults(ctx context.Context, projectID string, locations []string) (map[string][]resourcescope.CachedBackupVault, error) {
	args := m.Called(ctx, projectID, locations)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string][]resourcescope.CachedBackupVault), args.Error(1)
}

// mockBVLister is a test ProjectLocationLister that returns a fixed set of pairs.
type mockBVLister struct {
	pairs []resourcescope.ProjectLocation
	err   error
}

func (m *mockBVLister) ListProjectLocations(_ context.Context) ([]resourcescope.ProjectLocation, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.pairs, nil
}

func bvLister(pairs ...resourcescope.ProjectLocation) *mockBVLister {
	return &mockBVLister{pairs: pairs}
}

func plPair(project, location string) resourcescope.ProjectLocation {
	return resourcescope.ProjectLocation{ProjectID: project, Location: location}
}

func backupVault(uuid, name, projectID, sourceRegion string) *datamodel.BackupVault {
	r := sourceRegion
	return &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{UUID: uuid},
		Name:             name,
		Account:          &datamodel.Account{Name: projectID},
		SourceRegionName: &r,
	}
}

func cachedVault(uuid, name string) resourcescope.CachedBackupVault {
	return resourcescope.CachedBackupVault{UUID: uuid, Name: name}
}

func TestBackupVaultDetector_Name(t *testing.T) {
	d := NewBackupVaultDetector(&mockCCFEBackupVaultFetcher{}, bvLister())
	assert.Equal(t, "backup_vault", d.Name())
}

func TestBackupVaultDetector_Detect_GetMultipleFails(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(nil, errors.New("db error"))
	fetcher := &mockCCFEBackupVaultFetcher{}

	d := NewBackupVaultDetector(fetcher, bvLister(plPair("proj1", "us-central1")))
	records, err := d.Detect(ctx, storage)
	assert.Error(t, err)
	assert.Nil(t, records)
}

func TestBackupVaultDetector_Detect_ListerFails_ReturnsError(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(nil, nil)
	fetcher := &mockCCFEBackupVaultFetcher{}

	d := NewBackupVaultDetector(fetcher, &mockBVLister{err: errors.New("region not set")})
	records, err := d.Detect(ctx, storage)
	assert.Error(t, err)
	assert.Nil(t, records)
	fetcher.AssertNotCalled(t, "FetchCCFEBackupVaults")
}

func TestBackupVaultDetector_Detect_NoPairs_NoFetches(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(nil, nil)
	fetcher := &mockCCFEBackupVaultFetcher{}

	d := NewBackupVaultDetector(fetcher, bvLister())
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	fetcher.AssertNotCalled(t, "FetchCCFEBackupVaults")
}

// TestBackupVaultDetector_Detect_ZeroVCPVaults_StillQueriesCCFE verifies
// that even with zero VCP backup vaults, the detector still queries CCFE
// for every account and reports in_ccfe_not_in_vcp leaks.
func TestBackupVaultDetector_Detect_ZeroVCPVaults_StillQueriesCCFE(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(nil, nil)
	fetcher := &mockCCFEBackupVaultFetcher{}
	fetcher.On("FetchCCFEBackupVaults", ctx, "proj1", []string{"us-central1"}).
		Return(map[string][]resourcescope.CachedBackupVault{
			"us-central1": {cachedVault("ccfe-uuid-1", "ccfe-only-vault")},
		}, nil)

	d := NewBackupVaultDetector(fetcher, bvLister(plPair("proj1", "us-central1")))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, model.ResourceTypeBackupVault, records[0].ResourceType)
	assert.Equal(t, "ccfe-uuid-1", records[0].ResourceID)
	assert.Equal(t, "ccfe-only-vault", records[0].ResourceName)
	assert.Equal(t, ReasonInCCFENotInVCP, records[0].Reason)
	assert.Equal(t, "proj1", records[0].ProjectID)
	assert.Equal(t, "us-central1", records[0].Region)
	fetcher.AssertExpectations(t)
}

// TestBackupVaultDetector_Detect_ZonePairsCollapsedToRegion verifies
// that zone-level pairs from the shared ProjectLocationLister are
// deduplicated to a single region-level pair for backup vaults.
func TestBackupVaultDetector_Detect_ZonePairsCollapsedToRegion(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(nil, nil)
	fetcher := &mockCCFEBackupVaultFetcher{}
	fetcher.On("FetchCCFEBackupVaults", ctx, "proj1", []string{"us-central1"}).
		Return(map[string][]resourcescope.CachedBackupVault{
			"us-central1": {cachedVault("uuid-a", "vault-a")},
		}, nil).Once()

	d := NewBackupVaultDetector(fetcher, bvLister(
		plPair("proj1", "us-central1"),
		plPair("proj1", "us-central1-a"),
		plPair("proj1", "us-central1-b"),
		plPair("proj1", "us-central1-c"),
	))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "uuid-a", records[0].ResourceID)
	assert.Equal(t, ReasonInCCFENotInVCP, records[0].Reason)
	fetcher.AssertExpectations(t)
}

func TestBackupVaultDetector_Detect_CCFEReturnsNil_SkipsLocation(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	vaults := []*datamodel.BackupVault{
		backupVault("bv-uuid-1", "vault-a", "proj1", "us-central1"),
	}
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(vaults, nil)
	fetcher := &mockCCFEBackupVaultFetcher{}
	fetcher.On("FetchCCFEBackupVaults", ctx, "proj1", []string{"us-central1"}).
		Return(map[string][]resourcescope.CachedBackupVault{"us-central1": nil}, nil)

	d := NewBackupVaultDetector(fetcher, bvLister(plPair("proj1", "us-central1")))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	fetcher.AssertExpectations(t)
}

func TestBackupVaultDetector_Detect_InCCFENotInVCP(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	vaults := []*datamodel.BackupVault{
		backupVault("bv-uuid-1", "vault-a", "proj1", "us-central1"),
	}
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(vaults, nil)
	fetcher := &mockCCFEBackupVaultFetcher{}
	fetcher.On("FetchCCFEBackupVaults", ctx, "proj1", []string{"us-central1"}).
		Return(map[string][]resourcescope.CachedBackupVault{
			"us-central1": {
				cachedVault("bv-uuid-1", "vault-a"),
				cachedVault("ccfe-only-uuid", "ccfe-only"),
			},
		}, nil)

	d := NewBackupVaultDetector(fetcher, bvLister(plPair("proj1", "us-central1")))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Len(t, records, 1)
	assert.Equal(t, model.ResourceTypeBackupVault, records[0].ResourceType)
	assert.Equal(t, "ccfe-only-uuid", records[0].ResourceID)
	assert.Equal(t, "ccfe-only", records[0].ResourceName)
	assert.Equal(t, ReasonInCCFENotInVCP, records[0].Reason)
	assert.Equal(t, "proj1", records[0].ProjectID)
	assert.Equal(t, "us-central1", records[0].Region)
}

func TestBackupVaultDetector_Detect_InVCPNotInCCFE(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	vaults := []*datamodel.BackupVault{
		backupVault("bv-uuid-1", "vault-a", "proj1", "us-central1"),
		backupVault("bv-uuid-2", "vault-b", "proj1", "us-central1"),
	}
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(vaults, nil)
	fetcher := &mockCCFEBackupVaultFetcher{}
	fetcher.On("FetchCCFEBackupVaults", ctx, "proj1", []string{"us-central1"}).
		Return(map[string][]resourcescope.CachedBackupVault{
			"us-central1": {cachedVault("bv-uuid-1", "vault-a")},
		}, nil)

	d := NewBackupVaultDetector(fetcher, bvLister(plPair("proj1", "us-central1")))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Len(t, records, 1)
	assert.Equal(t, model.ResourceTypeBackupVault, records[0].ResourceType)
	assert.Equal(t, "bv-uuid-2", records[0].ResourceID)
	assert.Equal(t, "vault-b", records[0].ResourceName)
	assert.Equal(t, ReasonInVCPNotInCCFE, records[0].Reason)
	assert.Equal(t, "bv-uuid-2", records[0].Extra["uuid"])
}

func TestBackupVaultDetector_Detect_FetcherFails_SkipsProject(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	vaults := []*datamodel.BackupVault{
		backupVault("bv-uuid", "vault-a", "proj1", "us-central1"),
	}
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(vaults, nil)
	fetcher := &mockCCFEBackupVaultFetcher{}
	fetcher.On("FetchCCFEBackupVaults", ctx, "proj1", []string{"us-central1"}).
		Return(nil, errors.New("workflow error"))

	d := NewBackupVaultDetector(fetcher, bvLister(plPair("proj1", "us-central1")))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
}

func TestBackupVaultDetector_Detect_NoLeaks(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	vaults := []*datamodel.BackupVault{
		backupVault("bv-uuid", "vault-a", "proj1", "us-central1"),
	}
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(vaults, nil)
	fetcher := &mockCCFEBackupVaultFetcher{}
	fetcher.On("FetchCCFEBackupVaults", ctx, "proj1", []string{"us-central1"}).
		Return(map[string][]resourcescope.CachedBackupVault{
			"us-central1": {cachedVault("bv-uuid", "vault-a")},
		}, nil)

	d := NewBackupVaultDetector(fetcher, bvLister(plPair("proj1", "us-central1")))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
}

func TestBackupVaultDetector_Detect_LocationMissingFromFetcherResult_Skipped(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	vaults := []*datamodel.BackupVault{
		backupVault("bv-uuid-1", "vault-a", "proj1", "us-central1"),
	}
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(vaults, nil)
	fetcher := &mockCCFEBackupVaultFetcher{}
	fetcher.On("FetchCCFEBackupVaults", ctx, "proj1", []string{"us-central1"}).
		Return(map[string][]resourcescope.CachedBackupVault{}, nil)

	d := NewBackupVaultDetector(fetcher, bvLister(plPair("proj1", "us-central1")))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
}

func TestNewTemporalCCFEBackupVaultFetcher_NilClient_ReturnsError(t *testing.T) {
	f := NewTemporalCCFEBackupVaultFetcher(nil)
	require.NotNil(t, f)
	vaults, err := f.FetchCCFEBackupVaults(context.Background(), "proj-a", []string{"us-central1"})
	assert.Error(t, err)
	assert.Nil(t, vaults)
}

// TestBackupVaultDetector_Detect_OneWorkflowPerProject ensures
// enumerated pairs sharing a project collapse into one FetchCCFEBackupVaults
// call carrying all locations for that project.
func TestBackupVaultDetector_Detect_OneWorkflowPerProject(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(nil, nil)

	fetcher := &mockCCFEBackupVaultFetcher{}
	fetcher.On("FetchCCFEBackupVaults", ctx, "proj1", []string{"us-central1", "us-east1"}).
		Return(map[string][]resourcescope.CachedBackupVault{
			"us-central1": {},
			"us-east1":    {},
		}, nil).Once()
	fetcher.On("FetchCCFEBackupVaults", ctx, "proj2", []string{"us-central1"}).
		Return(map[string][]resourcescope.CachedBackupVault{
			"us-central1": {},
		}, nil).Once()

	d := NewBackupVaultDetector(fetcher, bvLister(
		plPair("proj1", "us-central1"),
		plPair("proj1", "us-east1"),
		plPair("proj2", "us-central1"),
	))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	fetcher.AssertExpectations(t)
}

// TestBackupVaultDetector_Detect_SameNameDifferentUUID verifies that the UUID
// comparison catches delete-and-recreate under the same name — which a
// name-based diff would miss.
func TestBackupVaultDetector_Detect_SameNameDifferentUUID(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	vaults := []*datamodel.BackupVault{
		backupVault("old-uuid", "vault-a", "proj1", "us-central1"),
	}
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(vaults, nil)
	fetcher := &mockCCFEBackupVaultFetcher{}
	// CCFE has a vault with the same name but a new UUID (recreated).
	fetcher.On("FetchCCFEBackupVaults", ctx, "proj1", []string{"us-central1"}).
		Return(map[string][]resourcescope.CachedBackupVault{
			"us-central1": {cachedVault("new-uuid", "vault-a")},
		}, nil)

	d := NewBackupVaultDetector(fetcher, bvLister(plPair("proj1", "us-central1")))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	// Both sides should appear: "new-uuid" in CCFE not in VCP, "old-uuid" in VCP not in CCFE.
	require.Len(t, records, 2)
	reasons := map[string]string{}
	for _, r := range records {
		reasons[r.ResourceID] = r.Reason
	}
	assert.Equal(t, ReasonInCCFENotInVCP, reasons["new-uuid"])
	assert.Equal(t, ReasonInVCPNotInCCFE, reasons["old-uuid"])
}

func TestRegionOnlyPairs(t *testing.T) {
	pairs := []resourcescope.ProjectLocation{
		{ProjectID: "p1", Location: "us-central1"},
		{ProjectID: "p1", Location: "us-central1-a"},
		{ProjectID: "p1", Location: "us-central1-b"},
		{ProjectID: "p2", Location: "europe-west1"},
		{ProjectID: "p2", Location: "europe-west1-b"},
		{ProjectID: "p2", Location: "europe-west1-c"},
	}
	got := regionOnlyPairs(pairs)
	require.Len(t, got, 2)
	assert.Equal(t, "p1", got[0].ProjectID)
	assert.Equal(t, "us-central1", got[0].Location)
	assert.Equal(t, "p2", got[1].ProjectID)
	assert.Equal(t, "europe-west1", got[1].Location)
}
