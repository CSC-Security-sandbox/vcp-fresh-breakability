package detectors

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/resourcescope"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

// mockCCFEPoolFetcher is the testing-side implementation of CCFEPoolFetcher.
// We keep it small enough to inline rather than bringing in a full
// mockery-generated mock.
type mockCCFEPoolFetcher struct {
	mock.Mock
}

func (m *mockCCFEPoolFetcher) FetchCCFEPools(ctx context.Context, projectID string, locations []string) (map[string][]resourcescope.CachedPool, error) {
	args := m.Called(ctx, projectID, locations)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string][]resourcescope.CachedPool), args.Error(1)
}

// mockKeyLister is a constructor-driven ProjectLocationLister so each test
// can pin the exact list of (project, location) pairs the detector should
// iterate.
type mockKeyLister struct {
	pairs []resourcescope.ProjectLocation
	err   error
	calls int
}

func (m *mockKeyLister) ListProjectLocations(_ context.Context) ([]resourcescope.ProjectLocation, error) {
	m.calls++
	if m.err != nil {
		return nil, m.err
	}
	return m.pairs, nil
}

func newKeyLister(pairs ...resourcescope.ProjectLocation) *mockKeyLister {
	return &mockKeyLister{pairs: pairs}
}

func pair(project, location string) resourcescope.ProjectLocation {
	return resourcescope.ProjectLocation{ProjectID: project, Location: location}
}

func poolView(id int64, uuid, name, projectID, primaryZone string, isRegionalHA bool) *datamodel.PoolView {
	return &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: id, UUID: uuid},
			Name:      name,
			Account:   &datamodel.Account{Name: projectID},
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone:  primaryZone,
				IsRegionalHA: isRegionalHA,
			},
		},
	}
}

func TestPoolDetector_Name(t *testing.T) {
	d := NewPoolDetector(&mockCCFEPoolFetcher{}, newKeyLister())
	assert.Equal(t, "pool", d.Name())
}

func TestPoolDetector_Detect_ListPoolsFails(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListPoolsSelective(ctx, mock.Anything, mock.Anything).Return(nil, errors.New("db error"))
	fetcher := &mockCCFEPoolFetcher{}

	d := NewPoolDetector(fetcher, newKeyLister())
	records, err := d.Detect(ctx, storage)
	assert.Error(t, err)
	assert.Nil(t, records)
}

func TestPoolDetector_Detect_KeyListerFails_ReturnsError(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListPoolsSelective(ctx, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	fetcher := &mockCCFEPoolFetcher{}

	d := NewPoolDetector(fetcher, &mockKeyLister{err: errors.New("env not set")})
	records, err := d.Detect(ctx, storage)
	assert.Error(t, err)
	assert.Nil(t, records)
	fetcher.AssertNotCalled(t, "FetchCCFEPools")
}

// TestPoolDetector_Detect_NoEnumeratedPairs_NoFetches is the empty-shard
// case (no accounts → no enumerated pairs). The detector must skip the
// CCFE diff entirely.
func TestPoolDetector_Detect_NoEnumeratedPairs_NoFetches(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListPoolsSelective(ctx, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	fetcher := &mockCCFEPoolFetcher{}

	d := NewPoolDetector(fetcher, newKeyLister())
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	fetcher.AssertNotCalled(t, "FetchCCFEPools")
}

// TestPoolDetector_Detect_OneWorkflowPerProject is the marquee structural
// guarantee: enumerated pairs that share a project must collapse into a
// single FetchCCFEPools call carrying every location for that project.
// Different projects each get their own call.
func TestPoolDetector_Detect_OneWorkflowPerProject(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListPoolsSelective(ctx, mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	fetcher := &mockCCFEPoolFetcher{}
	fetcher.On("FetchCCFEPools", ctx, "proj1", []string{"us-central1", "us-central1-a", "us-central1-b"}).
		Return(map[string][]resourcescope.CachedPool{
			"us-central1":   {},
			"us-central1-a": {},
			"us-central1-b": {},
		}, nil).Once()
	fetcher.On("FetchCCFEPools", ctx, "proj2", []string{"us-central1"}).
		Return(map[string][]resourcescope.CachedPool{"us-central1": {}}, nil).Once()

	d := NewPoolDetector(fetcher, newKeyLister(
		pair("proj1", "us-central1"),
		pair("proj1", "us-central1-a"),
		pair("proj1", "us-central1-b"),
		pair("proj2", "us-central1"),
	))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	fetcher.AssertExpectations(t)
}

// TestPoolDetector_Detect_NilCCFEResult_SkipsPair covers the "CCFE
// disabled or transient miss" branch — the activity returned (nil, nil)
// after Temporal's retries gave up. The workflow keeps the location in
// the map with a nil value; the detector must skip that pair.
func TestPoolDetector_Detect_NilCCFEResult_SkipsPair(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{
		poolView(1, "pool-uuid", "pool-name", "proj1", "us-central1-a", false),
	}
	storage.EXPECT().ListPoolsSelective(ctx, mock.Anything, mock.Anything).Return(pools, nil)
	fetcher := &mockCCFEPoolFetcher{}
	fetcher.On("FetchCCFEPools", ctx, "proj1", []string{"us-central1-a"}).
		Return(map[string][]resourcescope.CachedPool{"us-central1-a": nil}, nil)

	d := NewPoolDetector(fetcher, newKeyLister(pair("proj1", "us-central1-a")))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	fetcher.AssertExpectations(t)
}

// TestPoolDetector_Detect_MissingLocationKey_SkipsPair covers the
// "workflow couldn't fetch this location" case: an activity exhausted
// retries and the workflow omitted the location from the map. The
// detector must skip that pair too.
func TestPoolDetector_Detect_MissingLocationKey_SkipsPair(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{
		poolView(1, "pool-uuid", "pool-name", "proj1", "us-central1-a", false),
	}
	storage.EXPECT().ListPoolsSelective(ctx, mock.Anything, mock.Anything).Return(pools, nil)
	fetcher := &mockCCFEPoolFetcher{}
	// us-central1-a was requested but workflow returned an empty map (or a map
	// without the key) because the activity exhausted retries.
	fetcher.On("FetchCCFEPools", ctx, "proj1", []string{"us-central1-a"}).
		Return(map[string][]resourcescope.CachedPool{}, nil)

	d := NewPoolDetector(fetcher, newKeyLister(pair("proj1", "us-central1-a")))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	fetcher.AssertExpectations(t)
}

func TestPoolDetector_Detect_InCCFENotInVCP(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{
		poolView(1, "uuid-vcp", "vcp-pool", "proj1", "us-central1-a", false),
	}
	storage.EXPECT().ListPoolsSelective(ctx, mock.Anything, mock.Anything).Return(pools, nil)
	fetcher := &mockCCFEPoolFetcher{}
	fetcher.On("FetchCCFEPools", ctx, "proj1", []string{"us-central1-a"}).
		Return(map[string][]resourcescope.CachedPool{
			"us-central1-a": {
				{UUID: "uuid-vcp", Name: "vcp-pool"},
				{UUID: "uuid-ccfe-only", Name: "ccfe-extra-pool"},
			},
		}, nil)

	d := NewPoolDetector(fetcher, newKeyLister(pair("proj1", "us-central1-a")))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Len(t, records, 1)
	assert.Equal(t, model.ResourceTypePool, records[0].ResourceType)
	assert.Equal(t, "uuid-ccfe-only", records[0].ResourceID)
	assert.Equal(t, "ccfe-extra-pool", records[0].ResourceName)
	assert.Equal(t, "proj1", records[0].ProjectID)
	assert.Equal(t, "us-central1-a", records[0].Region)
	assert.Equal(t, ReasonInCCFENotInVCP, records[0].Reason)
	assert.Equal(t, "uuid-ccfe-only", records[0].Extra["uuid"])
}

func TestPoolDetector_Detect_InVCPNotInCCFE(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{
		poolView(1, "uuid-1", "vcp-pool-1", "proj1", "us-central1-a", false),
		poolView(2, "uuid-2", "vcp-pool-2", "proj1", "us-central1-a", false),
	}
	storage.EXPECT().ListPoolsSelective(ctx, mock.Anything, mock.Anything).Return(pools, nil)
	fetcher := &mockCCFEPoolFetcher{}
	// CCFE only knows about uuid-1 -> uuid-2 must be flagged as in_vcp_not_in_ccfe.
	fetcher.On("FetchCCFEPools", ctx, "proj1", []string{"us-central1-a"}).
		Return(map[string][]resourcescope.CachedPool{
			"us-central1-a": {{UUID: "uuid-1", Name: "vcp-pool-1"}},
		}, nil)

	d := NewPoolDetector(fetcher, newKeyLister(pair("proj1", "us-central1-a")))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Len(t, records, 1)
	assert.Equal(t, "uuid-2", records[0].ResourceID)
	assert.Equal(t, "vcp-pool-2", records[0].ResourceName)
	assert.Equal(t, ReasonInVCPNotInCCFE, records[0].Reason)
}

// TestPoolDetector_Detect_NameReusedAcrossUUIDs guards the "name can be
// reused" case the UUID-keyed comparison is meant to catch.
func TestPoolDetector_Detect_NameReusedAcrossUUIDs(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{
		poolView(1, "uuid-vcp", "shared-name", "proj1", "us-central1-a", false),
	}
	storage.EXPECT().ListPoolsSelective(ctx, mock.Anything, mock.Anything).Return(pools, nil)
	fetcher := &mockCCFEPoolFetcher{}
	fetcher.On("FetchCCFEPools", ctx, "proj1", []string{"us-central1-a"}).
		Return(map[string][]resourcescope.CachedPool{
			"us-central1-a": {{UUID: "uuid-ccfe", Name: "shared-name"}},
		}, nil)

	d := NewPoolDetector(fetcher, newKeyLister(pair("proj1", "us-central1-a")))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	require.Len(t, records, 2)

	byReason := map[string]model.LeakRecord{}
	for _, r := range records {
		byReason[r.Reason] = r
	}
	require.Contains(t, byReason, ReasonInCCFENotInVCP)
	require.Contains(t, byReason, ReasonInVCPNotInCCFE)
	assert.Equal(t, "uuid-ccfe", byReason[ReasonInCCFENotInVCP].ResourceID)
	assert.Equal(t, "uuid-vcp", byReason[ReasonInVCPNotInCCFE].ResourceID)
}

// TestPoolDetector_Detect_FetchFails_SkipsProject ensures a permanent
// workflow failure (e.g. Temporal infra error or workflow reported error)
// is logged and the project is skipped rather than aborting the whole
// detector — other projects can still produce useful leak records.
func TestPoolDetector_Detect_FetchFails_SkipsProject(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{
		poolView(1, "uuid-1", "pool-1", "proj1", "us-central1-a", false),
		poolView(2, "uuid-2", "pool-2", "proj2", "us-central1-a", false),
	}
	storage.EXPECT().ListPoolsSelective(ctx, mock.Anything, mock.Anything).Return(pools, nil)
	fetcher := &mockCCFEPoolFetcher{}
	fetcher.On("FetchCCFEPools", ctx, "proj1", []string{"us-central1-a"}).
		Return(nil, errors.New("temporal boom"))
	// Other project still gets fetched.
	fetcher.On("FetchCCFEPools", ctx, "proj2", []string{"us-central1-a"}).
		Return(map[string][]resourcescope.CachedPool{
			"us-central1-a": {{UUID: "uuid-2", Name: "pool-2"}},
		}, nil)

	d := NewPoolDetector(fetcher, newKeyLister(
		pair("proj1", "us-central1-a"),
		pair("proj2", "us-central1-a"),
	))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	fetcher.AssertExpectations(t)
}

func TestPoolDetector_Detect_NoLeaks_SameInBoth(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{
		poolView(1, "pool-uuid", "pool-name", "proj1", "us-central1-a", false),
	}
	storage.EXPECT().ListPoolsSelective(ctx, mock.Anything, mock.Anything).Return(pools, nil)
	fetcher := &mockCCFEPoolFetcher{}
	fetcher.On("FetchCCFEPools", ctx, "proj1", []string{"us-central1-a"}).
		Return(map[string][]resourcescope.CachedPool{
			"us-central1-a": {{UUID: "pool-uuid", Name: "pool-name"}},
		}, nil)

	d := NewPoolDetector(fetcher, newKeyLister(pair("proj1", "us-central1-a")))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
}

// TestPoolDetector_Detect_FetchesEnumeratedZonesEvenWhenVCPHasNoPools
// locks in the design intent: even zones VCP has no rows in must be
// fetched and diffed, so CCFE-only pools surface as in_ccfe_not_in_vcp.
func TestPoolDetector_Detect_FetchesEnumeratedZonesEvenWhenVCPHasNoPools(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	// VCP only has a pool in zone-a; the enumeration also includes region,
	// zone-b, and zone-c. The detector must still fetch all four locations
	// in one workflow call and report any CCFE-only pools the workflow
	// returned.
	pools := []*datamodel.PoolView{
		poolView(1, "uuid-vcp", "vcp-pool", "proj1", "australia-southeast1-a", false),
	}
	storage.EXPECT().ListPoolsSelective(ctx, mock.Anything, mock.Anything).Return(pools, nil)

	fetcher := &mockCCFEPoolFetcher{}
	fetcher.On("FetchCCFEPools", ctx, "proj1", []string{
		"australia-southeast1",
		"australia-southeast1-a",
		"australia-southeast1-b",
		"australia-southeast1-c",
	}).Return(map[string][]resourcescope.CachedPool{
		"australia-southeast1":   {},
		"australia-southeast1-a": {{UUID: "uuid-vcp", Name: "vcp-pool"}},
		"australia-southeast1-b": {},
		"australia-southeast1-c": {{UUID: "uuid-zone-c-only", Name: "ccfe-only-zone-c"}},
	}, nil).Once()

	d := NewPoolDetector(fetcher, newKeyLister(
		pair("proj1", "australia-southeast1"),
		pair("proj1", "australia-southeast1-a"),
		pair("proj1", "australia-southeast1-b"),
		pair("proj1", "australia-southeast1-c"),
	))
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "uuid-zone-c-only", records[0].ResourceID)
	assert.Equal(t, "australia-southeast1-c", records[0].Region)
	assert.Equal(t, ReasonInCCFENotInVCP, records[0].Reason)
	fetcher.AssertExpectations(t)
}

func TestNewTemporalCCFEPoolFetcher_NilClient_ReturnsError(t *testing.T) {
	f := NewTemporalCCFEPoolFetcher(nil)
	require.NotNil(t, f)
	pools, err := f.FetchCCFEPools(context.Background(), "proj-a", []string{"us-central1"})
	assert.Error(t, err)
	assert.Nil(t, pools)
}

func TestNewTemporalZoneFetcher_NilClient_ReturnsError(t *testing.T) {
	f := NewTemporalZoneFetcher(nil)
	require.NotNil(t, f)
	zones, err := f.GetRegionZones(context.Background(), "us-central1")
	assert.Error(t, err)
	assert.Nil(t, zones)
}
