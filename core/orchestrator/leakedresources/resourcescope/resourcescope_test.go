package resourcescope

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

func mkPool(uuid, name, projectID, primaryZone string, isRegionalHA bool, withAccount bool, withAttrs bool) *datamodel.PoolView {
	pv := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: uuid},
			Name:      name,
		},
	}
	if withAccount {
		pv.Pool.Account = &datamodel.Account{Name: projectID}
	}
	if withAttrs {
		pv.Pool.PoolAttributes = &datamodel.PoolAttributes{
			PrimaryZone:  primaryZone,
			IsRegionalHA: isRegionalHA,
		}
	}
	return pv
}

func TestKey_FormatsAsProjectSlashLocation(t *testing.T) {
	k := ProjectLocation{ProjectID: "proj-a", Location: "us-central1-a"}.Key()
	assert.Equal(t, "proj-a/us-central1-a", k)
}

func TestGroupPoolsByProjectLocation_SkipsIncompletePools(t *testing.T) {
	ctx := context.Background()
	pools := []*datamodel.PoolView{
		mkPool("u1", "p1", "proj-a", "us-central1-a", false, true, true),
		mkPool("u2", "p2", "", "us-central1-a", false, false, true),       // no Account
		mkPool("u3", "p3", "proj-a", "", false, true, true),                // empty PrimaryZone
		mkPool("u4", "p4", "proj-a", "us-central1-a", false, true, false),  // no PoolAttributes
	}

	groups := GroupPoolsByProjectLocation(ctx, pools)
	assert.Len(t, groups, 1)
	for k, v := range groups {
		assert.Equal(t, "proj-a", k.ProjectID)
		assert.Equal(t, "us-central1-a", k.Location)
		assert.Len(t, v, 1)
		assert.Equal(t, "u1", v[0].UUID)
	}
}

func TestGroupPoolsByProjectLocation_RegionalUsesRegion(t *testing.T) {
	ctx := context.Background()
	pools := []*datamodel.PoolView{
		mkPool("u-zonal", "p1", "proj-a", "us-central1-a", false, true, true),
		mkPool("u-regional", "p2", "proj-a", "us-central1-a", true, true, true),
	}

	groups := GroupPoolsByProjectLocation(ctx, pools)
	assert.Len(t, groups, 2)

	keys := map[string]bool{}
	for k := range groups {
		keys[k.Key()] = true
	}
	assert.True(t, keys["proj-a/us-central1-a"], "zonal pool should use zone")
	assert.True(t, keys["proj-a/us-central1"], "regional pool should use region")
}

func TestGroupPoolsByProjectLocation_ZonalPoolWithRegionOnlyZone_FallsBackToRegion(t *testing.T) {
	ctx := context.Background()
	pools := []*datamodel.PoolView{
		mkPool("u1", "p1", "proj-a", "us-central1", false, true, true),
	}

	groups := GroupPoolsByProjectLocation(ctx, pools)
	assert.Len(t, groups, 1)
	for k := range groups {
		assert.Equal(t, "us-central1", k.Location)
	}
}

// TestGroupPoolsByProjectLocation_InvalidPrimaryZone_Skipped exercises the
// ParseRegionAndZone error branch: PrimaryZone values that don't match the
// "<region>(-<zone>)?" pattern must be skipped (logged at debug) rather than
// crash the grouping pass.
func TestGroupPoolsByProjectLocation_InvalidPrimaryZone_Skipped(t *testing.T) {
	ctx := context.Background()
	pools := []*datamodel.PoolView{
		mkPool("u-bad-1", "bad-1", "proj-a", "garbage", false, true, true),
		mkPool("u-bad-2", "bad-2", "proj-a", "INVALID_ZONE", true, true, true),
		mkPool("u-good", "good", "proj-a", "us-central1-a", false, true, true),
	}

	groups := GroupPoolsByProjectLocation(ctx, pools)
	assert.Len(t, groups, 1, "only the well-formed pool should produce a group")
	for k, v := range groups {
		assert.Equal(t, "us-central1-a", k.Location)
		assert.Len(t, v, 1)
		assert.Equal(t, "u-good", v[0].UUID)
	}
}

type fakeZoneFetcher struct {
	mock.Mock
}

func (f *fakeZoneFetcher) GetRegionZones(ctx context.Context, region string) ([]string, error) {
	args := f.Called(ctx, region)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func withRegion(t *testing.T, value string) {
	t.Helper()
	orig := env.Region
	env.Region = value
	t.Cleanup(func() { env.Region = orig })
}

func acct(name string) *database.AccountTelemetryData {
	return &database.AccountTelemetryData{Name: name}
}

func collectKeys(pairs []ProjectLocation) []string {
	out := make([]string, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, p.Key())
	}
	return out
}

func TestEnumerateProjectLocationKeys_HappyPath_RegionPlusZones(t *testing.T) {
	ctx := context.Background()
	withRegion(t, "australia-southeast1")

	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAccountsForTelemetry(ctx, mock.Anything).Return(
		[]*database.AccountTelemetryData{acct("100"), acct("200")}, nil,
	)

	zf := &fakeZoneFetcher{}
	zf.On("GetRegionZones", ctx, "australia-southeast1").Return(
		[]string{"australia-southeast1-c", "australia-southeast1-a", "australia-southeast1-b"}, nil,
	)

	pairs, err := EnumerateProjectLocationKeys(ctx, storage, zf)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{
		"100/australia-southeast1",
		"100/australia-southeast1-a",
		"100/australia-southeast1-b",
		"100/australia-southeast1-c",
		"200/australia-southeast1",
		"200/australia-southeast1-a",
		"200/australia-southeast1-b",
		"200/australia-southeast1-c",
	}, collectKeys(pairs))
}

func TestEnumerateProjectLocationKeys_LocalRegionUnset_ReturnsError(t *testing.T) {
	withRegion(t, "")
	storage := database.NewMockStorage(t)

	pairs, err := EnumerateProjectLocationKeys(context.Background(), storage, &fakeZoneFetcher{})
	assert.Error(t, err)
	assert.Nil(t, pairs)
}

func TestEnumerateProjectLocationKeys_NilZoneFetcher_ReturnsError(t *testing.T) {
	withRegion(t, "us-central1")
	storage := database.NewMockStorage(t)

	pairs, err := EnumerateProjectLocationKeys(context.Background(), storage, nil)
	assert.Error(t, err)
	assert.Nil(t, pairs)
}

func TestEnumerateProjectLocationKeys_NoAccounts_DoesNotCallZoneFetcher(t *testing.T) {
	ctx := context.Background()
	withRegion(t, "us-central1")

	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAccountsForTelemetry(ctx, mock.Anything).Return(
		[]*database.AccountTelemetryData{}, nil,
	)
	zf := &fakeZoneFetcher{}

	pairs, err := EnumerateProjectLocationKeys(ctx, storage, zf)
	require.NoError(t, err)
	assert.Empty(t, pairs)
	zf.AssertNotCalled(t, "GetRegionZones", mock.Anything, mock.Anything)
}

func TestEnumerateProjectLocationKeys_GetRegionZonesFails_FallsBackToRegionOnly(t *testing.T) {
	ctx := context.Background()
	withRegion(t, "us-central1")

	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAccountsForTelemetry(ctx, mock.Anything).Return(
		[]*database.AccountTelemetryData{acct("100"), acct("200")}, nil,
	)

	zf := &fakeZoneFetcher{}
	zf.On("GetRegionZones", ctx, "us-central1").Return(nil, errors.New("temporal boom"))

	pairs, err := EnumerateProjectLocationKeys(ctx, storage, zf)
	require.NoError(t, err)
	assert.ElementsMatch(t,
		[]string{"100/us-central1", "200/us-central1"},
		collectKeys(pairs),
	)
}

func TestEnumerateProjectLocationKeys_FiltersBlankZonesAndAccounts(t *testing.T) {
	ctx := context.Background()
	withRegion(t, "us-central1")

	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAccountsForTelemetry(ctx, mock.Anything).Return(
		[]*database.AccountTelemetryData{acct("100"), {Name: ""}, nil},
		nil,
	)

	zf := &fakeZoneFetcher{}
	zf.On("GetRegionZones", ctx, "us-central1").Return([]string{"us-central1-a", "", "us-central1-b"}, nil)

	pairs, err := EnumerateProjectLocationKeys(ctx, storage, zf)
	require.NoError(t, err)
	assert.ElementsMatch(t,
		[]string{"100/us-central1", "100/us-central1-a", "100/us-central1-b"},
		collectKeys(pairs),
	)
}

func TestEnumerateProjectLocationKeys_ListAccountsFails_PropagatesError(t *testing.T) {
	ctx := context.Background()
	withRegion(t, "us-central1")

	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAccountsForTelemetry(ctx, mock.Anything).Return(nil, errors.New("db error"))

	pairs, err := EnumerateProjectLocationKeys(ctx, storage, &fakeZoneFetcher{})
	assert.Error(t, err)
	assert.Nil(t, pairs)
}
