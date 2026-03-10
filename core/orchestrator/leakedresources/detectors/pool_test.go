package detectors

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

type mockCCFEPoolLister struct {
	mock.Mock
}

func (m *mockCCFEPoolLister) ListStoragePools(ctx context.Context, projectID, location string) ([]string, error) {
	args := m.Called(ctx, projectID, location)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func poolView(id int64, uuid, name, projectID, primaryZone string) *datamodel.PoolView {
	return &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: id, UUID: uuid},
			Name:      name,
			Account:   &datamodel.Account{Name: projectID},
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: primaryZone,
			},
		},
	}
}

func TestPoolDetector_Name(t *testing.T) {
	ccfe := &mockCCFEPoolLister{}
	d := NewPoolDetector(ccfe)
	assert.Equal(t, "pool", d.Name())
}

func TestPoolDetector_Detect_ListPoolsFails(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(nil, errors.New("db error"))
	ccfe := &mockCCFEPoolLister{}

	d := NewPoolDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.Error(t, err)
	assert.Nil(t, records)
}

func TestPoolDetector_Detect_NoPools(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	ccfe := &mockCCFEPoolLister{}

	d := NewPoolDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	ccfe.AssertNotCalled(t, "ListStoragePools")
}

func TestPoolDetector_Detect_PoolsSkipped_NoAccountOrPrimaryZone(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	// Pool without Account and PoolAttributes is skipped, so no groups -> no CCFE calls
	pools := []*datamodel.PoolView{
		{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, Name: "p1"}},
	}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)
	ccfe := &mockCCFEPoolLister{}

	d := NewPoolDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	ccfe.AssertNotCalled(t, "ListStoragePools")
}

func TestPoolDetector_Detect_CCFEReturnsNil_SkipsPair(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{
		poolView(1, "pool-uuid", "pool-name", "proj1", "us-central1-a"),
	}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)
	ccfe := &mockCCFEPoolLister{}
	ccfe.On("ListStoragePools", ctx, "proj1", "us-central1-a").Return(nil, nil) // CCFE disabled; location is zone for zonal pool

	d := NewPoolDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	ccfe.AssertExpectations(t)
}

func TestPoolDetector_Detect_InCCFENotInVCP(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{
		poolView(1, "pool-uuid", "vcp-only-pool", "proj1", "us-central1-a"),
	}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)
	ccfe := &mockCCFEPoolLister{}
	ccfe.On("ListStoragePools", ctx, "proj1", "us-central1-a").Return([]string{"vcp-only-pool", "ccfe-extra-pool"}, nil)

	d := NewPoolDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Len(t, records, 1)
	assert.Equal(t, model.ResourceTypePool, records[0].ResourceType)
	assert.Equal(t, "ccfe-extra-pool", records[0].ResourceID)
	assert.Equal(t, "ccfe-extra-pool", records[0].ResourceName)
	assert.Equal(t, "proj1", records[0].ProjectID)
	assert.Equal(t, "us-central1-a", records[0].Region)
	assert.Equal(t, ReasonInCCFENotInVCP, records[0].Reason)
}

func TestPoolDetector_Detect_InVCPNotInCCFE(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{
		poolView(1, "pool-uuid-1", "vcp-pool-1", "proj1", "us-central1-a"),
		poolView(2, "pool-uuid-2", "vcp-pool-2", "proj1", "us-central1-a"),
	}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)
	ccfe := &mockCCFEPoolLister{}
	ccfe.On("ListStoragePools", ctx, "proj1", "us-central1-a").Return([]string{"vcp-pool-1"}, nil) // vcp-pool-2 missing in CCFE

	d := NewPoolDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Len(t, records, 1)
	assert.Equal(t, model.ResourceTypePool, records[0].ResourceType)
	assert.Equal(t, "pool-uuid-2", records[0].ResourceID)
	assert.Equal(t, "vcp-pool-2", records[0].ResourceName)
	assert.Equal(t, "proj1", records[0].ProjectID)
	assert.Equal(t, "us-central1-a", records[0].Region)
	assert.Equal(t, ReasonInVCPNotInCCFE, records[0].Reason)
	assert.Equal(t, "pool-uuid-2", records[0].Extra["uuid"])
}

func TestPoolDetector_Detect_CCFEFails_SkipsPair(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{
		poolView(1, "pool-uuid", "pool-name", "proj1", "us-central1-a"),
	}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)
	ccfe := &mockCCFEPoolLister{}
	ccfe.On("ListStoragePools", ctx, "proj1", "us-central1-a").Return(nil, errors.New("ccfe error"))

	d := NewPoolDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	ccfe.AssertExpectations(t)
}

func TestPoolDetector_Detect_NoLeaks_SameInBoth(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{
		poolView(1, "pool-uuid", "pool-name", "proj1", "us-central1-a"),
	}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)
	ccfe := &mockCCFEPoolLister{}
	ccfe.On("ListStoragePools", ctx, "proj1", "us-central1-a").Return([]string{"pool-name"}, nil)

	d := NewPoolDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
}

// TestPoolDetector_Detect_RegionalPool_UseRegionAsLocation ensures pools with PrimaryZone as a region
// (e.g. us-central1 with no zone suffix) trigger CCFE list with region as location.
func TestPoolDetector_Detect_RegionalPool_UseRegionAsLocation(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{
		poolView(1, "pool-uuid", "regional-pool", "proj1", "us-central1"), // region only, no zone
	}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)
	ccfe := &mockCCFEPoolLister{}
	ccfe.On("ListStoragePools", ctx, "proj1", "us-central1").Return([]string{"regional-pool"}, nil)

	d := NewPoolDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	ccfe.AssertExpectations(t)
}

// TestPoolDetector_Detect_ZonalAndRegional_SeparateGroups ensures zonal and regional pools in the same
// project/region result in separate CCFE calls (one per location scope).
func TestPoolDetector_Detect_ZonalAndRegional_SeparateGroups(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{
		poolView(1, "uuid-zonal", "zonal-pool", "proj1", "australia-southeast1-a"),
		poolView(2, "uuid-regional", "regional-pool", "proj1", "australia-southeast1"),
	}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)
	ccfe := &mockCCFEPoolLister{}
	ccfe.On("ListStoragePools", ctx, "proj1", "australia-southeast1-a").Return([]string{"zonal-pool"}, nil)
	ccfe.On("ListStoragePools", ctx, "proj1", "australia-southeast1").Return([]string{"regional-pool"}, nil)

	d := NewPoolDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	ccfe.AssertExpectations(t)
}
