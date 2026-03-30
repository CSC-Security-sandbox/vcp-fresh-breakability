package detectors

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	hyperscalerleakedresources "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/leakedresources"
)

type mockRegionalAddressLister struct {
	mock.Mock
}

func (m *mockRegionalAddressLister) ListRegionalAddresses(ctx context.Context, projectID, region string) ([]hyperscalerleakedresources.RegionalAddress, error) {
	args := m.Called(ctx, projectID, region)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]hyperscalerleakedresources.RegionalAddress), args.Error(1)
}

var _ RegionalAddressLister = (*mockRegionalAddressLister)(nil)

func TestInternalReservedIPDetector_Name(t *testing.T) {
	d := NewInternalReservedIPDetector(&mockRegionalAddressLister{}, time.Hour)
	assert.Equal(t, "internal_reserved_ip", d.Name())
}

func TestNewInternalReservedIPDetector_DefaultMinAge(t *testing.T) {
	d := NewInternalReservedIPDetector(&mockRegionalAddressLister{}, 0)
	assert.Equal(t, 6*time.Hour, d.minReservationAge)
}

func TestDefaultInternalReservedIPMinAge(t *testing.T) {
	origGetInt := envGetInt
	t.Cleanup(func() { envGetInt = origGetInt })

	envGetInt = func(key string, defaultVal int) int { return 0 }
	assert.Equal(t, 6*time.Hour, DefaultInternalReservedIPMinAge())

	envGetInt = func(key string, defaultVal int) int { return 12 }
	assert.Equal(t, 12*time.Hour, DefaultInternalReservedIPMinAge())
}

func TestInternalReservedIPDetector_Detect_NilDetector(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	var d *InternalReservedIPDetector
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Nil(t, records)
}

func TestInternalReservedIPDetector_Detect_ListPoolsFails(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(nil, errors.New("db error"))

	l := &mockRegionalAddressLister{}
	d := NewInternalReservedIPDetector(l, time.Hour)
	records, err := d.Detect(ctx, storage)
	assert.Error(t, err)
	assert.Nil(t, records)
	l.AssertNotCalled(t, "ListRegionalAddresses")
}

func readyPoolWithSubnet(tenantProject, subnet, zone string) *datamodel.PoolView {
	return &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-1"},
			State:     models.LifeCycleStateREADY,
			ClusterDetails: datamodel.ClusterDetails{
				RegionalTenantProject: tenantProject,
				SubnetNames:           []string{subnet},
			},
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: zone,
			},
		},
	}
}

func TestInternalReservedIPDetector_Detect_LeakOldUnassignedInternal(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{readyPoolWithSubnet("proj-tenant", "mysub", "us-central1-a")}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)

	subURL := "https://www.googleapis.com/compute/v1/projects/proj-tenant/regions/us-central1/subnetworks/mysub"
	addrs := []hyperscalerleakedresources.RegionalAddress{
		{
			ResourceName:       "https://www.googleapis.com/compute/v1/projects/proj-tenant/regions/us-central1/addresses/leaked-ip",
			Name:               "leaked-ip",
			IP:                 "10.1.1.5",
			Subnetwork:         subURL,
			AddressType:        "INTERNAL",
			Status:             "RESERVED",
			Users:              nil,
			CreationTimestamp:  time.Now().UTC().Add(-3 * time.Hour).Format(time.RFC3339),
			CreationTime:       time.Now().UTC().Add(-3 * time.Hour),
			CreationTimeParsed: true,
		},
	}

	l := &mockRegionalAddressLister{}
	l.On("ListRegionalAddresses", ctx, "proj-tenant", "us-central1").Return(addrs, nil)

	d := NewInternalReservedIPDetector(l, 1*time.Hour)
	records, err := d.Detect(ctx, storage)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, model.ResourceTypeInternalReservedIP, records[0].ResourceType)
	assert.Equal(t, ReasonInternalReservedIPUnassignedCapacity, records[0].Reason)
	assert.Equal(t, "proj-tenant", records[0].ProjectID)
	assert.Equal(t, "us-central1", records[0].Region)
	assert.Equal(t, "leaked-ip", records[0].ResourceName)
	assert.Equal(t, "10.1.1.5", records[0].Extra["ip"])
	assert.Equal(t, "mysub", records[0].Extra["subnet"])
	assert.Contains(t, records[0].Extra["pool_uuids"], "pool-1")
}

func TestInternalReservedIPDetector_Detect_SkipsTooNewReservation(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{readyPoolWithSubnet("proj-tenant", "mysub", "us-central1-a")}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)

	subURL := "https://www.googleapis.com/compute/v1/projects/proj-tenant/regions/us-central1/subnetworks/mysub"
	addrs := []hyperscalerleakedresources.RegionalAddress{
		{
			ResourceName:       "https://www.googleapis.com/compute/v1/projects/proj-tenant/regions/us-central1/addresses/new-ip",
			Name:               "new-ip",
			IP:                 "10.1.1.6",
			Subnetwork:         subURL,
			AddressType:        "INTERNAL",
			Status:             "RESERVED",
			Users:              nil,
			CreationTime:       time.Now().UTC().Add(-30 * time.Minute),
			CreationTimeParsed: true,
		},
	}

	l := &mockRegionalAddressLister{}
	l.On("ListRegionalAddresses", ctx, "proj-tenant", "us-central1").Return(addrs, nil)

	d := NewInternalReservedIPDetector(l, 1*time.Hour)
	records, err := d.Detect(ctx, storage)
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestInternalReservedIPDetector_Detect_SkipsExternalAndInUse(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{readyPoolWithSubnet("proj-tenant", "mysub", "us-central1-a")}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)

	subURL := "https://www.googleapis.com/compute/v1/projects/proj-tenant/regions/us-central1/subnetworks/mysub"
	old := time.Now().UTC().Add(-3 * time.Hour)
	addrs := []hyperscalerleakedresources.RegionalAddress{
		{
			Name: "ext", IP: "x", Subnetwork: subURL,
			AddressType: "EXTERNAL", Status: "RESERVED", Users: nil,
			CreationTime: old, CreationTimeParsed: true,
		},
		{
			Name: "in-use", IP: "10.0.0.1", Subnetwork: subURL,
			AddressType: "INTERNAL", Status: "RESERVED", Users: []string{"https://compute.googleapis.com/..."},
			CreationTime: old, CreationTimeParsed: true,
		},
		{
			Name: "wrong-sub", IP: "10.0.0.2",
			Subnetwork:  "https://www.googleapis.com/compute/v1/projects/proj-tenant/regions/us-central1/subnetworks/other",
			AddressType: "INTERNAL", Status: "RESERVED", Users: nil,
			CreationTime: old, CreationTimeParsed: true,
		},
	}

	l := &mockRegionalAddressLister{}
	l.On("ListRegionalAddresses", ctx, "proj-tenant", "us-central1").Return(addrs, nil)

	d := NewInternalReservedIPDetector(l, 1*time.Hour)
	records, err := d.Detect(ctx, storage)
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestInternalReservedIPDetector_Detect_CoversAdditionalSkipBranches(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{
		nil,
		{Pool: datamodel.Pool{}}, // nil PoolAttributes
		{Pool: datamodel.Pool{PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: ""}}}, // empty zone
		{Pool: datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "bad-zone"},
			State:          models.LifeCycleStateREADY,
			PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "not-a-zone"},
		}},
		{Pool: datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "no-tp"},
			State:          models.LifeCycleStateREADY,
			PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-central1-a"},
			ClusterDetails: datamodel.ClusterDetails{SubnetNames: []string{"sn1"}},
		}},
		{Pool: datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "no-sn"},
			State:          models.LifeCycleStateREADY,
			PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-central1-a"},
			ClusterDetails: datamodel.ClusterDetails{RegionalTenantProject: "tp"},
		}},
		readyPoolWithSubnet("tp", "sn1", "us-central1-a"),
	}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)

	addrs := []hyperscalerleakedresources.RegionalAddress{
		// non-RESERVED branch
		{Name: "in-use-status", AddressType: "INTERNAL", Status: "IN_USE", Subnetwork: "https://www.googleapis.com/compute/v1/projects/tp/regions/us-central1/subnetworks/sn1"},
		// unparsed timestamp branch
		{Name: "bad-time", AddressType: "INTERNAL", Status: "RESERVED", Subnetwork: "https://www.googleapis.com/compute/v1/projects/tp/regions/us-central1/subnetworks/sn1", CreationTimeParsed: false},
		// no subnetwork base branch
		{Name: "no-subnet-base", AddressType: "INTERNAL", Status: "RESERVED", Subnetwork: "", CreationTimeParsed: true, CreationTime: time.Now().Add(-2 * time.Hour)},
		// no resource name branch (fallback key generation)
		{Name: "fallback-key", ResourceName: "", AddressType: "INTERNAL", Status: "RESERVED", Subnetwork: "https://www.googleapis.com/compute/v1/projects/tp/regions/us-central1/subnetworks/sn1", CreationTimeParsed: true, CreationTime: time.Now().Add(-2 * time.Hour)},
	}
	l := &mockRegionalAddressLister{}
	l.On("ListRegionalAddresses", ctx, "tp", "us-central1").Return(addrs, nil)

	d := NewInternalReservedIPDetector(l, time.Hour)
	records, err := d.Detect(ctx, storage)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Contains(t, records[0].ResourceID, "projects/tp/regions/us-central1/addresses/fallback-key")
}

func TestInternalReservedIPDetector_Detect_SkipsNonReadyPool(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	p := readyPoolWithSubnet("proj-tenant", "mysub", "us-central1-a")
	p.State = "CREATING"
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{p}, nil)

	l := &mockRegionalAddressLister{}
	d := NewInternalReservedIPDetector(l, time.Hour)
	records, err := d.Detect(ctx, storage)
	require.NoError(t, err)
	assert.Empty(t, records)
	l.AssertNotCalled(t, "ListRegionalAddresses")
}

func TestInternalReservedIPDetector_Detect_ListAddressesErrorContinuesEmpty(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{readyPoolWithSubnet("proj-tenant", "mysub", "us-central1-a")}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)

	l := &mockRegionalAddressLister{}
	l.On("ListRegionalAddresses", ctx, "proj-tenant", "us-central1").Return(nil, errors.New("api error"))

	d := NewInternalReservedIPDetector(l, time.Hour)
	records, err := d.Detect(ctx, storage)
	require.NoError(t, err)
	assert.Empty(t, records)
}
