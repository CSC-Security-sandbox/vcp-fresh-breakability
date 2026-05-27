package detectors

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/ipscan"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	hyperscalerleakedresources "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/leakedresources"
)

// newInternalReservedIPDetectorForTest constructs a detector with the
// workflow-submission path swapped out, mirroring the helper pattern used by
// the VM and disk detector tests.
func newInternalReservedIPDetectorForTest(submit func(ctx context.Context, in ipscan.ScanInput) (*ipscan.ScanOutput, error), minAge time.Duration) *InternalReservedIPDetector {
	if minAge <= 0 {
		minAge = time.Hour
	}
	return &InternalReservedIPDetector{
		submitWorkflow:    submit,
		minReservationAge: minAge,
		taskQueue:         "test-queue",
	}
}

// staticIPSubmitter returns a submit function that always yields the same scan output.
func staticIPSubmitter(out *ipscan.ScanOutput, err error) func(context.Context, ipscan.ScanInput) (*ipscan.ScanOutput, error) {
	return func(_ context.Context, _ ipscan.ScanInput) (*ipscan.ScanOutput, error) { return out, err }
}

// scanResultFor builds a one-group ScanOutput for a (project, region) pair.
func scanResultFor(project, region string, addrs []hyperscalerleakedresources.RegionalAddress) *ipscan.ScanOutput {
	return &ipscan.ScanOutput{
		Results: []ipscan.ScanResult{{Project: project, Region: region, Addresses: addrs}},
	}
}

func TestInternalReservedIPDetector_Name(t *testing.T) {
	d := NewInternalReservedIPDetector(time.Hour)
	assert.Equal(t, "internal_reserved_ip", d.Name())
}

func TestNewInternalReservedIPDetector_DefaultMinAge(t *testing.T) {
	d := NewInternalReservedIPDetector(0)
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

	called := false
	d := newInternalReservedIPDetectorForTest(func(context.Context, ipscan.ScanInput) (*ipscan.ScanOutput, error) {
		called = true
		return nil, nil
	}, time.Hour)
	records, err := d.Detect(ctx, storage)
	assert.Error(t, err)
	assert.Nil(t, records)
	assert.False(t, called, "submit must not run when listing pools fails")
}

func TestInternalReservedIPDetector_Detect_NoTargets_SkipsScan(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	called := false
	d := newInternalReservedIPDetectorForTest(func(context.Context, ipscan.ScanInput) (*ipscan.ScanOutput, error) {
		called = true
		return nil, nil
	}, time.Hour)
	records, err := d.Detect(ctx, storage)
	require.NoError(t, err)
	assert.Empty(t, records)
	assert.False(t, called, "submit must not run when there are no (project,region) targets")
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

	var captured ipscan.ScanInput
	d := newInternalReservedIPDetectorForTest(func(_ context.Context, in ipscan.ScanInput) (*ipscan.ScanOutput, error) {
		captured = in
		return scanResultFor("proj-tenant", "us-central1", addrs), nil
	}, time.Hour)

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

	require.Len(t, captured.Targets, 1)
	assert.Equal(t, "proj-tenant", captured.Targets[0].Project)
	assert.Equal(t, "us-central1", captured.Targets[0].Region)
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

	d := newInternalReservedIPDetectorForTest(staticIPSubmitter(scanResultFor("proj-tenant", "us-central1", addrs), nil), time.Hour)
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

	d := newInternalReservedIPDetectorForTest(staticIPSubmitter(scanResultFor("proj-tenant", "us-central1", addrs), nil), time.Hour)
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
		{Name: "in-use-status", AddressType: "INTERNAL", Status: "IN_USE", Subnetwork: "https://www.googleapis.com/compute/v1/projects/tp/regions/us-central1/subnetworks/sn1"},
		{Name: "bad-time", AddressType: "INTERNAL", Status: "RESERVED", Subnetwork: "https://www.googleapis.com/compute/v1/projects/tp/regions/us-central1/subnetworks/sn1", CreationTimeParsed: false},
		{Name: "no-subnet-base", AddressType: "INTERNAL", Status: "RESERVED", Subnetwork: "", CreationTimeParsed: true, CreationTime: time.Now().Add(-2 * time.Hour)},
		{Name: "fallback-key", ResourceName: "", AddressType: "INTERNAL", Status: "RESERVED", Subnetwork: "https://www.googleapis.com/compute/v1/projects/tp/regions/us-central1/subnetworks/sn1", CreationTimeParsed: true, CreationTime: time.Now().Add(-2 * time.Hour)},
	}

	d := newInternalReservedIPDetectorForTest(staticIPSubmitter(scanResultFor("tp", "us-central1", addrs), nil), time.Hour)
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

	called := false
	d := newInternalReservedIPDetectorForTest(func(context.Context, ipscan.ScanInput) (*ipscan.ScanOutput, error) {
		called = true
		return nil, nil
	}, time.Hour)
	records, err := d.Detect(ctx, storage)
	require.NoError(t, err)
	assert.Empty(t, records)
	assert.False(t, called, "non-READY pools must not produce any (project,region) target")
}

func TestInternalReservedIPDetector_Detect_WorkflowError(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{readyPoolWithSubnet("proj-tenant", "mysub", "us-central1-a")}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)

	d := newInternalReservedIPDetectorForTest(staticIPSubmitter(nil, errors.New("temporal unavailable")), time.Hour)
	records, err := d.Detect(ctx, storage)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scan workflow")
	assert.Nil(t, records)
}

func TestInternalReservedIPDetector_Detect_NilOutput(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{readyPoolWithSubnet("proj-tenant", "mysub", "us-central1-a")}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)

	d := newInternalReservedIPDetectorForTest(staticIPSubmitter(nil, nil), time.Hour)
	records, err := d.Detect(ctx, storage)
	require.NoError(t, err)
	assert.Nil(t, records)
}

func TestInternalReservedIPDetector_Detect_PartialFailures_DoNotAbort(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{
		readyPoolWithSubnet("proj-a", "sn-a", "us-central1-a"),
	}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)

	old := time.Now().UTC().Add(-3 * time.Hour)
	out := &ipscan.ScanOutput{
		Results: []ipscan.ScanResult{
			{
				Project: "proj-a",
				Region:  "us-central1",
				Addresses: []hyperscalerleakedresources.RegionalAddress{
					{
						Name: "leaked", IP: "10.0.0.5",
						Subnetwork:         "https://www.googleapis.com/compute/v1/projects/proj-a/regions/us-central1/subnetworks/sn-a",
						AddressType:        "INTERNAL",
						Status:             "RESERVED",
						CreationTime:       old,
						CreationTimeParsed: true,
					},
				},
			},
		},
		PartialFailures: []ipscan.ProjectRegionFailure{
			{Project: "other", Region: "europe-west1", Error: "permission denied"},
		},
	}

	d := newInternalReservedIPDetectorForTest(staticIPSubmitter(out, nil), time.Hour)
	records, err := d.Detect(ctx, storage)
	require.NoError(t, err, "partial failures must not abort detection")
	require.Len(t, records, 1)
	assert.Equal(t, "leaked", records[0].ResourceName)
}

func TestInternalReservedIPDetector_Detect_IgnoresUnaskedResultGroup(t *testing.T) {
	// Defensive: workflow returns a (project, region) we never asked about.
	// Detector should silently drop it rather than panic / index miss.
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{readyPoolWithSubnet("proj-a", "sn-a", "us-central1-a")}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)

	old := time.Now().UTC().Add(-3 * time.Hour)
	out := &ipscan.ScanOutput{
		Results: []ipscan.ScanResult{
			{
				Project: "stranger",
				Region:  "us-east4",
				Addresses: []hyperscalerleakedresources.RegionalAddress{
					{Name: "x", AddressType: "INTERNAL", Status: "RESERVED", CreationTime: old, CreationTimeParsed: true,
						Subnetwork: "https://www.googleapis.com/compute/v1/projects/stranger/regions/us-east4/subnetworks/sn-a"},
				},
			},
		},
	}
	d := newInternalReservedIPDetectorForTest(staticIPSubmitter(out, nil), time.Hour)
	records, err := d.Detect(ctx, storage)
	require.NoError(t, err)
	assert.Empty(t, records)
}
