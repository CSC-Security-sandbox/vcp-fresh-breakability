package detectors

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/vmscan"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

func newVMDetectorForTest(submit func(ctx context.Context, in vmscan.ScanInput) (*vmscan.ScanOutput, error), labelKey string, minAge time.Duration) *VMDetector {
	return &VMDetector{
		submitWorkflow: submit,
		labelKey:       labelKey,
		minVMAge:       minAge,
		taskQueue:      "test-queue",
	}
}

// vmOldTS returns a timestamp 24h in the past (comfortably older than any minAge used in tests).
func vmOldTS() string { return time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339) }

func TestVMDetector_Name(t *testing.T) {
	d := NewVMDetector()
	assert.Equal(t, "vm_orphan", d.Name())
}

func TestVMDetector_Detect_NilDetector(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	var d *VMDetector
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Nil(t, records)
}

func TestVMDetector_Detect_ListAllTpProjectsFails(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return(nil, errors.New("db error"))

	called := false
	d := newVMDetectorForTest(func(ctx context.Context, in vmscan.ScanInput) (*vmscan.ScanOutput, error) {
		called = true
		return nil, nil
	}, "pool_uuid", time.Hour)
	records, err := d.Detect(ctx, storage)
	assert.Error(t, err)
	assert.Nil(t, records)
	assert.Contains(t, err.Error(), "list all tp projects")
	assert.False(t, called, "submit should not run when listing RTP projects fails")
}

func TestVMDetector_Detect_NoTpProjects_SkipsScan(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{}, nil)

	called := false
	d := newVMDetectorForTest(func(ctx context.Context, in vmscan.ScanInput) (*vmscan.ScanOutput, error) {
		called = true
		return nil, nil
	}, "pool_uuid", time.Hour)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	assert.False(t, called, "submit must not run when there are no tenant projects to scan")
}

func TestVMDetector_Detect_ListPoolsFails(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(nil, errors.New("db error"))

	called := false
	d := newVMDetectorForTest(func(ctx context.Context, in vmscan.ScanInput) (*vmscan.ScanOutput, error) {
		called = true
		return nil, nil
	}, "pool_uuid", time.Hour)
	records, err := d.Detect(ctx, storage)
	assert.Error(t, err)
	assert.Nil(t, records)
	assert.Contains(t, err.Error(), "list pools")
	assert.False(t, called)
}

func TestVMDetector_Detect_NoLeaks_ActivePool(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{
		{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-uuid-1"}}},
	}, nil)

	d := newVMDetectorForTest(func(ctx context.Context, in vmscan.ScanInput) (*vmscan.ScanOutput, error) {
		return &vmscan.ScanOutput{
			Items: []vmscan.GCEInstanceItem{
				{
					Project:           "rtp-1",
					Name:              "gcnv-abc-01",
					Zone:              "us-central1-a",
					SelfLink:          "https://compute/projects/rtp-1/zones/us-central1-a/instances/gcnv-abc-01",
					Labels:            map[string]string{"pool_uuid": "pool-uuid-1", "deployment_id": "gcnv-abc"},
					CreationTimestamp: vmOldTS(),
				},
			},
		}, nil
	}, "pool_uuid", time.Hour)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
}

func TestVMDetector_Detect_OrphanVM_DeletedPool(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	d := newVMDetectorForTest(func(ctx context.Context, in vmscan.ScanInput) (*vmscan.ScanOutput, error) {
		return &vmscan.ScanOutput{
			Items: []vmscan.GCEInstanceItem{
				{
					Project:           "rtp-1",
					Name:              "gcnv-abc-01",
					Zone:              "us-central1-a",
					SelfLink:          "https://compute/projects/rtp-1/zones/us-central1-a/instances/gcnv-abc-01",
					MachineType:       "n2-standard-8",
					Status:            "RUNNING",
					Labels:            map[string]string{"pool_uuid": "deleted-pool-uuid", "deployment_id": "gcnv-abc"},
					CreationTimestamp: vmOldTS(),
				},
			},
		}, nil
	}, "pool_uuid", time.Hour)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, model.ResourceTypeVM, records[0].ResourceType)
	assert.Equal(t, "https://compute/projects/rtp-1/zones/us-central1-a/instances/gcnv-abc-01", records[0].ResourceID)
	assert.Equal(t, "gcnv-abc-01", records[0].ResourceName)
	assert.Equal(t, "rtp-1", records[0].ProjectID)
	assert.Equal(t, "us-central1", records[0].Region)
	assert.Equal(t, ReasonVMOrphanPoolMissing, records[0].Reason)
	assert.Equal(t, "deleted-pool-uuid", records[0].Extra["pool_uuid"])
	assert.Equal(t, "gcnv-abc", records[0].Extra["deployment_id"])
	assert.Equal(t, "n2-standard-8", records[0].Extra["machine_type"])
	assert.Equal(t, "RUNNING", records[0].Extra["status"])
	assert.Equal(t, "us-central1-a", records[0].Extra["zone"])
}

func TestVMDetector_Detect_OrphanVM_MissingLabel(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{
		{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-uuid-1"}}},
	}, nil)

	d := newVMDetectorForTest(func(ctx context.Context, in vmscan.ScanInput) (*vmscan.ScanOutput, error) {
		return &vmscan.ScanOutput{
			Items: []vmscan.GCEInstanceItem{
				{Project: "rtp-1", Name: "mystery-vm", Zone: "us-east4-b", Labels: map[string]string{}, CreationTimestamp: vmOldTS()},
			},
		}, nil
	}, "pool_uuid", time.Hour)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, ReasonVMOrphanPoolMissing, records[0].Reason)
	assert.Equal(t, "", records[0].Extra["pool_uuid"])
}

func TestVMDetector_Detect_MixedVMs(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{
		{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "active-pool"}}},
	}, nil)

	ts := vmOldTS()
	d := newVMDetectorForTest(func(ctx context.Context, in vmscan.ScanInput) (*vmscan.ScanOutput, error) {
		return &vmscan.ScanOutput{
			Items: []vmscan.GCEInstanceItem{
				{Project: "rtp-1", Name: "vm-ok", Zone: "z1", Labels: map[string]string{"pool_uuid": "active-pool"}, CreationTimestamp: ts},
				{Project: "rtp-1", Name: "vm-orphan", Zone: "z1", Labels: map[string]string{"pool_uuid": "gone-pool"}, CreationTimestamp: ts},
				{Project: "rtp-1", Name: "vm-no-label", Zone: "z2", Labels: map[string]string{}, CreationTimestamp: ts},
			},
		}, nil
	}, "pool_uuid", time.Hour)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	require.Len(t, records, 2)

	names := map[string]bool{}
	for _, r := range records {
		names[r.ResourceName] = true
	}
	assert.True(t, names["vm-orphan"])
	assert.True(t, names["vm-no-label"])
}

func TestVMDetector_Detect_SkipsTooYoungOrphan(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	young := time.Now().UTC().Add(-30 * time.Minute).Format(time.RFC3339)
	d := newVMDetectorForTest(func(ctx context.Context, in vmscan.ScanInput) (*vmscan.ScanOutput, error) {
		return &vmscan.ScanOutput{
			Items: []vmscan.GCEInstanceItem{
				{Project: "rtp-1", Name: "fresh-orphan", Zone: "us-central1-a", SelfLink: "self-link",
					Status: "PROVISIONING", CreationTimestamp: young,
					Labels: map[string]string{"pool_uuid": "gone-pool"}},
			},
		}, nil
	}, "pool_uuid", time.Hour)

	records, err := d.Detect(ctx, storage)
	require.NoError(t, err)
	assert.Empty(t, records, "orphan younger than minVMAge must not be reported")
}

func TestVMDetector_Detect_FlagsWhenCreationTimestampMissingOrUnparseable(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	d := newVMDetectorForTest(func(ctx context.Context, in vmscan.ScanInput) (*vmscan.ScanOutput, error) {
		return &vmscan.ScanOutput{
			Items: []vmscan.GCEInstanceItem{
				{Project: "rtp-1", Name: "no-ts", SelfLink: "self-1", Labels: map[string]string{"pool_uuid": "gone"}, CreationTimestamp: ""},
				{Project: "rtp-1", Name: "bad-ts", SelfLink: "self-2", Labels: map[string]string{"pool_uuid": "gone"}, CreationTimestamp: "not-a-date"},
			},
		}, nil
	}, "pool_uuid", time.Hour)

	records, err := d.Detect(ctx, storage)
	require.NoError(t, err)
	assert.Len(t, records, 2)
}

func TestVMDetector_Detect_WorkflowError(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	d := newVMDetectorForTest(func(ctx context.Context, in vmscan.ScanInput) (*vmscan.ScanOutput, error) {
		return nil, errors.New("temporal unavailable")
	}, "pool_uuid", time.Hour)

	records, err := d.Detect(ctx, storage)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scan workflow")
	assert.Nil(t, records)
}

func TestVMDetector_Detect_NilOutput(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	d := newVMDetectorForTest(func(ctx context.Context, in vmscan.ScanInput) (*vmscan.ScanOutput, error) {
		return nil, nil
	}, "pool_uuid", time.Hour)

	records, err := d.Detect(ctx, storage)
	require.NoError(t, err)
	assert.Nil(t, records)
}

func TestVMDetector_Detect_PartialFailures_StillProcessesRest(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1", "rtp-2"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	d := newVMDetectorForTest(func(ctx context.Context, in vmscan.ScanInput) (*vmscan.ScanOutput, error) {
		return &vmscan.ScanOutput{
			Items: []vmscan.GCEInstanceItem{{
				Project: "rtp-1", Zone: "us-central1-a", Name: "orphan", SelfLink: "self",
				Status: "RUNNING", CreationTimestamp: vmOldTS(),
				Labels: map[string]string{"pool_uuid": "gone-pool"},
			}},
			PartialFailures: []vmscan.ProjectFailure{{Project: "rtp-2", Error: "permission denied"}},
		}, nil
	}, "pool_uuid", time.Hour)

	records, err := d.Detect(ctx, storage)
	require.NoError(t, err, "partial failures must not abort detection")
	require.Len(t, records, 1)
	assert.Equal(t, "orphan", records[0].ResourceName)
}

func TestVMIsOlderThanHelper(t *testing.T) {
	now := time.Now().UTC()
	assert.True(t, vmIsOlderThan("", now, time.Hour))
	assert.True(t, vmIsOlderThan("not-a-date", now, time.Hour))
	assert.True(t, vmIsOlderThan(now.Add(-2*time.Hour).Format(time.RFC3339), now, time.Hour))
	assert.False(t, vmIsOlderThan(now.Add(-30*time.Minute).Format(time.RFC3339), now, time.Hour))
}

func TestDefaultVMMinAge(t *testing.T) {
	orig := envGetInt
	t.Cleanup(func() { envGetInt = orig })

	envGetInt = func(key string, defaultVal int) int { return 0 }
	assert.Equal(t, 6*time.Hour, defaultVMMinAge())

	envGetInt = func(key string, defaultVal int) int { return 12 }
	assert.Equal(t, 12*time.Hour, defaultVMMinAge())
}

func TestBuildVMLeakRecord_ResourceIDFallback(t *testing.T) {
	r := buildVMLeakRecord(vmscan.GCEInstanceItem{
		Project: "p", Zone: "us-central1-a", Name: "vm-x",
	}, "some-pool")
	assert.Equal(t, "projects/p/zones/us-central1-a/instances/vm-x", r.ResourceID)
	assert.Equal(t, "us-central1", r.Region)
}
