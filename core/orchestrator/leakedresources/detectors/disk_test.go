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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/diskscan"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

func newDiskDetectorForTest(submit func(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error), labelKey string, minAge time.Duration) *DiskDetector {
	return &DiskDetector{
		submitWorkflow: submit,
		labelKey:       labelKey,
		minDiskAge:     minAge,
		taskQueue:      "test-queue",
	}
}

// oldTS returns a timestamp 24h in the past (comfortably older than any minAge used in tests).
func oldTS() string { return time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339) }

func TestDiskDetector_Name(t *testing.T) {
	d := NewDiskDetector()
	assert.Equal(t, "disk_orphan", d.Name())
}

func TestDiskDetector_Detect_NilDetector(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	var d *DiskDetector
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Nil(t, records)
}

func TestDiskDetector_Detect_ListAllTpProjectsFails(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return(nil, errors.New("db error"))

	called := false
	d := newDiskDetectorForTest(func(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error) {
		called = true
		return nil, nil
	}, "pool_uuid", time.Hour)
	records, err := d.Detect(ctx, storage)
	assert.Error(t, err)
	assert.Nil(t, records)
	assert.Contains(t, err.Error(), "list all tp projects")
	assert.False(t, called, "submit should not run when listing RTP projects fails")
}

func TestDiskDetector_Detect_NoTpProjects_SkipsScan(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{}, nil)

	called := false
	d := newDiskDetectorForTest(func(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error) {
		called = true
		return nil, nil
	}, "pool_uuid", time.Hour)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	assert.False(t, called, "submit must not run when there are no tenant projects to scan")
}

func TestDiskDetector_Detect_ListPoolsFails(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(nil, errors.New("db error"))

	called := false
	d := newDiskDetectorForTest(func(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error) {
		called = true
		return nil, nil
	}, "pool_uuid", time.Hour)
	records, err := d.Detect(ctx, storage)
	assert.Error(t, err)
	assert.Nil(t, records)
	assert.Contains(t, err.Error(), "list pools")
	assert.False(t, called)
}

func TestDiskDetector_Detect_NoLeaks_ActivePool(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{
		{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-uuid-1"}}},
	}, nil)

	d := newDiskDetectorForTest(func(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error) {
		return &diskscan.ScanOutput{
			Items: []diskscan.GCEDiskItem{
				{
					Project:           "rtp-1",
					Name:              "gcnv-abc-01-disk-boot",
					Zone:              "us-central1-a",
					SelfLink:          "https://compute/projects/rtp-1/zones/us-central1-a/disks/gcnv-abc-01-disk-boot",
					Labels:            map[string]string{"pool_uuid": "pool-uuid-1", "deployment_id": "gcnv-abc"},
					CreationTimestamp: oldTS(),
				},
			},
		}, nil
	}, "pool_uuid", time.Hour)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
}

func TestDiskDetector_Detect_OrphanDisk_DeletedPool(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	d := newDiskDetectorForTest(func(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error) {
		return &diskscan.ScanOutput{
			Items: []diskscan.GCEDiskItem{
				{
					Project:           "rtp-1",
					Name:              "gcnv-abc-01-disk-boot",
					Zone:              "us-central1-a",
					SelfLink:          "https://compute/projects/rtp-1/zones/us-central1-a/disks/gcnv-abc-01-disk-boot",
					SizeGB:            10,
					Type:              "hyperdisk-balanced",
					Labels:            map[string]string{"pool_uuid": "deleted-pool-uuid", "deployment_id": "gcnv-abc"},
					CreationTimestamp: oldTS(),
				},
			},
		}, nil
	}, "pool_uuid", time.Hour)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, model.ResourceTypeDisk, records[0].ResourceType)
	assert.Equal(t, "https://compute/projects/rtp-1/zones/us-central1-a/disks/gcnv-abc-01-disk-boot", records[0].ResourceID)
	assert.Equal(t, "gcnv-abc-01-disk-boot", records[0].ResourceName)
	assert.Equal(t, "rtp-1", records[0].ProjectID)
	assert.Equal(t, "us-central1", records[0].Region)
	assert.Equal(t, ReasonDiskOrphanPoolMissing, records[0].Reason)
	assert.Equal(t, "deleted-pool-uuid", records[0].Extra["pool_uuid"])
	assert.Equal(t, "gcnv-abc", records[0].Extra["deployment_id"])
	assert.Equal(t, "10", records[0].Extra["size_gb"])
}

func TestDiskDetector_Detect_OrphanDisk_MissingLabel(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{
		{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-uuid-1"}}},
	}, nil)

	d := newDiskDetectorForTest(func(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error) {
		return &diskscan.ScanOutput{
			Items: []diskscan.GCEDiskItem{
				{Project: "rtp-1", Name: "mystery-disk", Zone: "us-east4-b", Labels: map[string]string{}, CreationTimestamp: oldTS()},
			},
		}, nil
	}, "pool_uuid", time.Hour)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, ReasonDiskOrphanPoolMissing, records[0].Reason)
	assert.Equal(t, "", records[0].Extra["pool_uuid"])
}

func TestDiskDetector_Detect_MixedDisks(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{
		{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "active-pool"}}},
	}, nil)

	ts := oldTS()
	d := newDiskDetectorForTest(func(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error) {
		return &diskscan.ScanOutput{
			Items: []diskscan.GCEDiskItem{
				{Project: "rtp-1", Name: "disk-ok", Zone: "z1", Labels: map[string]string{"pool_uuid": "active-pool"}, CreationTimestamp: ts},
				{Project: "rtp-1", Name: "disk-orphan", Zone: "z1", Labels: map[string]string{"pool_uuid": "gone-pool"}, CreationTimestamp: ts},
				{Project: "rtp-1", Name: "disk-no-label", Zone: "z2", Labels: map[string]string{}, CreationTimestamp: ts},
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
	assert.True(t, names["disk-orphan"])
	assert.True(t, names["disk-no-label"])
}

func TestDiskDetector_Detect_SkipsTooYoungOrphan(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	young := time.Now().UTC().Add(-30 * time.Minute).Format(time.RFC3339)
	d := newDiskDetectorForTest(func(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error) {
		return &diskscan.ScanOutput{
			Items: []diskscan.GCEDiskItem{
				{Project: "rtp-1", Name: "fresh-orphan", Zone: "us-central1-a", SelfLink: "self-link",
					Status: "READY", CreationTimestamp: young,
					Labels: map[string]string{"pool_uuid": "gone-pool"}},
			},
		}, nil
	}, "pool_uuid", time.Hour)

	records, err := d.Detect(ctx, storage)
	require.NoError(t, err)
	assert.Empty(t, records, "orphan younger than minDiskAge must not be reported")
}

func TestDiskDetector_Detect_FlagsWhenCreationTimestampMissingOrUnparseable(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	d := newDiskDetectorForTest(func(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error) {
		return &diskscan.ScanOutput{
			Items: []diskscan.GCEDiskItem{
				{Project: "rtp-1", Name: "no-ts", SelfLink: "self-1", Labels: map[string]string{"pool_uuid": "gone"}, CreationTimestamp: ""},
				{Project: "rtp-1", Name: "bad-ts", SelfLink: "self-2", Labels: map[string]string{"pool_uuid": "gone"}, CreationTimestamp: "not-a-date"},
			},
		}, nil
	}, "pool_uuid", time.Hour)

	records, err := d.Detect(ctx, storage)
	require.NoError(t, err)
	assert.Len(t, records, 2)
}

func TestDiskDetector_Detect_WorkflowError(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	d := newDiskDetectorForTest(func(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error) {
		return nil, errors.New("temporal unavailable")
	}, "pool_uuid", time.Hour)
	records, err := d.Detect(ctx, storage)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scan workflow")
	assert.Nil(t, records)
}

func TestDiskDetector_Detect_NilOutput(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	d := newDiskDetectorForTest(func(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error) {
		return nil, nil
	}, "pool_uuid", time.Hour)
	records, err := d.Detect(ctx, storage)
	require.NoError(t, err)
	assert.Nil(t, records)
}

func TestDiskDetector_Detect_PartialFailures_StillProcessesRest(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1", "rtp-2"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	d := newDiskDetectorForTest(func(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error) {
		return &diskscan.ScanOutput{
			Items: []diskscan.GCEDiskItem{
				{Project: "rtp-1", Name: "leaked-disk", Zone: "z1", SizeGB: 64,
					Labels: map[string]string{"pool_uuid": "dead-pool"}, CreationTimestamp: oldTS()},
			},
			PartialFailures: []diskscan.ProjectFailure{{Project: "rtp-2", Error: "permission denied"}},
		}, nil
	}, "pool_uuid", time.Hour)
	records, err := d.Detect(ctx, storage)
	require.NoError(t, err, "partial failures must not abort detection")
	require.Len(t, records, 1)
	assert.Equal(t, "leaked-disk", records[0].ResourceName)
}

func TestDiskDetector_Detect_SelfLinkFallback(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	d := newDiskDetectorForTest(func(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error) {
		return &diskscan.ScanOutput{
			Items: []diskscan.GCEDiskItem{
				{Project: "rtp-1", Name: "orphan-disk", Zone: "us-east1-b", SelfLink: "",
					Labels: map[string]string{"pool_uuid": "gone"}, CreationTimestamp: oldTS()},
			},
		}, nil
	}, "pool_uuid", time.Hour)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "projects/rtp-1/zones/us-east1-b/disks/orphan-disk", records[0].ResourceID)
}

func TestDiskDetector_Detect_NilLabelsMap(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	d := newDiskDetectorForTest(func(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error) {
		return &diskscan.ScanOutput{
			Items: []diskscan.GCEDiskItem{
				{Project: "rtp-1", Name: "no-labels-disk", Zone: "z1", Labels: nil, CreationTimestamp: oldTS()},
			},
		}, nil
	}, "pool_uuid", time.Hour)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, ReasonDiskOrphanPoolMissing, records[0].Reason)
}

func TestDiskDetector_Detect_DedupesRTPsAndForwardsToWorkflow(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListAllTpProjects(ctx).Return([]string{"rtp-2", "rtp-1"}, nil)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	var captured diskscan.ScanInput
	d := newDiskDetectorForTest(func(ctx context.Context, in diskscan.ScanInput) (*diskscan.ScanOutput, error) {
		captured = in
		return &diskscan.ScanOutput{}, nil
	}, "pool_uuid", time.Hour)

	_, err := d.Detect(ctx, storage)
	require.NoError(t, err)
	assert.Equal(t, []string{"rtp-1", "rtp-2"}, captured.ProjectIDs, "projects must be sorted for determinism")
}

func TestDiskIsOlderThanHelper(t *testing.T) {
	now := time.Now().UTC()
	assert.True(t, diskIsOlderThan("", now, time.Hour))
	assert.True(t, diskIsOlderThan("not-a-date", now, time.Hour))
	assert.True(t, diskIsOlderThan(now.Add(-2*time.Hour).Format(time.RFC3339), now, time.Hour))
	assert.False(t, diskIsOlderThan(now.Add(-30*time.Minute).Format(time.RFC3339), now, time.Hour))
}

func TestDefaultDiskMinAge(t *testing.T) {
	orig := envGetInt
	t.Cleanup(func() { envGetInt = orig })

	envGetInt = func(key string, defaultVal int) int { return 0 }
	assert.Equal(t, 6*time.Hour, defaultDiskMinAge())

	envGetInt = func(key string, defaultVal int) int { return 12 }
	assert.Equal(t, 12*time.Hour, defaultDiskMinAge())
}
