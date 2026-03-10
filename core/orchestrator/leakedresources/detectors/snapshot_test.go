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

func TestSnapshotOrphanDetector_Name(t *testing.T) {
	d := NewSnapshotOrphanDetector()
	assert.Equal(t, "snapshot_orphan", d.Name())
}

func TestSnapshotOrphanDetector_Detect_ListAccountsFails(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().GetAccounts(ctx, false, mock.Anything).Return(nil, errors.New("db error"))

	d := NewSnapshotOrphanDetector()
	records, err := d.Detect(ctx, storage)
	assert.Error(t, err)
	assert.Nil(t, records)
	assert.Contains(t, err.Error(), "list accounts")
}

func TestSnapshotOrphanDetector_Detect_ListVolumesFails(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().GetAccounts(ctx, false, mock.Anything).Return([]*datamodel.Account{}, nil)
	storage.EXPECT().ListVolumes(ctx, mock.Anything).Return(nil, errors.New("db error"))

	d := NewSnapshotOrphanDetector()
	records, err := d.Detect(ctx, storage)
	assert.Error(t, err)
	assert.Nil(t, records)
	assert.Contains(t, err.Error(), "list volumes")
}

func TestSnapshotOrphanDetector_Detect_GetSnapshotsFails(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().GetAccounts(ctx, false, mock.Anything).Return([]*datamodel.Account{}, nil)
	storage.EXPECT().ListVolumes(ctx, mock.Anything).Return([]*datamodel.Volume{}, nil)
	storage.EXPECT().GetSnapshotsWithCondition(ctx, mock.Anything).Return(nil, errors.New("db error"))

	d := NewSnapshotOrphanDetector()
	records, err := d.Detect(ctx, storage)
	assert.Error(t, err)
	assert.Nil(t, records)
	assert.Contains(t, err.Error(), "list snapshots")
}

func TestSnapshotOrphanDetector_Detect_NoLeaks(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	volumes := []*datamodel.Volume{
		{BaseModel: datamodel.BaseModel{ID: 1, UUID: "vol-1"}},
		{BaseModel: datamodel.BaseModel{ID: 2, UUID: "vol-2"}},
	}
	snapshots := []*datamodel.Snapshot{
		{BaseModel: datamodel.BaseModel{UUID: "snap-1"}, VolumeID: 1, Name: "s1"},
		{BaseModel: datamodel.BaseModel{UUID: "snap-2"}, VolumeID: 2, Name: "s2"},
	}
	storage.EXPECT().GetAccounts(ctx, false, mock.Anything).Return([]*datamodel.Account{}, nil)
	storage.EXPECT().ListVolumes(ctx, mock.Anything).Return(volumes, nil)
	storage.EXPECT().GetSnapshotsWithCondition(ctx, mock.Anything).Return(snapshots, nil)

	d := NewSnapshotOrphanDetector()
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
}

func TestSnapshotOrphanDetector_Detect_OrphanSnapshot(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	volumes := []*datamodel.Volume{
		{BaseModel: datamodel.BaseModel{ID: 1, UUID: "vol-1"}},
	}
	// GetSnapshotsWithCondition does not preload Account, so snap.Account is nil; ProjectID comes from accountIDToName map.
	snapshots := []*datamodel.Snapshot{
		{BaseModel: datamodel.BaseModel{UUID: "snap-ok"}, VolumeID: 1, Name: "s-ok"},
		{BaseModel: datamodel.BaseModel{UUID: "snap-orphan"}, VolumeID: 999, Name: "s-orphan", AccountID: 10},
	}
	accounts := []*datamodel.Account{
		{BaseModel: datamodel.BaseModel{ID: 10}, Name: "proj1"},
	}
	storage.EXPECT().GetAccounts(ctx, false, mock.Anything).Return(accounts, nil)
	storage.EXPECT().ListVolumes(ctx, mock.Anything).Return(volumes, nil)
	storage.EXPECT().GetSnapshotsWithCondition(ctx, mock.Anything).Return(snapshots, nil)

	d := NewSnapshotOrphanDetector()
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Len(t, records, 1)
	assert.Equal(t, model.ResourceTypeSnapshot, records[0].ResourceType)
	assert.Equal(t, "snap-orphan", records[0].ResourceID)
	assert.Equal(t, "s-orphan", records[0].ResourceName)
	assert.Equal(t, "proj1", records[0].ProjectID)
	assert.Equal(t, ReasonSnapshotOrphanVolumeMissing, records[0].Reason)
	assert.Equal(t, "999", records[0].Extra["volume_id"])
	assert.Equal(t, "10", records[0].Extra["account_id"])
}

func TestSnapshotOrphanDetector_Detect_EmptyVolumes_AllSnapshotsOrphan(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().GetAccounts(ctx, false, mock.Anything).Return([]*datamodel.Account{}, nil)
	storage.EXPECT().ListVolumes(ctx, mock.Anything).Return([]*datamodel.Volume{}, nil)
	snapshots := []*datamodel.Snapshot{
		{BaseModel: datamodel.BaseModel{UUID: "snap1"}, VolumeID: 1, Name: "s1"},
	}
	storage.EXPECT().GetSnapshotsWithCondition(ctx, mock.Anything).Return(snapshots, nil)

	d := NewSnapshotOrphanDetector()
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Len(t, records, 1)
	assert.Equal(t, "snap1", records[0].ResourceID)
	assert.Equal(t, ReasonSnapshotOrphanVolumeMissing, records[0].Reason)
}
