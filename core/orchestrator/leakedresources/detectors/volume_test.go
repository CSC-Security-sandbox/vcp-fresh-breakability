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

func TestVolumeOrphanDetector_Name(t *testing.T) {
	d := NewVolumeOrphanDetector()
	assert.Equal(t, "volume_orphan", d.Name())
}

func TestVolumeOrphanDetector_Detect_ListPoolsFails(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(nil, errors.New("db error"))

	d := NewVolumeOrphanDetector()
	records, err := d.Detect(ctx, storage)
	assert.Error(t, err)
	assert.Nil(t, records)
	assert.Contains(t, err.Error(), "list pools")
}

func TestVolumeOrphanDetector_Detect_ListVolumesFails(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	storage.EXPECT().ListVolumes(ctx, mock.Anything).Return(nil, errors.New("db error"))

	d := NewVolumeOrphanDetector()
	records, err := d.Detect(ctx, storage)
	assert.Error(t, err)
	assert.Nil(t, records)
	assert.Contains(t, err.Error(), "list volumes")
}

func TestVolumeOrphanDetector_Detect_NoLeaks(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{
		{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}},
		{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 2}}},
	}
	volumes := []*datamodel.Volume{
		{BaseModel: datamodel.BaseModel{UUID: "v1"}, PoolID: 1, Name: "vol1"},
		{BaseModel: datamodel.BaseModel{UUID: "v2"}, PoolID: 2, Name: "vol2"},
	}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)
	storage.EXPECT().ListVolumes(ctx, mock.Anything).Return(volumes, nil)

	d := NewVolumeOrphanDetector()
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
}

func TestVolumeOrphanDetector_Detect_OrphanVolume(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pools := []*datamodel.PoolView{
		{Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}},
	}
	volumes := []*datamodel.Volume{
		{BaseModel: datamodel.BaseModel{UUID: "v-ok"}, PoolID: 1, Name: "vol-ok"},
		{BaseModel: datamodel.BaseModel{UUID: "v-orphan"}, PoolID: 999, Name: "vol-orphan", Account: &datamodel.Account{Name: "proj1"}},
	}
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(pools, nil)
	storage.EXPECT().ListVolumes(ctx, mock.Anything).Return(volumes, nil)

	d := NewVolumeOrphanDetector()
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Len(t, records, 1)
	assert.Equal(t, model.ResourceTypeVolume, records[0].ResourceType)
	assert.Equal(t, "v-orphan", records[0].ResourceID)
	assert.Equal(t, "vol-orphan", records[0].ResourceName)
	assert.Equal(t, "proj1", records[0].ProjectID)
	assert.Equal(t, ReasonVolumeOrphanPoolMissing, records[0].Reason)
	assert.Equal(t, "999", records[0].Extra["pool_id"])
}

func TestVolumeOrphanDetector_Detect_EmptyPools_AllVolumesOrphan(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().ListPools(ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)
	volumes := []*datamodel.Volume{
		{BaseModel: datamodel.BaseModel{UUID: "v1"}, PoolID: 1, Name: "vol1"},
	}
	storage.EXPECT().ListVolumes(ctx, mock.Anything).Return(volumes, nil)

	d := NewVolumeOrphanDetector()
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Len(t, records, 1)
	assert.Equal(t, "v1", records[0].ResourceID)
	assert.Equal(t, ReasonVolumeOrphanPoolMissing, records[0].Reason)
}
