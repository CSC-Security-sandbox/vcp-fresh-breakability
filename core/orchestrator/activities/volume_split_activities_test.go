package activities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

func TestUpdateCloneSharedBytesInDB_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := VolumeSplitActivity{SE: mockStorage}
	ctx := context.Background()
	volumeUUID := "vol-uuid-1"
	clonesSharedBytes := uint64(0)

	mockStorage.On("UpdateVolumeFields", ctx, volumeUUID, map[string]interface{}{
		"clones_shared_bytes": clonesSharedBytes,
	}).Return(nil)

	err := activity.UpdateCloneSharedBytesInDB(ctx, volumeUUID, clonesSharedBytes)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateCloneSharedBytesInDB_WithNonZeroValue(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := VolumeSplitActivity{SE: mockStorage}
	ctx := context.Background()
	volumeUUID := "vol-uuid-2"
	clonesSharedBytes := uint64(1000)

	mockStorage.On("UpdateVolumeFields", ctx, volumeUUID, map[string]interface{}{
		"clones_shared_bytes": clonesSharedBytes,
	}).Return(nil)

	err := activity.UpdateCloneSharedBytesInDB(ctx, volumeUUID, clonesSharedBytes)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateCloneSharedBytesInDB_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := VolumeSplitActivity{SE: mockStorage}
	ctx := context.Background()
	volumeUUID := "vol-uuid-3"
	clonesSharedBytes := uint64(0)
	expectedErr := errors.New("database error")

	mockStorage.On("UpdateVolumeFields", ctx, volumeUUID, map[string]interface{}{
		"clones_shared_bytes": clonesSharedBytes,
	}).Return(expectedErr)

	err := activity.UpdateCloneSharedBytesInDB(ctx, volumeUUID, clonesSharedBytes)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database error")
	mockStorage.AssertExpectations(t)
}

func TestUpdateCloneSharedBytesInDB_EmptyVolumeUUID(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := VolumeSplitActivity{SE: mockStorage}
	ctx := context.Background()
	volumeUUID := ""
	clonesSharedBytes := uint64(0)

	mockStorage.On("UpdateVolumeFields", ctx, volumeUUID, map[string]interface{}{
		"clones_shared_bytes": clonesSharedBytes,
	}).Return(nil)

	err := activity.UpdateCloneSharedBytesInDB(ctx, volumeUUID, clonesSharedBytes)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateCloneSharedBytesInDB_LargeValue(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := VolumeSplitActivity{SE: mockStorage}
	ctx := context.Background()
	volumeUUID := "vol-uuid-4"
	clonesSharedBytes := uint64(1099511627776) // 1 TiB

	mockStorage.On("UpdateVolumeFields", ctx, volumeUUID, map[string]interface{}{
		"clones_shared_bytes": clonesSharedBytes,
	}).Return(nil)

	err := activity.UpdateCloneSharedBytesInDB(ctx, volumeUUID, clonesSharedBytes)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

