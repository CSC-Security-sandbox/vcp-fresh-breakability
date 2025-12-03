package activities

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"go.temporal.io/sdk/testsuite"
)

func TestUpdateCloneSharedBytesInDB_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumeSplitActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateCloneSharedBytesInDB)
	volumeUUID := "vol-uuid-1"
	clonesSharedBytes := uint64(0)

	mockStorage.On("UpdateVolumeFields", mock.Anything, volumeUUID, map[string]interface{}{
		"clones_shared_bytes": clonesSharedBytes,
	}).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateCloneSharedBytesInDB, volumeUUID, clonesSharedBytes)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateCloneSharedBytesInDB_WithNonZeroValue(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumeSplitActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateCloneSharedBytesInDB)
	volumeUUID := "vol-uuid-2"
	clonesSharedBytes := uint64(1000)

	mockStorage.On("UpdateVolumeFields", mock.Anything, volumeUUID, map[string]interface{}{
		"clones_shared_bytes": clonesSharedBytes,
	}).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateCloneSharedBytesInDB, volumeUUID, clonesSharedBytes)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateCloneSharedBytesInDB_Error(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumeSplitActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateCloneSharedBytesInDB)
	volumeUUID := "vol-uuid-3"
	clonesSharedBytes := uint64(0)
	expectedErr := errors.New("database error")

	mockStorage.On("UpdateVolumeFields", mock.Anything, volumeUUID, map[string]interface{}{
		"clones_shared_bytes": clonesSharedBytes,
	}).Return(expectedErr)

	_, err := env.ExecuteActivity(activity.UpdateCloneSharedBytesInDB, volumeUUID, clonesSharedBytes)
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateCloneSharedBytesInDB_EmptyVolumeUUID(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumeSplitActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateCloneSharedBytesInDB)
	volumeUUID := ""
	clonesSharedBytes := uint64(0)

	mockStorage.On("UpdateVolumeFields", mock.Anything, volumeUUID, map[string]interface{}{
		"clones_shared_bytes": clonesSharedBytes,
	}).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateCloneSharedBytesInDB, volumeUUID, clonesSharedBytes)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateCloneSharedBytesInDB_LargeValue(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := VolumeSplitActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdateCloneSharedBytesInDB)
	volumeUUID := "vol-uuid-4"
	clonesSharedBytes := uint64(1099511627776) // 1 TiB

	mockStorage.On("UpdateVolumeFields", mock.Anything, volumeUUID, map[string]interface{}{
		"clones_shared_bytes": clonesSharedBytes,
	}).Return(nil)

	_, err := env.ExecuteActivity(activity.UpdateCloneSharedBytesInDB, volumeUUID, clonesSharedBytes)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}
