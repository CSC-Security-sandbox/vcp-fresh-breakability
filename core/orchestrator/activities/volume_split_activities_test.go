package activities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/hydrationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	utilserrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
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

// TestCleanupSplitSnapshot_NilVolumeAttributes covers the early-return when volume has no
// VolumeAttributes (lines 49-54).
func TestCleanupSplitSnapshot_NilVolumeAttributes(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	act := VolumeSplitActivity{SE: mockStorage}
	env.RegisterActivity(act.CleanupSplitSnapshot)

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		Name:      "test-volume",
	}

	_, err := env.ExecuteActivity(act.CleanupSplitSnapshot, volume)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// TestCleanupSplitSnapshot_NilCloneParentInfo covers the early-return when CloneParentInfo is nil.
func TestCleanupSplitSnapshot_NilCloneParentInfo(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	act := VolumeSplitActivity{SE: mockStorage}
	env.RegisterActivity(act.CleanupSplitSnapshot)

	volume := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{UUID: "vol-uuid"},
		Name:             "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{},
	}

	_, err := env.ExecuteActivity(act.CleanupSplitSnapshot, volume)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// TestCleanupSplitSnapshot_EmptyParentSnapshotUUID covers the early-return when
// ParentSnapshotUUID is empty (line 51).
func TestCleanupSplitSnapshot_EmptyParentSnapshotUUID(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	act := VolumeSplitActivity{SE: mockStorage}
	env.RegisterActivity(act.CleanupSplitSnapshot)

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		Name:      "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			CloneParentInfo: &datamodel.CloneParentInfo{
				ParentVolumeUUID:   "parent-vol-uuid",
				ParentSnapshotUUID: "", // empty — should skip
			},
		},
	}

	_, err := env.ExecuteActivity(act.CleanupSplitSnapshot, volume)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// TestCleanupSplitSnapshot_EmptyParentVolumeUUID covers the early-return when
// ParentVolumeUUID is empty (line 52).
func TestCleanupSplitSnapshot_EmptyParentVolumeUUID(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	act := VolumeSplitActivity{SE: mockStorage}
	env.RegisterActivity(act.CleanupSplitSnapshot)

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		Name:      "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			CloneParentInfo: &datamodel.CloneParentInfo{
				ParentVolumeUUID:   "", // empty — should skip
				ParentSnapshotUUID: "parent-snap-uuid",
			},
		},
	}

	_, err := env.ExecuteActivity(act.CleanupSplitSnapshot, volume)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// TestCleanupSplitSnapshot_GetParentVolumeError covers the warn-and-return path when
// GetVolume fails (lines 58-62).
func TestCleanupSplitSnapshot_GetParentVolumeError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	act := VolumeSplitActivity{SE: mockStorage}
	env.RegisterActivity(act.CleanupSplitSnapshot)

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		Name:      "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			CloneParentInfo: &datamodel.CloneParentInfo{
				ParentVolumeUUID:   "parent-vol-uuid",
				ParentSnapshotUUID: "parent-snap-uuid",
			},
		},
	}

	mockStorage.On("GetVolume", mock.Anything, "parent-vol-uuid").
		Return(nil, errors.New("db error"))

	_, err := env.ExecuteActivity(act.CleanupSplitSnapshot, volume)
	// Failure is treated as a warning — activity still returns nil.
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// TestCleanupSplitSnapshot_GetParentSnapshotError covers the warn-and-return path when
// GetSnapshotByUUID fails (lines 64-69).
func TestCleanupSplitSnapshot_GetParentSnapshotError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	act := VolumeSplitActivity{SE: mockStorage}
	env.RegisterActivity(act.CleanupSplitSnapshot)

	parentVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 10, UUID: "parent-vol-uuid"},
		AccountID: 1,
	}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		Name:      "test-volume",
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			CloneParentInfo: &datamodel.CloneParentInfo{
				ParentVolumeUUID:   "parent-vol-uuid",
				ParentSnapshotUUID: "parent-snap-uuid",
			},
		},
	}

	mockStorage.On("GetVolume", mock.Anything, "parent-vol-uuid").Return(parentVolume, nil)
	mockStorage.On("GetSnapshotByUUID", mock.Anything, "parent-snap-uuid", parentVolume.AccountID, parentVolume.ID).
		Return(nil, errors.New("snapshot db error"))

	_, err := env.ExecuteActivity(act.CleanupSplitSnapshot, volume)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// TestCleanupSplitSnapshot_GetCloneSnapshotError covers the warn-and-return path when
// GetSnapshotByNameAndVolumeId fails (lines 71-76).
func TestCleanupSplitSnapshot_GetCloneSnapshotError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	act := VolumeSplitActivity{SE: mockStorage}
	env.RegisterActivity(act.CleanupSplitSnapshot)

	parentVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 10, UUID: "parent-vol-uuid"},
		AccountID: 1,
	}
	parentSnapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: 20, UUID: "parent-snap-uuid"},
		Name:      "snap-name",
	}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 5, UUID: "vol-uuid"},
		Name:      "test-volume",
		AccountID: 2,
		VolumeAttributes: &datamodel.VolumeAttributes{
			CloneParentInfo: &datamodel.CloneParentInfo{
				ParentVolumeUUID:   "parent-vol-uuid",
				ParentSnapshotUUID: "parent-snap-uuid",
			},
		},
	}

	mockStorage.On("GetVolume", mock.Anything, "parent-vol-uuid").Return(parentVolume, nil)
	mockStorage.On("GetSnapshotByUUID", mock.Anything, "parent-snap-uuid", parentVolume.AccountID, parentVolume.ID).
		Return(parentSnapshot, nil)
	mockStorage.On("GetSnapshotByNameAndVolumeId", mock.Anything, "snap-name", volume.AccountID, volume.ID).
		Return(nil, errors.New("lookup error"))

	_, err := env.ExecuteActivity(act.CleanupSplitSnapshot, volume)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// TestCleanupSplitSnapshot_NilCloneSnapshot covers the debug-and-return path when
// GetSnapshotByNameAndVolumeId returns nil (lines 77-80).
func TestCleanupSplitSnapshot_NilCloneSnapshot(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	act := VolumeSplitActivity{SE: mockStorage}
	env.RegisterActivity(act.CleanupSplitSnapshot)

	parentVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 10, UUID: "parent-vol-uuid"},
		AccountID: 1,
	}
	parentSnapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: 20, UUID: "parent-snap-uuid"},
		Name:      "snap-name",
	}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 5, UUID: "vol-uuid"},
		Name:      "test-volume",
		AccountID: 2,
		VolumeAttributes: &datamodel.VolumeAttributes{
			CloneParentInfo: &datamodel.CloneParentInfo{
				ParentVolumeUUID:   "parent-vol-uuid",
				ParentSnapshotUUID: "parent-snap-uuid",
			},
		},
	}

	mockStorage.On("GetVolume", mock.Anything, "parent-vol-uuid").Return(parentVolume, nil)
	mockStorage.On("GetSnapshotByUUID", mock.Anything, "parent-snap-uuid", parentVolume.AccountID, parentVolume.ID).
		Return(parentSnapshot, nil)
	mockStorage.On("GetSnapshotByNameAndVolumeId", mock.Anything, "snap-name", volume.AccountID, volume.ID).
		Return((*datamodel.Snapshot)(nil), nil)

	_, err := env.ExecuteActivity(act.CleanupSplitSnapshot, volume)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// TestCleanupSplitSnapshot_DeleteSnapshotNotFound covers the warn-and-return path when
// DeleteSnapshot returns a not-found error (lines 86-89).
func TestCleanupSplitSnapshot_DeleteSnapshotNotFound(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	act := VolumeSplitActivity{SE: mockStorage}
	env.RegisterActivity(act.CleanupSplitSnapshot)

	parentVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 10, UUID: "parent-vol-uuid"},
		AccountID: 1,
	}
	parentSnapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: 20, UUID: "parent-snap-uuid"},
		Name:      "snap-name",
	}
	cloneSnapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: 30, UUID: "clone-snap-uuid"},
		Name:      "snap-name",
	}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 5, UUID: "vol-uuid"},
		Name:      "test-volume",
		AccountID: 2,
		VolumeAttributes: &datamodel.VolumeAttributes{
			CloneParentInfo: &datamodel.CloneParentInfo{
				ParentVolumeUUID:   "parent-vol-uuid",
				ParentSnapshotUUID: "parent-snap-uuid",
			},
		},
	}

	mockStorage.On("GetVolume", mock.Anything, "parent-vol-uuid").Return(parentVolume, nil)
	mockStorage.On("GetSnapshotByUUID", mock.Anything, "parent-snap-uuid", parentVolume.AccountID, parentVolume.ID).
		Return(parentSnapshot, nil)
	mockStorage.On("GetSnapshotByNameAndVolumeId", mock.Anything, "snap-name", volume.AccountID, volume.ID).
		Return(cloneSnapshot, nil)
	mockStorage.On("DeleteSnapshot", mock.Anything, "clone-snap-uuid").
		Return((*datamodel.Snapshot)(nil), utilserrors.NewNotFoundErr("Snapshot", nil))

	_, err := env.ExecuteActivity(act.CleanupSplitSnapshot, volume)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// TestCleanupSplitSnapshot_DeleteSnapshotError covers the error-return path when
// DeleteSnapshot returns a non-not-found error (lines 90-91).
func TestCleanupSplitSnapshot_DeleteSnapshotError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	act := VolumeSplitActivity{SE: mockStorage}
	env.RegisterActivity(act.CleanupSplitSnapshot)

	parentVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 10, UUID: "parent-vol-uuid"},
		AccountID: 1,
	}
	parentSnapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: 20, UUID: "parent-snap-uuid"},
		Name:      "snap-name",
	}
	cloneSnapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: 30, UUID: "clone-snap-uuid"},
		Name:      "snap-name",
	}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 5, UUID: "vol-uuid"},
		Name:      "test-volume",
		AccountID: 2,
		VolumeAttributes: &datamodel.VolumeAttributes{
			CloneParentInfo: &datamodel.CloneParentInfo{
				ParentVolumeUUID:   "parent-vol-uuid",
				ParentSnapshotUUID: "parent-snap-uuid",
			},
		},
	}

	mockStorage.On("GetVolume", mock.Anything, "parent-vol-uuid").Return(parentVolume, nil)
	mockStorage.On("GetSnapshotByUUID", mock.Anything, "parent-snap-uuid", parentVolume.AccountID, parentVolume.ID).
		Return(parentSnapshot, nil)
	mockStorage.On("GetSnapshotByNameAndVolumeId", mock.Anything, "snap-name", volume.AccountID, volume.ID).
		Return(cloneSnapshot, nil)
	mockStorage.On("DeleteSnapshot", mock.Anything, "clone-snap-uuid").
		Return((*datamodel.Snapshot)(nil), errors.New("delete failed"))

	_, err := env.ExecuteActivity(act.CleanupSplitSnapshot, volume)
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

// TestCleanupSplitSnapshot_HydrationError covers the warn-and-continue path when
// HydrateBatchSnapshotstoCCFE fails (lines 95-99) — the activity still returns nil.
func TestCleanupSplitSnapshot_HydrationError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	act := VolumeSplitActivity{SE: mockStorage}
	env.RegisterActivity(act.CleanupSplitSnapshot)

	orig := hydrationActivities.HydrateBatchSnapshotstoCCFE
	defer func() { hydrationActivities.HydrateBatchSnapshotstoCCFE = orig }()
	hydrationActivities.HydrateBatchSnapshotstoCCFE = func(_ context.Context, _ []*datamodel.Snapshot, _ []*datamodel.Snapshot) error {
		return errors.New("hydration failed")
	}

	parentVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 10, UUID: "parent-vol-uuid"},
		AccountID: 1,
	}
	parentSnapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: 20, UUID: "parent-snap-uuid"},
		Name:      "snap-name",
	}
	cloneSnapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: 30, UUID: "clone-snap-uuid"},
		Name:      "snap-name",
	}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 5, UUID: "vol-uuid"},
		Name:      "test-volume",
		AccountID: 2,
		VolumeAttributes: &datamodel.VolumeAttributes{
			CloneParentInfo: &datamodel.CloneParentInfo{
				ParentVolumeUUID:   "parent-vol-uuid",
				ParentSnapshotUUID: "parent-snap-uuid",
			},
		},
	}

	mockStorage.On("GetVolume", mock.Anything, "parent-vol-uuid").Return(parentVolume, nil)
	mockStorage.On("GetSnapshotByUUID", mock.Anything, "parent-snap-uuid", parentVolume.AccountID, parentVolume.ID).
		Return(parentSnapshot, nil)
	mockStorage.On("GetSnapshotByNameAndVolumeId", mock.Anything, "snap-name", volume.AccountID, volume.ID).
		Return(cloneSnapshot, nil)
	mockStorage.On("DeleteSnapshot", mock.Anything, "clone-snap-uuid").
		Return(cloneSnapshot, nil)

	_, err := env.ExecuteActivity(act.CleanupSplitSnapshot, volume)
	// Hydration failure is a warning — activity still returns nil.
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// TestCleanupSplitSnapshot_Success covers the full happy path (lines 46-102).
func TestCleanupSplitSnapshot_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	act := VolumeSplitActivity{SE: mockStorage}
	env.RegisterActivity(act.CleanupSplitSnapshot)

	orig := hydrationActivities.HydrateBatchSnapshotstoCCFE
	defer func() { hydrationActivities.HydrateBatchSnapshotstoCCFE = orig }()
	hydrationActivities.HydrateBatchSnapshotstoCCFE = func(_ context.Context, _ []*datamodel.Snapshot, _ []*datamodel.Snapshot) error {
		return nil
	}

	parentVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 10, UUID: "parent-vol-uuid"},
		AccountID: 1,
	}
	parentSnapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: 20, UUID: "parent-snap-uuid"},
		Name:      "snap-name",
	}
	cloneSnapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{ID: 30, UUID: "clone-snap-uuid"},
		Name:      "snap-name",
	}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 5, UUID: "vol-uuid"},
		Name:      "test-volume",
		AccountID: 2,
		VolumeAttributes: &datamodel.VolumeAttributes{
			CloneParentInfo: &datamodel.CloneParentInfo{
				ParentVolumeUUID:   "parent-vol-uuid",
				ParentSnapshotUUID: "parent-snap-uuid",
			},
		},
	}

	mockStorage.On("GetVolume", mock.Anything, "parent-vol-uuid").Return(parentVolume, nil)
	mockStorage.On("GetSnapshotByUUID", mock.Anything, "parent-snap-uuid", parentVolume.AccountID, parentVolume.ID).
		Return(parentSnapshot, nil)
	mockStorage.On("GetSnapshotByNameAndVolumeId", mock.Anything, "snap-name", volume.AccountID, volume.ID).
		Return(cloneSnapshot, nil)
	mockStorage.On("DeleteSnapshot", mock.Anything, "clone-snap-uuid").
		Return(cloneSnapshot, nil)

	_, err := env.ExecuteActivity(act.CleanupSplitSnapshot, volume)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}
