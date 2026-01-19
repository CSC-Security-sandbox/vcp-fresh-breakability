package supervisorhandler

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestSnapshotDeleteHandler_JobTypes(t *testing.T) {
	handler := NewSnapshotDeleteHandler()
	jobTypes := handler.JobTypes()

	require.Len(t, jobTypes, 1)
	require.Contains(t, jobTypes, models.JobTypeDeleteSnapshot)
}

func TestNewSnapshotDeleteHandler(t *testing.T) {
	handler := NewSnapshotDeleteHandler()
	require.NotNil(t, handler)
	require.IsType(t, &SnapshotDeleteHandler{}, handler)
}

func TestSnapshotDeleteHandler_Handle_SkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewSnapshotDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "snapshot-uuid"},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetSnapshotByUUID", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestSnapshotDeleteHandler_Handle_SkipsMissingResourceUUID(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewSnapshotDeleteHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetSnapshotByUUID", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestSnapshotDeleteHandler_Handle_SnapshotNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewSnapshotDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "snapshot-uuid",
			PayloadAttributes: map[string]interface{}{
				"account_id": float64(123),
				"volume_id":  float64(456),
			},
		},
	}

	storage.EXPECT().GetSnapshotByUUID(mock.Anything, "snapshot-uuid", int64(123), int64(456)).Return((*datamodel.Snapshot)(nil), vsaerrors.NewNotFoundErr("snapshot", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateSnapshot", mock.Anything, mock.Anything)
}

func TestSnapshotDeleteHandler_Handle_GetSnapshotError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewSnapshotDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "snapshot-uuid",
			PayloadAttributes: map[string]interface{}{
				"account_id": float64(123),
			},
		},
	}

	expectedErr := errors.New("database error")
	storage.EXPECT().GetSnapshotByUUID(mock.Anything, "snapshot-uuid", int64(123), int64(0)).Return((*datamodel.Snapshot)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "load snapshot for delete cleanup")
}

func TestSnapshotDeleteHandler_Handle_SkipsNonDeletingState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewSnapshotDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "snapshot-uuid",
			PayloadAttributes: map[string]interface{}{
				"account_id": float64(123),
				"volume_id":  float64(456),
			},
		},
	}

	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{UUID: "snapshot-uuid"},
		State:     models.LifeCycleStateREADY,
	}
	storage.EXPECT().GetSnapshotByUUID(mock.Anything, "snapshot-uuid", int64(123), int64(456)).Return(snapshot, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateSnapshot", mock.Anything, mock.Anything)
}

func TestSnapshotDeleteHandler_Handle_SuccessWithPreviousState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewSnapshotDeleteHandler()

	previousState := models.LifeCycleStateREADY
	previousStateDetails := models.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "snapshot-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
			PayloadAttributes: map[string]interface{}{
				"account_id": float64(123),
				"volume_id":  float64(456),
			},
		},
	}

	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{UUID: "snapshot-uuid"},
		State:     models.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetSnapshotByUUID(mock.Anything, "snapshot-uuid", int64(123), int64(456)).Return(snapshot, nil).Once()
	storage.EXPECT().UpdateSnapshot(mock.Anything, mock.MatchedBy(func(s *datamodel.Snapshot) bool {
		return s.State == previousState && s.StateDetails == previousStateDetails
	})).Return(snapshot, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestSnapshotDeleteHandler_Handle_SuccessWithFallbackToReady(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewSnapshotDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "snapshot-uuid",
			PayloadAttributes: map[string]interface{}{
				"account_id": float64(123),
			},
		},
	}

	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{UUID: "snapshot-uuid"},
		State:     models.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetSnapshotByUUID(mock.Anything, "snapshot-uuid", int64(123), int64(0)).Return(snapshot, nil).Once()
	storage.EXPECT().UpdateSnapshot(mock.Anything, mock.MatchedBy(func(s *datamodel.Snapshot) bool {
		return s.State == models.LifeCycleStateREADY && s.StateDetails == models.LifeCycleStateReadyDetails
	})).Return(snapshot, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestSnapshotDeleteHandler_Handle_UpdateSnapshotError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewSnapshotDeleteHandler()

	previousState := models.LifeCycleStateREADY
	previousStateDetails := models.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "snapshot-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
			PayloadAttributes: map[string]interface{}{
				"account_id": float64(123),
			},
		},
	}

	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{UUID: "snapshot-uuid"},
		State:     models.LifeCycleStateDeleting,
	}
	expectedErr := errors.New("update failed")
	storage.EXPECT().GetSnapshotByUUID(mock.Anything, "snapshot-uuid", int64(123), int64(0)).Return(snapshot, nil).Once()
	storage.EXPECT().UpdateSnapshot(mock.Anything, mock.Anything).Return((*datamodel.Snapshot)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "revert snapshot")
}

func TestSnapshotDeleteHandler_Handle_EmptyPayloadAttributes(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewSnapshotDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:      "snapshot-uuid",
			PayloadAttributes: map[string]interface{}{},
		},
	}

	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{UUID: "snapshot-uuid"},
		State:     models.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetSnapshotByUUID(mock.Anything, "snapshot-uuid", int64(0), int64(0)).Return(snapshot, nil).Once()
	storage.EXPECT().UpdateSnapshot(mock.Anything, mock.Anything).Return(snapshot, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestSnapshotDeleteHandler_Handle_InvalidPayloadAttributeTypes(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewSnapshotDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "snapshot-uuid",
			PayloadAttributes: map[string]interface{}{
				"account_id": "not-a-number", // Wrong type
				"volume_id":  "not-a-number", // Wrong type
			},
		},
	}

	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{UUID: "snapshot-uuid"},
		State:     models.LifeCycleStateDeleting,
	}
	// Should use 0 when type assertion fails
	storage.EXPECT().GetSnapshotByUUID(mock.Anything, "snapshot-uuid", int64(0), int64(0)).Return(snapshot, nil).Once()
	storage.EXPECT().UpdateSnapshot(mock.Anything, mock.Anything).Return(snapshot, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestSnapshotDeleteHandler_Handle_PartialPayloadAttributes(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewSnapshotDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "snapshot-uuid",
			PayloadAttributes: map[string]interface{}{
				"account_id": float64(123),
				// volume_id missing
			},
		},
	}

	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{UUID: "snapshot-uuid"},
		State:     models.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetSnapshotByUUID(mock.Anything, "snapshot-uuid", int64(123), int64(0)).Return(snapshot, nil).Once()
	storage.EXPECT().UpdateSnapshot(mock.Anything, mock.Anything).Return(snapshot, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

