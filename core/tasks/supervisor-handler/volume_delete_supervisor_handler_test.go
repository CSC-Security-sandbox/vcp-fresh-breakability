package supervisorhandler

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestVolumeDeleteHandler_JobTypes(t *testing.T) {
	handler := NewVolumeDeleteHandler()
	jobTypes := handler.JobTypes()

	require.Len(t, jobTypes, 3)
	require.Contains(t, jobTypes, datamodel.JobTypeDeleteVolume)
	require.Contains(t, jobTypes, datamodel.JobTypeDeleteLargeVolume)
	require.Contains(t, jobTypes, datamodel.JobTypeFlexCacheDeleteVolume)
}

func TestNewVolumeDeleteHandler(t *testing.T) {
	handler := NewVolumeDeleteHandler()
	require.NotNil(t, handler)
	require.IsType(t, &VolumeDeleteHandler{}, handler)
}

func TestVolumeDeleteHandler_Handle_SkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "volume-uuid"},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetVolume", mock.Anything, mock.Anything)
}

func TestVolumeDeleteHandler_Handle_SkipsMissingResourceUUID(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeDeleteHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetVolume", mock.Anything, mock.Anything)
}

func TestVolumeDeleteHandler_Handle_VolumeNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "volume-uuid"},
	}

	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return((*datamodel.Volume)(nil), vsaerrors.NewNotFoundErr("volume", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything)
}

func TestVolumeDeleteHandler_Handle_GetVolumeError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "volume-uuid"},
	}

	expectedErr := errors.New("database error")
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return((*datamodel.Volume)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "load volume")
}

func TestVolumeDeleteHandler_Handle_SkipsNonDeletingState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "volume-uuid"},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		State:     datamodel.LifeCycleStateREADY,
	}
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return(volume, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything)
}

func TestVolumeDeleteHandler_Handle_SuccessWithPreviousState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeDeleteHandler()

	previousState := datamodel.LifeCycleStateREADY
	previousStateDetails := datamodel.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "volume-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return(volume, nil).Once()
	storage.EXPECT().UpdateVolumeFields(mock.Anything, "volume-uuid", mock.MatchedBy(func(m map[string]interface{}) bool {
		return m["state"] == previousState && m["state_details"] == previousStateDetails
	})).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestVolumeDeleteHandler_Handle_SuccessWithFallbackToReady(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "volume-uuid",
			// PreviousState is empty, should fallback to READY
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return(volume, nil).Once()
	storage.EXPECT().UpdateVolumeFields(mock.Anything, "volume-uuid", mock.MatchedBy(func(m map[string]interface{}) bool {
		return m["state"] == datamodel.LifeCycleStateREADY && m["state_details"] == datamodel.LifeCycleStateAvailableDetails
	})).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestVolumeDeleteHandler_Handle_UpdateVolumeFieldsError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeDeleteHandler()

	previousState := datamodel.LifeCycleStateREADY
	previousStateDetails := datamodel.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "volume-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	expectedErr := errors.New("update failed")
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return(volume, nil).Once()
	storage.EXPECT().UpdateVolumeFields(mock.Anything, "volume-uuid", mock.Anything).Return(expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "revert volume state")
}

func TestVolumeDeleteHandler_Handle_DeleteLargeVolumeJobType(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeDeleteHandler()

	job := &datamodel.Job{
		Type: string(datamodel.JobTypeDeleteLargeVolume),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "volume-uuid",
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return(volume, nil).Once()
	storage.EXPECT().UpdateVolumeFields(mock.Anything, "volume-uuid", mock.Anything).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestVolumeDeleteHandler_Handle_FlexCacheDeleteVolumeJobType(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeDeleteHandler()

	job := &datamodel.Job{
		Type: string(datamodel.JobTypeFlexCacheDeleteVolume),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "volume-uuid",
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return(volume, nil).Once()
	storage.EXPECT().UpdateVolumeFields(mock.Anything, "volume-uuid", mock.Anything).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

// Tests for PROCESSING state timeout handling

func TestVolumeDeleteHandler_Handle_ProcessingTimeout_TransitionsDeletingToError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeDeleteHandler()

	job := &datamodel.Job{
		State:         string(datamodel.JobsStatePROCESSING),
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "volume-uuid"},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return(volume, nil).Once()
	storage.EXPECT().UpdateVolumeFields(mock.Anything, "volume-uuid", mock.MatchedBy(func(m map[string]interface{}) bool {
		return m["state"] == datamodel.LifeCycleStateError && m["state_details"] == datamodel.LifeCycleStateDeletionErrorDetails
	})).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestVolumeDeleteHandler_Handle_ProcessingTimeout_SkipsNonDeletingState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeDeleteHandler()

	job := &datamodel.Job{
		State:         string(datamodel.JobsStatePROCESSING),
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "volume-uuid"},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		State:     datamodel.LifeCycleStateREADY,
	}
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return(volume, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything)
}

func TestVolumeDeleteHandler_Handle_ProcessingTimeout_VolumeNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeDeleteHandler()

	job := &datamodel.Job{
		State:         string(datamodel.JobsStatePROCESSING),
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "volume-uuid"},
	}

	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return((*datamodel.Volume)(nil), vsaerrors.NewNotFoundErr("volume", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything)
}

func TestVolumeDeleteHandler_Handle_ProcessingTimeout_GetVolumeError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeDeleteHandler()

	job := &datamodel.Job{
		State:         string(datamodel.JobsStatePROCESSING),
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "volume-uuid"},
	}

	expectedErr := errors.New("database error")
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return((*datamodel.Volume)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "load volume for PROCESSING timeout")
}

func TestVolumeDeleteHandler_Handle_ProcessingTimeout_UpdateVolumeFieldsError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeDeleteHandler()

	job := &datamodel.Job{
		State:         string(datamodel.JobsStatePROCESSING),
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "volume-uuid"},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	expectedErr := errors.New("update failed")
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return(volume, nil).Once()
	storage.EXPECT().UpdateVolumeFields(mock.Anything, "volume-uuid", mock.Anything).Return(expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "update volume state to ERROR")
}

func TestVolumeDeleteHandler_Handle_NewStateTimeout_RevertsVolumeState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeDeleteHandler()

	job := &datamodel.Job{
		State: string(datamodel.JobsStateNEW),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "volume-uuid",
			PreviousState:        datamodel.LifeCycleStateREADY,
			PreviousStateDetails: datamodel.LifeCycleStateReadyDetails,
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return(volume, nil).Once()
	storage.EXPECT().UpdateVolumeFields(mock.Anything, "volume-uuid", mock.MatchedBy(func(m map[string]interface{}) bool {
		return m["state"] == datamodel.LifeCycleStateREADY && m["state_details"] == datamodel.LifeCycleStateReadyDetails
	})).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}
