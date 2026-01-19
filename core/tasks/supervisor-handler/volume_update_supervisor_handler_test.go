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

func TestVolumeUpdateHandler_JobTypes(t *testing.T) {
	handler := NewVolumeUpdateHandler()
	jobTypes := handler.JobTypes()

	require.Len(t, jobTypes, 2)
	require.Contains(t, jobTypes, models.JobTypeUpdateVolume)
	require.Contains(t, jobTypes, models.JobTypeUpdateVolumeInReplication)
}

func TestNewVolumeUpdateHandler(t *testing.T) {
	handler := NewVolumeUpdateHandler()
	require.NotNil(t, handler)
	require.IsType(t, &VolumeUpdateHandler{}, handler)
}

func TestVolumeUpdateHandler_Handle_SkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "volume-uuid"},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetVolume", mock.Anything, mock.Anything)
}

func TestVolumeUpdateHandler_Handle_SkipsMissingResourceUUID(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeUpdateHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetVolume", mock.Anything, mock.Anything)
}

func TestVolumeUpdateHandler_Handle_VolumeNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "volume-uuid"},
	}

	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return((*datamodel.Volume)(nil), vsaerrors.NewNotFoundErr("volume", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything)
}

func TestVolumeUpdateHandler_Handle_GetVolumeError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "volume-uuid"},
	}

	expectedErr := errors.New("database error")
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return((*datamodel.Volume)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "load volume")
}

func TestVolumeUpdateHandler_Handle_SkipsNonUpdatingState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "volume-uuid"},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		State:     models.LifeCycleStateREADY,
	}
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return(volume, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything)
}

func TestVolumeUpdateHandler_Handle_SuccessWithPreviousState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeUpdateHandler()

	previousState := models.LifeCycleStateREADY
	previousStateDetails := models.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "volume-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		State:     models.LifeCycleStateUpdating,
	}
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return(volume, nil).Once()
	storage.EXPECT().UpdateVolumeFields(mock.Anything, "volume-uuid", mock.MatchedBy(func(m map[string]interface{}) bool {
		return m["state"] == previousState && m["state_details"] == previousStateDetails
	})).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestVolumeUpdateHandler_Handle_SuccessWithFallbackToReady(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "volume-uuid",
			// PreviousState is empty, should fallback to READY
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		State:     models.LifeCycleStateUpdating,
	}
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return(volume, nil).Once()
	storage.EXPECT().UpdateVolumeFields(mock.Anything, "volume-uuid", mock.MatchedBy(func(m map[string]interface{}) bool {
		return m["state"] == models.LifeCycleStateREADY && m["state_details"] == models.LifeCycleStateAvailableDetails
	})).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestVolumeUpdateHandler_Handle_UpdateVolumeFieldsError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeUpdateHandler()

	previousState := models.LifeCycleStateREADY
	previousStateDetails := models.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "volume-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		State:     models.LifeCycleStateUpdating,
	}
	expectedErr := errors.New("update failed")
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return(volume, nil).Once()
	storage.EXPECT().UpdateVolumeFields(mock.Anything, "volume-uuid", mock.Anything).Return(expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "revert volume state")
}

func TestVolumeUpdateHandler_Handle_WithPreviousStateDetails(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeUpdateHandler()

	previousState := models.LifeCycleStateREADY
	previousStateDetails := "Custom volume state details"

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "volume-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		State:     models.LifeCycleStateUpdating,
	}
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return(volume, nil).Once()
	storage.EXPECT().UpdateVolumeFields(mock.Anything, "volume-uuid", mock.MatchedBy(func(m map[string]interface{}) bool {
		return m["state"] == previousState && m["state_details"] == previousStateDetails
	})).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestVolumeUpdateHandler_Handle_UpdateVolumeInReplicationJobType(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeUpdateHandler()

	job := &datamodel.Job{
		Type: string(models.JobTypeUpdateVolumeInReplication),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "volume-uuid",
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		State:     models.LifeCycleStateUpdating,
	}
	storage.EXPECT().GetVolume(mock.Anything, "volume-uuid").Return(volume, nil).Once()
	storage.EXPECT().UpdateVolumeFields(mock.Anything, "volume-uuid", mock.Anything).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

