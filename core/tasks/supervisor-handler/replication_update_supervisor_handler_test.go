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

func TestReplicationUpdateHandler_JobTypes(t *testing.T) {
	handler := NewReplicationUpdateHandler()
	jobTypes := handler.JobTypes()

	require.Len(t, jobTypes, 3)
	require.Contains(t, jobTypes, models.JobTypeUpdateVolumeReplicationInternal)
	require.Contains(t, jobTypes, models.JobTypeUpdateVolumeReplication)
	require.Contains(t, jobTypes, models.JobTypeUpdateVolumeReplicationAttributes)
}

func TestNewReplicationUpdateHandler(t *testing.T) {
	handler := NewReplicationUpdateHandler()
	require.NotNil(t, handler)
	require.IsType(t, &ReplicationUpdateHandler{}, handler)
}

func TestReplicationUpdateHandler_Handle_SkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "replication-uuid"},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetVolumeReplication", mock.Anything, mock.Anything)
}

func TestReplicationUpdateHandler_Handle_SkipsMissingResourceUUID(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationUpdateHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetVolumeReplication", mock.Anything, mock.Anything)
}

func TestReplicationUpdateHandler_Handle_ReplicationNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "replication-uuid"},
	}

	storage.EXPECT().GetVolumeReplication(mock.Anything, "replication-uuid").Return((*datamodel.VolumeReplication)(nil), vsaerrors.NewNotFoundErr("replication", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateVolumeReplicationStates", mock.Anything, mock.Anything)
}

func TestReplicationUpdateHandler_Handle_GetReplicationError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "replication-uuid"},
	}

	expectedErr := errors.New("database error")
	storage.EXPECT().GetVolumeReplication(mock.Anything, "replication-uuid").Return((*datamodel.VolumeReplication)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "load replication for update cleanup")
}

func TestReplicationUpdateHandler_Handle_SkipsNonUpdatingState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "replication-uuid"},
	}

	replication := &datamodel.VolumeReplication{
		BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
		State:     models.LifeCycleStateAvailable,
	}
	storage.EXPECT().GetVolumeReplication(mock.Anything, "replication-uuid").Return(replication, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateVolumeReplicationStates", mock.Anything, mock.Anything)
}

func TestReplicationUpdateHandler_Handle_SuccessWithPreviousState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationUpdateHandler()

	previousState := models.LifeCycleStateREADY
	previousStateDetails := models.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "replication-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	replication := &datamodel.VolumeReplication{
		BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
		State:     models.LifeCycleStateUpdating,
	}
	storage.EXPECT().GetVolumeReplication(mock.Anything, "replication-uuid").Return(replication, nil).Once()
	storage.EXPECT().UpdateVolumeReplicationStates(mock.Anything, mock.MatchedBy(func(r *datamodel.VolumeReplication) bool {
		return r.State == previousState && r.StateDetails == previousStateDetails
	})).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestReplicationUpdateHandler_Handle_SuccessWithFallbackToAvailable(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "replication-uuid",
			// PreviousState is empty, should fallback to AVAILABLE
		},
	}

	replication := &datamodel.VolumeReplication{
		BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
		State:     models.LifeCycleStateUpdating,
	}
	storage.EXPECT().GetVolumeReplication(mock.Anything, "replication-uuid").Return(replication, nil).Once()
	storage.EXPECT().UpdateVolumeReplicationStates(mock.Anything, mock.MatchedBy(func(r *datamodel.VolumeReplication) bool {
		return r.State == models.LifeCycleStateAvailable && r.StateDetails == models.LifeCycleStateAvailableDetails
	})).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestReplicationUpdateHandler_Handle_UpdateReplicationStatesError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationUpdateHandler()

	previousState := models.LifeCycleStateREADY
	previousStateDetails := models.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "replication-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	replication := &datamodel.VolumeReplication{
		BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
		State:     models.LifeCycleStateUpdating,
	}
	expectedErr := errors.New("update failed")
	storage.EXPECT().GetVolumeReplication(mock.Anything, "replication-uuid").Return(replication, nil).Once()
	storage.EXPECT().UpdateVolumeReplicationStates(mock.Anything, mock.Anything).Return(expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "revert replication")
}

func TestReplicationUpdateHandler_Handle_UpdateVolumeReplicationAttributesJobType(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationUpdateHandler()

	job := &datamodel.Job{
		Type: string(models.JobTypeUpdateVolumeReplicationAttributes),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "replication-uuid",
		},
	}

	replication := &datamodel.VolumeReplication{
		BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
		State:     models.LifeCycleStateUpdating,
	}
	storage.EXPECT().GetVolumeReplication(mock.Anything, "replication-uuid").Return(replication, nil).Once()
	storage.EXPECT().UpdateVolumeReplicationStates(mock.Anything, mock.Anything).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestReplicationUpdateHandler_Handle_UpdateVolumeReplicationInternalJobType(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationUpdateHandler()

	job := &datamodel.Job{
		Type: string(models.JobTypeUpdateVolumeReplicationInternal),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "replication-uuid",
		},
	}

	replication := &datamodel.VolumeReplication{
		BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
		State:     models.LifeCycleStateUpdating,
	}
	storage.EXPECT().GetVolumeReplication(mock.Anything, "replication-uuid").Return(replication, nil).Once()
	storage.EXPECT().UpdateVolumeReplicationStates(mock.Anything, mock.Anything).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

