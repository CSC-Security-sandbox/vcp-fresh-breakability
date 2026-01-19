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

func TestReplicationDeleteHandler_JobTypes(t *testing.T) {
	handler := NewReplicationDeleteHandler()
	jobTypes := handler.JobTypes()

	require.Len(t, jobTypes, 2)
	require.Contains(t, jobTypes, models.JobTypeDeleteVolumeReplicationInternal)
	require.Contains(t, jobTypes, models.JobTypeDeleteVolumeReplication)
}

func TestNewReplicationDeleteHandler(t *testing.T) {
	handler := NewReplicationDeleteHandler()
	require.NotNil(t, handler)
	require.IsType(t, &ReplicationDeleteHandler{}, handler)
}

func TestReplicationDeleteHandler_Handle_SkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "replication-uuid"},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetVolumeReplication", mock.Anything, mock.Anything)
}

func TestReplicationDeleteHandler_Handle_SkipsMissingResourceUUID(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationDeleteHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetVolumeReplication", mock.Anything, mock.Anything)
}

func TestReplicationDeleteHandler_Handle_ReplicationNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "replication-uuid"},
	}

	storage.EXPECT().GetVolumeReplication(mock.Anything, "replication-uuid").Return((*datamodel.VolumeReplication)(nil), vsaerrors.NewNotFoundErr("replication", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateVolumeReplicationStates", mock.Anything, mock.Anything)
}

func TestReplicationDeleteHandler_Handle_GetReplicationError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "replication-uuid"},
	}

	expectedErr := errors.New("database error")
	storage.EXPECT().GetVolumeReplication(mock.Anything, "replication-uuid").Return((*datamodel.VolumeReplication)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "load replication for delete cleanup")
}

func TestReplicationDeleteHandler_Handle_SkipsNonDeletingState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationDeleteHandler()

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

func TestReplicationDeleteHandler_Handle_SuccessWithPreviousState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationDeleteHandler()

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
		State:     models.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetVolumeReplication(mock.Anything, "replication-uuid").Return(replication, nil).Once()
	storage.EXPECT().UpdateVolumeReplicationStates(mock.Anything, mock.MatchedBy(func(r *datamodel.VolumeReplication) bool {
		return r.State == previousState && r.StateDetails == previousStateDetails
	})).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestReplicationDeleteHandler_Handle_SuccessWithFallbackToAvailable(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "replication-uuid",
			// PreviousState is empty, should fallback to AVAILABLE
		},
	}

	replication := &datamodel.VolumeReplication{
		BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
		State:     models.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetVolumeReplication(mock.Anything, "replication-uuid").Return(replication, nil).Once()
	storage.EXPECT().UpdateVolumeReplicationStates(mock.Anything, mock.MatchedBy(func(r *datamodel.VolumeReplication) bool {
		return r.State == models.LifeCycleStateAvailable && r.StateDetails == models.LifeCycleStateAvailableDetails
	})).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestReplicationDeleteHandler_Handle_UpdateReplicationStatesError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationDeleteHandler()

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
		State:     models.LifeCycleStateDeleting,
	}
	expectedErr := errors.New("update failed")
	storage.EXPECT().GetVolumeReplication(mock.Anything, "replication-uuid").Return(replication, nil).Once()
	storage.EXPECT().UpdateVolumeReplicationStates(mock.Anything, mock.Anything).Return(expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "revert replication")
}

func TestReplicationDeleteHandler_Handle_DeleteVolumeReplicationInternalJobType(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationDeleteHandler()

	job := &datamodel.Job{
		Type: string(models.JobTypeDeleteVolumeReplicationInternal),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "replication-uuid",
		},
	}

	replication := &datamodel.VolumeReplication{
		BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
		State:     models.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetVolumeReplication(mock.Anything, "replication-uuid").Return(replication, nil).Once()
	storage.EXPECT().UpdateVolumeReplicationStates(mock.Anything, mock.Anything).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

