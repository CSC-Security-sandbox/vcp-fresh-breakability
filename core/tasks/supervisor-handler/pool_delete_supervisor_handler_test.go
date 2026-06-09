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

func TestPoolDeleteHandler_JobTypes(t *testing.T) {
	handler := NewPoolDeleteHandler()
	jobTypes := handler.JobTypes()

	require.Len(t, jobTypes, 2)
	require.Contains(t, jobTypes, datamodel.JobTypeDeletePool)
	require.Contains(t, jobTypes, datamodel.JobTypeDeleteLargePool)
}

func TestNewPoolDeleteHandler(t *testing.T) {
	handler := NewPoolDeleteHandler()
	require.NotNil(t, handler)
	require.IsType(t, &PoolDeleteHandler{}, handler)
}

func TestPoolDeleteHandler_Handle_DeleteLargePoolJobType(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolDeleteHandler()

	job := &datamodel.Job{
		Type: string(datamodel.JobTypeDeleteLargePool),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "pool-uuid",
		},
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return(pool, nil).Once()
	storage.EXPECT().UpdatePoolState(mock.Anything, pool, datamodel.LifeCycleStateAvailable, datamodel.LifeCycleStateAvailableDetails).Return(pool, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestPoolDeleteHandler_Handle_SkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "pool-uuid"},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetPoolByUUID", mock.Anything, mock.Anything)
}

func TestPoolDeleteHandler_Handle_SkipsMissingResourceUUID(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolDeleteHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetPoolByUUID", mock.Anything, mock.Anything)
}

func TestPoolDeleteHandler_Handle_PoolNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "pool-uuid"},
	}

	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return((*datamodel.Pool)(nil), vsaerrors.NewNotFoundErr("pool", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdatePoolState", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestPoolDeleteHandler_Handle_GetPoolError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "pool-uuid"},
	}

	expectedErr := errors.New("database error")
	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return((*datamodel.Pool)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "load pool")
}

func TestPoolDeleteHandler_Handle_SkipsNonDeletingState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "pool-uuid"},
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		State:     datamodel.LifeCycleStateAvailable,
	}
	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return(pool, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdatePoolState", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestPoolDeleteHandler_Handle_SuccessWithPreviousState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolDeleteHandler()

	previousState := datamodel.LifeCycleStateREADY
	previousStateDetails := datamodel.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "pool-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return(pool, nil).Once()
	storage.EXPECT().UpdatePoolState(mock.Anything, pool, previousState, previousStateDetails).Return(pool, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestPoolDeleteHandler_Handle_SuccessWithFallbackToAvailable(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "pool-uuid",
			// PreviousState is empty, should fallback to AVAILABLE
		},
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return(pool, nil).Once()
	storage.EXPECT().UpdatePoolState(mock.Anything, pool, datamodel.LifeCycleStateAvailable, datamodel.LifeCycleStateAvailableDetails).Return(pool, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestPoolDeleteHandler_Handle_UpdatePoolStateError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolDeleteHandler()

	previousState := datamodel.LifeCycleStateREADY
	previousStateDetails := datamodel.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "pool-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	expectedErr := errors.New("update failed")
	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return(pool, nil).Once()
	storage.EXPECT().UpdatePoolState(mock.Anything, pool, previousState, previousStateDetails).Return((*datamodel.Pool)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "revert pool state")
}

// Tests for PROCESSING state timeout handling

func TestPoolDeleteHandler_Handle_ProcessingTimeout_TransitionsDeletingToError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolDeleteHandler()

	job := &datamodel.Job{
		State:         string(datamodel.JobsStatePROCESSING),
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "pool-uuid"},
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return(pool, nil).Once()
	storage.EXPECT().UpdatePoolState(mock.Anything, pool, datamodel.LifeCycleStateError, datamodel.LifeCycleStateDeletionErrorDetails).Return(pool, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestPoolDeleteHandler_Handle_ProcessingTimeout_SkipsNonDeletingState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolDeleteHandler()

	job := &datamodel.Job{
		State:         string(datamodel.JobsStatePROCESSING),
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "pool-uuid"},
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		State:     datamodel.LifeCycleStateAvailable,
	}
	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return(pool, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdatePoolState", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestPoolDeleteHandler_Handle_ProcessingTimeout_PoolNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolDeleteHandler()

	job := &datamodel.Job{
		State:         string(datamodel.JobsStatePROCESSING),
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "pool-uuid"},
	}

	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return((*datamodel.Pool)(nil), vsaerrors.NewNotFoundErr("pool", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdatePoolState", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestPoolDeleteHandler_Handle_ProcessingTimeout_GetPoolError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolDeleteHandler()

	job := &datamodel.Job{
		State:         string(datamodel.JobsStatePROCESSING),
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "pool-uuid"},
	}

	expectedErr := errors.New("database error")
	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return((*datamodel.Pool)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "load pool for PROCESSING timeout")
}

func TestPoolDeleteHandler_Handle_ProcessingTimeout_UpdatePoolStateError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolDeleteHandler()

	job := &datamodel.Job{
		State:         string(datamodel.JobsStatePROCESSING),
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "pool-uuid"},
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	expectedErr := errors.New("update failed")
	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return(pool, nil).Once()
	storage.EXPECT().UpdatePoolState(mock.Anything, pool, datamodel.LifeCycleStateError, datamodel.LifeCycleStateDeletionErrorDetails).Return((*datamodel.Pool)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "update pool state to ERROR")
}

func TestPoolDeleteHandler_Handle_NewStateTimeout_RevertsPoolState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolDeleteHandler()

	// Job in NEW state should trigger revert behavior
	job := &datamodel.Job{
		State: string(datamodel.JobsStateNEW),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "pool-uuid",
			PreviousState:        datamodel.LifeCycleStateREADY,
			PreviousStateDetails: datamodel.LifeCycleStateReadyDetails,
		},
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return(pool, nil).Once()
	storage.EXPECT().UpdatePoolState(mock.Anything, pool, datamodel.LifeCycleStateREADY, datamodel.LifeCycleStateReadyDetails).Return(pool, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}
