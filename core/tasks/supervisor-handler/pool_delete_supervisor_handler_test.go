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

func TestPoolDeleteHandler_JobTypes(t *testing.T) {
	handler := NewPoolDeleteHandler()
	jobTypes := handler.JobTypes()

	require.Len(t, jobTypes, 2)
	require.Contains(t, jobTypes, models.JobTypeDeletePool)
	require.Contains(t, jobTypes, models.JobTypeDeleteLargePool)
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
		Type: string(models.JobTypeDeleteLargePool),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "pool-uuid",
		},
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		State:     models.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return(pool, nil).Once()
	storage.EXPECT().UpdatePoolState(mock.Anything, pool, models.LifeCycleStateAvailable, models.LifeCycleStateAvailableDetails).Return(pool, nil).Once()

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
		State:     models.LifeCycleStateAvailable,
	}
	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return(pool, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdatePoolState", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestPoolDeleteHandler_Handle_SuccessWithPreviousState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolDeleteHandler()

	previousState := models.LifeCycleStateREADY
	previousStateDetails := models.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "pool-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		State:     models.LifeCycleStateDeleting,
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
		State:     models.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return(pool, nil).Once()
	storage.EXPECT().UpdatePoolState(mock.Anything, pool, models.LifeCycleStateAvailable, models.LifeCycleStateAvailableDetails).Return(pool, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestPoolDeleteHandler_Handle_UpdatePoolStateError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolDeleteHandler()

	previousState := models.LifeCycleStateREADY
	previousStateDetails := models.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "pool-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		State:     models.LifeCycleStateDeleting,
	}
	expectedErr := errors.New("update failed")
	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return(pool, nil).Once()
	storage.EXPECT().UpdatePoolState(mock.Anything, pool, previousState, previousStateDetails).Return((*datamodel.Pool)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "revert pool state")
}

