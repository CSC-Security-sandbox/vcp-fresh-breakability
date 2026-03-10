package supervisorhandler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestPoolHandlerHandleSkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "pool-uuid"},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetPoolByUUID", mock.Anything, mock.Anything)
}

func TestPoolHandlerHandleSkipsMissingResource(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetPoolByUUID", mock.Anything, mock.Anything)
}

func TestPoolHandlerHandleNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "pool-uuid"},
	}

	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return((*datamodel.Pool)(nil), vsaerrors.NewNotFoundErr("pool", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "DeletePool", mock.Anything, mock.Anything)
}

func TestPoolHandlerHandleSuccess(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "pool-uuid"},
	}

	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-uuid"}}
	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return(pool, nil).Once()
	storage.EXPECT().DeletePool(mock.Anything, pool).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestPoolHandler_Handle_NewStateTimeout_DeletesPool(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewPoolHandler()

	// Job in NEW state (or empty state) should trigger delete behavior
	job := &datamodel.Job{
		State:         string(models.JobsStateNEW),
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "pool-uuid"},
	}

	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-uuid"}}
	storage.EXPECT().GetPoolByUUID(mock.Anything, "pool-uuid").Return(pool, nil).Once()
	storage.EXPECT().DeletePool(mock.Anything, pool).Return(nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdatePoolState", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}
