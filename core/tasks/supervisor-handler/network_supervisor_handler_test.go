package supervisorhandler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

func TestNetworkHandlerHandleSkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewNetworkHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{PoolUUID: "pool-uuid"},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetPoolByUUID", mock.Anything, mock.Anything)
}

func TestNetworkHandlerHandleSkipsMissingJobAttributes(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewNetworkHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetPoolByUUID", mock.Anything, mock.Anything)
}

func TestNetworkHandlerHandleWithPoolUUID(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewNetworkHandler()

	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "job-uuid",
		},
		JobAttributes: &datamodel.JobAttributes{
			PoolUUID: "pool-uuid",
		},
	}

	// Handler only logs, doesn't fetch pool from storage
	storage.AssertNotCalled(t, "GetPoolByUUID", mock.Anything, mock.Anything)

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestNetworkHandlerHandleSuccessWithPool(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewNetworkHandler()

	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "job-uuid",
		},
		JobAttributes: &datamodel.JobAttributes{
			PoolUUID: "pool-uuid",
		},
	}

	// Handler only logs, doesn't fetch pool from storage or perform cleanup
	storage.AssertNotCalled(t, "GetPoolByUUID", mock.Anything, mock.Anything)
	storage.AssertNotCalled(t, "DeletePool", mock.Anything, mock.Anything)

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestNetworkHandlerHandleSuccessWithoutPool(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewNetworkHandler()

	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "job-uuid",
		},
		JobAttributes: &datamodel.JobAttributes{},
	}

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	// Network handler doesn't perform VCP-side cleanup when no pool UUID, only logs
	storage.AssertNotCalled(t, "GetPoolByUUID", mock.Anything, mock.Anything)
	storage.AssertNotCalled(t, "DeletePool", mock.Anything, mock.Anything)
}

func TestNetworkHandlerJobTypes(t *testing.T) {
	handler := NewNetworkHandler()
	jobTypes := handler.JobTypes()

	require.Len(t, jobTypes, 2)
	require.Contains(t, jobTypes, datamodel.JobTypeCreateSubnet)
	require.Contains(t, jobTypes, datamodel.JobTypeCreateLargeSubnet)
}
