package supervisorhandler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestReplicationHandlerJobTypes(t *testing.T) {
	handler := NewReplicationHandler()
	require.ElementsMatch(t, []models.JobType{
		models.JobTypeCreateVolumeReplication,
		models.JobTypeCreateVolumeReplicationInternal,
	}, handler.JobTypes())
}

func TestReplicationHandlerHandleSkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetVolumeReplication", mock.Anything, mock.Anything)
}

func TestReplicationHandlerHandleSkipsMissingResource(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetVolumeReplication", mock.Anything, mock.Anything)
}

func TestReplicationHandlerHandleNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationHandler()

	job := &datamodel.Job{JobAttributes: &datamodel.JobAttributes{ResourceUUID: "replication-uuid"}}

	storage.EXPECT().GetVolumeReplication(mock.Anything, "replication-uuid").
		Return((*datamodel.VolumeReplication)(nil), vsaerrors.NewNotFoundErr("volume replication", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "DeleteVolumeReplication", mock.Anything, mock.Anything)
}

func TestReplicationHandlerHandleSuccess(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewReplicationHandler()

	replication := &datamodel.VolumeReplication{
		BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
	}

	job := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "job-uuid"},
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "replication-uuid"},
	}

	storage.EXPECT().GetVolumeReplication(mock.Anything, "replication-uuid").
		Return(replication, nil).Once()
	storage.EXPECT().DeleteVolumeReplication(mock.Anything, replication).
		Return(replication, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}
