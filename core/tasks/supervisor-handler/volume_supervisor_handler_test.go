package supervisorhandler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestVolumeHandlerHandleSkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "volume-uuid"},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "DeleteVolumeAndChildResources", mock.Anything, mock.Anything)
}

func TestVolumeHandlerHandleSkipsMissingResource(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "DeleteVolumeAndChildResources", mock.Anything, mock.Anything)
}

func TestVolumeHandlerHandleNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "volume-uuid"},
	}

	storage.EXPECT().DeleteVolumeAndChildResources(mock.Anything, "volume-uuid").Return((*datamodel.Volume)(nil), vsaerrors.NewNotFoundErr("volume", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestVolumeHandlerHandleSuccess(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "volume-uuid"},
	}

	storage.EXPECT().DeleteVolumeAndChildResources(mock.Anything, "volume-uuid").Return(&datamodel.Volume{}, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestVolumeHandler_Handle_NewStateTimeout_DeletesVolume(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewVolumeHandler()

	job := &datamodel.Job{
		State:         string(datamodel.JobsStateNEW),
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "volume-uuid"},
	}

	storage.EXPECT().DeleteVolumeAndChildResources(mock.Anything, "volume-uuid").Return(&datamodel.Volume{}, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything)
}
