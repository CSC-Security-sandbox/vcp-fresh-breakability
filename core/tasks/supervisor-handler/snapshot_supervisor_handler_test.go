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

func TestSnapshotHandlerJobTypes(t *testing.T) {
	handler := NewSnapshotHandler()
	require.ElementsMatch(t, []datamodel.JobType{
		datamodel.JobTypeCreateSnapshot,
	}, handler.JobTypes())
}

func TestSnapshotHandlerHandleSkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewSnapshotHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "DeleteSnapshot", mock.Anything, mock.Anything)
}

func TestSnapshotHandlerHandleSkipsMissingResource(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewSnapshotHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "DeleteSnapshot", mock.Anything, mock.Anything)
}

func TestSnapshotHandlerHandleNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewSnapshotHandler()

	job := &datamodel.Job{JobAttributes: &datamodel.JobAttributes{ResourceUUID: "snapshot-uuid"}}

	storage.EXPECT().DeleteSnapshot(mock.Anything, "snapshot-uuid").
		Return((*datamodel.Snapshot)(nil), vsaerrors.NewNotFoundErr("snapshot", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestSnapshotHandlerHandleSuccess(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewSnapshotHandler()

	job := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "job-uuid"},
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "snapshot-uuid"},
	}

	storage.EXPECT().DeleteSnapshot(mock.Anything, "snapshot-uuid").
		Return(&datamodel.Snapshot{BaseModel: datamodel.BaseModel{UUID: "snapshot-uuid"}}, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}
