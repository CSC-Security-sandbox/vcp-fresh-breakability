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

func TestBackupHandlerJobTypes(t *testing.T) {
	handler := NewBackupHandler()

	require.ElementsMatch(t, []datamodel.JobType{
		datamodel.JobTypeCreateBackup,
		datamodel.JobTypeCreateScheduledBackup,
	}, handler.JobTypes())
}

func TestBackupHandlerHandleSkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "DeleteBackup", mock.Anything, mock.Anything)
}

func TestBackupHandlerHandleSkipsMissingResource(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "DeleteBackup", mock.Anything, mock.Anything)
}

func TestBackupHandlerHandleNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupHandler()

	job := &datamodel.Job{JobAttributes: &datamodel.JobAttributes{ResourceUUID: "backup-uuid"}}

	storage.EXPECT().DeleteBackup(mock.Anything, "backup-uuid").
		Return((*datamodel.Backup)(nil), vsaerrors.NewNotFoundErr("backup", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestBackupHandlerHandleSuccess(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupHandler()

	job := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "job-uuid"},
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "backup-uuid"},
	}

	storage.EXPECT().DeleteBackup(mock.Anything, "backup-uuid").
		Return(&datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: "backup-uuid"}}, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}
