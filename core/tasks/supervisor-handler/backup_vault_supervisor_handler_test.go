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

func TestBackupVaultHandler_JobTypes(t *testing.T) {
	handler := NewBackupVaultHandler()
	jobTypes := handler.JobTypes()

	require.Len(t, jobTypes, 1)
	require.Contains(t, jobTypes, models.JobTypeCreateBackupVault)
}

func TestNewBackupVaultHandler(t *testing.T) {
	handler := NewBackupVaultHandler()
	require.NotNil(t, handler)
	require.IsType(t, &BackupVaultHandler{}, handler)
}

func TestBackupVaultHandler_Handle_SkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupVaultHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "vault-uuid"},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "DeleteBackupVaultInVCP", mock.Anything, mock.Anything)
}

func TestBackupVaultHandler_Handle_SkipsMissingResourceUUID(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupVaultHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "DeleteBackupVaultInVCP", mock.Anything, mock.Anything)
}

func TestBackupVaultHandler_Handle_BackupVaultNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupVaultHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "vault-uuid"},
	}

	storage.EXPECT().DeleteBackupVaultInVCP(mock.Anything, "vault-uuid").
		Return((*datamodel.BackupVault)(nil), vsaerrors.NewNotFoundErr("backup vault", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestBackupVaultHandler_Handle_DeleteBackupVaultInVCPError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupVaultHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "vault-uuid"},
	}

	expectedErr := errors.New("database error")
	storage.EXPECT().DeleteBackupVaultInVCP(mock.Anything, "vault-uuid").
		Return((*datamodel.BackupVault)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "delete backup vault from VCP")
}

func TestBackupVaultHandler_Handle_Success(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupVaultHandler()

	job := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "job-uuid"},
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "vault-uuid"},
	}

	storage.EXPECT().DeleteBackupVaultInVCP(mock.Anything, "vault-uuid").
		Return(&datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: "vault-uuid"}}, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}
