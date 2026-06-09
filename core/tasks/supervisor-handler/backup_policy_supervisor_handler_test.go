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

func TestBackupPolicyHandler_JobTypes(t *testing.T) {
	handler := NewBackupPolicyHandler()
	jobTypes := handler.JobTypes()

	require.Len(t, jobTypes, 1)
	require.Contains(t, jobTypes, datamodel.JobTypeCreateBackupPolicy)
}

func TestNewBackupPolicyHandler(t *testing.T) {
	handler := NewBackupPolicyHandler()
	require.NotNil(t, handler)
	require.IsType(t, &BackupPolicyHandler{}, handler)
}

func TestBackupPolicyHandler_Handle_SkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "policy-uuid"},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "DeleteBackupPolicy", mock.Anything, mock.Anything)
}

func TestBackupPolicyHandler_Handle_SkipsMissingResourceUUID(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "DeleteBackupPolicy", mock.Anything, mock.Anything)
}

func TestBackupPolicyHandler_Handle_BackupPolicyNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "policy-uuid"},
	}

	storage.EXPECT().DeleteBackupPolicy(mock.Anything, "policy-uuid").
		Return((*datamodel.BackupPolicy)(nil), vsaerrors.NewNotFoundErr("backup policy", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestBackupPolicyHandler_Handle_DeleteBackupPolicyError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "policy-uuid"},
	}

	expectedErr := errors.New("database error")
	storage.EXPECT().DeleteBackupPolicy(mock.Anything, "policy-uuid").
		Return((*datamodel.BackupPolicy)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "delete backup policy from VCP")
}

func TestBackupPolicyHandler_Handle_Success(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyHandler()

	job := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "job-uuid"},
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "policy-uuid"},
	}

	storage.EXPECT().DeleteBackupPolicy(mock.Anything, "policy-uuid").
		Return(&datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: "policy-uuid"}}, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}
