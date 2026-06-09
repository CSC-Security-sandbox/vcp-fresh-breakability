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

func TestBackupDeleteHandler_JobTypes(t *testing.T) {
	handler := NewBackupDeleteHandler()
	jobTypes := handler.JobTypes()

	require.Len(t, jobTypes, 1)
	require.Contains(t, jobTypes, datamodel.JobTypeDeleteBackup)
}

func TestNewBackupDeleteHandler(t *testing.T) {
	handler := NewBackupDeleteHandler()
	require.NotNil(t, handler)
	require.IsType(t, &BackupDeleteHandler{}, handler)
}

func TestBackupDeleteHandler_Handle_SkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "backup-uuid"},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetBackup", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupDeleteHandler_Handle_SkipsMissingResourceUUID(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupDeleteHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetBackup", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupDeleteHandler_Handle_BackupNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "backup-uuid",
			PayloadAttributes: map[string]interface{}{
				"backup_vault_uuid": "vault-uuid",
				"account_name":      "account",
			},
		},
	}

	storage.EXPECT().GetBackup(mock.Anything, "vault-uuid", "backup-uuid", "account").Return((*datamodel.Backup)(nil), vsaerrors.NewNotFoundErr("backup", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateBackupState", mock.Anything, mock.Anything)
}

func TestBackupDeleteHandler_Handle_GetBackupError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "backup-uuid",
			PayloadAttributes: map[string]interface{}{
				"backup_vault_uuid": "vault-uuid",
			},
		},
	}

	expectedErr := errors.New("database error")
	storage.EXPECT().GetBackup(mock.Anything, "vault-uuid", "backup-uuid", "").Return((*datamodel.Backup)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "load backup for delete cleanup")
}

func TestBackupDeleteHandler_Handle_SkipsNonDeletingState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "backup-uuid",
			PayloadAttributes: map[string]interface{}{
				"backup_vault_uuid": "vault-uuid",
			},
		},
	}

	backup := &datamodel.Backup{
		BaseModel: datamodel.BaseModel{UUID: "backup-uuid"},
		State:     datamodel.LifeCycleStateAvailable,
	}
	storage.EXPECT().GetBackup(mock.Anything, "vault-uuid", "backup-uuid", "").Return(backup, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateBackupState", mock.Anything, mock.Anything)
}

func TestBackupDeleteHandler_Handle_SuccessWithPreviousState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupDeleteHandler()

	previousState := datamodel.LifeCycleStateREADY
	previousStateDetails := datamodel.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "backup-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
			PayloadAttributes: map[string]interface{}{
				"backup_vault_uuid": "vault-uuid",
				"account_name":      "account",
			},
		},
	}

	backup := &datamodel.Backup{
		BaseModel: datamodel.BaseModel{UUID: "backup-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetBackup(mock.Anything, "vault-uuid", "backup-uuid", "account").Return(backup, nil).Once()
	storage.EXPECT().UpdateBackupState(mock.Anything, mock.MatchedBy(func(b *datamodel.Backup) bool {
		return b.State == previousState && b.StateDetails == previousStateDetails
	})).Return(backup, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestBackupDeleteHandler_Handle_SuccessWithFallbackToAvailable(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "backup-uuid",
			PayloadAttributes: map[string]interface{}{
				"backup_vault_uuid": "vault-uuid",
			},
		},
	}

	backup := &datamodel.Backup{
		BaseModel: datamodel.BaseModel{UUID: "backup-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetBackup(mock.Anything, "vault-uuid", "backup-uuid", "").Return(backup, nil).Once()
	storage.EXPECT().UpdateBackupState(mock.Anything, mock.MatchedBy(func(b *datamodel.Backup) bool {
		return b.State == datamodel.LifeCycleStateAvailable && b.StateDetails == datamodel.LifeCycleStateAvailableDetails
	})).Return(backup, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestBackupDeleteHandler_Handle_UpdateBackupStateError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupDeleteHandler()

	previousState := datamodel.LifeCycleStateREADY
	previousStateDetails := datamodel.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "backup-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
			PayloadAttributes: map[string]interface{}{
				"backup_vault_uuid": "vault-uuid",
			},
		},
	}

	backup := &datamodel.Backup{
		BaseModel: datamodel.BaseModel{UUID: "backup-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	expectedErr := errors.New("update failed")
	storage.EXPECT().GetBackup(mock.Anything, "vault-uuid", "backup-uuid", "").Return(backup, nil).Once()
	storage.EXPECT().UpdateBackupState(mock.Anything, mock.Anything).Return((*datamodel.Backup)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "revert backup")
}

func TestBackupDeleteHandler_Handle_InvalidPayloadAttributeTypes(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "backup-uuid",
			PayloadAttributes: map[string]interface{}{
				"backup_vault_uuid": 456,              // Wrong type
				"account_name":      []string{"test"}, // Wrong type
			},
		},
	}

	backup := &datamodel.Backup{
		BaseModel: datamodel.BaseModel{UUID: "backup-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetBackup(mock.Anything, "", "backup-uuid", "").Return(backup, nil).Once()
	storage.EXPECT().UpdateBackupState(mock.Anything, mock.Anything).Return(backup, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}
