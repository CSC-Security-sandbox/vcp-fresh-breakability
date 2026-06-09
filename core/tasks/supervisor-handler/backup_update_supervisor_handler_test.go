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

func TestBackupUpdateHandler_JobTypes(t *testing.T) {
	handler := NewBackupUpdateHandler()
	jobTypes := handler.JobTypes()

	require.Len(t, jobTypes, 1)
	require.Contains(t, jobTypes, datamodel.JobTypeUpdateBackup)
}

func TestNewBackupUpdateHandler(t *testing.T) {
	handler := NewBackupUpdateHandler()
	require.NotNil(t, handler)
	require.IsType(t, &BackupUpdateHandler{}, handler)
}

func TestBackupUpdateHandler_Handle_SkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "backup-uuid"},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetBackup", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupUpdateHandler_Handle_SkipsMissingResourceUUID(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupUpdateHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetBackup", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupUpdateHandler_Handle_BackupNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupUpdateHandler()

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

func TestBackupUpdateHandler_Handle_GetBackupError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupUpdateHandler()

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
	require.Contains(t, err.Error(), "load backup for update cleanup")
}

func TestBackupUpdateHandler_Handle_SkipsNonUpdatingState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupUpdateHandler()

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

func TestBackupUpdateHandler_Handle_SuccessWithPreviousState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupUpdateHandler()

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
		State:     datamodel.LifeCycleStateUpdating,
	}
	storage.EXPECT().GetBackup(mock.Anything, "vault-uuid", "backup-uuid", "account").Return(backup, nil).Once()
	storage.EXPECT().UpdateBackupState(mock.Anything, mock.MatchedBy(func(b *datamodel.Backup) bool {
		return b.State == previousState && b.StateDetails == previousStateDetails
	})).Return(backup, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestBackupUpdateHandler_Handle_SuccessWithFallbackToAvailable(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupUpdateHandler()

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
		State:     datamodel.LifeCycleStateUpdating,
	}
	storage.EXPECT().GetBackup(mock.Anything, "vault-uuid", "backup-uuid", "").Return(backup, nil).Once()
	storage.EXPECT().UpdateBackupState(mock.Anything, mock.MatchedBy(func(b *datamodel.Backup) bool {
		return b.State == datamodel.LifeCycleStateAvailable && b.StateDetails == datamodel.LifeCycleStateAvailableDetails
	})).Return(backup, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestBackupUpdateHandler_Handle_UpdateBackupStateError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupUpdateHandler()

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
		State:     datamodel.LifeCycleStateUpdating,
	}
	expectedErr := errors.New("update failed")
	storage.EXPECT().GetBackup(mock.Anything, "vault-uuid", "backup-uuid", "").Return(backup, nil).Once()
	storage.EXPECT().UpdateBackupState(mock.Anything, mock.Anything).Return((*datamodel.Backup)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "revert backup")
}

func TestBackupUpdateHandler_Handle_EmptyPayloadAttributes(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:      "backup-uuid",
			PayloadAttributes: map[string]interface{}{},
		},
	}

	backup := &datamodel.Backup{
		BaseModel: datamodel.BaseModel{UUID: "backup-uuid"},
		State:     datamodel.LifeCycleStateUpdating,
	}
	storage.EXPECT().GetBackup(mock.Anything, "", "backup-uuid", "").Return(backup, nil).Once()
	storage.EXPECT().UpdateBackupState(mock.Anything, mock.Anything).Return(backup, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestBackupUpdateHandler_Handle_InvalidPayloadAttributeTypes(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "backup-uuid",
			PayloadAttributes: map[string]interface{}{
				"backup_vault_uuid": 123,  // Wrong type
				"account_name":      true, // Wrong type
			},
		},
	}

	backup := &datamodel.Backup{
		BaseModel: datamodel.BaseModel{UUID: "backup-uuid"},
		State:     datamodel.LifeCycleStateUpdating,
	}
	// Should use empty strings when type assertion fails
	storage.EXPECT().GetBackup(mock.Anything, "", "backup-uuid", "").Return(backup, nil).Once()
	storage.EXPECT().UpdateBackupState(mock.Anything, mock.Anything).Return(backup, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestBackupUpdateHandler_Handle_PartialPayloadAttributes(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "backup-uuid",
			PayloadAttributes: map[string]interface{}{
				"backup_vault_uuid": "vault-uuid",
				// account_name missing
			},
		},
	}

	backup := &datamodel.Backup{
		BaseModel: datamodel.BaseModel{UUID: "backup-uuid"},
		State:     datamodel.LifeCycleStateUpdating,
	}
	storage.EXPECT().GetBackup(mock.Anything, "vault-uuid", "backup-uuid", "").Return(backup, nil).Once()
	storage.EXPECT().UpdateBackupState(mock.Anything, mock.Anything).Return(backup, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}
