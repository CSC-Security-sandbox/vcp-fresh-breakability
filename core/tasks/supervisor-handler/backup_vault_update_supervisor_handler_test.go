package supervisorhandler

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestBackupVaultUpdateHandler_JobTypes(t *testing.T) {
	handler := NewBackupVaultUpdateHandler()
	jobTypes := handler.JobTypes()

	require.Len(t, jobTypes, 1)
	require.Contains(t, jobTypes, models.JobTypeUpdateBackupVault)
}

func TestNewBackupVaultUpdateHandler(t *testing.T) {
	handler := NewBackupVaultUpdateHandler()
	require.NotNil(t, handler)
	require.IsType(t, &BackupVaultUpdateHandler{}, handler)
}

func TestBackupVaultUpdateHandler_Handle_SkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupVaultUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "vault-uuid"},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetBackupVault", mock.Anything, mock.Anything)
}

func TestBackupVaultUpdateHandler_Handle_SkipsMissingResourceUUID(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupVaultUpdateHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetBackupVault", mock.Anything, mock.Anything)
}

func TestBackupVaultUpdateHandler_Handle_BackupVaultNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupVaultUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "vault-uuid"},
	}

	storage.EXPECT().GetBackupVault(mock.Anything, "vault-uuid").
		Return((*datamodel.BackupVault)(nil), vsaerrors.NewNotFoundErr("backup vault", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateBackupVaultState", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupVaultUpdateHandler_Handle_GetBackupVaultError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupVaultUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "vault-uuid"},
	}

	expectedErr := errors.New("database error")
	storage.EXPECT().GetBackupVault(mock.Anything, "vault-uuid").
		Return((*datamodel.BackupVault)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "load backup vault for update cleanup")
}

func TestBackupVaultUpdateHandler_Handle_SkipsNonUpdatingState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupVaultUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "vault-uuid"},
	}

	backupVault := &datamodel.BackupVault{
		BaseModel:             datamodel.BaseModel{UUID: "vault-uuid"},
		LifeCycleState:        models.LifeCycleStateAvailable,
		LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
	}
	storage.EXPECT().GetBackupVault(mock.Anything, "vault-uuid").Return(backupVault, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateBackupVaultState", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupVaultUpdateHandler_Handle_SuccessWithPreviousState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupVaultUpdateHandler()

	previousState := models.LifeCycleStateREADY
	previousStateDetails := models.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "vault-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	backupVault := &datamodel.BackupVault{
		BaseModel:             datamodel.BaseModel{UUID: "vault-uuid"},
		LifeCycleState:        models.LifeCycleStateUpdating,
		LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
	}
	storage.EXPECT().GetBackupVault(mock.Anything, "vault-uuid").Return(backupVault, nil).Once()
	storage.EXPECT().UpdateBackupVaultState(mock.Anything, backupVault, previousState, previousStateDetails).
		Return(backupVault, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestBackupVaultUpdateHandler_Handle_SuccessWithFallbackToReady(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupVaultUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "vault-uuid"},
	}

	backupVault := &datamodel.BackupVault{
		BaseModel:             datamodel.BaseModel{UUID: "vault-uuid"},
		LifeCycleState:        models.LifeCycleStateUpdating,
		LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
	}
	storage.EXPECT().GetBackupVault(mock.Anything, "vault-uuid").Return(backupVault, nil).Once()
	storage.EXPECT().UpdateBackupVaultState(mock.Anything, backupVault, models.LifeCycleStateREADY, models.LifeCycleStateAvailableDetails).
		Return(backupVault, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestBackupVaultUpdateHandler_Handle_UpdateBackupVaultStateError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupVaultUpdateHandler()

	previousState := models.LifeCycleStateREADY
	previousStateDetails := models.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "vault-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	backupVault := &datamodel.BackupVault{
		BaseModel:             datamodel.BaseModel{UUID: "vault-uuid"},
		LifeCycleState:        models.LifeCycleStateUpdating,
		LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
	}
	expectedErr := errors.New("update failed")
	storage.EXPECT().GetBackupVault(mock.Anything, "vault-uuid").Return(backupVault, nil).Once()
	storage.EXPECT().UpdateBackupVaultState(mock.Anything, backupVault, previousState, previousStateDetails).
		Return((*datamodel.BackupVault)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "revert backup vault")
}
