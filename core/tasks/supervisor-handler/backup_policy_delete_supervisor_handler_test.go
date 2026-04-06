package supervisorhandler

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestBackupPolicyDeleteHandler_JobTypes(t *testing.T) {
	handler := NewBackupPolicyDeleteHandler()
	jobTypes := handler.JobTypes()

	require.Len(t, jobTypes, 1)
	require.Contains(t, jobTypes, models.JobTypeDeleteBackupPolicy)
}

func TestNewBackupPolicyDeleteHandler(t *testing.T) {
	handler := NewBackupPolicyDeleteHandler()
	require.NotNil(t, handler)
	require.IsType(t, &BackupPolicyDeleteHandler{}, handler)
}

func TestBackupPolicyDeleteHandler_Handle_SkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "policy-uuid"},
		AccountID:     sql.NullInt64{Int64: testAccountID, Valid: true},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetBackupPolicyByUUIDAndOwnerID", mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupPolicyDeleteHandler_Handle_SkipsMissingResourceUUID(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyDeleteHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetBackupPolicyByUUIDAndOwnerID", mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupPolicyDeleteHandler_Handle_BackupPolicyNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "policy-uuid"},
		AccountID:     sql.NullInt64{Int64: testAccountID, Valid: true},
	}

	storage.EXPECT().GetBackupPolicyByUUIDAndOwnerID(mock.Anything, "policy-uuid", testAccountID).
		Return((*datamodel.BackupPolicy)(nil), vsaerrors.NewNotFoundErr("backup policy", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateBackupPolicy", mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupPolicyDeleteHandler_Handle_GetBackupPolicyError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "policy-uuid"},
		AccountID:     sql.NullInt64{Int64: testAccountID, Valid: true},
	}

	expectedErr := errors.New("database error")
	storage.EXPECT().GetBackupPolicyByUUIDAndOwnerID(mock.Anything, "policy-uuid", testAccountID).
		Return((*datamodel.BackupPolicy)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "load backup policy for delete cleanup")
}

func TestBackupPolicyDeleteHandler_Handle_SkipsNonDeletingState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "policy-uuid"},
		AccountID:     sql.NullInt64{Int64: testAccountID, Valid: true},
	}

	backupPolicy := &datamodel.BackupPolicy{
		BaseModel:             datamodel.BaseModel{UUID: "policy-uuid"},
		LifeCycleState:        models.LifeCycleStateAvailable,
		LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
	}
	storage.EXPECT().GetBackupPolicyByUUIDAndOwnerID(mock.Anything, "policy-uuid", testAccountID).Return(backupPolicy, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateBackupPolicy", mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupPolicyDeleteHandler_Handle_SuccessWithPreviousState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyDeleteHandler()

	previousState := models.LifeCycleStateREADY
	previousStateDetails := models.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "policy-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
		AccountID: sql.NullInt64{Int64: testAccountID, Valid: true},
	}

	backupPolicy := &datamodel.BackupPolicy{
		BaseModel:             datamodel.BaseModel{UUID: "policy-uuid"},
		LifeCycleState:        models.LifeCycleStateDeleting,
		LifeCycleStateDetails: models.LifeCycleStateDeletingDetails,
	}
	storage.EXPECT().GetBackupPolicyByUUIDAndOwnerID(mock.Anything, "policy-uuid", testAccountID).Return(backupPolicy, nil).Once()
	storage.EXPECT().UpdateBackupPolicy(mock.Anything, "policy-uuid", mock.MatchedBy(func(updates map[string]interface{}) bool {
		s, _ := updates["life_cycle_state"].(string)
		d, _ := updates["life_cycle_state_details"].(string)
		return s == previousState && d == previousStateDetails
	})).Return(backupPolicy, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestBackupPolicyDeleteHandler_Handle_SuccessWithFallbackToReady(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "policy-uuid"},
		AccountID:     sql.NullInt64{Int64: testAccountID, Valid: true},
	}

	backupPolicy := &datamodel.BackupPolicy{
		BaseModel:             datamodel.BaseModel{UUID: "policy-uuid"},
		LifeCycleState:        models.LifeCycleStateDeleting,
		LifeCycleStateDetails: models.LifeCycleStateDeletingDetails,
	}
	storage.EXPECT().GetBackupPolicyByUUIDAndOwnerID(mock.Anything, "policy-uuid", testAccountID).Return(backupPolicy, nil).Once()
	storage.EXPECT().UpdateBackupPolicy(mock.Anything, "policy-uuid", mock.MatchedBy(func(updates map[string]interface{}) bool {
		s, _ := updates["life_cycle_state"].(string)
		d, _ := updates["life_cycle_state_details"].(string)
		return s == models.LifeCycleStateREADY && d == models.LifeCycleStateAvailableDetails
	})).Return(backupPolicy, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestBackupPolicyDeleteHandler_Handle_UpdateBackupPolicyError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyDeleteHandler()

	previousState := models.LifeCycleStateREADY
	previousStateDetails := models.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "policy-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
		AccountID: sql.NullInt64{Int64: testAccountID, Valid: true},
	}

	backupPolicy := &datamodel.BackupPolicy{
		BaseModel:             datamodel.BaseModel{UUID: "policy-uuid"},
		LifeCycleState:        models.LifeCycleStateDeleting,
		LifeCycleStateDetails: models.LifeCycleStateDeletingDetails,
	}
	expectedErr := errors.New("update failed")
	storage.EXPECT().GetBackupPolicyByUUIDAndOwnerID(mock.Anything, "policy-uuid", testAccountID).Return(backupPolicy, nil).Once()
	storage.EXPECT().UpdateBackupPolicy(mock.Anything, "policy-uuid", mock.Anything).
		Return((*datamodel.BackupPolicy)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "revert backup policy")
}
