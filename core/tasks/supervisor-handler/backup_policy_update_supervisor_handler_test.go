package supervisorhandler

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

const testAccountID int64 = 9999

func TestBackupPolicyUpdateHandler_JobTypes(t *testing.T) {
	handler := NewBackupPolicyUpdateHandler()
	jobTypes := handler.JobTypes()

	require.Len(t, jobTypes, 1)
	require.Contains(t, jobTypes, datamodel.JobTypeUpdateBackupPolicy)
}

func TestNewBackupPolicyUpdateHandler(t *testing.T) {
	handler := NewBackupPolicyUpdateHandler()
	require.NotNil(t, handler)
	require.IsType(t, &BackupPolicyUpdateHandler{}, handler)
}

func TestBackupPolicyUpdateHandler_Handle_SkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "policy-uuid"},
		AccountID:     sql.NullInt64{Int64: testAccountID, Valid: true},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetBackupPolicyByUUIDAndOwnerID", mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupPolicyUpdateHandler_Handle_SkipsMissingResourceUUID(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyUpdateHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetBackupPolicyByUUIDAndOwnerID", mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupPolicyUpdateHandler_Handle_BackupPolicyNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyUpdateHandler()

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

func TestBackupPolicyUpdateHandler_Handle_GetBackupPolicyError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "policy-uuid"},
		AccountID:     sql.NullInt64{Int64: testAccountID, Valid: true},
	}

	expectedErr := errors.New("database error")
	storage.EXPECT().GetBackupPolicyByUUIDAndOwnerID(mock.Anything, "policy-uuid", testAccountID).
		Return((*datamodel.BackupPolicy)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "load backup policy for update cleanup")
}

func TestBackupPolicyUpdateHandler_Handle_SkipsNonUpdatingState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "policy-uuid"},
		AccountID:     sql.NullInt64{Int64: testAccountID, Valid: true},
	}

	backupPolicy := &datamodel.BackupPolicy{
		BaseModel:             datamodel.BaseModel{UUID: "policy-uuid"},
		LifeCycleState:        datamodel.LifeCycleStateAvailable,
		LifeCycleStateDetails: datamodel.LifeCycleStateAvailableDetails,
	}
	storage.EXPECT().GetBackupPolicyByUUIDAndOwnerID(mock.Anything, "policy-uuid", testAccountID).Return(backupPolicy, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateBackupPolicy", mock.Anything, mock.Anything, mock.Anything)
}

func TestBackupPolicyUpdateHandler_Handle_SuccessWithPreviousState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyUpdateHandler()

	previousState := datamodel.LifeCycleStateREADY
	previousStateDetails := datamodel.LifeCycleStateReadyDetails

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
		LifeCycleState:        datamodel.LifeCycleStateUpdating,
		LifeCycleStateDetails: datamodel.LifeCycleStateUpdatingDetails,
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

func TestBackupPolicyUpdateHandler_Handle_SuccessWithFallbackToReady(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyUpdateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "policy-uuid"},
		AccountID:     sql.NullInt64{Int64: testAccountID, Valid: true},
	}

	backupPolicy := &datamodel.BackupPolicy{
		BaseModel:             datamodel.BaseModel{UUID: "policy-uuid"},
		LifeCycleState:        datamodel.LifeCycleStateUpdating,
		LifeCycleStateDetails: datamodel.LifeCycleStateUpdatingDetails,
	}
	storage.EXPECT().GetBackupPolicyByUUIDAndOwnerID(mock.Anything, "policy-uuid", testAccountID).Return(backupPolicy, nil).Once()
	storage.EXPECT().UpdateBackupPolicy(mock.Anything, "policy-uuid", mock.MatchedBy(func(updates map[string]interface{}) bool {
		s, _ := updates["life_cycle_state"].(string)
		d, _ := updates["life_cycle_state_details"].(string)
		return s == datamodel.LifeCycleStateREADY && d == datamodel.LifeCycleStateAvailableDetails
	})).Return(backupPolicy, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestBackupPolicyUpdateHandler_Handle_UpdateBackupPolicyError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewBackupPolicyUpdateHandler()

	previousState := datamodel.LifeCycleStateREADY
	previousStateDetails := datamodel.LifeCycleStateReadyDetails

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
		LifeCycleState:        datamodel.LifeCycleStateUpdating,
		LifeCycleStateDetails: datamodel.LifeCycleStateUpdatingDetails,
	}
	expectedErr := errors.New("update failed")
	storage.EXPECT().GetBackupPolicyByUUIDAndOwnerID(mock.Anything, "policy-uuid", testAccountID).Return(backupPolicy, nil).Once()
	storage.EXPECT().UpdateBackupPolicy(mock.Anything, "policy-uuid", mock.Anything).
		Return((*datamodel.BackupPolicy)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "revert backup policy")
}
