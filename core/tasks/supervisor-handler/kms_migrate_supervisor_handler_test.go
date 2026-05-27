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

func TestKmsMigrateHandler_JobTypes(t *testing.T) {
	handler := NewKmsMigrateHandler()
	jobTypes := handler.JobTypes()

	require.Len(t, jobTypes, 1)
	require.Contains(t, jobTypes, models.JobTypeMigrateKmsConfig)
}

func TestNewKmsMigrateHandler(t *testing.T) {
	handler := NewKmsMigrateHandler()
	require.NotNil(t, handler)
	require.IsType(t, &KmsMigrateHandler{}, handler)
}

func TestKmsMigrateHandler_Handle_SkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsMigrateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "kms-uuid"},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetKmsConfig", mock.Anything, mock.Anything)
}

func TestKmsMigrateHandler_Handle_SkipsMissingResourceUUID(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsMigrateHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetKmsConfig", mock.Anything, mock.Anything)
}

func TestKmsMigrateHandler_Handle_SkipsNilJobAttributes(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsMigrateHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{JobAttributes: nil}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetKmsConfig", mock.Anything, mock.Anything)
}

func TestKmsMigrateHandler_Handle_KmsConfigNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsMigrateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "kms-uuid"},
	}

	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return((*datamodel.KmsConfig)(nil), vsaerrors.NewNotFoundErr("kms", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateKmsConfigState", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestKmsMigrateHandler_Handle_GetKmsConfigError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsMigrateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "kms-uuid"},
	}

	expectedErr := errors.New("database error")
	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return((*datamodel.KmsConfig)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "load KMS config for migrate cleanup")
}

func TestKmsMigrateHandler_Handle_SkipsNonMigratingState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsMigrateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "kms-uuid"},
	}

	kmsConfig := &datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
		State:     models.LifeCycleStateREADY,
	}
	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return(kmsConfig, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateKmsConfigState", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestKmsMigrateHandler_Handle_SuccessWithPreviousState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsMigrateHandler()

	previousState := models.LifeCycleStateREADY
	previousStateDetails := models.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "kms-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	kmsConfig := &datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
		State:     models.LifeCycleStateMigrating,
	}
	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return(kmsConfig, nil).Once()
	storage.EXPECT().UpdateKmsConfigState(mock.Anything, "kms-uuid", previousState, previousStateDetails).Return(kmsConfig, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestKmsMigrateHandler_Handle_SuccessWithPreviousStateInUse(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsMigrateHandler()

	previousState := models.LifeCycleStateInUse
	previousStateDetails := models.LifeCycleStateInUseDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "kms-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	kmsConfig := &datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
		State:     models.LifeCycleStateMigrating,
	}
	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return(kmsConfig, nil).Once()
	storage.EXPECT().UpdateKmsConfigState(mock.Anything, "kms-uuid", previousState, previousStateDetails).Return(kmsConfig, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestKmsMigrateHandler_Handle_SuccessWithFallbackToReady(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsMigrateHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "kms-uuid",
			// PreviousState is empty, should fallback to READY
		},
	}

	kmsConfig := &datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
		State:     models.LifeCycleStateMigrating,
	}
	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return(kmsConfig, nil).Once()
	storage.EXPECT().UpdateKmsConfigState(mock.Anything, "kms-uuid", models.LifeCycleStateREADY, models.LifeCycleStateReadyDetails).Return(kmsConfig, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestKmsMigrateHandler_Handle_UpdateKmsConfigStateError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsMigrateHandler()

	previousState := models.LifeCycleStateREADY
	previousStateDetails := models.LifeCycleStateReadyDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "kms-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	kmsConfig := &datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
		State:     models.LifeCycleStateMigrating,
	}
	expectedErr := errors.New("update failed")
	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return(kmsConfig, nil).Once()
	storage.EXPECT().UpdateKmsConfigState(mock.Anything, "kms-uuid", previousState, previousStateDetails).Return((*datamodel.KmsConfig)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "revert KMS config")
}

func TestKmsMigrateHandler_Handle_WithPreviousStateDetails(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsMigrateHandler()

	previousState := models.LifeCycleStateREADY
	previousStateDetails := "Custom state details"

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "kms-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	kmsConfig := &datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
		State:     models.LifeCycleStateMigrating,
	}
	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return(kmsConfig, nil).Once()
	storage.EXPECT().UpdateKmsConfigState(mock.Anything, "kms-uuid", previousState, previousStateDetails).Return(kmsConfig, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestKmsMigrateHandler_Handle_CompleteSuccessPath(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsMigrateHandler()

	// Test the complete success path including the final logger.Infof call
	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "test-job-uuid",
		},
		Type: string(models.JobTypeMigrateKmsConfig),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "kms-uuid",
			PreviousState:        models.LifeCycleStateREADY,
			PreviousStateDetails: models.LifeCycleStateReadyDetails,
		},
	}

	kmsConfig := &datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
		State:     models.LifeCycleStateMigrating,
	}
	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return(kmsConfig, nil).Once()
	storage.EXPECT().UpdateKmsConfigState(mock.Anything, "kms-uuid", models.LifeCycleStateREADY, models.LifeCycleStateReadyDetails).Return(kmsConfig, nil).Once()

	// This should execute all code including logger initialization and final logger.Infof
	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}
