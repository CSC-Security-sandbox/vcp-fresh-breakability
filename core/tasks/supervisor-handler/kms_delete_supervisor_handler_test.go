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

func TestKmsDeleteHandler_JobTypes(t *testing.T) {
	handler := NewKmsDeleteHandler()
	jobTypes := handler.JobTypes()

	require.Len(t, jobTypes, 1)
	require.Contains(t, jobTypes, datamodel.JobTypeDeleteKmsConfig)
}

func TestKmsDeleteHandler_Handle_SkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "kms-uuid"},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetKmsConfig", mock.Anything, mock.Anything)
}

func TestKmsDeleteHandler_Handle_SkipsMissingResourceUUID(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsDeleteHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetKmsConfig", mock.Anything, mock.Anything)
}

func TestKmsDeleteHandler_Handle_KmsConfigNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "kms-uuid"},
	}

	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return((*datamodel.KmsConfig)(nil), vsaerrors.NewNotFoundErr("kms", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateKmsConfigState", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestKmsDeleteHandler_Handle_GetKmsConfigError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "kms-uuid"},
	}

	expectedErr := errors.New("database error")
	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return((*datamodel.KmsConfig)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "load KMS config for delete cleanup")
}

func TestKmsDeleteHandler_Handle_SkipsNonDeletingState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "kms-uuid"},
	}

	kmsConfig := &datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
		State:     datamodel.LifeCycleStateCreated,
	}
	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return(kmsConfig, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "UpdateKmsConfigState", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestKmsDeleteHandler_Handle_SuccessWithPreviousState(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsDeleteHandler()

	previousState := datamodel.LifeCycleStateCreated
	previousStateDetails := datamodel.LifeCycleStateCreatedDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "kms-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	kmsConfig := &datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return(kmsConfig, nil).Once()
	storage.EXPECT().UpdateKmsConfigState(mock.Anything, "kms-uuid", previousState, previousStateDetails).Return(kmsConfig, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestKmsDeleteHandler_Handle_SuccessWithFallbackToCreated(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsDeleteHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "kms-uuid",
			// PreviousState is empty, should fallback to CREATED
		},
	}

	kmsConfig := &datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return(kmsConfig, nil).Once()
	storage.EXPECT().UpdateKmsConfigState(mock.Anything, "kms-uuid", datamodel.LifeCycleStateCreated, datamodel.LifeCycleStateCreatedDetails).Return(kmsConfig, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}

func TestKmsDeleteHandler_Handle_UpdateKmsConfigStateError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewKmsDeleteHandler()

	previousState := datamodel.LifeCycleStateCreated
	previousStateDetails := datamodel.LifeCycleStateCreatedDetails

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:         "kms-uuid",
			PreviousState:        previousState,
			PreviousStateDetails: previousStateDetails,
		},
	}

	kmsConfig := &datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
		State:     datamodel.LifeCycleStateDeleting,
	}
	expectedErr := errors.New("update failed")
	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return(kmsConfig, nil).Once()
	storage.EXPECT().UpdateKmsConfigState(mock.Anything, "kms-uuid", previousState, previousStateDetails).Return((*datamodel.KmsConfig)(nil), expectedErr).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.Error(t, err)
	require.Contains(t, err.Error(), "revert KMS config")
}

func TestNewKmsDeleteHandler(t *testing.T) {
	handler := NewKmsDeleteHandler()
	require.NotNil(t, handler)
	require.IsType(t, &KmsDeleteHandler{}, handler)
}
