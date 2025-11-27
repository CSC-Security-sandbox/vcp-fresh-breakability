package supervisorhandler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestCmekHandlerHandleSkipsNonTimeout(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "kms-uuid"},
	}

	err := handler.Handle(context.Background(), job, Event("OTHER"), storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetKmsConfig", mock.Anything, mock.Anything)
}

func TestCmekHandlerHandleSkipsMissingResource(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	err := handler.Handle(context.Background(), &datamodel.Job{}, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "GetKmsConfig", mock.Anything, mock.Anything)
}

func TestCmekHandlerHandleNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	job := &datamodel.Job{
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "kms-uuid"},
	}

	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return((*datamodel.KmsConfig)(nil), vsaerrors.NewNotFoundErr("kms", nil)).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
	storage.AssertNotCalled(t, "DeleteKmsConfig", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestCmekHandlerHandleSuccess(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := NewCmekHandler()

	kmsConfig := &datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "kms-uuid"},
	}

	job := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "job-uuid"},
		CorrelationID: utils.RandomUUID(),
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: "kms-uuid"},
	}

	storage.EXPECT().GetKmsConfig(mock.Anything, "kms-uuid").Return(kmsConfig, nil).Once()
	storage.EXPECT().DeleteKmsConfig(mock.Anything, "kms-uuid", models.LifeCycleStateError, WorkflowTimeoutDetail).Return(kmsConfig, nil).Once()

	err := handler.Handle(context.Background(), job, EventTimeout, storage)
	require.NoError(t, err)
}
