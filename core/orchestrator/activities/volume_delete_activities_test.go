package activities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestDeleteVolume_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := VolumeDeleteActivity{
		SE: mockStorage,
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeID := "test-volume-id"
	expectedVolume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volumeID}}

	mockStorage.On("DeleteVolume", ctx, volumeID).Return(expectedVolume, nil)

	// Act
	err := activity.DeleteVolume(ctx, expectedVolume)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteVolume_Success_VolumeAlreadyDeleted(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := VolumeDeleteActivity{
		SE: mockStorage,
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeID := "test-volume-id"
	expectedVolume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volumeID}}

	mockStorage.On("DeleteVolume", ctx, volumeID).Return(nil, utilErrors.NewNotFoundErr("volume", nil))

	// Act
	err := activity.DeleteVolume(ctx, expectedVolume)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteVolume_Failure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeID := "test-volume-id"
	expectedError := errors.New("volume not found")

	mockStorage.On("DeleteVolume", ctx, volumeID).Return(nil, expectedError)

	// Act
	err := activity.DeleteVolume(ctx, &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volumeID}})

	// Assert
	assert.Error(t, err)
	assert.EqualError(t, err, expectedError.Error())
	mockStorage.AssertExpectations(t)
}

func TestDeleteVolumeInONTAP_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	GetProviderByNode = func(ctx context.Context, node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := VolumeDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeExternalUUID := "uuid-123"
	volumeName := "test-volume"

	node := &models.Node{}

	// Mock the DeleteVolume method
	mockProvider.On("DeleteVolume", volumeExternalUUID, volumeName).Return(nil)

	// Act
	err := activity.DeleteVolumeInONTAP(ctx, volumeExternalUUID, volumeName, node)

	// Assert
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestDeleteVolumeInONTAP_Failure(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	GetProviderByNode = func(ctx context.Context, node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := VolumeDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeExternalUUID := "uuid-123"
	volumeName := "test-volume"

	node := &models.Node{}
	expectedError := errors.New("failed to delete volume in ONTAP")

	// Mock the DeleteVolume method
	mockProvider.On("DeleteVolume", volumeExternalUUID, volumeName).Return(expectedError)

	// Act
	err := activity.DeleteVolumeInONTAP(ctx, volumeExternalUUID, volumeName, node)

	// Assert
	assert.Error(t, err)
	assert.EqualError(t, err, expectedError.Error())
	mockProvider.AssertExpectations(t)
}
