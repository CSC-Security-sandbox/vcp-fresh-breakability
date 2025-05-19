package activities_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestDeleteVolume_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeDeleteActivity{
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

func TestDeleteVolume_Failure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeDeleteActivity{SE: mockStorage}
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
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := activities.VolumeDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
	}
	node := &models.Node{}

	// Mock the DeleteVolume method
	mockProvider.On("DeleteVolume", volume.VolumeAttributes.ExternalUUID, volume.Name).Return(nil)

	// Act
	err := activity.DeleteVolumeInONTAP(ctx, volume, node)

	// Assert
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestDeleteVolumeInONTAP_Failure(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := activities.VolumeDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
	}
	node := &models.Node{}
	expectedError := errors.New("failed to delete volume in ONTAP")

	// Mock the DeleteVolume method
	mockProvider.On("DeleteVolume", volume.VolumeAttributes.ExternalUUID, volume.Name).Return(expectedError)

	// Act
	err := activity.DeleteVolumeInONTAP(ctx, volume, node)

	// Assert
	assert.Error(t, err)
	assert.EqualError(t, err, expectedError.Error())
	mockProvider.AssertExpectations(t)
}
