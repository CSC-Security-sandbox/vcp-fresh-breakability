package activities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestVolumeRevertActivity_RevertVolume_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)

	// Save original function and restore after test
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	// Mock GetProviderByNode to return the mock provider
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeRevertActivity{
		SE: mockStorage,
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "volume-uuid",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
		},
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
	}

	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "snapshot-uuid",
		},
		Name: "test-snapshot",
		SnapshotAttributes: &datamodel.SnapshotAttributes{
			ExternalUUID: "external-snapshot-uuid",
		},
	}

	node := &models.Node{
		Name: "test-node",
	}

	params := vsa.RevertVolumeParams{
		VolumeID:   "external-volume-uuid",
		SnapshotID: "external-snapshot-uuid",
	}

	// Setup mocks
	mockProvider.On("RevertVolume", vsa.RevertVolumeParams{
		VolumeID:        "external-volume-uuid",
		SnapshotID:      "external-snapshot-uuid",
		SnapshotName:    "test-snapshot",
		SvmName:         "test-svm",
		PreRevertVolume: volume,
	}).Return(nil)

	mockStorage.On("RevertedVolume", ctx, volume, snapshot).Return(nil)

	// Act
	err := activity.RevertVolume(ctx, volume, snapshot, node, params)

	// Assert
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
	mockStorage.AssertExpectations(t)
}

func TestVolumeRevertActivity_RevertVolume_GetProviderByNodeFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)

	// Save original function and restore after test
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	// Mock GetProviderByNode to return error
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, errors.New("failed to get provider")
	}

	activity := VolumeRevertActivity{
		SE: mockStorage,
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "volume-uuid",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
		},
	}

	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "snapshot-uuid",
		},
		Name: "test-snapshot",
		SnapshotAttributes: &datamodel.SnapshotAttributes{
			ExternalUUID: "external-snapshot-uuid",
		},
	}

	node := &models.Node{
		Name: "test-node",
	}

	params := vsa.RevertVolumeParams{
		VolumeID:   "external-volume-uuid",
		SnapshotID: "external-snapshot-uuid",
	}

	// Act
	err := activity.RevertVolume(ctx, volume, snapshot, node, params)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get provider")
}

func TestVolumeRevertActivity_RevertVolume_RevertVolumeFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)

	// Save original function and restore after test
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	// Mock GetProviderByNode to return the mock provider
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeRevertActivity{
		SE: mockStorage,
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "volume-uuid",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
		},
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
	}

	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "snapshot-uuid",
		},
		Name: "test-snapshot",
		SnapshotAttributes: &datamodel.SnapshotAttributes{
			ExternalUUID: "external-snapshot-uuid",
		},
	}

	node := &models.Node{
		Name: "test-node",
	}

	params := vsa.RevertVolumeParams{
		VolumeID:   "external-volume-uuid",
		SnapshotID: "external-snapshot-uuid",
	}

	expectedError := errors.New("volume revert failed")

	// Setup mocks
	mockProvider.On("RevertVolume", vsa.RevertVolumeParams{
		VolumeID:        "external-volume-uuid",
		SnapshotID:      "external-snapshot-uuid",
		SnapshotName:    "test-snapshot",
		SvmName:         "test-svm",
		PreRevertVolume: volume,
	}).Return(expectedError)

	// Act
	err := activity.RevertVolume(ctx, volume, snapshot, node, params)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
	mockProvider.AssertExpectations(t)
}

func TestVolumeRevertActivity_RevertVolume_RevertedVolumeFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)

	// Save original function and restore after test
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	// Mock GetProviderByNode to return the mock provider
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeRevertActivity{
		SE: mockStorage,
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "volume-uuid",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
		},
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
	}

	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "snapshot-uuid",
		},
		Name: "test-snapshot",
		SnapshotAttributes: &datamodel.SnapshotAttributes{
			ExternalUUID: "external-snapshot-uuid",
		},
	}

	node := &models.Node{
		Name: "test-node",
	}

	params := vsa.RevertVolumeParams{
		VolumeID:   "external-volume-uuid",
		SnapshotID: "external-snapshot-uuid",
	}

	expectedError := errors.New("failed to update volume in database")

	// Setup mocks
	mockProvider.On("RevertVolume", vsa.RevertVolumeParams{
		VolumeID:        "external-volume-uuid",
		SnapshotID:      "external-snapshot-uuid",
		SnapshotName:    "test-snapshot",
		SvmName:         "test-svm",
		PreRevertVolume: volume,
	}).Return(nil)

	mockStorage.On("RevertedVolume", ctx, volume, snapshot).Return(expectedError)

	// Act
	err := activity.RevertVolume(ctx, volume, snapshot, node, params)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
	mockProvider.AssertExpectations(t)
	mockStorage.AssertExpectations(t)
}
