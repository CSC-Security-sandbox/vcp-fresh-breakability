package activities_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestCreateSnapshotInONTAP(t *testing.T) {
	t.Run("WhenSnapshotIsCreatedSuccessfully", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
			return mockProvider
		}

		activity := activities.SnapshotCreateActivity{}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		snapshot := &datamodel.Snapshot{
			Name:        "test-snapshot",
			Description: "test-description",
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "volume-uuid",
				},
			},
		}
		node := &models.Node{}
		expectedResponse := &vsa.SnapshotProviderResponse{
			ProviderResponse: vsa.ProviderResponse{
				Name:         "snapshot-uuid",
				ExternalUUID: "abcdef-123456",
			},
			SizeInBytes: 1024,
		}

		mockProvider.On("CreateSnapshot", vsa.CreateSnapshotParams{
			VolumeUUID: "volume-uuid",
			Name:       "test-snapshot",
			Comment:    "test-description",
		}).Return(expectedResponse, nil)

		result, err := activity.CreateSnapshotInONTAP(ctx, snapshot, node)

		assert.NoError(t, err)
		assert.Equal(t, expectedResponse, result)
		mockProvider.AssertExpectations(t)
	})

	t.Run("WhenSnapshotIsCreatedSuccessfully", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
			return mockProvider
		}

		activity := activities.SnapshotCreateActivity{}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		snapshot := &datamodel.Snapshot{
			Name:        "test-snapshot",
			Description: "test-description",
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "volume-uuid",
				},
			},
		}
		node := &models.Node{}
		expectedResponse := &vsa.SnapshotProviderResponse{
			ProviderResponse: vsa.ProviderResponse{
				Name:         "snapshot-uuid",
				ExternalUUID: "abcdef-123456",
			},
			SizeInBytes: 1024,
		}

		mockProvider.On("CreateSnapshot", vsa.CreateSnapshotParams{
			VolumeUUID: "volume-uuid",
			Name:       "test-snapshot",
			Comment:    "test-description",
		}).Return(expectedResponse, nil)

		result, err := activity.CreateSnapshotInONTAP(ctx, snapshot, node)

		assert.NoError(t, err)
		assert.Equal(t, expectedResponse, result)
		mockProvider.AssertExpectations(t)
	})

	t.Run("WhenSnapshotCreationFails", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
			return mockProvider
		}

		activity := activities.SnapshotCreateActivity{}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		snapshot := &datamodel.Snapshot{
			Name:        "test-snapshot",
			Description: "test-description",
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "volume-uuid",
				},
			},
		}
		node := &models.Node{}
		expectedError := errors.New("failed to create snapshot")

		mockProvider.On("CreateSnapshot", vsa.CreateSnapshotParams{
			VolumeUUID: "volume-uuid",
			Name:       "test-snapshot",
			Comment:    "test-description",
		}).Return(nil, expectedError)

		result, err := activity.CreateSnapshotInONTAP(ctx, snapshot, node)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.EqualError(t, err, expectedError.Error())
		mockProvider.AssertExpectations(t)
	})
}

func TestUpdateSnapshotDetails(t *testing.T) {
	t.Run("WhenUpdateIsSuccessful", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.SnapshotCreateActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		snapshot := &datamodel.Snapshot{
			Name:         "test-snapshot",
			Description:  "test-description",
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailableDetails,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				ExternalUUID:           "abcdef-123456",
				SizeInBytes:            1024,
				LogicalSizeUsedInBytes: 1024,
			},
		}

		mockStorage.On("UpdateSnapshot", ctx, snapshot).Return(nil)

		err := activity.UpdateSnapshotDetails(ctx, snapshot, &vsa.SnapshotProviderResponse{
			ProviderResponse: vsa.ProviderResponse{
				ExternalUUID: "abcdef-123456",
				Name:         "test-snapshot",
			},
			SizeInBytes:        1024,
			LogicalSizeInBytes: 1024,
		})

		assert.NoError(t, err)
		assert.Equal(t, "abcdef-123456", snapshot.SnapshotAttributes.ExternalUUID)
		assert.Equal(t, int64(1024), snapshot.SnapshotAttributes.SizeInBytes)
		assert.Equal(t, models.LifeCycleStateREADY, snapshot.State)
		assert.Equal(t, models.LifeCycleStateAvailableDetails, snapshot.StateDetails)
		mockStorage.AssertExpectations(t)
	})

	t.Run("WhenUpdateFails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.SnapshotCreateActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		snapshot := &datamodel.Snapshot{
			SnapshotAttributes: &datamodel.SnapshotAttributes{},
		}
		expectedError := errors.New("failed to update snapshot")

		mockStorage.On("UpdateSnapshot", ctx, snapshot).Return(expectedError)

		err := activity.UpdateSnapshotDetails(ctx, snapshot, &vsa.SnapshotProviderResponse{
			ProviderResponse: vsa.ProviderResponse{
				ExternalUUID: "abcdef-123456",
				Name:         "test-snapshot",
			},
			SizeInBytes:        1024,
			LogicalSizeInBytes: 1024,
		})

		assert.Error(t, err)
		assert.EqualError(t, err, expectedError.Error())
		mockStorage.AssertExpectations(t)
	})

	t.Run("WhenUpdateIsSuccessfulWithError", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.SnapshotCreateActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		snapshot := &datamodel.Snapshot{
			Name:         "test-snapshot",
			Description:  "test-description",
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailableDetails,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				ExternalUUID:           "abcdef-123456",
				SizeInBytes:            1024,
				LogicalSizeUsedInBytes: 1024,
			},
		}

		mockStorage.On("UpdateSnapshot", ctx, snapshot).Return(nil)

		err := activity.UpdateSnapshotDetails(ctx, snapshot, nil)

		assert.NoError(t, err)
		assert.Equal(t, "abcdef-123456", snapshot.SnapshotAttributes.ExternalUUID)
		assert.Equal(t, int64(1024), snapshot.SnapshotAttributes.SizeInBytes)
		assert.Equal(t, models.LifeCycleStateError, snapshot.State)
		assert.Equal(t, models.LifeCycleStateCreationErrorDetails, snapshot.StateDetails)
		mockStorage.AssertExpectations(t)
	})
}
