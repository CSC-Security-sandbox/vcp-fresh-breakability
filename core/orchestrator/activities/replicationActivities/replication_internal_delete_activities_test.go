package replicationActivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestDeleteVolumeReplicationInternal(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := InternalVolumeReplicationDeleteActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}

		// Prepare test data
		params := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				EndpointType:          "type",
				SourceHostName:        "src-host",
				SourceSvmName:         "src-svm",
				SourceVolumeName:      "src-vol",
				DestinationHostName:   "dst-host",
				DestinationSvmName:    "dst-svm",
				ReplicationSchedule:   "schedule",
				DestinationVolumeName: "dst-vol",
				ExternalUUID:          "uuid",
			},
		}
		mockProvider.On("DeleteVolumeReplication", mock.Anything).Return(nil, errors.New("database error"))
		_, err := activity.DeleteVolumeReplication(ctx, params, node)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr))
		assert.Equal(tt, "database error", customErr.OriginalErr.Error())
		mockProvider.AssertExpectations(t)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := InternalVolumeReplicationDeleteActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}

		// Prepare test data
		params := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				EndpointType:          "type",
				SourceHostName:        "src-host",
				SourceSvmName:         "src-svm",
				SourceVolumeName:      "src-vol",
				DestinationHostName:   "dst-host",
				DestinationSvmName:    "dst-svm",
				ReplicationSchedule:   "schedule",
				DestinationVolumeName: "dst-vol",
				ExternalUUID:          "uuid",
			},
		}

		expectedResponse := &vsa.VolumeReplication{}
		mockProvider.On("DeleteVolumeReplication", mock.Anything).Return(expectedResponse, nil)

		res, err := activity.DeleteVolumeReplication(ctx, params, node)

		assert.NoError(t, err)
		assert.NotNil(t, res)
		mockProvider.AssertExpectations(t)
	})
}

func TestUpdateReplicationStateInDB(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := InternalVolumeReplicationDeleteActivity{
		SE: mockStorage,
	}
	ctx := context.Background()
	volumeRep := &datamodel.VolumeReplication{}

	mockStorage.On("UpdateVolumeReplicationStates", ctx, volumeRep).Return(nil)

	err := activity.UpdateReplicationStateInDBForDelete(ctx, volumeRep)
	assert.NoError(t, err)
	assert.Equal(t, models.LifeCycleStateError, volumeRep.State)
	assert.Equal(t, models.LifeCycleStateDeletionErrorDetails, volumeRep.StateDetails)
	mockStorage.AssertExpectations(t)
}

func TestUpdateReplicationStateInDB_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := InternalVolumeReplicationDeleteActivity{
		SE: mockStorage,
	}
	ctx := context.Background()
	volumeRep := &datamodel.VolumeReplication{}

	mockStorage.On("UpdateVolumeReplicationStates", ctx, volumeRep).Return(errors.New("database error"))

	err := activity.UpdateReplicationStateInDBForDelete(ctx, volumeRep)
	var customErr *vsaerrors.CustomError
	assert.True(t, vsaerrors.As(err, &customErr))
	assert.Equal(t, "database error", customErr.OriginalErr.Error())
	mockStorage.AssertExpectations(t)
}

func TestCleanupReplicationAfterReverse(t *testing.T) {
	t.Run("WhenProviderReturnsError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := InternalVolumeReplicationDeleteActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), "logger", struct{}{})
		node := &models.Node{}
		params := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				EndpointType:          "type",
				SourceHostName:        "src-host",
				SourceSvmName:         "src-svm",
				SourceVolumeName:      "src-vol",
				DestinationHostName:   "dst-host",
				DestinationSvmName:    "dst-svm",
				ReplicationSchedule:   "schedule",
				DestinationVolumeName: "dst-vol",
				ExternalUUID:          "uuid",
			},
		}
		mockProvider.On("DeleteVolumeReplication", mock.Anything).Return(nil, errors.New("provider error"))
		_, err := activity.CleanupReplicationAfterReverse(ctx, params, node)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr))
		assert.Equal(tt, "provider error", customErr.OriginalErr.Error())
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenProviderReturnsSuccess", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := InternalVolumeReplicationDeleteActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), "logger", struct{}{})
		node := &models.Node{}
		params := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				EndpointType:          "type",
				SourceHostName:        "src-host",
				SourceSvmName:         "src-svm",
				SourceVolumeName:      "src-vol",
				DestinationHostName:   "dst-host",
				DestinationSvmName:    "dst-svm",
				ReplicationSchedule:   "schedule",
				DestinationVolumeName: "dst-vol",
				ExternalUUID:          "uuid",
			},
		}
		expectedResponse := &vsa.VolumeReplication{}
		mockProvider.On("DeleteVolumeReplication", mock.Anything).Return(expectedResponse, nil)
		res, err := activity.CleanupReplicationAfterReverse(ctx, params, node)
		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		mockProvider.AssertExpectations(tt)
	})
}
