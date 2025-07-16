package replicationActivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestDeleteVolumeReplicationInternal(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		mockProvider.On("DeleteVolumeReplication", mock.Anything).Return(nil, errors.New("provider error"))
		_, err := activity.DeleteVolumeReplication(ctx, params, node)
		assert.Error(t, err)
		assert.Equal(t, "provider error", err.Error())
		mockProvider.AssertExpectations(t)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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

func TestUpdateVolumeReplicationDetailsForDelete(t *testing.T) {
	t.Run("WhenDeleteSucceeds", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := InternalVolumeReplicationDeleteActivity{
			SE: mockStorage,
		}
		ctx := context.Background()
		replication := &datamodel.VolumeReplication{}
		mockStorage.On("DeleteVolumeReplication", ctx, replication).Return(replication, nil)
		err := activity.UpdateVolumeReplicationDetailsForDelete(ctx, replication)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("WhenDeleteFails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := InternalVolumeReplicationDeleteActivity{
			SE: mockStorage,
		}
		ctx := context.Background()
		replication := &datamodel.VolumeReplication{}
		mockStorage.On("DeleteVolumeReplication", ctx, replication).Return(nil, errors.New("delete error"))
		err := activity.UpdateVolumeReplicationDetailsForDelete(ctx, replication)
		assert.Error(t, err)
		assert.Equal(t, "delete error", err.Error())
		mockStorage.AssertExpectations(t)
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

	mockStorage.On("UpdateVolumeReplicationStates", ctx, volumeRep).Return(errors.New("db error"))

	err := activity.UpdateReplicationStateInDBForDelete(ctx, volumeRep)
	assert.Error(t, err)
	assert.Equal(t, "db error", err.Error())
	mockStorage.AssertExpectations(t)
}
