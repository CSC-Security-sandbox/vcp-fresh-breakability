package replicationActivities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestDeleteVolumeReplicationRow(t *testing.T) {
	t.Run("WhenDeleteSucceeds", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := InternalVolumeReplicationRowDeleteActivity{
			SE: mockStorage,
		}
		ctx := context.Background()
		replication := &datamodel.VolumeReplication{}
		mockStorage.On("DeleteVolumeReplication", ctx, replication).Return(replication, nil)
		err := activity.DeleteVolumeReplicationRow(ctx, replication)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("WhenDeleteFails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := InternalVolumeReplicationRowDeleteActivity{
			SE: mockStorage,
		}
		ctx := context.Background()
		replication := &datamodel.VolumeReplication{}
		mockStorage.On("DeleteVolumeReplication", ctx, replication).Return(nil, errors.New("delete error"))
		err := activity.DeleteVolumeReplicationRow(ctx, replication)
		assert.Error(t, err)
		assert.Equal(t, "delete error", err.Error())
		mockStorage.AssertExpectations(t)
	})
}

func TestUpdateReplicationStateInDB(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := InternalVolumeReplicationRowDeleteActivity{
		SE: mockStorage,
	}
	ctx := context.Background()
	volumeRep := &datamodel.VolumeReplication{}

	mockStorage.On("UpdateVolumeReplicationStates", ctx, volumeRep).Return(nil)

	err := activity.UpdateReplicationStateInDB(ctx, volumeRep)
	assert.NoError(t, err)
	assert.Equal(t, models.LifeCycleStateError, volumeRep.State)
	assert.Equal(t, models.LifeCycleStateCreationErrorDetails, volumeRep.StateDetails)
	mockStorage.AssertExpectations(t)
}

func TestUpdateReplicationStateInDB_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := InternalVolumeReplicationRowDeleteActivity{
		SE: mockStorage,
	}
	ctx := context.Background()
	volumeRep := &datamodel.VolumeReplication{}

	mockStorage.On("UpdateVolumeReplicationStates", ctx, volumeRep).Return(errors.New("db error"))

	err := activity.UpdateReplicationStateInDB(ctx, volumeRep)
	assert.Error(t, err)
	assert.Equal(t, "db error", err.Error())
	mockStorage.AssertExpectations(t)
}
