package replicationActivities

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestUpdateVolumeReplicationInternal_WhenReplicationScheduleNil_ReturnsNil(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()

	activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := InternalVolumeReplicationUpdateActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	node := &models.Node{}

	params := &common.UpdateVolumeReplicationInternalParams{
		ReplicationSchedule: nil,
	}

	res, err := activity.UpdateVolumeReplicationOntap(ctx, params, node, "volume-external-uuid")

	assert.NoError(t, err)
	assert.Nil(t, res)
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeReplicationInternal_WhenProviderError_ReturnsError(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()

	activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	activity := InternalVolumeReplicationUpdateActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	node := &models.Node{}

	params := &common.UpdateVolumeReplicationInternalParams{
		ReplicationSchedule: nillable.GetStringPtr("error"),
	}

	mockProvider.On("UpdateVolumeReplication", mock.Anything).Return(nil, errors.New("provider error"))

	res, err := activity.UpdateVolumeReplicationOntap(ctx, params, node, "volume-external-uuid")

	assert.Error(t, err)
	assert.Nil(t, res)
	assert.Equal(t, "provider error", err.Error())
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeReplicationInternal_WhenSuccess_ReturnsUpdatedReplication(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := vsa.GetProviderByNode
	defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()

	activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	activity := InternalVolumeReplicationUpdateActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	node := &models.Node{}

	params := &common.UpdateVolumeReplicationInternalParams{
		ReplicationSchedule: nillable.GetStringPtr("daily"),
	}

	expectedResponse := &vsa.VolumeReplication{
		RelationshipID:      "volume-external-uuid",
		ReplicationSchedule: "daily",
	}

	mockProvider.On("UpdateVolumeReplication", mock.Anything).Return(expectedResponse, nil)

	res, err := activity.UpdateVolumeReplicationOntap(ctx, params, node, "volume-external-uuid")

	assert.NoError(t, err)
	assert.Equal(t, expectedResponse, res)
	mockProvider.AssertExpectations(t)
}

func TestUpdateClusterPeeringClusterLocation(t *testing.T) {
	t.Run("WhenClusterLocationIsNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := InternalVolumeReplicationUpdateActivity{SE: mockStorage}
		params := &common.UpdateVolumeReplicationInternalParams{
			ClusterLocation: nil,
		}
		replication := &datamodel.VolumeReplication{
			ClusterPeerId: sql.NullInt64{Int64: 1, Valid: true},
		}
		err := activity.UpdateClusterPeeringClusterLocation(ctx, params, replication)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenClusterPeerIdIsNotValid", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := InternalVolumeReplicationUpdateActivity{SE: mockStorage}
		clusterLocation := "us-west1"
		params := &common.UpdateVolumeReplicationInternalParams{
			ClusterLocation: &clusterLocation,
		}
		replication := &datamodel.VolumeReplication{
			ClusterPeerId: sql.NullInt64{Valid: false},
		}
		err := activity.UpdateClusterPeeringClusterLocation(ctx, params, replication)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenClusterPeerIsLoaded", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := InternalVolumeReplicationUpdateActivity{SE: mockStorage}
		clusterLocation := "us-west1"
		params := &common.UpdateVolumeReplicationInternalParams{
			ClusterLocation: &clusterLocation,
		}
		clusterPeer := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{ID: 1},
			ClusterPeeringAttributes: &datamodel.ClusterPeeringAttributes{
				PassPhrase: nillable.GetStringPtr("passphrase"),
			},
		}
		replication := &datamodel.VolumeReplication{
			ClusterPeerId: sql.NullInt64{Int64: 1, Valid: true},
			ClusterPeer:   clusterPeer,
		}
		mockStorage.On("UpdateClusterPeeringRow", ctx, mock.MatchedBy(func(cp *datamodel.ClusterPeerings) bool {
			return cp.ID == 1 && cp.ClusterPeeringAttributes != nil && cp.ClusterPeeringAttributes.ClusterLocation != nil && *cp.ClusterPeeringAttributes.ClusterLocation == clusterLocation
		})).Return(nil)
		err := activity.UpdateClusterPeeringClusterLocation(ctx, params, replication)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenUpdateClusterPeeringRowFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := InternalVolumeReplicationUpdateActivity{SE: mockStorage}
		clusterLocation := "us-west1"
		params := &common.UpdateVolumeReplicationInternalParams{
			ClusterLocation: &clusterLocation,
		}
		replication := &datamodel.VolumeReplication{
			ClusterPeerId: sql.NullInt64{Int64: 1, Valid: true},
			ClusterPeer:   &datamodel.ClusterPeerings{},
		}
		expectedError := errors.New("database error")
		mockStorage.On("UpdateClusterPeeringRow", ctx, mock.Anything).Return(expectedError)
		err := activity.UpdateClusterPeeringClusterLocation(ctx, params, replication)
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}
