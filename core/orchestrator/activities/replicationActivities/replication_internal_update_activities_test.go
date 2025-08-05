package replicationActivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestUpdateVolumeReplicationInternal_WhenReplicationScheduleNil_ReturnsNil(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

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
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

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
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

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
