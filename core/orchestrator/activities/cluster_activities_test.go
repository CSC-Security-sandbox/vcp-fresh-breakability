package activities_test

import (
	"context"
	"errors"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"testing"
)

func TestAcceptClusterPeer(t *testing.T) {
	t.Run("AcceptClusterPeerReturnsErrorWhenProviderFails", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
			return mockProvider
		}

		activity := activities.ClusterPeerActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		timex := time.Now()
		mockProvider.On("ListClusterPeers").Return(nil, nil)
		mockProvider.On("AcceptClusterPeer", mock.Anything).Return(nil, errors.New("provider error"))
		_, err := activity.AcceptClusterPeer(ctx, &commonparams.ClusterPeerParams{ExpiryTime: &timex}, node)

		assert.Error(t, err)
		assert.Equal(t, "provider error", err.Error())
		mockProvider.AssertExpectations(t)
	})
	t.Run("AcceptClusterPeerReturnsErrorWhenListClusterPeersFails", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
			return mockProvider
		}

		activity := activities.ClusterPeerActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}

		mockProvider.On("ListClusterPeers").Return(nil, errors.New("list cluster peers error"))
		_, err := activity.AcceptClusterPeer(ctx, &commonparams.ClusterPeerParams{}, node)

		assert.Error(t, err)
		assert.Equal(t, "list cluster peers error", err.Error())
		mockProvider.AssertExpectations(t)
	})
	t.Run("AcceptClusterPeerReturnsErrorWhenNoMatchingIPs", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
			return mockProvider
		}

		activity := activities.ClusterPeerActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		params := &commonparams.ClusterPeerParams{
			PeerName:      "peer1",
			PeerAddresses: []string{"192.168.1.1"},
		}
		expectedResponse := &vsa.ClusterPeer{
			UUID:         "12345",
			ExternalUUID: "12345",
		}

		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{
			{
				PeerClusterName: "peer1",
				PeerAddresses:   []string{"192.168.1.2"},
				Availability:    "Available",
			},
		}, nil)
		mockProvider.On("AcceptClusterPeer", mock.Anything).Return(expectedResponse, nil)
		_, err := activity.AcceptClusterPeer(ctx, params, node)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})
}

func TestCreateClusterPeer(t *testing.T) {
	t.Run("TestCreateClusterPeer_Success", func(t *testing.T) {
		// Arrange
		mockProvider := new(vsa.MockProvider) // Use the mock provider
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

		// Mock GetProviderByNode to return the mock provider
		activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
			return mockProvider
		}

		activity := activities.ClusterPeerActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		expectedResponse := &vsa.ClusterPeer{
			UUID:         "12345",
			ExternalUUID: "12345",
		}

		mockProvider.On("CreateClusterPeer", mock.Anything).Return(expectedResponse, nil)
		_, err := activity.CreateClusterPeer(ctx, &commonparams.ClusterPeerParams{}, node)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})
	t.Run("CreateClusterPeerReturnsErrorWhenProviderFails", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
			return mockProvider
		}

		activity := activities.ClusterPeerActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}

		mockProvider.On("CreateClusterPeer", mock.Anything).Return(nil, errors.New("provider error"))
		_, err := activity.CreateClusterPeer(ctx, &commonparams.ClusterPeerParams{}, node)

		assert.Error(t, err)
		assert.Equal(t, "provider error", err.Error())
		mockProvider.AssertExpectations(t)
	})
}
