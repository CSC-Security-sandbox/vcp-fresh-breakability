package replicationActivities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestReverseVolumeReplication(t *testing.T) {
	t.Run("WhenGetProviderByNodeError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := InternalVolumeReplicationReverseActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID:          "ext-uuid",
				SourceVolumeName:      "src-vol",
				SourceSvmName:         "src-svm",
				DestinationVolumeName: "dest-vol",
				DestinationSvmName:    "dest-svm",
			},
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "vol-ext-uuid",
				},
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}

		_, err := activity.ReverseVolumeReplication(ctx, replication, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "provider error")
	})

	t.Run("WhenReverseVolumeReplicationError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)
		activity := InternalVolumeReplicationReverseActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID:          "ext-uuid",
				SourceVolumeName:      "src-vol",
				SourceSvmName:         "src-svm",
				DestinationVolumeName: "dest-vol",
				DestinationSvmName:    "dest-svm",
			},
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "vol-ext-uuid",
				},
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("ReverseVolumeReplication", mock.AnythingOfType("*vsa.VolumeReplication")).Return(nil, errors.New("reverse error"))

		_, err := activity.ReverseVolumeReplication(ctx, replication, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "reverse error")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenReverseVolumeReplicationConflictError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)
		activity := InternalVolumeReplicationReverseActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID:          "ext-uuid",
				SourceVolumeName:      "src-vol",
				SourceSvmName:         "src-svm",
				DestinationVolumeName: "dest-vol",
				DestinationSvmName:    "dest-svm",
			},
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "vol-ext-uuid",
				},
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		conflictError := errors.NewConflictErr("conflict error")
		mockProvider.On("ReverseVolumeReplication", mock.AnythingOfType("*vsa.VolumeReplication")).Return(nil, conflictError)

		_, err := activity.ReverseVolumeReplication(ctx, replication, node)

		assert.Error(tt, err)
		assert.True(tt, errors.IsNonRetryableErr(err))
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)
		activity := InternalVolumeReplicationReverseActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID:          "ext-uuid",
				SourceVolumeName:      "src-vol",
				SourceSvmName:         "src-svm",
				DestinationVolumeName: "dest-vol",
				DestinationSvmName:    "dest-svm",
			},
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "vol-ext-uuid",
				},
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		expectedResponse := &vsa.SnapmirrorDestination{
			RelationshipUUID: "snapmirror-uuid",
		}

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("ReverseVolumeReplication", mock.AnythingOfType("*vsa.VolumeReplication")).Return(expectedResponse, nil)

		res, err := activity.ReverseVolumeReplication(ctx, replication, node)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedResponse, res)
		mockProvider.AssertExpectations(tt)
	})
}

func TestUpdateVolumeReplicationReverseDetails(t *testing.T) {
	t.Run("WhenUpdateVolumeReplicationError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := InternalVolumeReplicationReverseActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "repl-uuid",
			},
			Name: "test-replication",
			ReplicationAttributes: &datamodel.ReplicationDetails{
				SourcePoolUUID:             "src-pool-uuid",
				SourceVolumeUUID:           "src-volume-uuid",
				SourceLocation:             "src-location",
				SourceHostName:             "src-hostname",
				SourceReplicationUUID:      "src-repl-uuid",
				SourceSvmName:              "src-svm",
				SourceVolumeName:           "src-volume",
				DestinationPoolUUID:        "dest-pool-uuid",
				DestinationVolumeUUID:      "dest-volume-uuid",
				DestinationLocation:        "dest-location",
				DestinationHostName:        "dest-hostname",
				DestinationReplicationUUID: "dest-repl-uuid",
				DestinationSvmName:         "dest-svm",
				DestinationVolumeName:      "dest-volume",
				EndpointType:               "dest",
				ExternalUUID:               "ext-uuid",
			},
		}

		mockStorage.On("UpdateVolumeReplicationFields", ctx, "repl-uuid", mock.AnythingOfType("map[string]interface {}")).Return(errors.New("database error"))

		err := activity.UpdateVolumeReplicationReverseDetails(ctx, replication)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "database error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := InternalVolumeReplicationReverseActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "repl-uuid",
			},
			Name: "test-replication",
			ReplicationAttributes: &datamodel.ReplicationDetails{
				SourcePoolUUID:             "src-pool-uuid",
				SourceVolumeUUID:           "src-volume-uuid",
				SourceLocation:             "src-location",
				SourceHostName:             "src-hostname",
				SourceReplicationUUID:      "src-repl-uuid",
				SourceSvmName:              "src-svm",
				SourceVolumeName:           "src-volume",
				DestinationPoolUUID:        "dest-pool-uuid",
				DestinationVolumeUUID:      "dest-volume-uuid",
				DestinationLocation:        "dest-location",
				DestinationHostName:        "dest-hostname",
				DestinationReplicationUUID: "dest-repl-uuid",
				DestinationSvmName:         "dest-svm",
				DestinationVolumeName:      "dest-volume",
				EndpointType:               "dest",
				ExternalUUID:               "ext-uuid",
			},
		}

		// Mock successful database update
		mockStorage.On("UpdateVolumeReplicationFields", ctx, "repl-uuid", mock.AnythingOfType("map[string]interface {}")).Return(nil)

		err := activity.UpdateVolumeReplicationReverseDetails(ctx, replication)

		assert.NoError(tt, err)

		// Verify that source and destination fields were swapped
		assert.Equal(tt, "dest-pool-uuid", replication.ReplicationAttributes.SourcePoolUUID)
		assert.Equal(tt, "dest-volume-uuid", replication.ReplicationAttributes.SourceVolumeUUID)
		assert.Equal(tt, "dest-location", replication.ReplicationAttributes.SourceLocation)
		assert.Equal(tt, "dest-hostname", replication.ReplicationAttributes.SourceHostName)
		assert.Equal(tt, "dest-repl-uuid", replication.ReplicationAttributes.SourceReplicationUUID)
		assert.Equal(tt, "dest-svm", replication.ReplicationAttributes.SourceSvmName)
		assert.Equal(tt, "dest-volume", replication.ReplicationAttributes.SourceVolumeName)

		assert.Equal(tt, "src-pool-uuid", replication.ReplicationAttributes.DestinationPoolUUID)
		assert.Equal(tt, "src-volume-uuid", replication.ReplicationAttributes.DestinationVolumeUUID)
		assert.Equal(tt, "src-location", replication.ReplicationAttributes.DestinationLocation)
		assert.Equal(tt, "src-hostname", replication.ReplicationAttributes.DestinationHostName)
		assert.Equal(tt, "src-repl-uuid", replication.ReplicationAttributes.DestinationReplicationUUID)
		assert.Equal(tt, "src-svm", replication.ReplicationAttributes.DestinationSvmName)
		assert.Equal(tt, "src-volume", replication.ReplicationAttributes.DestinationVolumeName)

		// Verify that endpoint type was set to "src" and external UUID was cleared
		assert.Equal(tt, "src", replication.ReplicationAttributes.EndpointType)
		assert.Equal(tt, "", replication.ReplicationAttributes.ExternalUUID)

		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenReplicationAttributesIsNil", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := InternalVolumeReplicationReverseActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "repl-uuid",
			},
			Name:                  "test-replication",
			ReplicationAttributes: nil,
		}

		// Mock successful database update
		mockStorage.On("UpdateVolumeReplicationFields", ctx, "repl-uuid", mock.AnythingOfType("map[string]interface {}")).Return(nil)

		err := activity.UpdateVolumeReplicationReverseDetails(ctx, replication)

		assert.NoError(tt, err)
		// Since ReplicationAttributes is nil, no swapping should occur
		assert.Nil(tt, replication.ReplicationAttributes)
		mockStorage.AssertExpectations(tt)
	})
}

func TestUpdateVolumeTypeForReverse(t *testing.T) {
	t.Run("WhenUpdateVolumeFieldsError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := InternalVolumeReplicationReverseActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: "dest-vol-uuid",
			},
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					IsDataProtection: true,
				},
			},
		}

		mockStorage.On("UpdateVolumeFields", ctx, "dest-vol-uuid", mock.AnythingOfType("map[string]interface {}")).Return(errors.New("update error"))

		err := activity.UpdateVolumeTypeForReverse(ctx, replication)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "update error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := InternalVolumeReplicationReverseActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: "dest-vol-uuid",
			},
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					IsDataProtection: true,
				},
			},
		}

		mockStorage.On("UpdateVolumeFields", ctx, "dest-vol-uuid", mock.AnythingOfType("map[string]interface {}")).Return(nil)

		err := activity.UpdateVolumeTypeForReverse(ctx, replication)

		assert.NoError(tt, err)
		assert.False(tt, replication.Volume.VolumeAttributes.IsDataProtection)
		mockStorage.AssertExpectations(tt)
	})
}
