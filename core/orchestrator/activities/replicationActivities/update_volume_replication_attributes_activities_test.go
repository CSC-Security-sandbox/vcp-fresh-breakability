package replicationActivities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestGetSnapmirrorDetailsFromOntap(t *testing.T) {
	t.Run("WhenVolumeReplicationNotFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := UpdateVolumeReplicationAttributesActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		result := &replication.UpdateVolumeReplicationAttributesResult{
			Event: &replication.UpdateVolumeReplicationAttributesEvent{
				UpdateVolumeReplicationAttributesParams: &models.UpdateVolumeReplicationAttributesParams{
					VolumeReplicationId: "replication-id",
				},
			},
		}

		mockStorage.On("GetVolumeReplication", ctx, "replication-id").Return(nil, errors.New("not found"))

		_, err := activity.GetSnapmirrorDetailsFromOntap(ctx, result)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "An internal error occurred.")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenNodesNotFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := UpdateVolumeReplicationAttributesActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		volReplication := &datamodel.VolumeReplication{
			Volume: &datamodel.Volume{
				PoolID: 1,
			},
		}

		result := &replication.UpdateVolumeReplicationAttributesResult{
			Event: &replication.UpdateVolumeReplicationAttributesEvent{
				UpdateVolumeReplicationAttributesParams: &models.UpdateVolumeReplicationAttributesParams{
					VolumeReplicationId: "replication-id",
				},
			},
		}

		mockStorage.On("GetVolumeReplication", ctx, "replication-id").Return(volReplication, nil)
		mockStorage.On("GetNodesByPoolID", ctx, int64(1)).Return([]*datamodel.Node{}, errors.New("nodes not found"))

		_, err := activity.GetSnapmirrorDetailsFromOntap(ctx, result)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "An internal error occurred.")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetProviderError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := UpdateVolumeReplicationAttributesActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		volReplication := &datamodel.VolumeReplication{
			Volume: &datamodel.Volume{
				PoolID: 1,
				Pool: &datamodel.Pool{
					PoolCredentials: &datamodel.PoolCredentials{
						Password: "password",
					},
					DeploymentName: "deployment",
				},
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationSvmName:    "dest-svm",
				DestinationVolumeName: "dest-volume",
				SourceSvmName:         "src-svm",
				SourceVolumeName:      "src-volume",
			},
		}

		nodes := []*datamodel.Node{{Name: "node1"}}

		result := &replication.UpdateVolumeReplicationAttributesResult{
			Event: &replication.UpdateVolumeReplicationAttributesEvent{
				UpdateVolumeReplicationAttributesParams: &models.UpdateVolumeReplicationAttributesParams{
					VolumeReplicationId: "replication-id",
				},
			},
		}

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}

		mockStorage.On("GetVolumeReplication", ctx, "replication-id").Return(volReplication, nil)
		mockStorage.On("GetNodesByPoolID", ctx, int64(1)).Return(nodes, nil)

		_, err := activity.GetSnapmirrorDetailsFromOntap(ctx, result)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "An internal error occurred.")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetVolumeReplicationFromSrcAndDstPathError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)
		activity := UpdateVolumeReplicationAttributesActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		volReplication := &datamodel.VolumeReplication{
			Volume: &datamodel.Volume{
				PoolID: 1,
				Pool: &datamodel.Pool{
					PoolCredentials: &datamodel.PoolCredentials{
						Password: "password",
					},
					DeploymentName: "deployment",
				},
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationSvmName:    "dest-svm",
				DestinationVolumeName: "dest-volume",
				SourceSvmName:         "src-svm",
				SourceVolumeName:      "src-volume",
			},
		}

		nodes := []*datamodel.Node{{Name: "node1"}}

		result := &replication.UpdateVolumeReplicationAttributesResult{
			Event: &replication.UpdateVolumeReplicationAttributesEvent{
				UpdateVolumeReplicationAttributesParams: &models.UpdateVolumeReplicationAttributesParams{
					VolumeReplicationId: "replication-id",
				},
			},
		}

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetVolumeReplicationFromSrcAndDstPath", mock.AnythingOfType("*vsa.VolumeReplication")).Return(nil, errors.New("ontap error"))
		mockStorage.On("GetVolumeReplication", ctx, "replication-id").Return(volReplication, nil)
		mockStorage.On("GetNodesByPoolID", ctx, int64(1)).Return(nodes, nil)

		_, err := activity.GetSnapmirrorDetailsFromOntap(ctx, result)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "An internal error occurred.")
		mockStorage.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)
		activity := UpdateVolumeReplicationAttributesActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		volReplication := &datamodel.VolumeReplication{
			Volume: &datamodel.Volume{
				PoolID: 1,
				Pool: &datamodel.Pool{
					PoolCredentials: &datamodel.PoolCredentials{
						Password: "password",
					},
					DeploymentName: "deployment",
				},
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationSvmName:    "dest-svm",
				DestinationVolumeName: "dest-volume",
				SourceSvmName:         "src-svm",
				SourceVolumeName:      "src-volume",
			},
		}

		nodes := []*datamodel.Node{{Name: "node1"}}
		expectedReplicationDetails := &vsa.VolumeReplication{
			RelationshipID: "relationship-id",
			MirrorState:    "Snapmirrored",
		}

		result := &replication.UpdateVolumeReplicationAttributesResult{
			Event: &replication.UpdateVolumeReplicationAttributesEvent{
				UpdateVolumeReplicationAttributesParams: &models.UpdateVolumeReplicationAttributesParams{
					VolumeReplicationId: "replication-id",
				},
			},
		}

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetVolumeReplicationFromSrcAndDstPath", mock.AnythingOfType("*vsa.VolumeReplication")).Return(expectedReplicationDetails, nil)
		mockStorage.On("GetVolumeReplication", ctx, "replication-id").Return(volReplication, nil)
		mockStorage.On("GetNodesByPoolID", ctx, int64(1)).Return(nodes, nil)

		res, err := activity.GetSnapmirrorDetailsFromOntap(ctx, result)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedReplicationDetails, res.ReplicationDetails)
		assert.Equal(tt, volReplication, res.DbVolReplication)
		mockStorage.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})
}

func TestUpdateReplicationAttributes(t *testing.T) {
	t.Run("WhenVolumeReplicationInternalIsNil", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := UpdateVolumeReplicationAttributesActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		result := &replication.UpdateVolumeReplicationAttributesResult{
			Event: &replication.UpdateVolumeReplicationAttributesEvent{
				UpdateVolumeReplicationAttributesParams: &models.UpdateVolumeReplicationAttributesParams{
					VolumeReplicationId:       "replication-id",
					VolumeReplicationInternal: nil,
				},
			},
		}

		_, err := activity.UpdateReplicationAttributes(ctx, result)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Invalid input parameters provided")
	})

	t.Run("WhenGetVolumeReplicationError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := UpdateVolumeReplicationAttributesActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		result := &replication.UpdateVolumeReplicationAttributesResult{
			Event: &replication.UpdateVolumeReplicationAttributesEvent{
				UpdateVolumeReplicationAttributesParams: &models.UpdateVolumeReplicationAttributesParams{
					VolumeReplicationId: "replication-id",
					VolumeReplicationInternal: &gcpgenserver.VolumeReplicationInternalV1beta{
						EndpointType: gcpgenserver.VolumeReplicationInternalV1betaEndpointTypeDst,
					},
				},
			},
		}

		mockStorage.On("GetVolumeReplication", ctx, "replication-id").Return(nil, errors.New("not found"))

		_, err := activity.UpdateReplicationAttributes(ctx, result)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "An internal error occurred.")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenUpdateVolumeReplicationError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := UpdateVolumeReplicationAttributesActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		dbReplication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{},
		}

		result := &replication.UpdateVolumeReplicationAttributesResult{
			Event: &replication.UpdateVolumeReplicationAttributesEvent{
				UpdateVolumeReplicationAttributesParams: &models.UpdateVolumeReplicationAttributesParams{
					VolumeReplicationId: "replication-id",
					VolumeReplicationInternal: &gcpgenserver.VolumeReplicationInternalV1beta{
						EndpointType: gcpgenserver.VolumeReplicationInternalV1betaEndpointTypeDst,
					},
				},
			},
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "dest-location",
					DestinationReplicationUUID: "dest-uuid",
					SourceLocation:             "src-location",
					SourceReplicationUUID:      "src-uuid",
				},
			},
			ReplicationDetails: &vsa.VolumeReplication{
				RelationshipID: "relationship-id",
				MirrorState:    "Snapmirrored",
			},
		}

		mockStorage.On("GetVolumeReplication", ctx, "replication-id").Return(dbReplication, nil)
		mockStorage.On("UpdateVolumeReplication", ctx, mock.AnythingOfType("*datamodel.VolumeReplication")).Return(errors.New("update error"))

		_, err := activity.UpdateReplicationAttributes(ctx, result)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "An internal error occurred.")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := UpdateVolumeReplicationAttributesActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		dbReplication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{},
		}

		sourceVolumeUuid := gcpgenserver.OptString{Value: "src-volume-uuid", Set: true}
		sourcePoolUuid := gcpgenserver.OptString{Value: "src-pool-uuid", Set: true}
		destVolumeUuid := gcpgenserver.OptString{Value: "dest-volume-uuid", Set: true}
		destPoolUuid := gcpgenserver.OptString{Value: "dest-pool-uuid", Set: true}

		result := &replication.UpdateVolumeReplicationAttributesResult{
			Event: &replication.UpdateVolumeReplicationAttributesEvent{
				UpdateVolumeReplicationAttributesParams: &models.UpdateVolumeReplicationAttributesParams{
					VolumeReplicationId: "replication-id",
					VolumeReplicationInternal: &gcpgenserver.VolumeReplicationInternalV1beta{
						EndpointType:          gcpgenserver.VolumeReplicationInternalV1betaEndpointTypeDst,
						SourceHostName:        "src-host",
						SourceServerName:      "src-svm",
						SourceVolumeName:      "src-volume",
						SourceVolumeUuid:      sourceVolumeUuid,
						SourcePoolUuid:        sourcePoolUuid,
						DestinationHostName:   "dest-host",
						DestinationServerName: "dest-svm",
						DestinationVolumeName: "dest-volume",
						DestinationVolumeUuid: destVolumeUuid,
						DestinationPoolUuid:   destPoolUuid,
					},
				},
			},
			DbVolReplication: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "dest-location",
					DestinationReplicationUUID: "dest-uuid",
					SourceLocation:             "src-location",
					SourceReplicationUUID:      "src-uuid",
				},
			},
			ReplicationDetails: &vsa.VolumeReplication{
				RelationshipID:     "relationship-id",
				MirrorState:        "Snapmirrored",
				RelationshipStatus: "Idle",
			},
		}

		mockStorage.On("GetVolumeReplication", ctx, "replication-id").Return(dbReplication, nil)
		mockStorage.On("UpdateVolumeReplication", ctx, mock.AnythingOfType("*datamodel.VolumeReplication")).Return(nil)

		res, err := activity.UpdateReplicationAttributes(ctx, result)

		assert.NoError(tt, err)
		assert.Equal(tt, result, res)
		mockStorage.AssertExpectations(tt)
	})
}

func TestUpdateVolumeTypeOnNewDestination(t *testing.T) {
	t.Run("WhenGetVolumeError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := UpdateVolumeReplicationAttributesActivity{SE: mockStorage}

		result := &replication.UpdateVolumeReplicationAttributesResult{
			DbVolReplication: &datamodel.VolumeReplication{
				Volume: &datamodel.Volume{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid"},
				},
			},
		}

		mockStorage.EXPECT().GetVolume(ctx, "volume-uuid").Return(nil, errors.New("volume not found"))

		err := activity.UpdateVolumeTypeOnNewDestination(ctx, result)

		assert.Error(tt, err)
		assert.Equal(tt, "volume not found", err.Error())
	})

	t.Run("WhenUpdateVolumeFieldsError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := UpdateVolumeReplicationAttributesActivity{SE: mockStorage}

		result := &replication.UpdateVolumeReplicationAttributesResult{
			DbVolReplication: &datamodel.VolumeReplication{
				Volume: &datamodel.Volume{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid"},
				},
			},
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
		}

		mockStorage.EXPECT().GetVolume(ctx, "volume-uuid").Return(volume, nil)
		mockStorage.EXPECT().UpdateVolumeFields(ctx, "volume-uuid", mock.MatchedBy(func(updates map[string]interface{}) bool {
			attrs, ok := updates["volume_attributes"].(*datamodel.VolumeAttributes)
			return ok && attrs.IsDataProtection == true
		})).Return(errors.New("update failed"))

		err := activity.UpdateVolumeTypeOnNewDestination(ctx, result)

		assert.Error(tt, err)
		assert.Equal(tt, "update failed", err.Error())
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := &database.MockStorage{}
		activity := UpdateVolumeReplicationAttributesActivity{SE: mockStorage}

		result := &replication.UpdateVolumeReplicationAttributesResult{
			DbVolReplication: &datamodel.VolumeReplication{
				Volume: &datamodel.Volume{
					BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid"},
				},
			},
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
		}

		mockStorage.EXPECT().GetVolume(ctx, "volume-uuid").Return(volume, nil)
		mockStorage.EXPECT().UpdateVolumeFields(ctx, "volume-uuid", mock.MatchedBy(func(updates map[string]interface{}) bool {
			attrs, ok := updates["volume_attributes"].(*datamodel.VolumeAttributes)
			return ok && attrs.IsDataProtection == true
		})).Return(nil)

		err := activity.UpdateVolumeTypeOnNewDestination(ctx, result)

		assert.NoError(tt, err)
	})
}
