package replicationActivities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
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

		params := &common.CreateVolumeReplicationInternalParams{
			VolumeReplication: &models.VolumeReplication{
				Name: "test-replication",
				ReplicationAttributes: &models.ReplicationDetails{
					DestinationHostName:   "dest-host",
					DestinationSvmName:    "dest-svm",
					DestinationVolumeName: "dest-vol",
					SourceHostName:        "src-host",
					SourceSvmName:         "src-svm",
					SourceVolumeName:      "src-vol",
					ReplicationSchedule:   "hourly",
					ReplicationPolicy:     "MirrorAllSnapshots",
				},
			},
			ReverseResync: true,
		}

		node := &models.Node{
			Name: "test-node",
		}

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}

		_, err := activity.ReverseVolumeReplication(ctx, params, node, "vol-ext-uuid")

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "provider error")
	})

	t.Run("WhenCreateVolumeReplicationError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)
		activity := InternalVolumeReplicationReverseActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		params := &common.CreateVolumeReplicationInternalParams{
			VolumeReplication: &models.VolumeReplication{
				Name: "test-replication",
				ReplicationAttributes: &models.ReplicationDetails{
					DestinationHostName:   "dest-host",
					DestinationSvmName:    "dest-svm",
					DestinationVolumeName: "dest-vol",
					SourceHostName:        "src-host",
					SourceSvmName:         "src-svm",
					SourceVolumeName:      "src-vol",
					ReplicationSchedule:   "hourly",
					ReplicationPolicy:     "MirrorAllSnapshots",
				},
			},
			ReverseResync: true,
		}

		node := &models.Node{
			Name: "test-node",
		}

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("CreateVolumeReplication", mock.AnythingOfType("*vsa.CreateVolumeReplicationParams")).Return(nil, errors.New("create error"))

		_, err := activity.ReverseVolumeReplication(ctx, params, node, "vol-ext-uuid")

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "create error")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenCreateVolumeReplicationConflictError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)
		activity := InternalVolumeReplicationReverseActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		params := &common.CreateVolumeReplicationInternalParams{
			VolumeReplication: &models.VolumeReplication{
				Name: "test-replication",
				ReplicationAttributes: &models.ReplicationDetails{
					DestinationHostName:   "dest-host",
					DestinationSvmName:    "dest-svm",
					DestinationVolumeName: "dest-vol",
					SourceHostName:        "src-host",
					SourceSvmName:         "src-svm",
					SourceVolumeName:      "src-vol",
					ReplicationSchedule:   "hourly",
					ReplicationPolicy:     "MirrorAllSnapshots",
				},
			},
			ReverseResync: true,
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
		mockProvider.On("CreateVolumeReplication", mock.AnythingOfType("*vsa.CreateVolumeReplicationParams")).Return(nil, conflictError)

		_, err := activity.ReverseVolumeReplication(ctx, params, node, "vol-ext-uuid")

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "conflict error")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)
		activity := InternalVolumeReplicationReverseActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		params := &common.CreateVolumeReplicationInternalParams{
			VolumeReplication: &models.VolumeReplication{
				Name: "test-replication",
				ReplicationAttributes: &models.ReplicationDetails{
					DestinationHostName:   "dest-host",
					DestinationSvmName:    "dest-svm",
					DestinationVolumeName: "dest-vol",
					SourceHostName:        "src-host",
					SourceSvmName:         "src-svm",
					SourceVolumeName:      "src-vol",
					ReplicationSchedule:   "hourly",
					ReplicationPolicy:     "MirrorAllSnapshots",
				},
			},
			ReverseResync: true,
		}

		node := &models.Node{
			Name: "test-node",
		}

		expectedResponse := &vsa.VolumeReplication{
			Name:                stringPtr("test-replication-response"),
			EndpointType:        "dst",
			SourceHostName:      "dest-host",
			SourceSVMName:       "dest-svm",
			SourceVolumeName:    "dest-vol",
			DestinationHostName: "src-host",
			DestinationSVMName:  "src-svm",
			ReplicationSchedule: "hourly",
			ReplicationPolicy:   "MirrorAllSnapshots",
		}

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("CreateVolumeReplication", mock.AnythingOfType("*vsa.CreateVolumeReplicationParams")).Return(expectedResponse, nil)

		res, err := activity.ReverseVolumeReplication(ctx, params, node, "vol-ext-uuid")

		assert.NoError(tt, err)
		assert.Equal(tt, expectedResponse, res)
		mockProvider.AssertExpectations(tt)
	})
}

func TestUpdateVolumeTypeForNewDestination(t *testing.T) {
	t.Run("WhenUpdateVolumeFieldsError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := InternalVolumeReplicationReverseActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				SourceVolumeUUID: "src-vol-uuid",
			},
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					IsDataProtection: false,
				},
			},
		}

		mockStorage.On("UpdateVolumeFields", ctx, "src-vol-uuid", mock.AnythingOfType("map[string]interface {}")).Return(errors.New("update error"))

		err := activity.UpdateVolumeTypeForNewDestination(ctx, replication)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "update error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenVolumeAttributesNil", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := InternalVolumeReplicationReverseActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				SourceVolumeUUID: "src-vol-uuid",
			},
			Volume: &datamodel.Volume{
				VolumeAttributes: nil,
			},
		}

		mockStorage.On("UpdateVolumeFields", ctx, "src-vol-uuid", mock.AnythingOfType("map[string]interface {}")).Return(nil)

		err := activity.UpdateVolumeTypeForNewDestination(ctx, replication)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := InternalVolumeReplicationReverseActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				SourceVolumeUUID: "src-vol-uuid",
			},
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					IsDataProtection: false,
				},
			},
		}

		mockStorage.On("UpdateVolumeFields", ctx, "src-vol-uuid", mock.AnythingOfType("map[string]interface {}")).Return(nil)

		err := activity.UpdateVolumeTypeForNewDestination(ctx, replication)

		assert.NoError(tt, err)
		assert.True(tt, replication.Volume.VolumeAttributes.IsDataProtection)
		mockStorage.AssertExpectations(tt)
	})
}

func TestConvertReplicationDataModelToModel(t *testing.T) {
	t.Run("WhenInputIsNil", func(tt *testing.T) {
		result := ConvertReplicationDataModelToModel(nil)
		assert.Nil(tt, result)
	})

	t.Run("WhenReplicationAttributesIsNil", func(tt *testing.T) {
		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name:                  "test-replication",
			Description:           "test description",
			State:                 "available",
			Uri:                   "test-uri",
			RemoteUri:             "test-remote-uri",
			AccountID:             123,
			VolumeID:              456,
			ReplicationAttributes: nil,
		}

		result := ConvertReplicationDataModelToModel(replication)

		assert.NotNil(tt, result)
		assert.Equal(tt, "test-uuid", result.UUID)
		assert.Equal(tt, "test-replication", result.Name)
		assert.Equal(tt, "test description", result.Description)
		assert.Equal(tt, "available", result.State)
		assert.Equal(tt, "test-uri", result.Uri)
		assert.Equal(tt, "test-remote-uri", result.RemoteUri)
		assert.Equal(tt, int64(123), result.AccountID)
		assert.Equal(tt, int64(456), result.VolumeID)
		assert.Nil(tt, result.ReplicationAttributes)
	})

	t.Run("WhenFullConversion", func(tt *testing.T) {
		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name:        "test-replication",
			Description: "test description",
			State:       "available",
			Uri:         "test-uri",
			RemoteUri:   "test-remote-uri",
			AccountID:   789,
			VolumeID:    101112,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				EndpointType:               "src",
				ReplicationType:            "async",
				ReplicationSchedule:        "hourly",
				SourcePoolUUID:             "src-pool-uuid",
				SourceVolumeUUID:           "src-vol-uuid",
				SourceLocation:             "us-east1",
				SourceHostName:             "src-host",
				SourceReplicationUUID:      "src-rep-uuid",
				SourceSvmName:              "src-svm",
				SourceVolumeName:           "src-vol",
				DestinationPoolUUID:        "dest-pool-uuid",
				DestinationVolumeUUID:      "dest-vol-uuid",
				DestinationLocation:        "us-west1",
				DestinationHostName:        "dest-host",
				DestinationReplicationUUID: "dest-rep-uuid",
				DestinationSvmName:         "dest-svm",
				DestinationVolumeName:      "dest-vol",
			},
		}

		result := ConvertReplicationDataModelToModel(replication)

		assert.NotNil(tt, result)
		assert.Equal(tt, "test-uuid", result.UUID)
		assert.Equal(tt, "test-replication", result.Name)
		assert.Equal(tt, "test description", result.Description)
		assert.Equal(tt, "available", result.State)
		assert.Equal(tt, "test-uri", result.Uri)
		assert.Equal(tt, "test-remote-uri", result.RemoteUri)
		assert.Equal(tt, int64(789), result.AccountID)
		assert.Equal(tt, int64(101112), result.VolumeID)

		assert.NotNil(tt, result.ReplicationAttributes)
		assert.Equal(tt, "src", result.ReplicationAttributes.EndpointType)
		assert.Equal(tt, "async", result.ReplicationAttributes.ReplicationType)
		assert.Equal(tt, "hourly", result.ReplicationAttributes.ReplicationSchedule)
		assert.Equal(tt, "src-pool-uuid", result.ReplicationAttributes.SourcePoolUUID)
		assert.Equal(tt, "src-vol-uuid", result.ReplicationAttributes.SourceVolumeUUID)
		assert.Equal(tt, "us-east1", result.ReplicationAttributes.SourceRegion)
		assert.Equal(tt, "src-host", result.ReplicationAttributes.SourceHostName)
		assert.Equal(tt, "src-rep-uuid", result.ReplicationAttributes.SourceReplicationUUID)
		assert.Equal(tt, "src-svm", result.ReplicationAttributes.SourceSvmName)
		assert.Equal(tt, "src-vol", result.ReplicationAttributes.SourceVolumeName)
		assert.Equal(tt, "dest-pool-uuid", result.ReplicationAttributes.DestinationPoolUUID)
		assert.Equal(tt, "dest-vol-uuid", result.ReplicationAttributes.DestinationVolumeUUID)
		assert.Equal(tt, "us-west1", result.ReplicationAttributes.DestinationRegion)
		assert.Equal(tt, "dest-host", result.ReplicationAttributes.DestinationHostName)
		assert.Equal(tt, "dest-rep-uuid", result.ReplicationAttributes.DestinationReplicationUUID)
		assert.Equal(tt, "dest-svm", result.ReplicationAttributes.DestinationSvmName)
		assert.Equal(tt, "dest-vol", result.ReplicationAttributes.DestinationVolumeName)
	})
}
