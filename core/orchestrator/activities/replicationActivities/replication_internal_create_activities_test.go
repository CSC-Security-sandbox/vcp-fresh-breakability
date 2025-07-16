package replicationActivities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestCreateVolumeReplicationInternal(t *testing.T) {
	t.Run("WhenGetProviderByNodeError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("get provider error")
		}

		activity := InternalVolumeReplicationActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}

		params := &commonparams.CreateVolumeReplicationInternalParams{
			VolumeReplication: &models.VolumeReplication{
				ReplicationAttributes: &models.ReplicationDetails{
					EndpointType:          "dst",
					SourceHostName:        "source-host",
					SourceSvmName:         "source-svm",
					SourceVolumeName:      "source-volume",
					DestinationHostName:   "destination-host",
					DestinationSvmName:    "destination-svm",
					ReplicationSchedule:   "daily",
					ReplicationPolicy:     "replication-policy",
					DestinationVolumeName: "destination-volume",
				},
			},
		}
		_, err := activity.CreateVolumeReplicationInternal(ctx, params, node, "volume-external-uuid")

		assert.Error(t, err)
		assert.Equal(t, "get provider error", err.Error())
		mockProvider.AssertExpectations(t)
	})
	t.Run("WhenError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := InternalVolumeReplicationActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}

		params := &commonparams.CreateVolumeReplicationInternalParams{
			VolumeReplication: &models.VolumeReplication{
				ReplicationAttributes: &models.ReplicationDetails{
					EndpointType:          "dst",
					SourceHostName:        "source-host",
					SourceSvmName:         "source-svm",
					SourceVolumeName:      "source-volume",
					DestinationHostName:   "destination-host",
					DestinationSvmName:    "destination-svm",
					ReplicationSchedule:   "daily",
					ReplicationPolicy:     "replication-policy",
					DestinationVolumeName: "destination-volume",
				},
			},
		}
		mockProvider.On("CreateVolumeReplication", mock.Anything).Return(nil, errors.New("provider error"))
		_, err := activity.CreateVolumeReplicationInternal(ctx, params, node, "volume-external-uuid")

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

		activity := InternalVolumeReplicationActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}

		params := &commonparams.CreateVolumeReplicationInternalParams{
			VolumeReplication: &models.VolumeReplication{
				ReplicationAttributes: &models.ReplicationDetails{
					EndpointType:          "dst",
					SourceHostName:        "source-host",
					SourceSvmName:         "source-svm",
					SourceVolumeName:      "source-volume",
					DestinationHostName:   "destination-host",
					DestinationSvmName:    "destination-svm",
					ReplicationSchedule:   "daily",
					ReplicationPolicy:     "replication-policy",
					DestinationVolumeName: "destination-volume",
				},
			},
		}

		expectedResponse := &vsa.VolumeReplication{}
		mockProvider.On("CreateVolumeReplication", mock.Anything).Return(expectedResponse, nil)

		res, err := activity.CreateVolumeReplicationInternal(ctx, params, node, "volume-external-uuid")

		assert.NoError(t, err)
		assert.Equal(t, expectedResponse, res)
		mockProvider.AssertExpectations(t)
	})
}

func TestUpdateVolumeReplicationInternal(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := InternalVolumeReplicationActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		replication := &datamodel.VolumeReplication{
			State: "initial",
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID: "external-uuid",
			},
		}

		vsaModel := &vsa.VolumeReplication{}
		mockStorage.On("UpdateVolumeReplication", ctx, replication).Return(errors.New("storage error"))
		err := activity.UpdateVolumeReplicationDetails(ctx, replication, vsaModel)

		assert.Error(t, err)
		assert.Equal(t, "storage error", err.Error())
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := InternalVolumeReplicationActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		replication := &datamodel.VolumeReplication{
			State: "initial",
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID: "external-uuid",
			},
		}
		vsaModel := &vsa.VolumeReplication{}
		mockStorage.On("UpdateVolumeReplication", ctx, replication).Return(nil)
		err := activity.UpdateVolumeReplicationDetails(ctx, replication, vsaModel)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(tt)
	})
}

func TestHydrateReplicationCreate(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		hydrationEnabled = true
		activity := InternalVolumeReplicationActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		accountName := "project-name"
		replication := &datamodel.VolumeReplication{
			Name:  "name",
			State: "creating",
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation:        "destination-location",
				DestinationReplicationUUID: "uuid",
			},
		}
		originalHydrateVolumeReplication := HydrateVolumeReplication
		defer func() { HydrateVolumeReplication = originalHydrateVolumeReplication }()

		HydrateVolumeReplication = func(ctx context.Context, createReplicationResponse models.VolumeReplication, project string) error {
			return errors.New("hydration error")
		}
		err := activity.HydrateReplicationCreate(ctx, replication, accountName)

		assert.Error(t, err)
		assert.Equal(t, "hydration error", err.Error())
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		hydrationEnabled = true
		activity := InternalVolumeReplicationActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		accountName := "project-name"
		replication := &datamodel.VolumeReplication{
			Name:  "name",
			State: "creating",
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation:        "destination-location",
				DestinationReplicationUUID: "uuid",
			},
		}
		originalHydrateVolumeReplication := HydrateVolumeReplication
		defer func() { HydrateVolumeReplication = originalHydrateVolumeReplication }()

		HydrateVolumeReplication = func(ctx context.Context, createReplicationResponse models.VolumeReplication, project string) error {
			return nil
		}
		err := activity.HydrateReplicationCreate(ctx, replication, accountName)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(tt)
	})
}
