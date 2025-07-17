package replicationActivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

func TestCheckMountJob(t *testing.T) {
	t.Run("ReturnsErrorWhenGetVolumeReplicationFails", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockProvider.On("GetVolumeReplication", mock.Anything).Return(nil, errors.New("failed to get volume replication"))
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &MountJobActivity{}
		node := &models.Node{}
		dbRep := &datamodel.VolumeReplication{
			Account: &datamodel.Account{Name: "test-account"},
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "replication-uuid-1",
			},
			AccountID: 1,
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "volume-uuid-1",
				},
				PoolID: 1,
				Pool: &datamodel.Pool{
					PoolCredentials: &datamodel.PoolCredentials{
						Password: "password-1",
					},
				},
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeName: "destination-volume-name-1",
				DestinationHostName:   "destination-host-name-1",
				DestinationSvmName:    "destination-svm-name-1",
				ExternalUUID:          "external-uuid-1",
				ReplicationSchedule:   "10minutely",
			},
		}
		err := activity.CheckMountJob(context.Background(), dbRep, node, "test-account")

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to get volume replication")
		mockProvider.AssertExpectations(tt)
	})
	t.Run("ReturnsNilWhenMirrorStateIsSnapmirrored", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockProvider.On("GetVolumeReplication", mock.Anything).Return(&vsa.VolumeReplication{MirrorState: "snapmirrored"}, nil)
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &MountJobActivity{}
		node := &models.Node{}
		dbRep := &datamodel.VolumeReplication{
			Account: &datamodel.Account{Name: "test-account"},
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "replication-uuid-1",
			},
			AccountID: 1,
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "volume-uuid-1",
				},
				PoolID: 1,
				Pool: &datamodel.Pool{
					PoolCredentials: &datamodel.PoolCredentials{
						Password: "password-1",
					},
				},
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeName: "destination-volume-name-1",
				DestinationHostName:   "destination-host-name-1",
				DestinationSvmName:    "destination-svm-name-1",
				ExternalUUID:          "external-uuid-1",
				ReplicationSchedule:   "10minutely",
			},
		}
		err := activity.CheckMountJob(context.Background(), dbRep, node, "test-account")

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})
	t.Run("ReturnsErrorWhenGetProviderByNodeError", func(tt *testing.T) {
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}

		activity := &MountJobActivity{}
		node := &models.Node{}
		dbRep := &datamodel.VolumeReplication{
			Account: &datamodel.Account{Name: "test-account"},
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "replication-uuid-1",
			},
			AccountID: 1,
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "volume-uuid-1",
				},
				PoolID: 1,
				Pool: &datamodel.Pool{
					PoolCredentials: &datamodel.PoolCredentials{
						Password:      "password-1",
						SecretID:      "",
						CertificateID: "",
					},
				},
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeName: "destination-volume-name-1",
				DestinationHostName:   "destination-host-name-1",
				DestinationSvmName:    "destination-svm-name-1",
				ExternalUUID:          "external-uuid-1",
				ReplicationSchedule:   "10minutely",
			},
		}
		err := activity.CheckMountJob(context.Background(), dbRep, node, "test-account")

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to get provider")
	})
	t.Run("ReturnsErrorWhenMirrorStateIsNotSnapmirrored", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockProvider.On("GetVolumeReplication", mock.Anything).Return(&vsa.VolumeReplication{MirrorState: "initializing"}, nil)
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &MountJobActivity{}
		node := &models.Node{}
		dbRep := &datamodel.VolumeReplication{
			Account: &datamodel.Account{Name: "test-account"},
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "replication-uuid-1",
			},
			AccountID: 1,
			Volume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "volume-uuid-1",
				},
				PoolID: 1,
				Pool: &datamodel.Pool{
					PoolCredentials: &datamodel.PoolCredentials{
						Password: "password-1",
					},
				},
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeName: "destination-volume-name-1",
				DestinationHostName:   "destination-host-name-1",
				DestinationSvmName:    "destination-svm-name-1",
				ExternalUUID:          "external-uuid-1",
				ReplicationSchedule:   "10minutely",
			},
		}
		err := activity.CheckMountJob(context.Background(), dbRep, node, "test-account")

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "replication is not in snapmirrored state yet")
		mockProvider.AssertExpectations(tt)
	})
}

func TestGetReplicationFromOntap(t *testing.T) {
	t.Run("ReturnsReplicationWhenGetReplicationDetailsSucceeds", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockProvider.On("GetReplicationDetails", mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &MountJobActivity{}
		node := &models.Node{}
		dbReplication := &datamodel.VolumeReplication{
			Account:               &datamodel.Account{Name: "test-account"},
			ReplicationAttributes: &datamodel.ReplicationDetails{},
		}

		replication, err := activity.GetReplicationFromOntap(context.Background(), dbReplication, node, "test-account")

		assert.NoError(tt, err)
		assert.NotNil(tt, replication)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ReturnsErrorWhenGetReplicationDetailsFails", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockProvider.On("GetReplicationDetails", mock.Anything, mock.Anything).Return(nil, errors.New("failed to get replication details"))
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := &MountJobActivity{}
		node := &models.Node{}
		dbReplication := &datamodel.VolumeReplication{
			Account:               &datamodel.Account{Name: "test-account"},
			ReplicationAttributes: &datamodel.ReplicationDetails{},
		}

		replication, err := activity.GetReplicationFromOntap(context.Background(), dbReplication, node, "test-account")

		assert.Error(tt, err)
		assert.Nil(tt, replication)
		assert.Contains(tt, err.Error(), "failed to get replication details")
		mockProvider.AssertExpectations(tt)
	})
}

func TestUpdateReplicationInDB(t *testing.T) {
	t.Run("ReturnsNilWhenUpdateVolumeReplicationTransferStatsSucceeds", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockStorage.On("UpdateVolumeReplicationTransferStats", mock.Anything, mock.Anything).Return(nil)

		activity := &MountJobActivity{SE: mockStorage}
		replication := &datamodel.VolumeReplication{}

		err := activity.UpdateReplicationInDB(context.Background(), replication)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ReturnsErrorWhenUpdateVolumeReplicationTransferStatsFails", func(tt *testing.T) {
		mockStorage := new(database.MockStorage)
		mockStorage.On("UpdateVolumeReplicationTransferStats", mock.Anything, mock.Anything).Return(errors.New("update failed"))

		activity := &MountJobActivity{SE: mockStorage}
		replication := &datamodel.VolumeReplication{}

		err := activity.UpdateReplicationInDB(context.Background(), replication)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "update failed")
		mockStorage.AssertExpectations(tt)
	})
}

func TestGetReplication(t *testing.T) {
	t.Run("GetReplicationSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &MountJobActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		uuid := "uuid"
		expectedReplication := &datamodel.VolumeReplication{}

		mockStorage.On("GetVolumeReplication", ctx, uuid).Return(expectedReplication, nil)

		replication, err := activity.GetReplication(ctx, uuid)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedReplication, replication)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("GetReplicationError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &MountJobActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		uuid := "uuid"

		mockStorage.On("GetVolumeReplication", ctx, uuid).Return(nil, gorm.ErrInvalidDB)

		node, err := activity.GetReplication(ctx, uuid)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, node)
		mockStorage.AssertExpectations(tt)
	})
}
