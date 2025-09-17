package replicationActivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
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
		var customErr *errors2.CustomError
		assert.True(tt, errors2.As(err, &customErr), "Expected a CustomError")
		assert.ErrorContains(tt, customErr.OriginalErr, "failed to get replication details")
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
		var customErr *errors2.CustomError
		assert.True(tt, errors2.As(err, &customErr), "Expected a CustomError")
		assert.ErrorContains(tt, customErr.OriginalErr, "update failed")
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

func TestGetLunDetailsFromOntap(t *testing.T) {
	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		defer func() {
			activitiesGetProviderByNode = hyperscaler.GetProviderByNode
		}()
		mockStorage := database.NewMockStorage(tt)
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider by node")
		}
		activity := &MountJobActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		replication := &datamodel.VolumeReplication{}
		_, err := activity.GetLunDetailsFromOntap(ctx, replication, node)
		assert.Error(tt, err)
	})
	t.Run("WhenGetLunFails", func(tt *testing.T) {
		defer func() {
			activitiesGetProviderByNode = hyperscaler.GetProviderByNode
		}()
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		mockProvider.On("LunGet", mock.Anything, mock.Anything).Return(nil, errors.New("failed to get LUN"))
		activity := &MountJobActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		replication := &datamodel.VolumeReplication{
			Volume: &datamodel.Volume{},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationSvmName:    "svm-name",
				DestinationVolumeName: "volume-name",
				SourceVolumeName:      "source-volume-name",
			},
		}
		_, err := activity.GetLunDetailsFromOntap(ctx, replication, node)
		assert.Error(tt, err)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		defer func() {
			activitiesGetProviderByNode = hyperscaler.GetProviderByNode
		}()
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		lunDetails := &vsa.LunResponse{
			ProviderResponse: vsa.ProviderResponse{
				Name:         "/test/vol/lun_vol",
				ExternalUUID: "lun-uuid",
			},
			Size:         1073741824,
			SerialNumber: "123412214",
		}
		mockProvider.On("LunGet", mock.Anything, mock.Anything).Return(lunDetails, nil)
		activity := &MountJobActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		replication := &datamodel.VolumeReplication{
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					BlockDevices: &[]datamodel.BlockDevice{{}},
				},
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationSvmName:    "svm-name",
				DestinationVolumeName: "volume-name",
				SourceVolumeName:      "source-volume-name",
			},
		}
		res, err := activity.GetLunDetailsFromOntap(ctx, replication, node)
		assert.NoError(tt, err)
		assert.Equal(tt, lunDetails, res)
	})
	t.Run("WhenVolumeAttributesContainsLun", func(tt *testing.T) {
		defer func() {
			activitiesGetProviderByNode = hyperscaler.GetProviderByNode
		}()
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		lunDetails := &vsa.LunResponse{
			ProviderResponse: vsa.ProviderResponse{
				Name:         "/test/vol/lun_vol",
				ExternalUUID: "lun-uuid",
			},
			Size:         1073741824,
			SerialNumber: "123412214",
		}
		lunParams := vsa.LunGetParams{
			SvmName:    "svm-name",
			VolumeName: "volume-name",
			LunName:    "lun_source-volume-name1",
		}
		mockProvider.On("LunGet", lunParams).Return(lunDetails, nil)
		activity := &MountJobActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		replication := &datamodel.VolumeReplication{
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					BlockDevices: &[]datamodel.BlockDevice{{
						Name: "lun_source-volume-name1",
					}},
				},
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationSvmName:    "svm-name",
				DestinationVolumeName: "volume-name",
				SourceVolumeName:      "source-volume-name",
			},
		}
		res, err := activity.GetLunDetailsFromOntap(ctx, replication, node)
		assert.NoError(tt, err)
		assert.Equal(tt, lunDetails, res)
	})
}

func TestUpdateVolumeLunDetailsInDB(t *testing.T) {
	t.Run("WhenUpdateVolumeFails", func(tt *testing.T) {
		defer func() {
			activitiesGetProviderByNode = hyperscaler.GetProviderByNode
		}()
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		lunDetails := &vsa.LunResponse{
			ProviderResponse: vsa.ProviderResponse{
				Name:         "/test/vol/lun_vol",
				ExternalUUID: "lun-uuid",
			},
			Size:         1073741824,
			SerialNumber: "123412214",
		}
		mockProvider.On("LunGet", mock.Anything, mock.Anything).Return(lunDetails, nil)
		activity := &MountJobActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		replication := &datamodel.VolumeReplication{
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					BlockDevices: &[]datamodel.BlockDevice{{}},
				},
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationSvmName:    "svm-name",
				DestinationVolumeName: "volume-name",
				SourceVolumeName:      "source-volume-name",
			},
		}
		mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update volume"))
		err := activity.UpdateVolumeLunDetailsInDB(ctx, replication, lunDetails)
		assert.Error(tt, err)
	})
	t.Run("WhenSuccess_UpdatesBlockDevicesAndMountStatus", func(tt *testing.T) {
		defer func() {
			activitiesGetProviderByNode = hyperscaler.GetProviderByNode
		}()
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		lunDetails := &vsa.LunResponse{
			ProviderResponse: vsa.ProviderResponse{
				Name:         "/test/vol/lun_vol",
				ExternalUUID: "lun-uuid",
			},
			Size:         1073741824,
			SerialNumber: "123412214",
		}
		mockProvider.On("LunGet", mock.Anything, mock.Anything).Return(lunDetails, nil)
		activity := &MountJobActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		replication := &datamodel.VolumeReplication{
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					BlockDevices: &[]datamodel.BlockDevice{{}},
					Mounted:      false, // Initially not mounted
				},
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationSvmName:    "svm-name",
				DestinationVolumeName: "volume-name",
				SourceVolumeName:      "source-volume-name",
				DestinationVolumeUUID: "dest-volume-uuid",
			},
		}

		// Capture the updates made to the volume
		var capturedUpdates map[string]interface{}
		mockStorage.On("UpdateVolumeFields", ctx, "dest-volume-uuid", mock.MatchedBy(func(updates map[string]interface{}) bool {
			capturedUpdates = updates
			return true
		})).Return(nil)

		err := activity.UpdateVolumeLunDetailsInDB(ctx, replication, lunDetails)
		assert.NoError(tt, err)

		// Verify the updates
		volumeAttrs, ok := capturedUpdates["volume_attributes"].(*datamodel.VolumeAttributes)
		assert.True(tt, ok, "volume_attributes should be present in updates")
		assert.True(tt, volumeAttrs.Mounted, "Mounted flag should be set to true")

		// Verify BlockDevice updates
		assert.NotNil(tt, volumeAttrs.BlockDevices)
		blockDevices := *volumeAttrs.BlockDevices
		assert.Equal(tt, 1, len(blockDevices))
		assert.Equal(tt, "lun_vol", blockDevices[0].Name)
		assert.Equal(tt, int64(1073741824), blockDevices[0].Size)
		assert.Equal(tt, "lun-uuid", blockDevices[0].LunUUID)
		assert.Equal(tt, "123412214", blockDevices[0].Identifier)
	})
}
