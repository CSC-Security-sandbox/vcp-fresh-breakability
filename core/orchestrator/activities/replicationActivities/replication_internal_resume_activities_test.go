package replicationActivities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestResumeVolumeReplication(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := vsa.GetProviderByNode
		defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()

		vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := InternalVolumeReplicationResumeActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		mirrorState := "broken_off"
		params := &datamodel.VolumeReplication{
			MirrorState: &mirrorState,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID: "external-uuid",
			},
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "volume-external-uuid",
				},
			},
		}
		mockProvider.On("ResyncVolumeReplication", mock.Anything).Return(nil, errors.New("provider error"))
		_, err := activity.ResumeVolumeReplication(ctx, params, node, false)
		assert.Error(t, err)
		assert.Equal(t, "provider error", err.Error())
		mockProvider.AssertExpectations(t)
	})
	t.Run("WhenGetProviderByNodeError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := vsa.GetProviderByNode
		defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()

		vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("get provider error")
		}

		activity := InternalVolumeReplicationResumeActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		mirrorState := "broken_off"
		params := &datamodel.VolumeReplication{
			MirrorState: &mirrorState,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID: "external-uuid",
			},
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "volume-external-uuid",
				},
			},
		}
		_, err := activity.ResumeVolumeReplication(ctx, params, node, false)
		assert.Error(t, err)
		assert.Equal(t, "get provider error", err.Error())
		mockProvider.AssertExpectations(t)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := vsa.GetProviderByNode
		defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()

		vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := InternalVolumeReplicationResumeActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		mirrorState := "broken_off"
		params := &datamodel.VolumeReplication{
			MirrorState: &mirrorState,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID: "external-uuid",
			},
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID:   "volume-external-uuid",
					Protocols:      []string{"iscsi"},
					FileProperties: &datamodel.FileProperties{JunctionPath: "/myVolume"},
				},
			},
		}
		expectedResponse := &vsa.VolumeReplication{}
		mockProvider.On("ResyncVolumeReplication", mock.Anything).Return(expectedResponse, nil)
		res, err := activity.ResumeVolumeReplication(ctx, params, node, false)

		assert.NoError(t, err)
		assert.Equal(t, expectedResponse, res)
		mockProvider.AssertExpectations(t)
	})
}

func TestGetSnapmirrorDetails(t *testing.T) {
	t.Run("WhenGetProviderByNodeError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := vsa.GetProviderByNode
		defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()

		vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("get provider error")
		}

		activity := InternalVolumeReplicationResumeActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		params := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID:          "external-uuid",
				DestinationVolumeName: "destination-volume-name",
				DestinationSvmName:    "destination-svm-name",
			},
		}
		_, err := activity.GetSnapmirrorDetails(ctx, params, node)
		assert.Error(t, err)
		assert.Equal(t, "get provider error", err.Error())
		mockProvider.AssertExpectations(t)
	})
	t.Run("WhenError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := vsa.GetProviderByNode
		defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()

		vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := InternalVolumeReplicationResumeActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		params := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID:          "external-uuid",
				DestinationVolumeName: "destination-volume-name",
				DestinationSvmName:    "destination-svm-name",
			},
		}
		mockProvider.On("GetReplicationDetails", mock.Anything, mock.Anything).Return(nil, errors.New("provider error"))
		_, err := activity.GetSnapmirrorDetails(ctx, params, node)
		assert.Error(t, err)
		assert.Equal(t, "provider error", err.Error())
		mockProvider.AssertExpectations(t)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := vsa.GetProviderByNode
		defer func() { vsa.GetProviderByNode = originalGetProviderByNode }()

		vsa.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := InternalVolumeReplicationResumeActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		params := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID:          "external-uuid",
				DestinationVolumeName: "destination-volume-name",
				DestinationSvmName:    "destination-svm-name",
			},
		}
		expectedResponse := &vsa.VolumeReplication{}
		mockProvider.On("GetReplicationDetails", mock.Anything, mock.Anything).Return(expectedResponse, nil)
		res, err := activity.GetSnapmirrorDetails(ctx, params, node)

		assert.NoError(t, err)
		assert.Equal(t, expectedResponse, res)
		mockProvider.AssertExpectations(t)
	})
}

func TestUpdateVolumeReplicationResumeDetails(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		activity := InternalVolumeReplicationResumeActivity{
			SE: mockStorage,
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		params := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID:          "external-uuid",
				DestinationVolumeName: "destination-volume-name",
				DestinationSvmName:    "destination-svm-name",
			},
		}
		vsaResumeResponse := &vsa.VolumeReplication{
			MirrorState:           "snapmirrored",
			RelationshipStatus:    "idle",
			TotalTransferBytes:    1000,
			TotalTransferTimeSecs: 60,
			LastTransferSize:      500,
			LastTransferError:     "",
			LastTransferDuration:  30,
		}
		mockStorage.On("UpdateVolumeReplication", ctx, params).Return(errors.New("update error"))
		err := activity.UpdateVolumeReplicationResumeDetails(ctx, params, vsaResumeResponse)
		assert.Error(t, err)
		assert.Equal(t, "update error", err.Error())
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		activity := InternalVolumeReplicationResumeActivity{
			SE: mockStorage,
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		params := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID:          "external-uuid",
				DestinationVolumeName: "destination-volume-name",
				DestinationSvmName:    "destination-svm-name",
			},
		}
		vsaResumeResponse := &vsa.VolumeReplication{
			MirrorState:           "snapmirrored",
			RelationshipStatus:    "idle",
			TotalTransferBytes:    1000,
			TotalTransferTimeSecs: 60,
			LastTransferSize:      500,
			LastTransferError:     "",
			LastTransferDuration:  30,
		}
		mockStorage.On("UpdateVolumeReplication", ctx, params).Return(nil)
		err := activity.UpdateVolumeReplicationResumeDetails(ctx, params, vsaResumeResponse)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(tt)
	})
}

func TestUpdateVolumeType(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		activity := InternalVolumeReplicationResumeActivity{
			SE: mockStorage,
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		params := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: "destination-volume-uuid",
			},
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					IsDataProtection: false,
				},
			},
		}
		updates := make(map[string]interface{})
		updates["volume_attributes"] = params.Volume.VolumeAttributes
		mockStorage.On("UpdateVolumeFields", ctx, params.ReplicationAttributes.DestinationVolumeUUID, updates).Return(errors.New("update error"))
		err := activity.UpdateVolumeType(ctx, params)
		assert.Error(t, err)
		assert.Equal(t, "update error", err.Error())
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		activity := InternalVolumeReplicationResumeActivity{
			SE: mockStorage,
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		params := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: "destination-volume-uuid",
			},
			Volume: &datamodel.Volume{
				VolumeAttributes: &datamodel.VolumeAttributes{
					IsDataProtection: false,
				},
			},
		}
		updates := make(map[string]interface{})
		updates["volume_attributes"] = params.Volume.VolumeAttributes
		mockStorage.On("UpdateVolumeFields", ctx, params.ReplicationAttributes.DestinationVolumeUUID, updates).Return(nil)
		err := activity.UpdateVolumeType(ctx, params)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(tt)
	})
}
