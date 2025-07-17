package replicationActivities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestUpdateVolumeToNonDPVolume(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		activity := InternalStopVolumeReplicationActivity{
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
		err := activity.UpdateVolumeToNonDPVolume(ctx, params)
		assert.Error(t, err)
		assert.Equal(t, "update error", err.Error())
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		activity := InternalStopVolumeReplicationActivity{
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
		err := activity.UpdateVolumeToNonDPVolume(ctx, params)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(tt)
	})
}

func TestUpdateVolumeReplicationStopDetails(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		activity := InternalStopVolumeReplicationActivity{
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
		err := activity.UpdateVolumeReplicationStopDetails(ctx, params, vsaResumeResponse)
		assert.Error(t, err)
		assert.Equal(t, "update error", err.Error())
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		activity := InternalStopVolumeReplicationActivity{
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
		err := activity.UpdateVolumeReplicationStopDetails(ctx, params, vsaResumeResponse)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(tt)
	})
}

func TestGetReplicationFromDB(t *testing.T) {
	t.Run("WhenReplicationExists", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := InternalStopVolumeReplicationActivity{SE: mockStorage}

		ctx := context.Background()
		replicationUUID := "test-replication-uuid"
		expectedReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "test-replication-uuid",
			},
		}

		mockStorage.On("GetVolumeReplication", ctx, replicationUUID).Return(expectedReplication, nil)

		replication, err := activity.GetReplicationFromDB(ctx, replicationUUID)
		assert.NoError(tt, err)
		assert.NotNil(tt, replication)
		assert.Equal(tt, expectedReplication, replication)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenReplicationDoesNotExist", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := InternalStopVolumeReplicationActivity{SE: mockStorage}

		ctx := context.Background()
		replicationUUID := "non-existent-replication-uuid"

		mockStorage.On("GetVolumeReplication", ctx, replicationUUID).Return(nil, errors.NewNotFoundErr("Replication", nil))

		replication, err := activity.GetReplicationFromDB(ctx, replicationUUID)
		assert.Error(tt, err)
		assert.Nil(tt, replication)
		assert.Equal(tt, "Replication not found", err.Error())
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenDatabaseErrorOccurs", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := InternalStopVolumeReplicationActivity{SE: mockStorage}

		ctx := context.Background()
		replicationUUID := "test-replication-uuid"

		mockStorage.On("GetVolumeReplication", ctx, replicationUUID).Return(nil, errors.New("database error"))

		replication, err := activity.GetReplicationFromDB(ctx, replicationUUID)
		assert.Error(tt, err)
		assert.Nil(tt, replication)
		assert.Equal(tt, "database error", err.Error())
		mockStorage.AssertExpectations(tt)
	})
}

func TestBreakVolumeReplication(t *testing.T) {
	t.Run("BreakVolumeReplicationSucceeds", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		activity := InternalStopVolumeReplicationActivity{SE: mockStorage}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{}) // Ensure logger is added to context
		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID: "external-uuid",
			},
		}
		node := &models.Node{}
		testReplication := &vsa.VolumeReplication{
			ExternalUUID:       "external-uuid",
			MirrorState:        models.OntapBrokenOff,
			RelationshipStatus: models.SnapmirrorRelationshipIdle,
		}
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetVolumeReplication", mock.Anything).Return(testReplication, nil)
		mockProvider.On("BreakVolumeReplication", mock.Anything).Return(testReplication, nil)

		snapmirror, err := activity.BreakVolumeReplication(ctx, replication, node)
		assert.NoError(tt, err)
		assert.NotNil(tt, snapmirror)
		assert.Equal(tt, models.OntapBrokenOff, snapmirror.MirrorState)
		mockProvider.AssertExpectations(tt)
	})
	t.Run("BreakVolumeReplicationFailsToGetDetails", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		activity := InternalStopVolumeReplicationActivity{SE: mockStorage}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{}) // Ensure logger is added to context
		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID: "external-uuid",
			},
		}
		node := &models.Node{}
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetVolumeReplication", mock.Anything).Return(nil, errors.New("failed to get details"))

		snapmirror, err := activity.BreakVolumeReplication(ctx, replication, node)
		assert.Error(tt, err)
		assert.Nil(tt, snapmirror)
		assert.Equal(tt, "failed to get details", err.Error())
		mockProvider.AssertExpectations(tt)
	})
	t.Run("BreakVolumeReplicationFailsToBreak", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		activity := InternalStopVolumeReplicationActivity{SE: mockStorage}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{}) // Ensure logger is added to context
		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID: "external-uuid",
			},
		}
		node := &models.Node{}
		testReplication := &vsa.VolumeReplication{
			ExternalUUID:       "external-uuid",
			MirrorState:        models.OntapBrokenOff,
			RelationshipStatus: models.SnapmirrorRelationshipIdle,
		}
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetVolumeReplication", mock.Anything).Return(testReplication, nil)
		mockProvider.On("BreakVolumeReplication", mock.Anything).Return(nil, errors.New("failed to break replication"))

		snapmirror, err := activity.BreakVolumeReplication(ctx, replication, node)
		assert.Error(tt, err)
		assert.Nil(tt, snapmirror)
		assert.Equal(tt, "failed to break replication", err.Error())
		mockProvider.AssertExpectations(tt)
	})

	t.Run("BreakVolumeReplicationFailsWhenTransferring", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		activity := InternalStopVolumeReplicationActivity{SE: mockStorage}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{}) // Ensure logger is added to context
		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID: "external-uuid",
			},
		}
		node := &models.Node{}
		testReplication := &vsa.VolumeReplication{
			ExternalUUID:       "external-uuid",
			RelationshipStatus: models.SnapmirrorRelationshipTransferring,
		}
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetVolumeReplication", mock.Anything).Return(testReplication, nil)

		snapmirror, err := activity.BreakVolumeReplication(ctx, replication, node)
		assert.Error(tt, err)
		assert.Nil(tt, snapmirror)
		assert.Equal(tt, "Replication is in transferring state, cannot stop replication", err.Error())
		mockProvider.AssertExpectations(tt)
	})
}

func TestAbortVolumeReplication(t *testing.T) {
	t.Run("AbortVolumeReplicationSucceeds", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		activity := InternalStopVolumeReplicationActivity{SE: mockStorage}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID: "external-uuid",
			},
		}
		node := &models.Node{}
		testReplication := &vsa.VolumeReplication{
			ExternalUUID:       "external-uuid",
			RelationshipStatus: models.SnapmirrorRelationshipTransferring,
			TransferUUID:       "transfer-uuid",
		}
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetVolumeReplication", mock.Anything).Return(testReplication, nil)
		mockProvider.On("AbortVolumeReplication", mock.Anything).Return(testReplication, nil)

		snapmirror, err := activity.AbortVolumeReplication(ctx, replication, node, true)
		assert.NoError(tt, err)
		assert.NotNil(tt, snapmirror)
		assert.Equal(tt, models.SnapmirrorRelationshipAborted, snapmirror.RelationshipStatus)
		mockProvider.AssertExpectations(tt)
	})
	t.Run("AbortVolumeReplicationForceStopNotSet", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		activity := InternalStopVolumeReplicationActivity{SE: mockStorage}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID: "external-uuid",
			},
		}
		node := &models.Node{}

		snapmirror, err := activity.AbortVolumeReplication(ctx, replication, node, false)
		assert.NoError(tt, err)
		assert.Nil(tt, snapmirror)
		mockProvider.AssertExpectations(tt)
	})
	t.Run("AbortVolumeReplicationForceStopNotSet", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		activity := InternalStopVolumeReplicationActivity{SE: mockStorage}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID: "external-uuid",
			},
		}
		node := &models.Node{}

		snapmirror, err := activity.AbortVolumeReplication(ctx, replication, node, false)
		assert.NoError(tt, err)
		assert.Nil(tt, snapmirror)
		mockProvider.AssertExpectations(tt)
	})
	t.Run("AbortVolumeReplicationFailsToGetProvider", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := InternalStopVolumeReplicationActivity{SE: mockStorage}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID: "external-uuid",
			},
		}
		node := &models.Node{}
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}

		snapmirror, err := activity.AbortVolumeReplication(ctx, replication, node, true)
		assert.Error(tt, err)
		assert.Nil(tt, snapmirror)
		assert.Equal(tt, "failed to get provider", err.Error())
	})

	t.Run("AbortVolumeReplicationFailsToGetDetails", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		activity := InternalStopVolumeReplicationActivity{SE: mockStorage}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID: "external-uuid",
			},
		}
		node := &models.Node{}
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetVolumeReplication", mock.Anything).Return(nil, errors.New("failed to get details"))

		snapmirror, err := activity.AbortVolumeReplication(ctx, replication, node, true)
		assert.Error(tt, err)
		assert.Nil(tt, snapmirror)
		assert.Equal(tt, "failed to get details", err.Error())
		mockProvider.AssertExpectations(tt)
	})
	t.Run("AbortVolumeReplicationTransferUUIDMissing", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		activity := InternalStopVolumeReplicationActivity{SE: mockStorage}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID: "external-uuid",
			},
		}
		node := &models.Node{}
		testReplication := &vsa.VolumeReplication{
			ExternalUUID:       "external-uuid",
			RelationshipStatus: models.SnapmirrorRelationshipTransferring,
			TransferUUID:       "",
		}
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetVolumeReplication", mock.Anything).Return(testReplication, nil)

		snapmirror, err := activity.AbortVolumeReplication(ctx, replication, node, true)
		assert.NoError(tt, err)
		assert.NotNil(tt, snapmirror)
		assert.Equal(tt, "", snapmirror.TransferUUID)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("AbortVolumeReplicationFailsToAbort", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		activity := InternalStopVolumeReplicationActivity{SE: mockStorage}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		replication := &datamodel.VolumeReplication{
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID: "external-uuid",
			},
		}
		node := &models.Node{}
		testReplication := &vsa.VolumeReplication{
			ExternalUUID:       "external-uuid",
			RelationshipStatus: models.SnapmirrorRelationshipTransferring,
			TransferUUID:       "transfer-uuid",
		}
		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetVolumeReplication", mock.Anything).Return(testReplication, nil)
		mockProvider.On("AbortVolumeReplication", mock.Anything).Return(nil, errors.New("failed to abort replication"))

		snapmirror, err := activity.AbortVolumeReplication(ctx, replication, node, true)
		assert.Error(tt, err)
		assert.Nil(tt, snapmirror)
		assert.Equal(tt, "failed to abort replication", err.Error())
		mockProvider.AssertExpectations(tt)
	})
}

func TestGetSnapMirrorFromOntap(t *testing.T) {
	t.Run("GetSnapMirrorFromOntap_Success", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		activity := InternalStopVolumeReplicationActivity{SE: mockStorage}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		dbReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "replication-uuid",
			},
			Account: &datamodel.Account{
				Name: "account-name",
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID:          "external-uuid",
				DestinationVolumeName: "destination-volume-name",
				DestinationSvmName:    "destination-svm-name",
				DestinationHostName:   "destination-host-name",
			},
		}
		node := &models.Node{}
		expectedOntapRep := &vsa.VolumeReplication{ExternalUUID: "ontap-uuid"}

		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		mockProvider.On("GetReplicationDetails", mock.Anything, mock.Anything).Return(expectedOntapRep, nil)

		ontapRep, err := activity.GetSnapMirrorFromOntap(ctx, dbReplication, node)
		assert.NoError(t, err)
		assert.Equal(t, expectedOntapRep, ontapRep)
		mockProvider.AssertExpectations(t)
	})
	t.Run("GetSnapMirrorFromOntap_FailsToGetProvider", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := InternalStopVolumeReplicationActivity{SE: mockStorage}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		dbReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "replication-uuid",
			},
			Account: &datamodel.Account{
				Name: "account-name",
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID:          "external-uuid",
				DestinationVolumeName: "destination-volume-name",
				DestinationSvmName:    "destination-svm-name",
				DestinationHostName:   "destination-host-name",
			},
		}
		node := &models.Node{}

		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}

		ontapRep, err := activity.GetSnapMirrorFromOntap(ctx, dbReplication, node)
		assert.Error(tt, err)
		assert.Nil(tt, ontapRep)
		assert.Equal(tt, "failed to get provider", err.Error())
	})

	t.Run("GetSnapMirrorFromOntap_FailsToGetReplicationDetails", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		activity := InternalStopVolumeReplicationActivity{SE: mockStorage}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		dbReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				UUID: "replication-uuid",
			},
			Account: &datamodel.Account{
				Name: "account-name",
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				ExternalUUID:          "external-uuid",
				DestinationVolumeName: "destination-volume-name",
				DestinationSvmName:    "destination-svm-name",
				DestinationHostName:   "destination-host-name",
			},
		}
		node := &models.Node{}

		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		mockProvider.On("GetReplicationDetails", mock.Anything, mock.Anything).Return(nil, errors.New("failed to get replication details"))

		ontapRep, err := activity.GetSnapMirrorFromOntap(ctx, dbReplication, node)
		assert.Error(tt, err)
		assert.Nil(tt, ontapRep)
		assert.Equal(tt, "failed to get replication details", err.Error())
	})
}
