package replicationActivities

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestGetReplicationsFromDB(t *testing.T) {
	t.Run("WhenGetAccountReturnsError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReplicationInternalGetMultipleActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		params := &common.ReplicationInternalGetMultipleParams{
			ReplicationUUIDs:    []string{"replication-uuid-1", "replication-uuid-2"},
			AccountName:         "deathstar",
			ReplicationsFromDB:  nil,
			PoolUUIDs:           nil,
			PoolNodeMap:         nil,
			PoolReplicationsMap: nil,
			UpdatedReplications: nil,
		}
		expectedError := vsaerrors.New("that's no moon")
		mockStorage.On("GetAccount", ctx, "deathstar").Return(nil, expectedError)
		_, err := activity.GetReplicationsFromDB(ctx, params)

		assert.Error(t, err)
		assert.Equal(t, expectedError.Error(), err.Error())
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenListVolumeReplicationsReturnsError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReplicationInternalGetMultipleActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		params := &common.ReplicationInternalGetMultipleParams{
			ReplicationUUIDs:    []string{"replication-uuid-1", "replication-uuid-2"},
			AccountName:         "deathstar",
			ReplicationsFromDB:  nil,
			PoolUUIDs:           nil,
			PoolNodeMap:         nil,
			PoolReplicationsMap: nil,
			UpdatedReplications: nil,
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "uuid-deathstar",
			},
			Name: "deathstar",
		}

		filter := utils.CreateFilterWithConditions([]*utils.FilterCondition{
			utils.NewFilterCondition().WithConditions("account_id", "=", account.ID),
			utils.NewFilterCondition().WithConditions("uuid", "in", params.ReplicationUUIDs)})

		expectedError := vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, errors.New("Difficult to see; always in motion is the future."))

		mockStorage.On("GetAccount", ctx, "deathstar").Return(account, nil)
		mockStorage.On("ListVolumeReplications", ctx, *filter).Return(nil, errors.New("Difficult to see; always in motion is the future."))
		_, err := activity.GetReplicationsFromDB(ctx, params)

		assert.Error(t, err)
		assert.Equal(t, expectedError.Error(), err.Error())
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenNoReplicationsFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReplicationInternalGetMultipleActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		params := &common.ReplicationInternalGetMultipleParams{
			ReplicationUUIDs:    []string{"replication-uuid-1", "replication-uuid-2"},
			AccountName:         "deathstar",
			ReplicationsFromDB:  nil,
			PoolUUIDs:           nil,
			PoolNodeMap:         nil,
			PoolReplicationsMap: nil,
			UpdatedReplications: nil,
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "uuid-deathstar",
			},
			Name: "deathstar",
		}
		expectedResp := []*datamodel.VolumeReplication{}

		filter := utils.CreateFilterWithConditions([]*utils.FilterCondition{
			utils.NewFilterCondition().WithConditions("account_id", "=", account.ID),
			utils.NewFilterCondition().WithConditions("uuid", "in", params.ReplicationUUIDs)})

		expectedError := vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, errors.NewNotFoundErr("replication", nil))

		mockStorage.On("GetAccount", ctx, "deathstar").Return(account, nil)
		mockStorage.On("ListVolumeReplications", ctx, *filter).Return(expectedResp, nil)
		_, err := activity.GetReplicationsFromDB(ctx, params)

		assert.Error(t, err)
		assert.Equal(t, expectedError.Error(), err.Error())
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReplicationInternalGetMultipleActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		params := &common.ReplicationInternalGetMultipleParams{
			ReplicationUUIDs:    []string{"replication-uuid-1", "replication-uuid-2"},
			AccountName:         "deathstar",
			ReplicationsFromDB:  nil,
			PoolUUIDs:           nil,
			PoolNodeMap:         nil,
			PoolReplicationsMap: nil,
			UpdatedReplications: nil,
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "uuid-deathstar",
			},
			Name: "deathstar",
		}
		expectedResp := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "replication-uuid-1",
				},
				AccountID:    account.ID,
				State:        "available",
				StateDetails: "Replication is available",
				VolumeID:     1,
			},
			{
				BaseModel: datamodel.BaseModel{
					ID:   2,
					UUID: "replication-uuid-2",
				},
				AccountID:    account.ID,
				State:        "available",
				StateDetails: "Replication is available",
				VolumeID:     2,
			},
		}

		filter := utils.CreateFilterWithConditions([]*utils.FilterCondition{
			utils.NewFilterCondition().WithConditions("account_id", "=", account.ID),
			utils.NewFilterCondition().WithConditions("uuid", "in", params.ReplicationUUIDs)})

		mockStorage.On("GetAccount", ctx, "deathstar").Return(account, nil)
		mockStorage.On("ListVolumeReplications", ctx, *filter).Return(expectedResp, nil)

		resp, err := activity.GetReplicationsFromDB(ctx, params)

		assert.NoError(t, err)
		assert.Equal(t, expectedResp, resp.ReplicationsFromDB)

		mockStorage.AssertExpectations(tt)
	})
}

func TestGetNodesForPools(t *testing.T) {
	t.Run("WhenGetNodesByPoolIDReturnsError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReplicationInternalGetMultipleActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		params := &common.ReplicationInternalGetMultipleParams{
			ReplicationsFromDB: []*datamodel.VolumeReplication{
				{
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
					},
				},
			},
			ReplicationUUIDs:    []string{"replication-uuid-1", "replication-uuid-2"},
			AccountName:         "deathstar",
			PoolUUIDs:           nil,
			PoolNodeMap:         nil,
			PoolReplicationsMap: nil,
			UpdatedReplications: nil,
		}

		expectedError := vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, errors.New("I got a bad feeling about this"))

		mockStorage.On("GetNodesByPoolID", ctx, int64(1)).Return(nil, errors.New("I got a bad feeling about this"))
		_, err := activity.GetNodesForPools(ctx, params)

		assert.Error(t, err)
		assert.Equal(t, expectedError.Error(), err.Error())
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenNoNodesFoundForPool", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReplicationInternalGetMultipleActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		params := &common.ReplicationInternalGetMultipleParams{
			ReplicationsFromDB: []*datamodel.VolumeReplication{
				{
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
					},
				},
			},
			ReplicationUUIDs:    []string{"replication-uuid-1", "replication-uuid-2"},
			AccountName:         "deathstar",
			PoolUUIDs:           nil,
			PoolNodeMap:         nil,
			PoolReplicationsMap: nil,
			UpdatedReplications: nil,
		}
		expectedError := vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, errors.NewNotFoundErr("node", nil))
		mockStorage.On("GetNodesByPoolID", ctx, int64(1)).Return([]*datamodel.Node{}, nil)
		_, err := activity.GetNodesForPools(ctx, params)
		assert.Error(t, err)
		assert.Equal(t, expectedError.Error(), err.Error())
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReplicationInternalGetMultipleActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		params := &common.ReplicationInternalGetMultipleParams{
			ReplicationsFromDB: []*datamodel.VolumeReplication{
				{
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
					},
				},
			},
			ReplicationUUIDs:    []string{"replication-uuid-1", "replication-uuid-2"},
			AccountName:         "deathstar",
			PoolUUIDs:           nil,
			PoolNodeMap:         nil,
			PoolReplicationsMap: nil,
			UpdatedReplications: nil,
		}

		expectedNode1 := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "node-uuid-1",
			},
			Name:            "node-name-1",
			EndpointAddress: "10.0.0.0",
		}
		expectedNode2 := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "node-uuid-2",
			},
			Name:            "node-name-2",
			EndpointAddress: "10.0.0.1",
		}

		mockStorage.On("GetNodesByPoolID", ctx, int64(1)).Return([]*datamodel.Node{expectedNode1, expectedNode2}, nil)

		resp, err := activity.GetNodesForPools(ctx, params)

		assert.NoError(t, err)
		assert.Equal(t, map[int64]*datamodel.Node{1: expectedNode1}, resp.PoolNodeMap)

		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenSuccessMultipleReplications", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReplicationInternalGetMultipleActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		params := &common.ReplicationInternalGetMultipleParams{
			ReplicationsFromDB: []*datamodel.VolumeReplication{
				{
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
					},
				},
				{
					BaseModel: datamodel.BaseModel{
						ID:   2,
						UUID: "replication-uuid-2",
					},
					AccountID: 1,
					Volume: &datamodel.Volume{
						BaseModel: datamodel.BaseModel{
							ID:   2,
							UUID: "volume-uuid-2",
						},
						PoolID: 1,
					},
				},
			},
			ReplicationUUIDs:    []string{"replication-uuid-1", "replication-uuid-2"},
			AccountName:         "deathstar",
			PoolUUIDs:           nil,
			PoolNodeMap:         nil,
			PoolReplicationsMap: nil,
			UpdatedReplications: nil,
		}

		expectedNode1 := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "node-uuid-1",
			},
			Name:            "node-name-1",
			EndpointAddress: "10.0.0.0",
		}
		expectedNode2 := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "node-uuid-2",
			},
			Name:            "node-name-2",
			EndpointAddress: "10.0.0.1",
		}

		mockStorage.On("GetNodesByPoolID", ctx, int64(1)).Return([]*datamodel.Node{expectedNode1, expectedNode2}, nil)

		resp, err := activity.GetNodesForPools(ctx, params)

		assert.NoError(t, err)
		assert.Equal(t, map[int64]*datamodel.Node{1: expectedNode1}, resp.PoolNodeMap)

		mockStorage.AssertExpectations(tt)
	})
}

func TestGetReplicationsFromOntap(t *testing.T) {
	t.Run("WhenGetProviderByNodeError", func(tt *testing.T) {
		defer func() { activitiesGetProviderByNode = activities.GetProviderByNode }()

		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}

		activity := ReplicationInternalGetMultipleActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		expectedNode1 := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "node-uuid-1",
			},
			Name:            "node-name-1",
			EndpointAddress: "10.0.0.0",
		}

		params := &common.ReplicationInternalGetMultipleParams{
			ReplicationsFromDB: []*datamodel.VolumeReplication{
				{
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
							Username: "username-1",
							Password: "password-1",
						},
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationVolumeName: "destination-volume-name-1",
						DestinationHostName:   "destination-host-name-1",
						DestinationSvmName:    "destination-svm-name-1",
						ExternalUUID:          "external-uuid-1",
						ReplicationSchedule:   "hourly",
					},
				},
			},
			ReplicationUUIDs:    []string{"replication-uuid-1", "replication-uuid-2"},
			AccountName:         "deathstar",
			PoolUUIDs:           nil,
			PoolNodeMap:         map[int64]*datamodel.Node{1: expectedNode1},
			PoolReplicationsMap: nil,
			UpdatedReplications: nil,
		}

		_, err := activity.GetReplicationsFromOntap(ctx, params)

		assert.Error(t, err)
		assert.Equal(t, err.Error(), "provider error")
	})
	t.Run("WhenGetGetReplicationDetailsReturnsError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		defer func() { activitiesGetProviderByNode = activities.GetProviderByNode }()

		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := ReplicationInternalGetMultipleActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		expectedNode1 := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "node-uuid-1",
			},
			Name:            "node-name-1",
			EndpointAddress: "10.0.0.0",
		}

		params := &common.ReplicationInternalGetMultipleParams{
			ReplicationsFromDB: []*datamodel.VolumeReplication{
				{
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
							Username: "username-1",
							Password: "password-1",
						},
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationVolumeName: "destination-volume-name-1",
						DestinationHostName:   "destination-host-name-1",
						DestinationSvmName:    "destination-svm-name-1",
						ExternalUUID:          "external-uuid-1",
						ReplicationSchedule:   "hourly",
					},
				},
			},
			ReplicationUUIDs:    []string{"replication-uuid-1", "replication-uuid-2"},
			AccountName:         "deathstar",
			PoolUUIDs:           nil,
			PoolNodeMap:         map[int64]*datamodel.Node{1: expectedNode1},
			PoolReplicationsMap: nil,
			UpdatedReplications: nil,
		}

		expectedError := vsaerrors.NewVCPError(vsaerrors.ErrFailedToGetSnapmirrorDetailsFromOntap, errors.New("code cylinder malfunction"))
		mockProvider.On("GetReplicationDetails", mock.Anything).Return(nil, errors.New("code cylinder malfunction"))
		_, err := activity.GetReplicationsFromOntap(ctx, params)

		assert.Error(t, err)
		assert.Equal(t, expectedError.Error(), err.Error())
		mockProvider.AssertExpectations(tt)
	})
	t.Run("WhenOntapReturnsNotFound", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		defer func() { activitiesGetProviderByNode = activities.GetProviderByNode }()

		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := ReplicationInternalGetMultipleActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		expectedNode1 := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "node-uuid-1",
			},
			Name:            "node-name-1",
			EndpointAddress: "10.0.0.0",
		}

		params := &common.ReplicationInternalGetMultipleParams{
			ReplicationsFromDB: []*datamodel.VolumeReplication{
				{
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
							Username: "username-1",
							Password: "password-1",
						},
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationVolumeName: "destination-volume-name-1",
						DestinationHostName:   "destination-host-name-1",
						DestinationSvmName:    "destination-svm-name-1",
						ExternalUUID:          "external-uuid-1",
						ReplicationSchedule:   "10minutely",
					},
				},
			},
			ReplicationUUIDs:    []string{"replication-uuid-1", "replication-uuid-2"},
			AccountName:         "deathstar",
			PoolUUIDs:           nil,
			PoolNodeMap:         map[int64]*datamodel.Node{1: expectedNode1},
			PoolReplicationsMap: nil,
			UpdatedReplications: nil,
		}
		extUUID := "external-uuid-1"

		mockProvider.On("GetReplicationDetails", mock.Anything).Return(nil, errors.NewNotFoundErr("snapmirror", &extUUID))
		res, err := activity.GetReplicationsFromOntap(ctx, params)

		assert.NoError(tt, err)
		assert.Empty(tt, res.UpdatedReplications)

		mockProvider.AssertExpectations(tt)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		defer func() { activitiesGetProviderByNode = activities.GetProviderByNode }()

		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := ReplicationInternalGetMultipleActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		expectedNode1 := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "node-uuid-1",
			},
			Name:            "node-name-1",
			EndpointAddress: "10.0.0.0",
		}

		params := &common.ReplicationInternalGetMultipleParams{
			ReplicationsFromDB: []*datamodel.VolumeReplication{
				{
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
							Username: "username-1",
							Password: "password-1",
						},
					},
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationVolumeName: "destination-volume-name-1",
						DestinationHostName:   "destination-host-name-1",
						DestinationSvmName:    "destination-svm-name-1",
						ExternalUUID:          "external-uuid-1",
						ReplicationSchedule:   "daily",
					},
				},
			},
			ReplicationUUIDs:    []string{"replication-uuid-1", "replication-uuid-2"},
			AccountName:         "deathstar",
			PoolUUIDs:           nil,
			PoolNodeMap:         map[int64]*datamodel.Node{1: expectedNode1},
			PoolReplicationsMap: nil,
			UpdatedReplications: nil,
		}
		timeNow := time.Now()
		mirrorState := "mirrored"
		expectedResp := vsa.VolumeReplication{
			ExternalUUID:          "external-uuid-1",
			MirrorState:           mirrorState,
			RelationshipStatus:    "transferring",
			TotalProgress:         100,
			Healthy:               true,
			UnhealthyReason:       "",
			TotalTransferBytes:    1024,
			TotalTransferTimeSecs: 60,
			LastTransferSize:      1024,
			LastTransferError:     "",
			LastTransferEndTime:   &timeNow,
			ProgressLastUpdated:   &timeNow,
			LagTime:               10,
			LastTransferDuration:  30,
		}

		mockProvider.On("GetReplicationDetails", mock.Anything).Return(&expectedResp, nil)
		res, err := activity.GetReplicationsFromOntap(ctx, params)

		assert.NoError(tt, err)
		assert.Len(tt, res.UpdatedReplications, 1)
		assert.Equal(tt, "external-uuid-1", res.UpdatedReplications[0].ReplicationAttributes.ExternalUUID)
		assert.Equal(tt, &mirrorState, res.UpdatedReplications[0].MirrorState)

		mockProvider.AssertExpectations(tt)
	})
	t.Run("WhenSuccessNoRefresh", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		defer func() { activitiesGetProviderByNode = activities.GetProviderByNode }()

		activitiesGetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := ReplicationInternalGetMultipleActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		expectedNode1 := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "node-uuid-1",
			},
			Name:            "node-name-1",
			EndpointAddress: "10.0.0.0",
		}

		params := &common.ReplicationInternalGetMultipleParams{
			ReplicationsFromDB: []*datamodel.VolumeReplication{
				{
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
							Username: "username-1",
							Password: "password-1",
						},
					},
					LastUpdatedFromOntap: time.Now(),
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationVolumeName: "destination-volume-name-1",
						DestinationHostName:   "destination-host-name-1",
						DestinationSvmName:    "destination-svm-name-1",
						ExternalUUID:          "external-uuid-1",
						ReplicationSchedule:   "10minutely",
					},
				},
			},
			ReplicationUUIDs:    []string{"replication-uuid-1", "replication-uuid-2"},
			AccountName:         "deathstar",
			PoolUUIDs:           nil,
			PoolNodeMap:         map[int64]*datamodel.Node{1: expectedNode1},
			PoolReplicationsMap: nil,
			UpdatedReplications: nil,
		}
		res, err := activity.GetReplicationsFromOntap(ctx, params)

		assert.NoError(tt, err)
		assert.Empty(tt, res.UpdatedReplications)

		mockProvider.AssertExpectations(tt)
	})
}

func TestUpdateReplicationsInDB(t *testing.T) {
	t.Run("WhenNoUpdatedReplications", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReplicationInternalGetMultipleActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		params := &common.ReplicationInternalGetMultipleParams{
			UpdatedReplications: nil,
		}

		err := activity.UpdateReplicationsInDB(ctx, params)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenUpdateVolumeReplicationTransferStatsReturnsError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReplicationInternalGetMultipleActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "replication-uuid-1",
			},
			AccountID:            1,
			State:                "available",
			StateDetails:         "Replication is available",
			LastUpdatedFromOntap: time.Now(),
			VolumeID:             1,
		}

		params := &common.ReplicationInternalGetMultipleParams{
			UpdatedReplications: []*datamodel.VolumeReplication{replication},
		}

		expectedError := vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, errors.New("Failed to update replication in DB"))
		mockStorage.On("UpdateVolumeReplicationTransferStats", ctx, replication).Return(errors.New("Failed to update replication in DB"))

		err := activity.UpdateReplicationsInDB(ctx, params)

		assert.Error(t, err)
		assert.Equal(t, expectedError.Error(), err.Error())
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenUpdateVolumeReplicationTransferStatsSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := ReplicationInternalGetMultipleActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		replication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "replication-uuid-1",
			},
			AccountID:            1,
			State:                "available",
			StateDetails:         "Replication is available",
			LastUpdatedFromOntap: time.Now(),
			VolumeID:             1,
		}

		params := &common.ReplicationInternalGetMultipleParams{
			UpdatedReplications: []*datamodel.VolumeReplication{replication},
		}

		mockStorage.On("UpdateVolumeReplicationTransferStats", ctx, replication).Return(nil)

		err := activity.UpdateReplicationsInDB(ctx, params)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(tt)
	})
}
