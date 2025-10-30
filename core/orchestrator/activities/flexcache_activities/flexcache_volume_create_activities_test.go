package flexcache_activities

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	ontaprestmodel "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/flexcache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	utilserrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func makeResult(vol *datamodel.Volume, workflowID string) *flexcache.CreateFlexCacheResult {
	return &flexcache.CreateFlexCacheResult{
		DBVolume: vol,
		JobInput: &flexcache.JobActivityInput{
			ResourceUUID: vol.UUID,
			ResourceName: vol.Name,
			WorkflowID:   workflowID,
			AccountID:    vol.AccountID,
		},
	}
}

func minimalJob(uuid string, state string) *datamodel.Job {
	return &datamodel.Job{
		WorkflowID: "wf-id",
		BaseModel:  datamodel.BaseModel{UUID: uuid},
		State:      state,
	}
}

func allowAnyLogs(logger *log.MockLogger) {
	logger.On("Debugf", mock.Anything, mock.Anything).Maybe()
	logger.On("Debugf", mock.Anything, mock.Anything, mock.Anything).Maybe()
	logger.On("Debugf", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe()
	logger.On("Errorf", mock.Anything, mock.Anything).Maybe()
	logger.On("Errorf", mock.Anything, mock.Anything, mock.Anything).Maybe()
	logger.On("Warnf", mock.Anything, mock.Anything).Maybe()
	logger.On("Warnf", mock.Anything, mock.Anything, mock.Anything).Maybe()
	logger.On("Debug", mock.Anything).Maybe()
}

func TestFlexCacheVolumeCreateActivity_CreateFlexCacheVolumeInOntap(t *testing.T) {
	dbVolume := &datamodel.Volume{
		Svm:     &datamodel.Svm{Name: "svm-name"},
		Account: &datamodel.Account{Name: "account-name"},
		CacheParameters: &datamodel.CacheParameters{
			PeerSvmName:     "peer-svm",
			PeerVolumeName:  "peer-volume",
			PeerClusterName: "peer-cluster",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{},
		},
	}

	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: dbVolume}

		volumeResp := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{Name: "volume-name", ExternalUUID: "external-uuid"}}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().CreateFlexCacheVolume(mock.Anything).Return(volumeResp, nil)
		logger.EXPECT().Debug("flexcache volume created successfully")

		newResult, err := activity.CreateFlexCacheVolumeInOntapActivity(ctx, flexcacheResult)
		assert.NoError(tt, err)
		assert.Equal(tt, "volume-name", newResult.VolumeResponse.Name)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: dbVolume}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(nil, assert.AnError)

		_, err := activity.CreateFlexCacheVolumeInOntapActivity(ctx, flexcacheResult)
		assert.Error(tt, err)
	})

	t.Run("WhenCreateFlexCacheVolumeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: dbVolume}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().CreateFlexCacheVolume(mock.Anything).Return(nil, assert.AnError)

		_, err := activity.CreateFlexCacheVolumeInOntapActivity(ctx, flexcacheResult)
		assert.Error(tt, err)
	})
}

func TestFlexCacheVolumeCreateActivity_CreateClusterPeerInOntapActivity(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		logger := log.NewMockLogger(tt)
		expiry := time.Now().Add(time.Minute * 30)
		vol := &datamodel.Volume{
			Svm:     &datamodel.Svm{Name: "svm-name"},
			Account: &datamodel.Account{Name: "account-name"},
			CacheParameters: &datamodel.CacheParameters{
				PeerIpAddresses:   []string{"10.0.0.1", "10.0.0.2"},
				PeerClusterName:   "peer-cluster",
				CommandExpiryTime: &expiry,
			},
		}
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: vol}
		clusterPeer := &vsa.ClusterPeer{UUID: "cluster-peer-uuid"}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().CreateClusterPeer(mock.Anything).Return(clusterPeer, nil)
		logger.EXPECT().Infof("cluster peer created successfully with UUID: %s", clusterPeer.UUID)

		res, err := activity.CreateClusterPeerInOntapActivity(ctx, flexcacheResult)
		assert.NoError(tt, err)
		assert.Equal(tt, clusterPeer, res.ClusterPeer)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		vol := &datamodel.Volume{CacheParameters: &datamodel.CacheParameters{PeerIpAddresses: []string{"10.0.0.1"}, PeerClusterName: "peer-cluster"}}
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mm.EXPECT().utilGetLogger(ctx).Return(log.NewMockLogger(tt))
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(nil, assert.AnError)

		res, err := activity.CreateClusterPeerInOntapActivity(ctx, flexcacheResult)
		assert.Nil(tt, res)
		assert.Error(tt, err)
	})

	t.Run("WhenCreateClusterPeerFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		vol := &datamodel.Volume{CacheParameters: &datamodel.CacheParameters{PeerIpAddresses: []string{"10.0.0.1"}, PeerClusterName: "peer-cluster"}}
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mm.EXPECT().utilGetLogger(ctx).Return(log.NewMockLogger(tt))
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().CreateClusterPeer(mock.Anything).Return(nil, assert.AnError)

		res, err := activity.CreateClusterPeerInOntapActivity(ctx, flexcacheResult)
		assert.Nil(tt, res)
		assert.Error(tt, err)
		assert.Equal(tt, coremodels.ErrorDuringClusterPeerCode, vol.CacheParameters.CacheStateDetailsCode)
		assert.Equal(tt, "Cluster peering failed, please try again", vol.CacheParameters.CacheStateDetails)
	})
}

func TestFlexCacheVolumeCreateActivity_UpdateFlexCacheVolumeForClusterPeering(t *testing.T) {
	baseVolume := func() *datamodel.Volume {
		return &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Name:      "flexcache-vol",
			Svm:       &datamodel.Svm{Name: "svm-name"},
			Account:   &datamodel.Account{Name: "account-name"},
			CacheParameters: &datamodel.CacheParameters{
				PeerSvmName:     "peer-svm",
				PeerVolumeName:  "peer-volume",
				PeerClusterName: "peer-cluster",
				CacheState:      cvpModels.FlexCacheV1betaCacheStateCACHESTATEUNSPECIFIED,
			},
			VolumeAttributes: &datamodel.VolumeAttributes{FileProperties: &datamodel.FileProperties{}},
		}
	}

	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockProvider := vsa.NewMockProvider(tt)
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: mockStorage}
		ctx := context.Background()
		logger := log.NewMockLogger(tt)

		vol := baseVolume()
		pass := log.Secret("some-passphrase")
		externalUUID := "cluster-peer-external-uuid"
		expiry := strfmt.DateTime(time.Now().Add(time.Hour))
		clusterPeer := &vsa.ClusterPeer{ExternalUUID: externalUUID, Passphrase: &pass, ExpiryTime: &expiry}
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: vol, ClusterPeer: clusterPeer}

		interclusterLifs := []*vsa.InterclusterLif{{Address: ontaprestmodel.IPAddress("10.0.0.1")}, {Address: ontaprestmodel.IPAddress("10.0.0.2")}}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().GetInterclusterLIFs(vsa.InterclusterServicePolicyName).Return(interclusterLifs, nil)
		mockStorage.On("UpdateVolumeFields", ctx, vol.UUID, mock.MatchedBy(func(updates map[string]interface{}) bool {
			return updates["cache_parameters"] != nil && updates["cluster_peer_uuid"] != nil
		})).Return(nil)
		logger.EXPECT().Debug("cluster peer command updated successfully")

		res, err := activity.UpdateFlexCacheVolumeForClusterPeeringActivity(ctx, flexcacheResult)
		assert.NoError(tt, err)
		assert.Equal(tt, externalUUID, *res.DBVolume.ClusterPeerUUID)
		assert.NotNil(tt, res.DBVolume.CacheParameters.Command)
		assert.Contains(tt, *res.DBVolume.CacheParameters.Command, "cluster peer create")
		assert.Equal(tt, cvpModels.FlexCacheV1betaCacheStatePENDINGCLUSTERPEERING, res.DBVolume.CacheParameters.CacheState)
		assert.Equal(tt, cvpModels.FlexCacheV1betaCacheStateCACHESTATEUNSPECIFIED, res.DBVolume.CacheParameters.PreviousCacheState)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		vol := baseVolume()
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mm.EXPECT().utilGetLogger(ctx).Return(log.NewMockLogger(tt))
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(nil, assert.AnError)
		_, err := activity.UpdateFlexCacheVolumeForClusterPeeringActivity(ctx, flexcacheResult)
		assert.Error(tt, err)
	})

	t.Run("WhenGetInterclusterLIFsFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		vol := baseVolume()
		pass := log.Secret("some-passphrase")
		expiry := strfmt.DateTime(time.Now().Add(time.Hour))
		clusterPeer := &vsa.ClusterPeer{ExternalUUID: "peer-external-uuid", Passphrase: &pass, ExpiryTime: &expiry}
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: vol, ClusterPeer: clusterPeer}

		mm.EXPECT().utilGetLogger(ctx).Return(log.NewMockLogger(tt))
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().GetInterclusterLIFs(vsa.InterclusterServicePolicyName).Return(nil, assert.AnError)
		_, err := activity.UpdateFlexCacheVolumeForClusterPeeringActivity(ctx, flexcacheResult)
		assert.Error(tt, err)
	})

	t.Run("WhenUpdateVolumeFieldsFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockProvider := vsa.NewMockProvider(tt)
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: mockStorage}
		ctx := context.Background()
		vol := baseVolume()
		pass := log.Secret("some-passphrase")
		expiry := strfmt.DateTime(time.Now().Add(time.Hour))
		clusterPeer := &vsa.ClusterPeer{ExternalUUID: "peer-external-uuid", Passphrase: &pass, ExpiryTime: &expiry}
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: vol, ClusterPeer: clusterPeer}
		interclusterLifs := []*vsa.InterclusterLif{{Address: ontaprestmodel.IPAddress("10.0.0.1")}}

		mm.EXPECT().utilGetLogger(ctx).Return(log.NewMockLogger(tt))
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().GetInterclusterLIFs(vsa.InterclusterServicePolicyName).Return(interclusterLifs, nil)
		mockStorage.On("UpdateVolumeFields", ctx, vol.UUID, mock.Anything).Return(assert.AnError)
		_, err := activity.UpdateFlexCacheVolumeForClusterPeeringActivity(ctx, flexcacheResult)
		assert.Error(tt, err)
	})
}

func TestFlexCacheVolumeCreateActivity_WaitForClusterPeerActivity(t *testing.T) {
	baseVolume := func() *datamodel.Volume {
		clusterPeerUUID := "cluster-peer-uuid"
		return &datamodel.Volume{
			BaseModel:        datamodel.BaseModel{UUID: "volume-uuid"},
			Name:             "flexcache-vol",
			Svm:              &datamodel.Svm{Name: "svm-name"},
			Account:          &datamodel.Account{Name: "account-name"},
			CacheParameters:  &datamodel.CacheParameters{},
			VolumeAttributes: &datamodel.VolumeAttributes{FileProperties: &datamodel.FileProperties{}},
			ClusterPeerUUID:  &clusterPeerUUID}
	}

	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		vol := baseVolume()
		peer := &vsa.ClusterPeer{
			UUID:                "cluster-peer-uuid",
			AuthenticationState: vsa.ClusterPeerAuthenticationStateOK,
			Availability:        vsa.ClusterPeerAvailabilityStateAvailable,
		}
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: vol}
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().GetClusterPeer(*vol.ClusterPeerUUID).Return(peer, nil)
		res, err := activity.WaitForClusterPeerActivity(ctx, flexcacheResult)
		assert.NoError(tt, err)
		assert.Equal(tt, vol.ClusterPeerUUID, res.DBVolume.ClusterPeerUUID)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		vol := baseVolume()
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: vol}
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(nil, assert.AnError)
		_, err := activity.WaitForClusterPeerActivity(ctx, flexcacheResult)
		assert.Error(tt, err)
	})

	t.Run("WhenGetClusterPeerErrors", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		vol := baseVolume()
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: vol}
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().GetClusterPeer(*vol.ClusterPeerUUID).Return(nil, errors.New("any failure"))
		_, err := activity.WaitForClusterPeerActivity(ctx, flexcacheResult)
		assert.Error(tt, err)
	})

	t.Run("WhenClusterPeerProblem", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		vol := baseVolume()
		peer := &vsa.ClusterPeer{
			UUID:                "cluster-peer-uuid",
			AuthenticationState: vsa.ClusterPeerAuthenticationStateProblem,
			Availability:        vsa.ClusterPeerAvailabilityStateUnavailable,
		}
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().GetClusterPeer(*vol.ClusterPeerUUID).Return(peer, nil)
		_, err := activity.WaitForClusterPeerActivity(ctx, flexcacheResult)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Error during cluster peering")
	})

	t.Run("WhenClusterPeerNotReady", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		vol := baseVolume()
		peer := &vsa.ClusterPeer{
			UUID:                "cluster-peer-uuid",
			AuthenticationState: vsa.ClusterPeerAuthenticationStatePending,
			Availability:        vsa.ClusterPeerAvailabilityStateUnavailable,
		}
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: vol}
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().GetClusterPeer(*vol.ClusterPeerUUID).Return(peer, nil)
		_, err := activity.WaitForClusterPeerActivity(ctx, flexcacheResult)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "cluster peer is not ready yet")
	})
}

func TestFlexCacheVolumeCreateActivity_CreateSVMPeeringInOntapActivity(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		vol := &datamodel.Volume{
			Svm:     &datamodel.Svm{Name: "svm-name"},
			Account: &datamodel.Account{Name: "account-name"},
			CacheParameters: &datamodel.CacheParameters{
				PeerSvmName:     "peer-svm",
				PeerClusterName: "peer-cluster",
			},
		}
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: vol}
		svmPeer := &vsa.SvmPeer{UUID: "svm-peer-uuid", State: "peered"}

		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().CreateSVMPeer(mock.Anything).Return(svmPeer, nil)

		res, err := activity.CreateSVMPeeringInOntapActivity(ctx, flexcacheResult)
		assert.NoError(tt, err)
		assert.Equal(tt, svmPeer, res.SVMPeer)
		assert.Equal(tt, "svm-peer-uuid", res.SVMPeer.UUID)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		vol := &datamodel.Volume{
			Svm: &datamodel.Svm{Name: "svm-name"},
			CacheParameters: &datamodel.CacheParameters{
				PeerSvmName:     "peer-svm",
				PeerClusterName: "peer-cluster",
			},
		}
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(nil, assert.AnError)

		res, err := activity.CreateSVMPeeringInOntapActivity(ctx, flexcacheResult)
		assert.Nil(tt, res)
		assert.Error(tt, err)
	})

	t.Run("WhenCreateSVMPeerFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		vol := &datamodel.Volume{
			Svm: &datamodel.Svm{Name: "svm-name"},
			CacheParameters: &datamodel.CacheParameters{
				PeerSvmName:     "peer-svm",
				PeerClusterName: "peer-cluster",
			},
		}
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().CreateSVMPeer(mock.Anything).Return(nil, assert.AnError)

		res, err := activity.CreateSVMPeeringInOntapActivity(ctx, flexcacheResult)
		assert.Nil(tt, res)
		assert.Error(tt, err)
	})
}

func TestFlexCacheVolumeCreateActivity_UpdateFlexCacheVolumeForSVMPeeringActivity(t *testing.T) {
	baseVolume := func() *datamodel.Volume {
		pass := "old-passphrase"
		expiry := time.Now().Add(15 * time.Minute)
		return &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid-svm"},
			Name:      "flexcache-vol",
			Svm:       &datamodel.Svm{Name: "local-svm"},
			Account:   &datamodel.Account{Name: "account-name"},
			CacheParameters: &datamodel.CacheParameters{
				PeerSvmName:           "peer-svm",
				PeerClusterName:       "peer-cluster",
				CacheState:            cvpModels.FlexCacheV1betaCacheStatePENDINGCLUSTERPEERING,
				PreviousCacheState:    cvpModels.FlexCacheV1betaCacheStateCACHESTATEUNSPECIFIED,
				Passphrase:            &pass,
				Command:               nillable.ToPointer("old command"),
				CommandExpiryTime:     &expiry,
				CacheStateDetails:     "some prior detail",
				CacheStateDetailsCode: coremodels.WaitingForClusterPeeringCode,
			},
			VolumeAttributes: &datamodel.VolumeAttributes{FileProperties: &datamodel.FileProperties{}},
		}
	}

	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: mockStorage}
		ctx := context.Background()
		logger := log.NewMockLogger(tt)

		vol := baseVolume()
		svmPeer := &vsa.SvmPeer{UUID: "svm-peer-uuid", State: vsa.SvmPeerStatePeered}
		result := &flexcache.CreateFlexCacheResult{DBVolume: vol, SVMPeer: svmPeer}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mockStorage.On("UpdateVolumeFields", ctx, vol.UUID, mock.Anything).Return(nil).Once()
		logger.EXPECT().Debug("svm peer command updated successfully")

		updated, err := activity.UpdateFlexCacheVolumeForSVMPeeringActivity(ctx, result)
		assert.NoError(tt, err)
		assert.Equal(tt, "svm-peer-uuid", *updated.DBVolume.SvmPeerUUID)
		assert.Nil(tt, updated.DBVolume.CacheParameters.Passphrase)
		assert.Nil(tt, updated.DBVolume.CacheParameters.CommandExpiryTime)
		assert.NotNil(tt, updated.DBVolume.CacheParameters.Command)
		assert.Equal(tt, "vserver peer accept -vserver peer-svm -peer-vserver local-svm", *updated.DBVolume.CacheParameters.Command)
		assert.Equal(tt, cvpModels.FlexCacheV1betaCacheStatePENDINGSVMPEERING, updated.DBVolume.CacheParameters.CacheState)
		assert.Equal(tt, cvpModels.FlexCacheV1betaCacheStatePENDINGCLUSTERPEERING, updated.DBVolume.CacheParameters.PreviousCacheState)
		assert.Equal(tt, coremodels.WaitingForSVMPeeringCode, updated.DBVolume.CacheParameters.CacheStateDetailsCode)
		assert.Equal(tt, coremodels.WaitingForSVMPeering, updated.DBVolume.CacheParameters.CacheStateDetails)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenUpdateVolumeFieldsFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: mockStorage}
		ctx := context.Background()
		vol := baseVolume()
		svmPeer := &vsa.SvmPeer{UUID: "svm-peer-uuid-2", State: string(vsa.SvmPeerStatePeered)}
		result := &flexcache.CreateFlexCacheResult{DBVolume: vol, SVMPeer: svmPeer}

		mm.EXPECT().utilGetLogger(ctx).Return(log.NewMockLogger(tt))
		mockStorage.On("UpdateVolumeFields", ctx, vol.UUID, mock.Anything).Return(assert.AnError).Once()

		updated, err := activity.UpdateFlexCacheVolumeForSVMPeeringActivity(ctx, result)
		assert.Nil(tt, updated)
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}

func TestFlexCacheVolumeCreateActivity_WaitForSVMPeeringActivity(t *testing.T) {
	baseVolume := func() *datamodel.Volume {
		return &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid-wait-svm"},
			Name:      "flexcache-vol",
			Svm:       &datamodel.Svm{Name: "local-svm"},
			Account:   &datamodel.Account{Name: "account-name"},
			CacheParameters: &datamodel.CacheParameters{
				PeerSvmName: "peer-svm",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{FileProperties: &datamodel.FileProperties{}},
		}
	}

	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		vol := baseVolume()
		result := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().GetSVMPeer(&vol.Svm.Name, &vol.CacheParameters.PeerSvmName).Return(&vsa.SvmPeer{UUID: "svm-peer-uuid", State: vsa.SvmPeerStatePeered}, nil)

		res, err := activity.WaitForSVMPeeringActivity(ctx, result)
		assert.NoError(tt, err)
		assert.Equal(tt, result, res)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		vol := baseVolume()
		result := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(nil, assert.AnError)

		res, err := activity.WaitForSVMPeeringActivity(ctx, result)
		assert.Nil(tt, res)
		assert.Error(tt, err)
	})

	t.Run("WhenGetSVMPeerErrors", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		vol := baseVolume()
		result := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().GetSVMPeer(&vol.Svm.Name, &vol.CacheParameters.PeerSvmName).Return(nil, errors.New("any failure"))

		res, err := activity.WaitForSVMPeeringActivity(ctx, result)
		assert.Nil(tt, res)
		assert.Error(tt, err)
	})

	t.Run("WhenSVMPeerRejected", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		vol := baseVolume()
		result := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().GetSVMPeer(&vol.Svm.Name, &vol.CacheParameters.PeerSvmName).Return(&vsa.SvmPeer{UUID: "svm-peer-uuid", State: vsa.SvmPeerStateRejected}, nil)

		res, err := activity.WaitForSVMPeeringActivity(ctx, result)
		assert.Nil(tt, res)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Error during SVM peering")
	})

	t.Run("WhenSVMPeerNotReady", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		vol := baseVolume()
		result := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().GetSVMPeer(&vol.Svm.Name, &vol.CacheParameters.PeerSvmName).Return(&vsa.SvmPeer{UUID: "svm-peer-uuid", State: vsa.SvmPeerStateInitiated}, nil)

		res, err := activity.WaitForSVMPeeringActivity(ctx, result)
		assert.Nil(tt, res)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "svm peer is not ready yet")
	})
}

func TestFlexCacheVolumeCreateActivity_UpdateFlexCacheVolumeDetailsActivity(t *testing.T) {
	baseVolume := func() *datamodel.Volume {
		return &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid-svm"},
			Name:      "flexcache-vol",
			Svm:       &datamodel.Svm{Name: "local-svm"},
			Account:   &datamodel.Account{Name: "account-name"},
			CacheParameters: &datamodel.CacheParameters{
				Command:               nillable.ToPointer("old command"),
				CacheStateDetails:     "some prior detail",
				CacheStateDetailsCode: coremodels.WaitingForClusterPeeringCode,
			},
			VolumeAttributes: &datamodel.VolumeAttributes{FileProperties: &datamodel.FileProperties{}},
		}
	}

	t.Run("Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: mockStorage}
		ctx := context.Background()

		vol := baseVolume()
		svmPeer := &vsa.SvmPeer{UUID: "svm-peer-uuid", State: vsa.SvmPeerStatePeered}
		volumeResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "external-volume-uuid"}}
		result := &flexcache.CreateFlexCacheResult{
			DBVolume:       vol,
			SVMPeer:        svmPeer,
			VolumeResponse: volumeResponse,
		}

		mockStorage.EXPECT().UpdateVolumeFields(ctx, vol.UUID, mock.MatchedBy(func(updates map[string]interface{}) bool {
			state, ok := updates["state"].(string)
			stateDetails, ok2 := updates["state_details"].(string)
			return ok && ok2 && state == "READY" && stateDetails == "Available for use"
		})).Return(nil).Once()

		updated, err := activity.UpdateFlexCacheVolumeDetailsActivity(ctx, result)
		assert.NoError(tt, err)
		assert.Nil(tt, updated.DBVolume.CacheParameters.Command)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenUpdateVolumeFieldsFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: mockStorage}
		ctx := context.Background()
		vol := baseVolume()
		svmPeer := &vsa.SvmPeer{UUID: "svm-peer-uuid-2", State: string(vsa.SvmPeerStatePeered)}
		volumeResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "external-volume-uuid"}}
		result := &flexcache.CreateFlexCacheResult{
			DBVolume:       vol,
			SVMPeer:        svmPeer,
			VolumeResponse: volumeResponse,
		}

		mockStorage.On("UpdateVolumeFields", ctx, vol.UUID, mock.Anything).Return(assert.AnError).Once()

		updated, err := activity.UpdateFlexCacheVolumeDetailsActivity(ctx, result)
		assert.Nil(tt, updated)
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}

func TestFlexCacheVolumeCreateActivity_UpdateVolumeDetailsOnErrorActivity(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: mockStorage}
		ctx := context.Background()

		vol := &datamodel.Volume{
			BaseModel:       datamodel.BaseModel{UUID: "volume-uuid"},
			CacheParameters: &datamodel.CacheParameters{CacheStateDetails: "some-detail"},
		}
		result := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mockStorage.On("UpdateVolumeFields", ctx, vol.UUID, mock.MatchedBy(func(updates map[string]interface{}) bool {
			state, sOk := updates["state"].(string)
			stateDetails, sdOk := updates["state_details"].(string)
			cacheParams, cpOk := updates["cache_parameters"].(*datamodel.CacheParameters)
			return sOk && sdOk && cpOk && state == "ERROR" && stateDetails == "Error in creating" && cacheParams == vol.CacheParameters
		})).Return(nil).Once()

		err := activity.UpdateVolumeDetailsOnErrorActivity(ctx, result)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenUpdateVolumeFieldsFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: mockStorage}
		ctx := context.Background()

		vol := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "volume-uuid-2"}, CacheParameters: &datamodel.CacheParameters{}}
		result := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mockStorage.On("UpdateVolumeFields", ctx, vol.UUID, mock.Anything).Return(assert.AnError).Once()

		err := activity.UpdateVolumeDetailsOnErrorActivity(ctx, result)
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenShouldNotSetErrorState", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: mockStorage}
		ctx := context.Background()

		vol := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "volume-uuid-3"}, CacheParameters: &datamodel.CacheParameters{CacheStateDetailsCode: coremodels.ClusterPeeringExpiredCode}}
		result := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mockStorage.On("UpdateVolumeFields", ctx, vol.UUID, mock.MatchedBy(func(updates map[string]interface{}) bool {
			_, stateOk := updates["state"]
			_, stateDetailsOk := updates["state_details"]
			return !stateOk && !stateDetailsOk
		})).Return(nil).Once()

		err := activity.UpdateVolumeDetailsOnErrorActivity(ctx, result)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}

func TestCompleteFlexCacheCreateJobActivity(t *testing.T) {
	ctx := context.Background()
	act := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(t)}

	t.Run("MissingWorkflowID", func(tt *testing.T) {
		ctx := context.Background()

		job, err := act.CompleteFlexCacheCreateJobActivity(ctx, &flexcache.CreateFlexCacheResult{
			JobInput: &flexcache.JobActivityInput{
				WorkflowID:   "",
				ResourceName: "test-vol",
			},
		})

		assert.Nil(tt, job)
		assert.Error(tt, err)
	})
	t.Run("GetJobError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		act.SE = mockStorage
		mockStorage.On("GetJob", ctx, "wf-err").Return(nil, assert.AnError).Once()
		job, err := act.CompleteFlexCacheCreateJobActivity(ctx, &flexcache.CreateFlexCacheResult{
			JobInput: &flexcache.JobActivityInput{
				WorkflowID:   "wf-err",
				ResourceName: "wf-err",
			},
		})
		assert.Nil(tt, job)
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("JobNotFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		act.SE = mockStorage
		mockStorage.On("GetJob", ctx, "wf-missing").Return(nil, nil).Once()
		job, err := act.CompleteFlexCacheCreateJobActivity(ctx, &flexcache.CreateFlexCacheResult{
			JobInput: &flexcache.JobActivityInput{
				WorkflowID:   "wf-missing",
				ResourceName: "wf-missing",
			},
		})
		assert.Nil(tt, job)
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("AlreadyDone", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		act.SE = mockStorage
		j := minimalJob("job-done", string(models.JobsStateDONE))
		mockStorage.On("GetJob", ctx, "wf-done").Return(j, nil).Once()
		result, err := act.CompleteFlexCacheCreateJobActivity(ctx, &flexcache.CreateFlexCacheResult{
			JobInput: &flexcache.JobActivityInput{
				WorkflowID:   "wf-done",
				ResourceName: "wf-done",
			},
		})
		assert.NoError(tt, err)
		assert.Empty(tt, result.ErrorMessage)
		assert.Equal(tt, 0, result.ErrorTrackingID)
	})

	t.Run("AlreadyError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		act.SE = mockStorage
		j := minimalJob("job-err", string(models.JobsStateERROR))
		mockStorage.On("GetJob", ctx, "wf-err2").Return(j, nil).Once()
		result, err := act.CompleteFlexCacheCreateJobActivity(ctx, &flexcache.CreateFlexCacheResult{
			JobInput: &flexcache.JobActivityInput{
				WorkflowID:   "wf-err2",
				ResourceName: "wf-err2",
			},
		})
		assert.NoError(tt, err)
		assert.Empty(tt, result.ErrorMessage)
		assert.Equal(tt, 0, result.ErrorTrackingID)
	})

	t.Run("CompleteProcessing", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		act.SE = mockStorage
		j := &datamodel.Job{
			BaseModel:    datamodel.BaseModel{UUID: "job-proc"},
			State:        string(models.JobsStatePROCESSING),
			WorkflowID:   "wf-proc",
			TrackingID:   11,
			ErrorDetails: "ed",
		}
		mockStorage.On("GetJob", ctx, "wf-proc").Return(j, nil).Once()
		mockStorage.On("UpdateJob", ctx, j.UUID, string(models.JobsStateDONE), j.TrackingID, j.ErrorDetails).Return(nil).Once()
		result, err := act.CompleteFlexCacheCreateJobActivity(ctx, &flexcache.CreateFlexCacheResult{
			JobInput: &flexcache.JobActivityInput{
				WorkflowID:   "wf-proc",
				ResourceName: "wf-proc",
			},
		})
		assert.NoError(tt, err)
		assert.Empty(tt, result.ErrorMessage)
		assert.Equal(tt, 0, result.ErrorTrackingID)
		mockStorage.AssertExpectations(tt)
	})
}

func TestCreatePeeringJobActivity(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	act := &FlexCacheVolumeCreateActivity{SE: mockStorage}
	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "res-peering"},
		Name:      "res-peering",
		AccountID: 0,
	}
	result := makeResult(vol, "wf-peering")

	t.Run("ExistingProcessingReturns", func(tt *testing.T) {
		existing := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "j1"}, State: string(models.JobsStatePROCESSING), Type: string(models.JobTypeFlexCacheEstablishPeering)}
		mockStorage.On("GetJobByResourceUUID", ctx, result.JobInput.ResourceUUID, existing.Type).Return(existing, nil).Once()
		res, err := act.CreatePeeringJobActivity(ctx, result)
		assert.NoError(tt, err)
		assert.Empty(tt, res.ErrorMessage)
		assert.Equal(tt, 0, res.ErrorTrackingID)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ExistingDoneReturns", func(tt *testing.T) {
		existing := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "j2"}, State: string(models.JobsStateDONE), Type: string(models.JobTypeFlexCacheEstablishPeering)}
		mockStorage.On("GetJobByResourceUUID", ctx, result.JobInput.ResourceUUID, existing.Type).Return(existing, nil).Once()
		res, err := act.CreatePeeringJobActivity(ctx, result)
		assert.NoError(tt, err)
		assert.Empty(tt, res.ErrorMessage)
		assert.Equal(tt, 0, res.ErrorTrackingID)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CreateNewSuccess", func(tt *testing.T) {
		mockStorage.On("GetJobByResourceUUID", ctx, result.JobInput.ResourceUUID, string(models.JobTypeFlexCacheEstablishPeering)).Return(nil, errors.New("nf")).Once()
		mockStorage.On("CreateJob", ctx, mock.MatchedBy(func(j *datamodel.Job) bool {
			return j.Type == string(models.JobTypeFlexCacheEstablishPeering) && j.State == string(models.JobsStatePROCESSING)
		})).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "created"}, Type: string(models.JobTypeFlexCacheEstablishPeering), State: string(models.JobsStatePROCESSING)}, nil).Once()

		res, err := act.CreatePeeringJobActivity(ctx, result)
		assert.NoError(tt, err)
		assert.Empty(tt, res.ErrorMessage)
		assert.Equal(tt, 0, res.ErrorTrackingID)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CreateFails", func(tt *testing.T) {
		mockStorage.On("GetJobByResourceUUID", ctx, result.JobInput.ResourceUUID, string(models.JobTypeFlexCacheEstablishPeering)).Return(nil, errors.New("nf2")).Once()
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(nil, assert.AnError).Once()
		job, err := act.CreatePeeringJobActivity(ctx, result)
		assert.Nil(tt, job)
		assert.Error(tt, err)
	})
}

func TestCompletePeeringJobActivity(t *testing.T) {
	t.Run("CompletesProcessingJob", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		act := &FlexCacheVolumeCreateActivity{SE: mockStorage}

		vol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "res-uuid"},
			CacheParameters: &datamodel.CacheParameters{
				CacheState: cvpModels.FlexCacheV1betaCacheStatePENDINGCLUSTERPEERING,
			},
		}
		result := &flexcache.CreateFlexCacheResult{
			DBVolume: vol,
			JobInput: &flexcache.JobActivityInput{
				ResourceUUID: vol.UUID,
			},
		}

		job := &datamodel.Job{
			BaseModel:    datamodel.BaseModel{UUID: "jp2"},
			Type:         string(coremodels.JobTypeFlexCacheEstablishPeering),
			State:        string(coremodels.JobsStatePROCESSING),
			TrackingID:   0,
			ErrorDetails: "",
		}

		mockStorage.On("GetJobByResourceUUID", ctx, vol.UUID, job.Type).Return(job, nil).Once()
		mockStorage.On("UpdateJob", ctx, job.UUID, string(coremodels.JobsStateDONE), job.TrackingID, job.ErrorDetails).Return(nil).Once()

		err := act.CompletePeeringJobActivity(ctx, result)
		assert.NoError(tt, err)
		assert.Equal(tt, string(coremodels.JobsStateDONE), job.State)
		mockStorage.AssertExpectations(tt)
	})
}

func TestStartInternalJobActivity(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	act := &FlexCacheVolumeCreateActivity{SE: mockStorage}
	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "res-internal"},
		Svm:       &datamodel.Svm{Name: "svm-name"},
		Account:   &datamodel.Account{Name: "account-name"},
		CacheParameters: &datamodel.CacheParameters{
			PeerSvmName:     "peer-svm",
			PeerVolumeName:  "peer-volume",
			PeerClusterName: "peer-cluster",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{},
		},
	}
	result := makeResult(vol, "wf-peering")

	t.Run("ExistingProcessingReturns", func(tt *testing.T) {
		existing := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "i1"}, State: string(models.JobsStatePROCESSING), Type: string(models.JobTypeFlexCacheInternalPeering)}
		mockStorage.On("GetJobByResourceUUID", ctx, result.JobInput.ResourceUUID, existing.Type).Return(existing, nil).Once()
		res, err := act.StartInternalJobActivity(ctx, result)
		assert.NoError(tt, err)
		assert.Empty(tt, res.ErrorMessage)
		assert.Equal(tt, 0, res.ErrorTrackingID)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CreateInternalSuccess", func(tt *testing.T) {
		mockStorage.On("GetJobByResourceUUID", ctx, result.JobInput.ResourceUUID, string(models.JobTypeFlexCacheInternalPeering)).Return(nil, errors.New("nf")).Once()
		mockStorage.On("CreateJob", ctx, mock.MatchedBy(func(j *datamodel.Job) bool {
			return j.Type == string(models.JobTypeFlexCacheInternalPeering) && j.IsAdminJob
		})).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "int-created"}, Type: string(models.JobTypeFlexCacheInternalPeering), State: string(models.JobsStatePROCESSING)}, nil).Once()
		res, err := act.StartInternalJobActivity(ctx, result)
		assert.NoError(tt, err)
		assert.Empty(tt, res.ErrorMessage)
		assert.Equal(tt, 0, res.ErrorTrackingID)
		mockStorage.AssertExpectations(tt)
	})
}

func TestCompleteInternalJobActivity(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	act := &FlexCacheVolumeCreateActivity{SE: mockStorage}
	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "res-internal"},
		Svm:       &datamodel.Svm{Name: "svm-name"},
		Account:   &datamodel.Account{Name: "account-name"},
		CacheParameters: &datamodel.CacheParameters{
			PeerSvmName:     "peer-svm",
			PeerVolumeName:  "peer-volume",
			PeerClusterName: "peer-cluster",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{},
		},
	}
	result := makeResult(vol, "wf-peering")
	jobType := string(models.JobTypeFlexCacheInternalPeering)

	t.Run("NotPeeredKeepsProcessing", func(tt *testing.T) {
		job := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "ic1"}, Type: jobType, State: string(models.JobsStatePROCESSING)}
		err := act.CompleteInternalJobActivity(ctx, result)
		assert.NoError(tt, err)
		assert.Equal(tt, string(models.JobsStatePROCESSING), job.State)
	})

	t.Run("PeeredCompletes", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		act := &FlexCacheVolumeCreateActivity{SE: mockStorage}

		vol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "res-internal"},
			Name:      "res-internal",
			CacheParameters: &datamodel.CacheParameters{
				CacheState: cvpModels.FlexCacheV1betaCacheStatePEERED,
			},
		}
		result := makeResult(vol, "wf-internal")
		job := &datamodel.Job{
			BaseModel:    datamodel.BaseModel{UUID: "ic2"},
			Type:         string(coremodels.JobTypeFlexCacheInternalPeering),
			State:        string(coremodels.JobsStatePROCESSING),
			TrackingID:   0,
			ErrorDetails: "",
		}
		mockStorage.On("GetJobByResourceUUID", ctx, result.JobInput.ResourceUUID, job.Type).Return(job, nil).Once()
		mockStorage.On("UpdateJob", ctx, job.UUID, string(coremodels.JobsStateDONE), job.TrackingID, job.ErrorDetails).Return(nil).Once()

		err := act.CompleteInternalJobActivity(ctx, result)
		assert.NoError(tt, err)
		assert.Equal(tt, string(coremodels.JobsStateDONE), job.State)
		mockStorage.AssertExpectations(tt)
	})
}

func TestFailJobActivity(t *testing.T) {
	t.Run("GetJobError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		act := &FlexCacheVolumeCreateActivity{SE: mockStorage}
		resUUID := "res-fail"
		jobType := string(coremodels.JobTypeFlexCacheEstablishPeering)
		result := &flexcache.CreateFlexCacheResult{
			DBVolume:      &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: resUUID}},
			ActiveJobType: coremodels.JobTypeFlexCacheEstablishPeering,
		}
		mockStorage.On("GetJobByResourceUUID", ctx, resUUID, jobType).Return(nil, assert.AnError).Once()

		err := act.FailJobActivity(ctx, result)
		assert.Error(tt, err)

		mockStorage.AssertExpectations(tt)
	})

	t.Run("JobNil", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		act := &FlexCacheVolumeCreateActivity{SE: mockStorage}

		resUUID := "res-fail"
		jobType := string(models.JobTypeFlexCacheEstablishPeering)
		result := &flexcache.CreateFlexCacheResult{
			DBVolume:      &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: resUUID}},
			ActiveJobType: models.JobTypeFlexCacheEstablishPeering,
		}

		mockStorage.On("GetJobByResourceUUID", ctx, resUUID, jobType).Return(nil, nil).Once()

		err := act.FailJobActivity(ctx, result)
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("AlreadyDoneNoOp", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		act := &FlexCacheVolumeCreateActivity{SE: mockStorage}

		resUUID := "res-fail"
		jobType := string(models.JobTypeFlexCacheEstablishPeering)
		result := &flexcache.CreateFlexCacheResult{
			DBVolume:      &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: resUUID}},
			ActiveJobType: models.JobTypeFlexCacheEstablishPeering,
		}

		j := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "f1"}, Type: jobType, State: string(models.JobsStateDONE)}
		mockStorage.On("GetJobByResourceUUID", ctx, resUUID, jobType).Return(j, nil).Once()

		err := act.FailJobActivity(ctx, result)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("AlreadyErrorNoOp", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		act := &FlexCacheVolumeCreateActivity{SE: mockStorage}

		resUUID := "res-fail"
		jobType := string(models.JobTypeFlexCacheEstablishPeering)
		result := &flexcache.CreateFlexCacheResult{
			DBVolume:      &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: resUUID}},
			ActiveJobType: models.JobTypeFlexCacheEstablishPeering,
		}

		j := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "f2"}, Type: jobType, State: string(models.JobsStateERROR)}
		mockStorage.On("GetJobByResourceUUID", ctx, resUUID, jobType).Return(j, nil).Once()

		err := act.FailJobActivity(ctx, result)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("FailProcessingExplicit", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		act := &FlexCacheVolumeCreateActivity{SE: mockStorage}

		resUUID := "res-fail"
		jobType := string(models.JobTypeFlexCacheEstablishPeering)
		result := &flexcache.CreateFlexCacheResult{
			DBVolume:        &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: resUUID}},
			ActiveJobType:   models.JobTypeFlexCacheEstablishPeering,
			ErrorTrackingID: 999,
			ErrorMessage:    "boom",
		}

		j := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "f3"}, Type: jobType, State: string(models.JobsStatePROCESSING)}
		mockStorage.On("GetJobByResourceUUID", ctx, resUUID, jobType).Return(j, nil).Once()
		mockStorage.On("UpdateJob", ctx, j.UUID, string(models.JobsStateERROR), 999, "boom").Return(nil).Once()

		err := act.FailJobActivity(ctx, result)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("FailProcessingImplicitDefaults", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		act := &FlexCacheVolumeCreateActivity{SE: mockStorage}

		resUUID := "res-fail"
		jobType := string(models.JobTypeFlexCacheEstablishPeering)
		result := &flexcache.CreateFlexCacheResult{
			DBVolume:      &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: resUUID}},
			ActiveJobType: models.JobTypeFlexCacheEstablishPeering,
		}

		j := &datamodel.Job{
			BaseModel:    datamodel.BaseModel{UUID: "f4"},
			Type:         jobType,
			State:        string(models.JobsStatePROCESSING),
			TrackingID:   77,
			ErrorDetails: "prev",
		}
		mockStorage.On("GetJobByResourceUUID", ctx, resUUID, jobType).Return(j, nil).Once()
		mockStorage.On("UpdateJob", ctx, j.UUID, string(models.JobsStateERROR), j.TrackingID, j.ErrorDetails).Return(nil).Once()

		err := act.FailJobActivity(ctx, result)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}

func TestCompleteJobInternalPaths(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	act := &FlexCacheVolumeCreateActivity{SE: mockStorage}

	t.Run("InvalidOptions", func(tt *testing.T) {
		_, err := act.completeJob(ctx, completeJobOpts{
			GetErrCode: vsaerrors.ErrInternalPeeringJobFailed,
		})
		assert.Error(tt, err)
	})

	t.Run("UpdateFails", func(tt *testing.T) {
		job := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "cj2"}, Type: string(models.JobTypeFlexCacheInternalPeering), State: string(models.JobsStatePROCESSING)}
		mockStorage.On("GetJobByResourceUUID", ctx, "res-y", job.Type).Return(job, nil).Once()
		mockStorage.On("UpdateJob", ctx, job.UUID, string(models.JobsStateDONE), job.TrackingID, job.ErrorDetails).Return(assert.AnError).Once()
		res, err := act.completeJob(ctx, completeJobOpts{
			ResourceUUID:  "res-y",
			JobType:       job.Type,
			GetErrCode:    vsaerrors.ErrInternalPeeringJobFailed,
			UpdateErrCode: vsaerrors.ErrInternalPeeringJobFailed,
		})
		assert.Nil(tt, res)
		assert.Error(tt, err)
	})
}

func TestMapJobTypeToError(t *testing.T) {
	assert.Equal(t, vsaerrors.ErrEstablishPeeringJobFailed, mapJobTypeToError(string(models.JobTypeFlexCacheEstablishPeering)))
	assert.Equal(t, vsaerrors.ErrInternalPeeringJobFailed, mapJobTypeToError(string(models.JobTypeFlexCacheInternalPeering)))
	assert.Equal(t, vsaerrors.ErrCreatingFlexCacheVolume, mapJobTypeToError(string(models.JobTypeFlexCacheCreateVolume)))
	assert.Equal(t, vsaerrors.ErrInternalPeeringJobFailed, mapJobTypeToError("unknown"))
}

func TestLoggingPatchedForJobActivities(t *testing.T) {
	// Smoke test ensuring logger patch infra does not panic
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	act := &FlexCacheVolumeCreateActivity{SE: mockStorage}

	vol := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "res-peering"},
		Svm:       &datamodel.Svm{Name: "svm-name"},
		Account:   &datamodel.Account{Name: "account-name"},
		CacheParameters: &datamodel.CacheParameters{
			PeerSvmName:     "peer-svm",
			PeerVolumeName:  "peer-volume",
			PeerClusterName: "peer-cluster",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{},
		},
	}
	result := makeResult(vol, "wf-id")

	mm := newMonkeyMockAndPatch(t)
	logger := log.NewMockLogger(t)
	mm.EXPECT().utilGetLogger(ctx).Return(logger)
	allowAnyLogs(logger)

	mockStorage.On("GetJob", ctx, "wf-id").Return(minimalJob("u1", string(models.JobsStateDONE)), nil).Once()
	_, _ = act.CompleteFlexCacheCreateJobActivity(ctx, result)
}

func TestFlexCacheVolumeCreateActivity_HydrateFlexCacheState(t *testing.T) {
	baseVolume := func() *datamodel.Volume {
		return &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-hydrate"},
			Name:      "flexcache-vol-hydrate",
			State:     coremodels.LifeCycleStateREADY,
			CacheParameters: &datamodel.CacheParameters{
				CacheState: cvpModels.FlexCacheV1betaCacheStatePENDINGCLUSTERPEERING,
			},
			Svm: &datamodel.Svm{Name: "svm-hydrate"},
		}
	}

	makeResultWithEvent := func(vol *datamodel.Volume) *flexcache.CreateFlexCacheResult {
		return &flexcache.CreateFlexCacheResult{
			DBVolume: vol,
			Event: &flexcache.CreateFlexCacheEvent{
				LocationID:    "us-test1",
				ProjectNumber: "123456789",
			},
		}
	}

	t.Run("HydrationDisabled", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mm.EXPECT().utilGetLogger(mock.Anything).Return(logger)
		mm.EXPECT().isHydrationEnabled().Return(false)
		logger.EXPECT().Debugf("hydration is disabled, skipping HydrateFlexCacheState")

		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		vol := baseVolume()
		result := makeResultWithEvent(vol)

		res, err := activity.HydrateFlexCacheState(ctx, result)
		assert.NoError(tt, err)
		assert.Equal(tt, result, res)
	})

	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		ctx := context.Background()
		vol := baseVolume()
		result := makeResultWithEvent(vol)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}

		mm.EXPECT().isHydrationEnabled().Return(true)
		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().authGenerateCallbackToken(ctx).Return("tok-123", nil)
		mm.EXPECT().commonHydrateFlexCacheState(ctx, logger, result.Event.LocationID, result.Event.ProjectNumber, vol.Name, vol.CacheParameters.CacheState, vol.State, "tok-123").Return(nil)
		logger.EXPECT().Debugf("hydration completed successfully for volume: %s", vol.Name)

		res, err := activity.HydrateFlexCacheState(ctx, result)
		assert.NoError(tt, err)
		assert.Equal(tt, result, res)
	})

	t.Run("TokenGenerationError", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		ctx := context.Background()
		mm.EXPECT().isHydrationEnabled().Return(true)
		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().authGenerateCallbackToken(ctx).Return("", assert.AnError)
		logger.EXPECT().Error("Error when getting callback token", assert.AnError)

		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		vol := baseVolume()
		result := makeResultWithEvent(vol)

		res, err := activity.HydrateFlexCacheState(ctx, result)
		assert.Error(tt, err)
		assert.Equal(tt, result, res, "should return original result on token error")
	})

	t.Run("HydrationFailure", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		ctx := context.Background()
		mm.EXPECT().isHydrationEnabled().Return(true)
		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().authGenerateCallbackToken(ctx).Return("tok-456", nil)

		vol := baseVolume()
		result := makeResultWithEvent(vol)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}

		mm.EXPECT().commonHydrateFlexCacheState(ctx, logger, result.Event.LocationID, result.Event.ProjectNumber, vol.Name, vol.CacheParameters.CacheState, vol.State, "tok-456").Return(assert.AnError)
		logger.EXPECT().Errorf("Error when hydrating flexcache state: %v", assert.AnError)

		res, err := activity.HydrateFlexCacheState(ctx, result)
		assert.Error(tt, err)
		assert.Nil(tt, res, "result should be nil on hydration failure")
	})
}

func TestFlexCacheVolumeCreateActivity_EnsureClusterPeerActivity(t *testing.T) {
	newVolume := func() *datamodel.Volume {
		return &datamodel.Volume{
			CacheParameters: &datamodel.CacheParameters{},
		}
	}

	t.Run("WhenClusterPeerUUIDAbsent", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		provider := vsa.NewMockProvider(tt)
		act := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		vol := newVolume() // ClusterPeerUUID nil
		res := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(provider, nil)
		logger.EXPECT().Debug("cluster peer UUID absent; will create")

		out, err := act.EnsureClusterPeerInOntapActivity(ctx, res)
		assert.NoError(tt, err)
		assert.Equal(tt, flexcache.ActionCreate, out.ClusterPeerAction)
		assert.Nil(tt, out.ClusterPeer)
	})

	t.Run("WhenGetClusterPeerErrors", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		provider := vsa.NewMockProvider(tt)
		act := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		uuid := "peer-uuid"
		vol := newVolume()
		vol.ClusterPeerUUID = &uuid
		res := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(provider, nil)
		provider.EXPECT().GetClusterPeer(uuid).Return(nil, assert.AnError)
		logger.EXPECT().Infof(mock.MatchedBy(func(format string) bool {
			return true // accept the formatted Infof
		}), mock.Anything)

		out, err := act.EnsureClusterPeerInOntapActivity(ctx, res)
		assert.NoError(tt, err)
		assert.Equal(tt, flexcache.ActionCreate, out.ClusterPeerAction)
		assert.Nil(tt, out.ClusterPeer)
	})

	t.Run("WhenClusterPeerNilWithoutError", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		provider := vsa.NewMockProvider(tt)
		act := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		uuid := "peer-uuid"
		vol := newVolume()
		vol.ClusterPeerUUID = &uuid
		res := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(provider, nil)
		provider.EXPECT().GetClusterPeer(uuid).Return(nil, nil)
		logger.EXPECT().Infof(mock.Anything, mock.Anything)

		out, err := act.EnsureClusterPeerInOntapActivity(ctx, res)
		assert.NoError(tt, err)
		assert.Equal(tt, flexcache.ActionCreate, out.ClusterPeerAction)
	})

	t.Run("WhenClusterPeerInvalidStateProblem", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		provider := vsa.NewMockProvider(tt)
		act := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		uuid := "peer-uuid"
		vol := newVolume()
		vol.ClusterPeerUUID = &uuid
		res := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		cp := &vsa.ClusterPeer{
			UUID:                uuid,
			AuthenticationState: vsa.ClusterPeerAuthenticationStateProblem,
			Availability:        vsa.ClusterPeerAvailabilityStateUnavailable,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(provider, nil)
		provider.EXPECT().GetClusterPeer(uuid).Return(cp, nil)
		logger.EXPECT().Warnf(mock.Anything, cp.AuthenticationState, cp.Availability)

		out, err := act.EnsureClusterPeerInOntapActivity(ctx, res)
		assert.NoError(tt, err)
		assert.Equal(tt, flexcache.ActionCreate, out.ClusterPeerAction)
	})

	t.Run("WhenCommandExpiredRecreate", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		provider := vsa.NewMockProvider(tt)
		act := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		uuid := "peer-uuid"
		expired := time.Now().Add(-5 * time.Minute)
		expiredFmt := strfmt.DateTime(expired)
		vol := newVolume()
		vol.ClusterPeerUUID = &uuid
		res := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		cp := &vsa.ClusterPeer{
			UUID:                uuid,
			AuthenticationState: vsa.ClusterPeerAuthenticationStatePending,
			Availability:        vsa.ClusterPeerAvailabilityStatePending,
			ExpiryTime:          &expiredFmt,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(provider, nil)
		provider.EXPECT().GetClusterPeer(uuid).Return(cp, nil)
		logger.EXPECT().Infof(mock.Anything, uuid)

		out, err := act.EnsureClusterPeerInOntapActivity(ctx, res)
		assert.NoError(tt, err)
		assert.Equal(tt, flexcache.ActionCreate, out.ClusterPeerAction)
	})

	t.Run("WhenClusterPeerReady", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		provider := vsa.NewMockProvider(tt)
		act := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		uuid := "peer-uuid"
		vol := newVolume()
		vol.ClusterPeerUUID = &uuid
		res := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		cp := &vsa.ClusterPeer{
			UUID:                uuid,
			AuthenticationState: vsa.ClusterPeerAuthenticationStateOK,
			Availability:        vsa.ClusterPeerAvailabilityStateAvailable,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(provider, nil)
		provider.EXPECT().GetClusterPeer(uuid).Return(cp, nil)
		logger.EXPECT().Debug("reusing existing cluster peer (ready)")

		out, err := act.EnsureClusterPeerInOntapActivity(ctx, res)
		assert.NoError(tt, err)
		assert.Equal(tt, flexcache.ActionReady, out.ClusterPeerAction)
		assert.Equal(tt, cp, out.ClusterPeer)
	})

	t.Run("WhenClusterPeerWait", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		provider := vsa.NewMockProvider(tt)
		act := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		uuid := "peer-uuid"
		vol := newVolume()
		vol.ClusterPeerUUID = &uuid
		res := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		cp := &vsa.ClusterPeer{
			UUID:                uuid,
			AuthenticationState: vsa.ClusterPeerAuthenticationStatePending,
			Availability:        vsa.ClusterPeerAvailabilityStatePartial,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(provider, nil)
		provider.EXPECT().GetClusterPeer(uuid).Return(cp, nil)
		logger.EXPECT().Debugf(mock.Anything, cp.AuthenticationState, cp.Availability)

		out, err := act.EnsureClusterPeerInOntapActivity(ctx, res)
		assert.NoError(tt, err)
		assert.Equal(tt, flexcache.ActionWait, out.ClusterPeerAction)
		assert.Equal(tt, cp, out.ClusterPeer)
	})

	t.Run("WhenClusterPeerUnknownFallback", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		provider := vsa.NewMockProvider(tt)
		act := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		uuid := "peer-uuid"
		vol := newVolume()
		vol.ClusterPeerUUID = &uuid
		res := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		cp := &vsa.ClusterPeer{
			UUID:                uuid,
			AuthenticationState: vsa.ClusterPeerAuthenticationStateOK,
			Availability:        vsa.ClusterPeerAvailabilityStateUnavailable, // triggers fallback
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(provider, nil)
		provider.EXPECT().GetClusterPeer(uuid).Return(cp, nil)
		logger.EXPECT().Warnf(mock.Anything, cp.AuthenticationState, cp.Availability)

		out, err := act.EnsureClusterPeerInOntapActivity(ctx, res)
		assert.NoError(tt, err)
		assert.Equal(tt, flexcache.ActionCreate, out.ClusterPeerAction)
	})
}

func TestFlexCacheVolumeCreateActivity_EnsureSVMPeerActivity(t *testing.T) {
	newVolume := func() *datamodel.Volume {
		return &datamodel.Volume{
			Svm: &datamodel.Svm{Name: "local-svm"},
			CacheParameters: &datamodel.CacheParameters{
				PeerSvmName: "peer-svm",
			},
		}
	}

	t.Run("WhenSVMPeerUUIDAbsent", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		provider := vsa.NewMockProvider(tt)
		act := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		vol := newVolume() // SvmPeerUUID nil
		res := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(provider, nil)
		logger.EXPECT().Debug(mock.Anything)

		out, err := act.EnsureSVMPeerInOntapActivity(ctx, res)
		assert.NoError(tt, err)
		assert.Equal(tt, flexcache.ActionCreate, out.SVMPeerAction)
		assert.Nil(tt, out.SVMPeer)
	})

	t.Run("WhenGetSVMPeerErrors", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		provider := vsa.NewMockProvider(tt)
		act := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		u := "svm-peer-uuid"
		vol := newVolume()
		vol.SvmPeerUUID = &u
		res := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(provider, nil)
		provider.EXPECT().GetSVMPeer(&vol.Svm.Name, &vol.CacheParameters.PeerSvmName).Return(nil, assert.AnError)

		out, err := act.EnsureSVMPeerInOntapActivity(ctx, res)
		assert.Error(tt, err)
		assert.Nil(tt, out)
	})

	t.Run("WhenSVMPeerNilWithoutError", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		provider := vsa.NewMockProvider(tt)
		act := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		u := "svm-peer-uuid"
		vol := newVolume()
		vol.SvmPeerUUID = &u
		res := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(provider, nil)
		provider.EXPECT().GetSVMPeer(&vol.Svm.Name, &vol.CacheParameters.PeerSvmName).Return(nil, utilserrors.NewNotFoundErr("svmPeer", nil))
		logger.EXPECT().Infof(mock.Anything, mock.Anything)

		out, err := act.EnsureSVMPeerInOntapActivity(ctx, res)
		assert.NoError(tt, err)
		assert.Equal(tt, flexcache.ActionCreate, out.SVMPeerAction)
	})

	t.Run("WhenSVMPeerInvalidStateRejected", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		provider := vsa.NewMockProvider(tt)
		act := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		u := "svm-peer-uuid"
		vol := newVolume()
		vol.SvmPeerUUID = &u
		res := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		sp := &vsa.SvmPeer{
			UUID:  u,
			State: vsa.SvmPeerStateRejected,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(provider, nil)
		provider.EXPECT().GetSVMPeer(&vol.Svm.Name, &vol.CacheParameters.PeerSvmName).Return(sp, nil)
		logger.EXPECT().Warnf(mock.Anything, mock.Anything, mock.Anything).Maybe() // depending on implementation

		out, err := act.EnsureSVMPeerInOntapActivity(ctx, res)
		assert.NoError(tt, err)
		assert.Equal(tt, flexcache.ActionCreate, out.SVMPeerAction)
	})

	t.Run("WhenSVMPeerUnknownStateRecreate", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		provider := vsa.NewMockProvider(tt)
		act := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		u := "svm-peer-uuid"
		vol := newVolume()
		vol.SvmPeerUUID = &u
		// (Optional) expired time retained but not used by logic
		expired := time.Now().Add(-10 * time.Minute)
		vol.CacheParameters.CommandExpiryTime = &expired
		res := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		sp := &vsa.SvmPeer{
			UUID:  u,
			State: vsa.SvmPeerStateInitiated, // unrecognized -> fallback recreate
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(provider, nil)
		provider.EXPECT().GetSVMPeer(&vol.Svm.Name, &vol.CacheParameters.PeerSvmName).Return(sp, nil)
		logger.EXPECT().Warnf(mock.Anything, sp.State)

		out, err := act.EnsureSVMPeerInOntapActivity(ctx, res)
		assert.NoError(tt, err)
		assert.Equal(tt, flexcache.ActionCreate, out.SVMPeerAction)
	})

	t.Run("WhenSVMPeerReady", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		provider := vsa.NewMockProvider(tt)
		act := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		u := "svm-peer-uuid"
		vol := newVolume()
		vol.SvmPeerUUID = &u
		res := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		sp := &vsa.SvmPeer{
			UUID:  u,
			State: vsa.SvmPeerStatePeered,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(provider, nil)
		provider.EXPECT().GetSVMPeer(&vol.Svm.Name, &vol.CacheParameters.PeerSvmName).Return(sp, nil)
		logger.EXPECT().Debug(mock.Anything)

		out, err := act.EnsureSVMPeerInOntapActivity(ctx, res)
		assert.NoError(tt, err)
		assert.Equal(tt, flexcache.ActionReady, out.SVMPeerAction)
		assert.Equal(tt, sp, out.SVMPeer)
	})

	t.Run("WhenSVMPeerWait", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		provider := vsa.NewMockProvider(tt)
		act := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		u := "svm-peer-uuid"
		vol := newVolume()
		vol.SvmPeerUUID = &u
		res := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		sp := &vsa.SvmPeer{
			UUID:  u,
			State: vsa.SvmPeerStatePending, // updated: triggers wait branch
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(provider, nil)
		provider.EXPECT().GetSVMPeer(&vol.Svm.Name, &vol.CacheParameters.PeerSvmName).Return(sp, nil)
		logger.EXPECT().Debugf(mock.Anything, sp.State)

		out, err := act.EnsureSVMPeerInOntapActivity(ctx, res)
		assert.NoError(tt, err)
		assert.Equal(tt, flexcache.ActionWait, out.SVMPeerAction)
		assert.Equal(tt, sp, out.SVMPeer)
	})

	t.Run("WhenSVMPeerUnknownFallback", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		provider := vsa.NewMockProvider(tt)
		act := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()

		u := "svm-peer-uuid"
		vol := newVolume()
		vol.SvmPeerUUID = &u
		res := &flexcache.CreateFlexCacheResult{DBVolume: vol}

		sp := &vsa.SvmPeer{
			UUID:  u,
			State: "RANDOM_STATE",
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(provider, nil)
		provider.EXPECT().GetSVMPeer(&vol.Svm.Name, &vol.CacheParameters.PeerSvmName).Return(sp, nil)
		logger.EXPECT().Warnf(mock.Anything, mock.Anything, mock.Anything)

		out, err := act.EnsureSVMPeerInOntapActivity(ctx, res)
		assert.NoError(tt, err)
		assert.Equal(tt, flexcache.ActionCreate, out.SVMPeerAction)
	})
}
