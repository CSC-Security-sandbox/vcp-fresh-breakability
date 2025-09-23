package flexcache_activities

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	ontaprestmodel "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/flexcache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

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

		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().CreateClusterPeer(mock.Anything).Return(clusterPeer, nil)

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
				CacheState:      models.FlexCacheV1betaCacheStateCACHESTATEUNSPECIFIED,
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

		vol := baseVolume()
		pass := log.Secret("some-passphrase")
		externalUUID := "cluster-peer-external-uuid"
		expiry := strfmt.DateTime(time.Now().Add(time.Hour))
		clusterPeer := &vsa.ClusterPeer{ExternalUUID: externalUUID, Passphrase: &pass, ExpiryTime: &expiry}
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: vol, ClusterPeer: clusterPeer}

		interclusterLifs := []*vsa.InterclusterLif{{Address: ontaprestmodel.IPAddress("10.0.0.1")}, {Address: ontaprestmodel.IPAddress("10.0.0.2")}}

		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().GetInterclusterLIFs(vsa.InterclusterServicePolicyName).Return(interclusterLifs, nil)
		mockStorage.On("UpdateVolumeFields", ctx, vol.UUID, mock.MatchedBy(func(updates map[string]interface{}) bool {
			return updates["cache_parameters"] != nil && updates["cluster_peer_uuid"] != nil
		})).Return(nil)

		res, err := activity.UpdateFlexCacheVolumeForClusterPeeringActivity(ctx, flexcacheResult)
		assert.NoError(tt, err)
		assert.Equal(tt, externalUUID, *res.DBVolume.ClusterPeerUUID)
		assert.NotNil(tt, res.DBVolume.CacheParameters.Command)
		assert.Contains(tt, *res.DBVolume.CacheParameters.Command, "cluster peer create")
		assert.Equal(tt, models.FlexCacheV1betaCacheStatePENDINGCLUSTERPEERING, res.DBVolume.CacheParameters.CacheState)
		assert.Equal(tt, models.FlexCacheV1betaCacheStateCACHESTATEUNSPECIFIED, res.DBVolume.CacheParameters.PreviousCacheState)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		activity := &FlexCacheVolumeCreateActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		vol := baseVolume()
		flexcacheResult := &flexcache.CreateFlexCacheResult{DBVolume: vol}
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
