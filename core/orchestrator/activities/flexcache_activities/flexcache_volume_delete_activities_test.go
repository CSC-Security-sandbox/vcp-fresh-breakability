package flexcache_activities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/flexcache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestFlexCacheVolumeDeleteActivity_UnmountVolumeInOntapActivity(t *testing.T) {
	dbVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Name:      "test-volume",
		Svm:       &datamodel.Svm{Name: "svm-name"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}
	node := &models.Node{
		Name: "test-node",
	}

	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.DeleteFlexCacheResult{
			DBVolume: dbVolume,
			Node:     node,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().UnmountVolume("external-uuid").Return(&vsa.OntapAsyncResponse{JobUUID: "unmount-job-uuid"}, nil)
		logger.EXPECT().Debugf("FlexCache volume unmount job for volume with UUID %s initiated successfully", "volume-uuid")

		resp, err := activity.UnmountVolumeInOntapActivity(ctx, flexcacheResult)

		assert.NoError(tt, err, "UnmountVolumeInOntapActivity should complete successfully")
		assert.NotNil(tt, resp)
		assert.NotNil(tt, resp.UnmountJobResponse)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.DeleteFlexCacheResult{
			DBVolume: dbVolume,
			Node:     node,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(nil, assert.AnError)

		resp, err := activity.UnmountVolumeInOntapActivity(ctx, flexcacheResult)

		assert.Error(tt, err, "Function should return an error when GetProviderByNode fails")
		assert.Nil(tt, resp)
	})

	t.Run("WhenUnmountVolumeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.DeleteFlexCacheResult{
			DBVolume: dbVolume,
			Node:     node,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().UnmountVolume("external-uuid").Return(nil, assert.AnError)

		resp, err := activity.UnmountVolumeInOntapActivity(ctx, flexcacheResult)

		assert.Error(tt, err, "Function should return an error when UnmountVolume fails")
		assert.Nil(tt, resp)
	})

	t.Run("WhenNoExternalUUID", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.DeleteFlexCacheResult{
			DBVolume: &datamodel.Volume{
				Name: "test-volume",
				Svm:  &datamodel.Svm{Name: "svm-name"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "",
				},
			},
			Node: node,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		logger.EXPECT().Debug("no external UUID found for the volume, skipping unmount")

		resp, err := activity.UnmountVolumeInOntapActivity(ctx, flexcacheResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
	})
}

func TestFlexCacheVolumeDeleteActivity_DeleteFlexCacheVolumeInOntapActivity(t *testing.T) {
	dbVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Name:      "test-volume",
		Svm:       &datamodel.Svm{Name: "svm-name"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
		},
	}
	node := &models.Node{
		Name: "test-node",
	}

	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.DeleteFlexCacheResult{
			DBVolume: dbVolume,
			Node:     node,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().DeleteFlexCacheVolume("external-uuid", "test-volume").Return(&vsa.OntapAsyncResponse{JobUUID: "delete-job-uuid"}, nil)
		logger.EXPECT().Debugf("FlexCache volume delete job for volume with UUID %s initiated successfully", "volume-uuid")

		resp, err := activity.DeleteFlexCacheVolumeInOntapActivity(ctx, flexcacheResult)

		assert.NoError(tt, err, "DeleteFlexCacheVolumeInOntapActivity should complete successfully")
		assert.NotNil(tt, resp)
		assert.NotNil(tt, resp.DeleteJobResponse)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.DeleteFlexCacheResult{
			DBVolume: dbVolume,
			Node:     node,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(nil, assert.AnError)

		resp, err := activity.DeleteFlexCacheVolumeInOntapActivity(ctx, flexcacheResult)

		assert.Error(tt, err, "Function should return an error when GetProviderByNode fails")
		assert.Nil(tt, resp)
	})

	t.Run("WhenDeleteFlexCacheVolumeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.DeleteFlexCacheResult{
			DBVolume: dbVolume,
			Node:     node,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().DeleteFlexCacheVolume("external-uuid", "test-volume").Return(nil, assert.AnError)

		resp, err := activity.DeleteFlexCacheVolumeInOntapActivity(ctx, flexcacheResult)

		assert.Error(tt, err, "Function should return an error when DeleteFlexCacheVolume fails")
		assert.Nil(tt, resp)
	})

	t.Run("WhenNoExternalUUID", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: database.NewMockStorage(tt)}
		ctx := context.Background()
		flexcacheResult := &flexcache.DeleteFlexCacheResult{
			DBVolume: &datamodel.Volume{
				Name: "test-volume",
				Svm:  &datamodel.Svm{Name: "svm-name"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "",
				},
			},
			Node: node,
		}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		logger.EXPECT().Debug("no external UUID found for the volume, skipping delete")

		resp, err := activity.DeleteFlexCacheVolumeInOntapActivity(ctx, flexcacheResult)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Nil(tt, resp.DeleteJobResponse)
	})
}

func TestFlexCacheVolumeDeleteActivity_DeleteSVMPeeringInOntapActivity(t *testing.T) {
	baseVolume := func() *datamodel.Volume {
		return &datamodel.Volume{
			BaseModel:        datamodel.BaseModel{UUID: "volume-uuid-svm-peer"},
			Name:             "flexcache-vol",
			Svm:              &datamodel.Svm{Name: "local-svm"},
			VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "external-uuid"},
		}
	}

	createNode := func() *models.Node { return &models.Node{Name: "test-node"} }

	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		ctx := context.Background()
		vol := baseVolume()
		vol.CacheParameters = &datamodel.CacheParameters{PeerSvmName: "peer-svm"}
		result := &flexcache.DeleteFlexCacheResult{DBVolume: vol, Node: createNode()}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().GetSVMPeer(&vol.Svm.Name, &vol.CacheParameters.PeerSvmName).
			Return(&vsa.SvmPeer{UUID: "svm-peer-uuid"}, nil)
		mockProvider.EXPECT().DeleteSVMPeer("svm-peer-uuid", false).Return(nil)
		logger.EXPECT().Debugf("SVM peering with UUID %s deleted successfully", "svm-peer-uuid")

		resp, err := activity.DeleteSVMPeeringInOntapActivity(ctx, result)
		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		ctx := context.Background()
		vol := baseVolume()
		result := &flexcache.DeleteFlexCacheResult{DBVolume: vol, Node: createNode()}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(nil, assert.AnError)

		resp, err := activity.DeleteSVMPeeringInOntapActivity(ctx, result)
		assert.Error(tt, err)
		assert.Nil(tt, resp)
	})

	t.Run("WhenDeleteSVMPeerFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		ctx := context.Background()
		vol := baseVolume()
		vol.CacheParameters = &datamodel.CacheParameters{PeerSvmName: "peer-svm"}
		result := &flexcache.DeleteFlexCacheResult{DBVolume: vol, Node: createNode()}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().GetSVMPeer(&vol.Svm.Name, &vol.CacheParameters.PeerSvmName).
			Return(&vsa.SvmPeer{UUID: "svm-peer-uuid"}, nil)
		mockProvider.EXPECT().DeleteSVMPeer("svm-peer-uuid", false).Return(assert.AnError)

		resp, err := activity.DeleteSVMPeeringInOntapActivity(ctx, result)
		assert.Error(tt, err)
		assert.Nil(tt, resp)
	})

	t.Run("WhenNoSvmPeerUUID", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		ctx := context.Background()
		vol := baseVolume()
		vol.CacheParameters = &datamodel.CacheParameters{PeerSvmName: "peer-svm"}
		result := &flexcache.DeleteFlexCacheResult{DBVolume: vol, Node: createNode()}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().GetSVMPeer(&vol.Svm.Name, &vol.CacheParameters.PeerSvmName).
			Return(&vsa.SvmPeer{UUID: "svm-peer-uuid"}, assert.AnError)

		resp, err := activity.DeleteSVMPeeringInOntapActivity(ctx, result)
		assert.Error(tt, err)
		assert.Nil(tt, resp)
	})

	t.Run("FlexCacheInUse", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		provider := vsa.NewMockProvider(tt)
		storage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: storage}
		ctx := context.Background()
		vol := baseVolume()
		vol.CacheParameters = &datamodel.CacheParameters{PeerSvmName: "peer-svm"}
		res := &flexcache.DeleteFlexCacheResult{DBVolume: vol, Node: createNode()}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(provider, nil)
		provider.EXPECT().
			GetSVMPeer(&vol.Svm.Name, &vol.CacheParameters.PeerSvmName).
			Return(&vsa.SvmPeer{UUID: "svm-peer-uuid"}, nil)
		inUseErr := customerrors.New("The peer relationship is in use by FlexCache: details")
		provider.EXPECT().DeleteSVMPeer("svm-peer-uuid", false).Return(inUseErr)
		logger.EXPECT().
			Infof("Skipping SVM peer delete for %s: still in use (%s); leaving svm_peer_uuid unchanged", "svm-peer-uuid", inUseErr.Error())

		out, err := activity.DeleteSVMPeeringInOntapActivity(ctx, res)
		assert.NoError(tt, err)
		assert.Equal(tt, res, out)
		storage.AssertExpectations(tt)
	})

	t.Run("SnapMirrorInUse", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		provider := vsa.NewMockProvider(tt)
		storage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: storage}
		ctx := context.Background()
		vol := baseVolume()
		vol.CacheParameters = &datamodel.CacheParameters{PeerSvmName: "peer-svm"}
		res := &flexcache.DeleteFlexCacheResult{DBVolume: vol, Node: createNode()}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(provider, nil)
		provider.EXPECT().
			GetSVMPeer(&vol.Svm.Name, &vol.CacheParameters.PeerSvmName).
			Return(&vsa.SvmPeer{UUID: "svm-peer-uuid"}, nil)
		inUseErr := customerrors.New("Relationship is in use by SnapMirror in local cluster: details")
		provider.EXPECT().DeleteSVMPeer("svm-peer-uuid", false).Return(inUseErr)
		logger.EXPECT().
			Infof("Skipping SVM peer delete for %s: still in use (%s); leaving svm_peer_uuid unchanged", "svm-peer-uuid", inUseErr.Error())

		out, err := activity.DeleteSVMPeeringInOntapActivity(ctx, res)
		assert.NoError(tt, err)
		assert.Equal(tt, res, out)
		storage.AssertExpectations(tt)
	})
}

func TestFlexCacheVolumeDeleteActivity_DeleteClusterPeerInOntapActivity(t *testing.T) {
	baseVolume := func() *datamodel.Volume {
		return &datamodel.Volume{
			BaseModel:        datamodel.BaseModel{UUID: "volume-uuid-cluster-peer"},
			Name:             "flexcache-vol",
			Svm:              &datamodel.Svm{Name: "local-svm"},
			VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "external-uuid"},
		}
	}
	createNode := func() *models.Node { return &models.Node{Name: "test-node"} }
	clusterPeerUUID := "cluster-peer-uuid"
	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		ctx := context.Background()
		vol := baseVolume()
		result := &flexcache.DeleteFlexCacheResult{DBVolume: vol, Node: createNode(),
			ClusterPeeringRow: &datamodel.ClusterPeerings{OntapPeerUUID: clusterPeerUUID}}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().DeleteClusterPeer("cluster-peer-uuid").Return(nil)
		logger.EXPECT().Debugf("Cluster peering with UUID %s deleted successfully", "cluster-peer-uuid")

		resp, err := activity.DeleteClusterPeerInOntapActivity(ctx, result)
		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		ctx := context.Background()
		vol := baseVolume()
		result := &flexcache.DeleteFlexCacheResult{DBVolume: vol, Node: createNode()}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(nil, assert.AnError)

		resp, err := activity.DeleteClusterPeerInOntapActivity(ctx, result)
		assert.Error(tt, err)
		assert.Nil(tt, resp)
	})

	t.Run("WhenDeleteClusterPeerFails", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		ctx := context.Background()
		vol := baseVolume()
		result := &flexcache.DeleteFlexCacheResult{DBVolume: vol, Node: createNode(),
			ClusterPeeringRow: &datamodel.ClusterPeerings{OntapPeerUUID: clusterPeerUUID}}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)
		mockProvider.EXPECT().DeleteClusterPeer("cluster-peer-uuid").Return(assert.AnError)

		resp, err := activity.DeleteClusterPeerInOntapActivity(ctx, result)
		assert.Error(tt, err)
		assert.Nil(tt, resp)
	})

	t.Run("WhenNoClusterPeerUUID", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		logger := log.NewMockLogger(tt)
		mockProvider := vsa.NewMockProvider(tt)
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		ctx := context.Background()
		vol := baseVolume()
		result := &flexcache.DeleteFlexCacheResult{DBVolume: vol, Node: createNode(),
			ClusterPeeringRow: &datamodel.ClusterPeerings{OntapPeerUUID: ""}}

		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mm.EXPECT().hyperscalerGetProviderByNode(ctx, mock.Anything).Return(mockProvider, nil)

		resp, err := activity.DeleteClusterPeerInOntapActivity(ctx, result)
		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
	})
}

func TestFlexCacheVolumeDeleteActivity_DeleteClusterPeeringRowInDBActivity(t *testing.T) {
	ctx := context.Background()

	makeVolume := func() *datamodel.Volume {
		return &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid-del-cpr"},
			Name:      "flexcache-vol",
			AccountID: 11,
			PoolID:    22,
			CacheParameters: &datamodel.CacheParameters{
				PeerClusterName: "peer-cluster-A",
			},
		}
	}

	makeRow := func() *datamodel.ClusterPeerings {
		return &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{ID: 77, UUID: "cluster-peering-row-uuid"},
			AccountID: 11,
			PoolID:    22,
			State:     "PEERED",
		}
	}

	t.Run("Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		vol := makeVolume()
		row := makeRow()
		result := &flexcache.DeleteFlexCacheResult{DBVolume: vol, ClusterPeeringRow: row}

		logger := log.NewMockLogger(tt)
		mm := newMonkeyMockAndPatch(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(logger)

		mockStorage.EXPECT().
			DeleteClusterPeeringRow(ctx, row).
			Return(nil).Once()

		logger.EXPECT().Debugf("Cluster peering row with ID %d deleted successfully", row.ID)

		resp, err := activity.DeleteClusterPeeringRowInDBActivity(ctx, result)
		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("DeleteClusterPeeringRowFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		vol := makeVolume()
		row := makeRow()
		result := &flexcache.DeleteFlexCacheResult{DBVolume: vol, ClusterPeeringRow: row}

		logger := log.NewMockLogger(tt)
		mm := newMonkeyMockAndPatch(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(logger)

		mockStorage.EXPECT().
			DeleteClusterPeeringRow(ctx, row).
			Return(assert.AnError).Once()

		resp, err := activity.DeleteClusterPeeringRowInDBActivity(ctx, result)
		assert.Error(tt, err)
		assert.Nil(tt, resp)
		mockStorage.AssertExpectations(tt)
	})
}

func TestFlexCacheVolumeDeleteActivity_GetFlexCacheAndReplicationCountsOnClusterPeeringActivity(t *testing.T) {
	ctx := context.Background()

	makeVolume := func() *datamodel.Volume {
		return &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid-counts"},
			Name:      "flexcache-vol",
			AccountID: 101,
			PoolID:    202,
			CacheParameters: &datamodel.CacheParameters{
				PeerClusterName: "peer-cluster-X",
			},
		}
	}

	makeRow := func() *datamodel.ClusterPeerings {
		return &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{ID: 909, UUID: "cluster-peering-row-counts"},
			AccountID: 101,
			PoolID:    202,
			State:     "PEERED",
		}
	}

	t.Run("Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		vol := makeVolume()
		row := makeRow()
		result := &flexcache.DeleteFlexCacheResult{DBVolume: vol, ClusterPeeringRow: makeRow()}

		mockStorage.EXPECT().
			GetVolumeReplicationCountByClusterPeerID(ctx, row.ID).
			Return(int64(5), nil).Once()
		mockStorage.EXPECT().
			GetFlexCacheVolumeCountByClusterPeerID(ctx, row.ID).
			Return(int64(3), nil).Once()

		resp, err := activity.GetFlexCacheAndReplicationCountsOnClusterPeeringActivity(ctx, result)
		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, int64(5), resp.VolumeReplicationCountOnClusterPeering)
		assert.Equal(tt, int64(3), resp.FlexCacheVolumeCountOnClusterPeering)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ClusterPeerNotFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		vol := makeVolume()
		result := &flexcache.DeleteFlexCacheResult{DBVolume: vol}

		resp, err := activity.GetFlexCacheAndReplicationCountsOnClusterPeeringActivity(ctx, result)
		assert.NoError(tt, err, "Should not error when cluster peer is not found")
		assert.NotNil(tt, resp)
		assert.Equal(tt, int64(0), resp.VolumeReplicationCountOnClusterPeering)
		assert.Equal(tt, int64(0), resp.FlexCacheVolumeCountOnClusterPeering)
		assert.Nil(tt, resp.ClusterPeeringRow, "ClusterPeeringRow should remain nil when not found")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ReplicationCountFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		vol := makeVolume()
		row := makeRow()
		result := &flexcache.DeleteFlexCacheResult{DBVolume: vol, ClusterPeeringRow: row}

		mockStorage.EXPECT().
			GetVolumeReplicationCountByClusterPeerID(ctx, row.ID).
			Return(int64(0), assert.AnError).Once()

		resp, err := activity.GetFlexCacheAndReplicationCountsOnClusterPeeringActivity(ctx, result)
		assert.Error(tt, err)
		assert.Nil(tt, resp)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("FlexCacheCountFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		vol := makeVolume()
		row := makeRow()
		result := &flexcache.DeleteFlexCacheResult{DBVolume: vol, ClusterPeeringRow: row}

		mockStorage.EXPECT().
			GetVolumeReplicationCountByClusterPeerID(ctx, row.ID).
			Return(int64(7), nil).Once()
		mockStorage.EXPECT().
			GetFlexCacheVolumeCountByClusterPeerID(ctx, row.ID).
			Return(int64(0), assert.AnError).Once()

		resp, err := activity.GetFlexCacheAndReplicationCountsOnClusterPeeringActivity(ctx, result)
		assert.Error(tt, err)
		assert.Nil(tt, resp)
		mockStorage.AssertExpectations(tt)
	})
}

func TestFlexCacheVolumeDeleteActivity_UpdateClusterPeeringRowStateDeletedInDBActivity(t *testing.T) {
	ctx := context.Background()

	makeRow := func() *datamodel.ClusterPeerings {
		return &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{UUID: "cluster-peering-row-uuid"},
			AccountID: 11,
			PoolID:    22,
			State:     "PEERED",
		}
	}

	makeResult := func(row *datamodel.ClusterPeerings) *flexcache.DeleteFlexCacheResult {
		return &flexcache.DeleteFlexCacheResult{
			DBVolume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "vol-uuid-del-cpr"},
				AccountID: 11,
				PoolID:    22,
			},
			ClusterPeeringRow: row,
		}
	}

	t.Run("Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		row := makeRow()
		result := makeResult(row)

		logger := log.NewMockLogger(tt)
		mm := newMonkeyMockAndPatch(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mockStorage.
			On("UpdateClusterPeeringRow", ctx, mock.MatchedBy(func(r *datamodel.ClusterPeerings) bool {
				return r == row && r.State == models.CvpClusterPeeringStatusDELETED
			})).
			Return(nil).Once()
		logger.EXPECT().Infof("Cluster peering row with UUID %s updated to state %s", row.UUID, models.CvpClusterPeeringStatusDELETED)

		updated, err := activity.UpdateClusterPeeringRowStateDeletedInDBActivity(ctx, result)
		assert.NoError(tt, err)
		assert.NotNil(tt, updated)
		assert.Equal(tt, models.CvpClusterPeeringStatusDELETED, updated.ClusterPeeringRow.State)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("UpdateFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		row := makeRow()
		result := makeResult(row)

		logger := log.NewMockLogger(tt)
		mm := newMonkeyMockAndPatch(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		mockStorage.
			On("UpdateClusterPeeringRow", ctx, mock.MatchedBy(func(r *datamodel.ClusterPeerings) bool {
				return r == row && r.State == models.CvpClusterPeeringStatusDELETED
			})).
			Return(assert.AnError).Once()
		logger.EXPECT().Errorf("Failed to update cluster peering row in DB: %v", assert.AnError)

		updated, err := activity.UpdateClusterPeeringRowStateDeletedInDBActivity(ctx, result)
		assert.Error(tt, err)
		assert.Nil(tt, updated)
		mockStorage.AssertExpectations(tt)
	})
}

// go
func TestFlexCacheVolumeDeleteActivity_GetClusterPeeringFromDBActivity(t *testing.T) {
	ctx := context.Background()

	makeVolume := func() *datamodel.Volume {
		return &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
			AccountID: 11,
			PoolID:    22,
			Account:   &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 11}},
			Pool:      &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 22}},
			CacheParameters: &datamodel.CacheParameters{
				PeerClusterName: "peer-cluster-A",
			},
		}
	}

	t.Run("SuccessExistingRow", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		act := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		vol := makeVolume()
		result := &flexcache.DeleteFlexCacheResult{DBVolume: vol}

		existing := &datamodel.ClusterPeerings{
			BaseModel:      datamodel.BaseModel{UUID: "peer-row-1"},
			AccountID:      vol.AccountID,
			PoolID:         vol.PoolID,
			OnprempCluster: vol.CacheParameters.PeerClusterName,
		}

		logger := log.NewMockLogger(tt)
		mm := newMonkeyMockAndPatch(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		logger.EXPECT().Debugf(mock.Anything, mock.Anything).Maybe()

		mockStorage.EXPECT().
			GetClusterPeerByAccountIDExternalClusterAndPoolID(
				mock.Anything,
				vol.AccountID,
				vol.CacheParameters.PeerClusterName,
				vol.PoolID,
			).
			Return(existing, nil).Once()

		out, err := act.GetClusterPeeringFromDBActivity(ctx, result)
		assert.NoError(tt, err)
		assert.NotNil(tt, out)
		assert.Equal(tt, existing, out.ClusterPeeringRow)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("NotFoundReturnsNilRow", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		act := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		vol := makeVolume()
		result := &flexcache.DeleteFlexCacheResult{DBVolume: vol}

		notFoundErr := customerrors.NewNotFoundErr("cluster peering row", nil)

		logger := log.NewMockLogger(tt)
		mm := newMonkeyMockAndPatch(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		logger.EXPECT().
			Debugf(
				"Cluster peering row not found (account=%d cluster=%s pool=%d)",
				vol.AccountID,
				vol.CacheParameters.PeerClusterName,
				vol.PoolID,
			).Once()

		mockStorage.EXPECT().
			GetClusterPeerByAccountIDExternalClusterAndPoolID(
				mock.Anything,
				vol.AccountID,
				vol.CacheParameters.PeerClusterName,
				vol.PoolID,
			).
			Return(nil, notFoundErr).Once()

		out, err := act.GetClusterPeeringFromDBActivity(ctx, result)
		assert.NoError(tt, err)
		assert.NotNil(tt, out)
		assert.Nil(tt, out.ClusterPeeringRow)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("GetError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		act := &FlexCacheVolumeDeleteActivity{SE: mockStorage}
		vol := makeVolume()
		result := &flexcache.DeleteFlexCacheResult{DBVolume: vol}

		logger := log.NewMockLogger(tt)
		mm := newMonkeyMockAndPatch(tt)
		mm.EXPECT().utilGetLogger(ctx).Return(logger)
		logger.EXPECT().
			Errorf(
				"Failed to get cluster peering row from database: %v",
				assert.AnError,
			).Once()

		mockStorage.EXPECT().
			GetClusterPeerByAccountIDExternalClusterAndPoolID(
				mock.Anything,
				vol.AccountID,
				vol.CacheParameters.PeerClusterName,
				vol.PoolID,
			).
			Return(nil, assert.AnError).Once()

		out, err := act.GetClusterPeeringFromDBActivity(ctx, result)
		assert.Error(tt, err)
		assert.Nil(tt, out)
		mockStorage.AssertExpectations(tt)
	})
}
