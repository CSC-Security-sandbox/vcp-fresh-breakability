package gcp

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontap_rest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// checkDeleteProtectionAtDeleteAPI mirrors the delete-protection handling inlined in DeleteVolume.
func checkDeleteProtectionAtDeleteAPI(ctx context.Context, se database.Storage, volume *datamodel.Volume) error {
	if !enableVolDeleteProtection {
		return nil
	}
	protectionErr := activities.CheckDeleteProtection(ctx, volume, nil, se)
	if protectionErr == nil {
		return nil
	}
	var deny *vsaerrors.CustomError
	if errors.As(protectionErr, &deny) && deny.TrackingID == vsaerrors.ErrDeleteVolumeRestrictedAction {
		return customerrors.NewConflictErrWithTrackingID(deny.GetMessage(), deny.TrackingID)
	}
	return protectionErr
}

func TestCheckDeleteProtectionAtDeleteAPI(t *testing.T) {
	ctx := context.Background()

	t.Run("nil volume", func(t *testing.T) {
		assert.NoError(t, checkDeleteProtectionAtDeleteAPI(ctx, &database.MockStorage{}, nil))
	})

	t.Run("nil volume attributes", func(t *testing.T) {
		assert.NoError(t, checkDeleteProtectionAtDeleteAPI(ctx, &database.MockStorage{}, &datamodel.Volume{Name: "v"}))
	})

	t.Run("SAN without host group allows delete", func(t *testing.T) {
		volume := &datamodel.Volume{
			Name: "san-vol",
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols:         []string{utils.ProtocolISCSI},
				RestrictedActions: []string{activities.RestrictedActionDelete},
			},
		}
		assert.NoError(t, checkDeleteProtectionAtDeleteAPI(ctx, &database.MockStorage{}, volume))
	})

	t.Run("SAN host group returns conflict 7017", func(t *testing.T) {
		volume := &datamodel.Volume{
			Name: "san-vol",
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols:         []string{utils.ProtocolISCSI},
				RestrictedActions: []string{activities.RestrictedActionDelete},
				BlockProperties: &datamodel.BlockProperties{
					HostGroupDetails: []datamodel.HostGroupDetail{{HostGroupUUID: "hg-1"}},
				},
			},
		}
		err := checkDeleteProtectionAtDeleteAPI(ctx, &database.MockStorage{}, volume)
		require.Error(t, err)
		assert.True(t, customerrors.IsConflictErr(err))
		var conflict *customerrors.ConflictErr
		require.True(t, errors.As(err, &conflict))
		assert.Equal(t, vsaerrors.ErrDeleteVolumeRestrictedAction, conflict.GetTrackingID())
	})

	t.Run("SMB with DELETE restriction returns conflict 7017", func(t *testing.T) {
		volume := &datamodel.Volume{
			Name: "smb-vol",
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols:         []string{utils.ProtocolSMB},
				RestrictedActions: []string{activities.RestrictedActionDelete},
			},
		}
		err := checkDeleteProtectionAtDeleteAPI(ctx, &database.MockStorage{}, volume)
		require.Error(t, err)
		assert.True(t, customerrors.IsConflictErr(err))
		assert.Contains(t, err.Error(), "SMB")
	})

	t.Run("NFS with restriction missing pool returns validation error", func(t *testing.T) {
		volume := &datamodel.Volume{
			Name: "nfs-vol",
			Svm:  &datamodel.Svm{Name: "svm-1"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols:         []string{utils.ProtocolNFSv3},
				RestrictedActions: []string{activities.RestrictedActionDelete},
				ExternalUUID:      "ext-uuid",
			},
		}
		err := checkDeleteProtectionAtDeleteAPI(ctx, &database.MockStorage{}, volume)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pool is required")
	})

	t.Run("NFS with restriction no nodes returns error", func(t *testing.T) {
		se := &database.MockStorage{}
		poolID := int64(42)
		volume := &datamodel.Volume{
			Name: "nfs-vol",
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: poolID, UUID: "pool-uuid"}},
			Svm:  &datamodel.Svm{Name: "svm-1"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols:         []string{utils.ProtocolNFSv3},
				RestrictedActions: []string{activities.RestrictedActionDelete},
				ExternalUUID:      "ext-uuid",
			},
		}
		se.On("GetNodesByPoolID", ctx, poolID).Return([]*datamodel.Node{}, nil)
		err := checkDeleteProtectionAtDeleteAPI(ctx, se, volume)
		require.Error(t, err)
		se.AssertExpectations(t)
	})

	t.Run("NFS with connected clients returns conflict 7017", func(t *testing.T) {
		se := &database.MockStorage{}
		poolID := int64(7)
		volume := &datamodel.Volume{
			Name: "nfs-vol",
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: poolID, UUID: "pool-uuid"},
				DeploymentName: "deploy",
				PoolCredentials: &datamodel.PoolCredentials{
					Password: "pw",
				},
			},
			Svm: &datamodel.Svm{Name: "svm-1"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols:         []string{utils.ProtocolNFSv3},
				RestrictedActions: []string{activities.RestrictedActionDelete},
				ExternalUUID:      "ext-uuid",
			},
		}
		se.On("GetNodesByPoolID", ctx, poolID).Return([]*datamodel.Node{{
			BaseModel:       datamodel.BaseModel{UUID: "node-uuid"},
			EndpointAddress: "10.0.0.1",
		}}, nil)

		mockNAS := ontap_rest.NewMockNASClient(t)
		mockNAS.On("NfsClientsGet", mock.Anything).Return([]*ontap_rest.NfsClients{{}}, nil).Once()
		mockREST := ontap_rest.NewMockRESTClient(t)
		mockREST.EXPECT().NAS().Return(mockNAS).Once()

		orig := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(context.Context, *models.Node) (vsa.Provider, error) {
			return &deleteProtectionAPIRESTProvider{
				MockProvider: vsa.NewMockProvider(t),
				restClient:   mockREST,
			}, nil
		}
		t.Cleanup(func() { hyperscaler.GetProviderByNode = orig })

		err := checkDeleteProtectionAtDeleteAPI(ctx, se, volume)
		require.Error(t, err)
		assert.True(t, customerrors.IsConflictErr(err))
		assert.Contains(t, err.Error(), "52 hours")
		se.AssertExpectations(t)
	})

	t.Run("GetNodesByPoolID error propagates", func(t *testing.T) {
		se := &database.MockStorage{}
		poolID := int64(9)
		volume := &datamodel.Volume{
			Name: "nfs-vol",
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: poolID}},
			Svm:  &datamodel.Svm{Name: "svm-1"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols:         []string{utils.ProtocolNFSv3},
				RestrictedActions: []string{activities.RestrictedActionDelete},
				ExternalUUID:      "ext-uuid",
			},
		}
		se.On("GetNodesByPoolID", ctx, poolID).Return(nil, errors.New("db error"))
		err := checkDeleteProtectionAtDeleteAPI(ctx, se, volume)
		assert.EqualError(t, err, "db error")
		se.AssertExpectations(t)
	})
}

type deleteProtectionAPIRESTProvider struct {
	*vsa.MockProvider
	restClient ontap_rest.RESTClient
}

func (p *deleteProtectionAPIRESTProvider) CreateRESTClient() (ontap_rest.RESTClient, error) {
	return p.restClient, nil
}
