package gcp

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// checkDeleteProtectionAtDeleteAPI mirrors the delete-protection handling inlined in DeleteVolume.
func checkDeleteProtectionAtDeleteAPI(ctx context.Context, se database.Storage, volume *datamodel.Volume) error {
	if !enableVolDeleteProtection {
		return nil
	}
	if volume != nil && volume.VolumeAttributes != nil && utils.IsNFSProtocols(volume.VolumeAttributes.Protocols) {
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

	t.Run("NFS with restriction skips API check", func(t *testing.T) {
		volume := &datamodel.Volume{
			Name: "nfs-vol",
			Svm:  &datamodel.Svm{Name: "svm-1"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols:         []string{utils.ProtocolNFSv3},
				RestrictedActions: []string{activities.RestrictedActionDelete},
				ExternalUUID:      "ext-uuid",
			},
		}
		assert.NoError(t, checkDeleteProtectionAtDeleteAPI(ctx, &database.MockStorage{}, volume))
	})

	t.Run("Dual protocol NFS+SMB with restriction skips API check", func(t *testing.T) {
		volume := &datamodel.Volume{
			Name: "dual-vol",
			Svm:  &datamodel.Svm{Name: "svm-1"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols:         []string{utils.ProtocolNFSv3, utils.ProtocolSMB},
				RestrictedActions: []string{activities.RestrictedActionDelete},
				ExternalUUID:      "ext-uuid",
			},
		}
		assert.NoError(t, checkDeleteProtectionAtDeleteAPI(ctx, &database.MockStorage{}, volume))
	})

	t.Run("NFS with restriction skips API check even when no nodes", func(t *testing.T) {
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
		assert.NoError(t, checkDeleteProtectionAtDeleteAPI(ctx, se, volume))
	})

	t.Run("NFS with connected clients skips API check", func(t *testing.T) {
		volume := &datamodel.Volume{
			Name: "nfs-vol",
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 7, UUID: "pool-uuid"}},
			Svm: &datamodel.Svm{Name: "svm-1"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols:         []string{utils.ProtocolNFSv3},
				RestrictedActions: []string{activities.RestrictedActionDelete},
				ExternalUUID:      "ext-uuid",
			},
		}
		assert.NoError(t, checkDeleteProtectionAtDeleteAPI(ctx, &database.MockStorage{}, volume))
	})

	t.Run("NFS with storage error path is skipped at API", func(t *testing.T) {
		volume := &datamodel.Volume{
			Name: "nfs-vol",
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 9}},
			Svm:  &datamodel.Svm{Name: "svm-1"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols:         []string{utils.ProtocolNFSv3},
				RestrictedActions: []string{activities.RestrictedActionDelete},
				ExternalUUID:      "ext-uuid",
			},
		}
		assert.NoError(t, checkDeleteProtectionAtDeleteAPI(ctx, &database.MockStorage{}, volume))
	})
}
