package activities

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

const deleteProtectionSANHostGroupMessage = "Cannot delete the volume because it is associated with one or more host groups. Detach it from all host groups and try again."

// enableSanVolDeleteProtection enables classic SAN host-group delete protection.
// When true, delete is blocked whenever a host group is still attached (regardless of restricted_actions).
// When false, delete is blocked on host-group attachment only when restricted_actions contains DELETE.
var enableSanVolDeleteProtection = env.GetBool("SAN_VOL_DELETE_PROTECTION_ENABLED", true)

// CheckDeleteProtection enforces delete-protection rules before a volume delete proceeds.
//
// Returns nil when delete is allowed. Otherwise returns an error:
//   - *vsaerrors.CustomError with tracking 7017 for policy denials (and other VCP errors for ONTAP failures)
//   - customerrors for setup/validation issues (e.g. missing pool) at synchronous API time
//
// Pass node from the workflow (after CommonActivities.GetNode + hyperscaler.CreateNodeForProvider).
// At delete API time pass node=nil and se to resolve pool nodes the same way as DeleteVolumeWorkflow.
func CheckDeleteProtection(ctx context.Context, volume *datamodel.Volume, node *models.Node, se database.Storage) error {
	if volume == nil || volume.VolumeAttributes == nil {
		return nil
	}
	attrs := volume.VolumeAttributes

	// SAN: host-group delete rules depend on SAN_VOL_DELETE_PROTECTION_ENABLED.
	if utils.IsSanProtocols(attrs.Protocols) {
		if shouldBlockSanDeleteForAttachedHostGroup(attrs) {
			return newDeleteProtectionError(deleteProtectionSANHostGroupMessage)
		}
		return nil
	}

	if !HasVolumeDeleteRestriction(attrs) {
		return nil
	}

	// SMB-only: DELETE restriction is not supported.
	if utils.IsSMBOnlyProtocols(attrs.Protocols) {
		return newDeleteProtectionError(fmt.Sprintf("volume %s has DELETE restriction which is not supported for SMB volumes", volume.Name))
	}

	// NFS (including dual NFS+SMB).
	if utils.IsNFSProtocols(attrs.Protocols) {
		ontapNode := node
		if ontapNode == nil {
			if se == nil {
				return vsaerrors.NewVCPError(
					vsaerrors.ErrInternalServerError,
					customerrors.NewNonRetryableErr("ONTAP node is required for NFS delete protection check"),
				)
			}
			var err error
			ontapNode, err = vsa.GetOntapNode(ctx, se, volume)
			if err != nil {
				return err
			}
		}

		clients, err := nfsClientsForVolume(ctx, ontapNode, volume)
		if err != nil {
			return vsaerrors.NewVCPError(vsaerrors.ErrInternalServerError, err)
		}
		if len(clients) > 0 {
			return newDeleteProtectionError("")
		}
	}

	return nil
}

func newDeleteProtectionError(message string) *vsaerrors.CustomError {
	err := vsaerrors.NewVCPError(vsaerrors.ErrDeleteVolumeRestrictedAction, nil)
	if message != "" {
		err.Message = message
	}
	return err
}

func nfsClientsForVolume(ctx context.Context, node *models.Node, volume *datamodel.Volume) ([]*ontapRest.NfsClients, error) {
	attrs := volume.VolumeAttributes
	if volume.Svm == nil {
		return nil, customerrors.New("volume SVM is not set")
	}
	if attrs == nil || attrs.ExternalUUID == "" {
		return nil, customerrors.New("volume external UUID is not set")
	}

	provider, err := vsa.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, err
	}
	restClient, err := provider.CreateRESTClient()
	if err != nil {
		return nil, err
	}

	svmName := volume.Svm.Name
	return restClient.NAS().NfsClientsGet(&ontapRest.NfsClientsGetParams{
		VolumeUUID: &attrs.ExternalUUID,
		SvmName:    &svmName,
	})
}

func shouldBlockSanDeleteForAttachedHostGroup(attrs *datamodel.VolumeAttributes) bool {
	if enableSanVolDeleteProtection {
		return sanVolumeHasHostGroupAttachedToLUN(attrs)
	}
	if !HasVolumeDeleteRestriction(attrs) {
		return false
	}
	return sanVolumeHasHostGroupAttachedToLUN(attrs)
}

func sanVolumeHasHostGroupAttachedToLUN(attrs *datamodel.VolumeAttributes) bool {
	if attrs == nil {
		return false
	}
	if attrs.BlockProperties != nil && hasHostGroupDetailsAttached(attrs.BlockProperties.HostGroupDetails) {
		return true
	}
	if attrs.BlockDevices != nil {
		for _, blockDevice := range *attrs.BlockDevices {
			if hasHostGroupDetailsAttached(blockDevice.HostGroupDetails) {
				return true
			}
		}
	}
	return false
}

func hasHostGroupDetailsAttached(details []datamodel.HostGroupDetail) bool {
	for _, detail := range details {
		if detail.HostGroupUUID != "" {
			return true
		}
	}
	return false
}
