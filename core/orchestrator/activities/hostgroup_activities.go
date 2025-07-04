package activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type HostGroupUpdateActivity struct {
	SE database.Storage
}

var (
	getAllInitiators        = _getAllInitiators
	isHGResourceUpdated     = _isHGResourceUpdated
	updateHGDetailsInVolume = _updateHGDetailsInVolume
	handleQNsInHostGroup    = _handleQNsInHostGroup
)

func (hgu *HostGroupUpdateActivity) UpdateIGroups(ctx context.Context, hg *datamodel.HostGroup) error {
	logger := util.GetLogger(ctx)

	volumes, err := hgu.SE.GetAllVolumesForHG(ctx, hg.UUID, hg.AccountID)
	if err != nil {
		return err
	}

	var updatedHG = make(map[string]bool)
	for _, volume := range volumes {
		// If the HostGroup resource is already updated for the volume, skip it
		if _, ok := updatedHG[volume.Pool.UUID]; ok {
			// Update db to save the HostGroup details
			if err = updateHGDetailsInVolume(volume, hgu.SE, hg, ctx); err != nil {
				return err
			}
			continue
		}

		// If the db volume QNs and current HostGroup QNs are same then skip updating the hg
		if isHGResourceUpdated(volume.VolumeAttributes.BlockProperties.HostGroupDetails, hg.UUID, hg.Hosts.Hosts) {
			logger.Infof("Host group %s is already up to date for volume %s, skipping update", hg.Name, volume.Name)
			continue
		}
		nodes, err := hgu.SE.GetNodesByPoolID(ctx, volume.PoolID)
		if err != nil {
			logger.Errorf("Failed to get nodes for pool %d: %v", volume.PoolID, err)
			continue
		}

		provider, getErr := GetProviderByNode(ctx, common.CreateNodeForProvider(common.NodeProviderInput{Nodes: nodes, Username: volume.Pool.Username, Password: volume.Pool.Password, SecretID: volume.Pool.SecretID}))
		if getErr != nil {
			return vsaerrors.WrapAsTemporalApplicationError(getErr)
		}
		err = handleQNsInHostGroup(logger, hg, provider)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}

		// Update db to save the HostGroup details
		if err = updateHGDetailsInVolume(volume, hgu.SE, hg, ctx); err != nil {
			return err
		}

		updatedHG[volume.Pool.UUID] = true
	}

	conditions := [][]interface{}{{"account_id = ?", hg.AccountID}}
	pools, err := hgu.SE.ListPools(ctx, conditions)
	if err != nil {
		logger.Errorf("Failed to get pools for account: %s, error: %s", hg.AccountID, err.Error())
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	for _, pool := range pools {
		if _, ok := updatedHG[pool.UUID]; ok {
			continue
		}

		nodes, err := hgu.SE.GetNodesByPoolID(ctx, pool.ID)
		if err != nil {
			logger.Errorf("Failed to get nodes for pool %d: %v", pool.ID, err)
			continue
		}
		provider, getErr := GetProviderByNode(ctx, common.CreateNodeForProvider(common.NodeProviderInput{Nodes: nodes, Username: pool.Username, Password: pool.Password, SecretID: pool.SecretID}))
		if getErr != nil {
			return vsaerrors.WrapAsTemporalApplicationError(getErr)
		}
		err = handleQNsInHostGroup(logger, hg, provider)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
	}

	return nil
}

func _handleQNsInHostGroup(logger log.Logger, hg *datamodel.HostGroup, provider vsa.Provider) error {
	hostGroupExists, hostGroup, err := provider.IgroupExists(hg.Name, nil)
	if err != nil {
		return err
	}
	if !hostGroupExists {
		logger.Infof("Host group %s does not exist", hg.Name)
		return nil
	}

	initiatorsToAdd, initiatorsToDelete := utils.GetArrayDiff(getAllInitiators(hostGroup.IgroupInlineInitiators), hg.Hosts.Hosts)

	logger.Debugf("IQNs diff, Add Initiator: %s and Delete InitiatorsQNs: %s", initiatorsToAdd, initiatorsToDelete)
	if len(initiatorsToAdd) > 0 {
		err = provider.IgroupAddInitiator(vsa.IgroupAddInitiator{
			Initiator:  initiatorsToAdd,
			IgroupUUID: *hostGroup.UUID,
		})
		if err != nil {
			return err
		}
	}

	if len(initiatorsToDelete) > 0 {
		for _, initiatorToDelete := range initiatorsToDelete {
			err = provider.IgroupDeleteInitiator(vsa.IgroupDeleteInitiator{
				InitiatorName: initiatorToDelete,
				IgroupUUID:    *hostGroup.UUID,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func _updateHGDetailsInVolume(volume *datamodel.Volume, se database.Storage, hg *datamodel.HostGroup, ctx context.Context) error {
	for indx, hostDetails := range volume.VolumeAttributes.BlockProperties.HostGroupDetails {
		if hostDetails.HostGroupUUID == hg.UUID {
			volume.VolumeAttributes.BlockProperties.HostGroupDetails[indx].HostQNs = hg.Hosts.Hosts
		}
	}

	if err := se.UpdateVolume(ctx, volume); err != nil {
		return err
	}
	return nil
}

func _isHGResourceUpdated(hgDetails []datamodel.HostGroupDetail, hostUUID string, hosts []string) bool {
	for _, hg := range hgDetails {
		if hg.HostGroupUUID != hostUUID {
			continue
		}

		if !utils.IsSliceEqual(hg.HostQNs, hosts) {
			return false // Host group is not same as the one in db
		}
		return true
	}
	return true
}

func _getAllInitiators(initiators []*models.IgroupInlineInitiatorsInlineArrayItem) []string {
	initiatorNames := make([]string, 0)
	if len(initiators) == 0 {
		return initiatorNames
	}
	for _, initiator := range initiators {
		initiatorNames = append(initiatorNames, *initiator.Name)
	}
	return initiatorNames
}
