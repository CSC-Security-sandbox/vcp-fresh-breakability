package activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var prepareFieldsForUpdate = getUpdatedFieldsFromParams

type VolumeUpdateActivity struct {
	SE database.Storage
}

// UpdateVolumeInONTAP updates the volume in ONTAP
func (a *VolumeUpdateActivity) UpdateVolumeInONTAP(ctx context.Context, volume *datamodel.Volume, params *common.UpdateVolumeParams, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider := GetProviderByNode(node)
	err := provider.UpdateVolume(vsa.UpdateVolumeParams{
		// Set the necessary parameters for updating the volume
		UUID: volume.VolumeAttributes.ExternalUUID,
		Size: params.QuotaInBytes,
	})
	if err != nil {
		logger.Errorf("Failed to update volume %s in ontap: %v", volume.Name, err)
		return err
	}

	logger.Debugf("Volume %s updated successfully in ontap", volume.Name)
	return nil
}

// UpdateLun updates the LUN associated with the volume in the VSA cluster
func (a *VolumeUpdateActivity) UpdateLun(ctx context.Context, volume *datamodel.Volume, params *common.UpdateVolumeParams, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider := GetProviderByNode(node)
	lunName := utils.GetLunName(volume.Name)
	err := provider.LunUpdate(vsa.LunUpdateParams{
		// Set the necessary parameters for updating the volume
		UUID:       volume.VolumeAttributes.BlockProperties.LunUUID,
		LunName:    lunName,
		VolumeName: volume.Name,
		SvmName:    volume.Svm.Name,
		Size:       params.QuotaInBytes,
	})
	if err != nil {
		if errors.IsConflictErr(err) {
			logger.Debugf("Lun %s size is same as existing size", volume.Name)
			return nil
		}
		logger.Errorf("Failed to update lun %s in vsa cluster: %v", volume.Name, err)
		return err
	}

	logger.Debugf("Lun %s updated successfully in vsa cluster", volume.Name)
	return nil
}

// UpdateVolumeInDB updates the volume in the database with the new parameters
func (a *VolumeUpdateActivity) UpdateVolumeInDB(ctx context.Context, volume *datamodel.Volume, params *common.UpdateVolumeParams) error {
	logger := util.GetLogger(ctx)

	updatedFields := prepareFieldsForUpdate(volume, params)
	// Update the volume in the database
	err := a.SE.UpdateVolumeFields(ctx, volume.UUID, updatedFields)
	if err != nil {
		logger.Errorf("Failed to update volume %s in the database: %v", volume.Name, err)
		return err
	}

	logger.Debugf("Volume %s updated successfully in the database", volume.Name)
	return nil
}

// getUpdatedFieldsFromParams prepares the fields to be updated in the database
func getUpdatedFieldsFromParams(volume *datamodel.Volume, params *common.UpdateVolumeParams) map[string]interface{} {
	updates := make(map[string]interface{})
	if params.Description != "" {
		updates["description"] = params.Description
	}

	if params.QuotaInBytes != 0 {
		updates["size_in_bytes"] = params.QuotaInBytes
	}

	if params.Labels != nil {
		jsonbLabels := make(datamodel.JSONB)
		for k, v := range params.Labels {
			jsonbLabels[k] = v
		}
		if volume.VolumeAttributes == nil {
			volume.VolumeAttributes = &datamodel.VolumeAttributes{}
		}
		volume.VolumeAttributes.Labels = &jsonbLabels
		updates["volume_attributes"] = volume.VolumeAttributes
	}

	updates["state"] = models.LifeCycleStateREADY
	updates["state_details"] = models.LifeCycleStateAvailableDetails

	return updates
}
