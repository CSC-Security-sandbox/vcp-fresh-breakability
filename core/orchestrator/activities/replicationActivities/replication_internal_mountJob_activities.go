package replicationActivities

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	failedStates = []string{models.SnapmirrorRelationshipFailed, models.SnapmirrorRelationshipAborted, models.SnapmirrorRelationshipHardAborted}
)

type MountJobActivity struct {
	SE database.Storage
}

func (j *MountJobActivity) CheckMountJob(ctx context.Context, dbReplication *datamodel.VolumeReplication, node *models.Node, accountName string) error {
	logger := util.GetLogger(ctx)
	provider, err := activitiesGetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	replication := convertToSnapmirrorGetParams(dbReplication, accountName)
	snapmirror, err := provider.GetVolumeReplication(replication)
	if err != nil {
		logger.Errorf("Failed to get replication details from Ontap for replication %s: %v", dbReplication.UUID, err)
		return utilErrors.NewNonRetryableErr(err.Error())
	}
	if snapmirror.MirrorState == models.OntapSnapmirrored {
		logger.Infof("Status is snapmirrored. External_UUID: %s", replication.ExternalUUID)
		return nil
	} else {
		if slices.Contains(failedStates, snapmirror.RelationshipStatus) {
			logger.Infof("Replication %s is in a failed state: %s", dbReplication.UUID, snapmirror.RelationshipStatus)
			err = errors.New("replication is in a failed state: " + snapmirror.RelationshipStatus)
			return utilErrors.NewNonRetryableErr(err.Error())
		}

		logger.Info("Status is not snapmirrored yet.", "External_UUID", replication.ExternalUUID, "state", snapmirror.MirrorState)
		return errors.New("replication is not in snapmirrored state yet")
	}
}

func (j *MountJobActivity) GetReplicationFromOntap(ctx context.Context, dbReplication *datamodel.VolumeReplication, node *models.Node, accountName string) (*datamodel.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	provider, err := activitiesGetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	replicationParams := convertToSnapmirrorGetParams(dbReplication, accountName)
	ontapRep, err := provider.GetReplicationDetails(ctx, replicationParams)
	if err != nil {
		logger.Errorf("Failed to get replication details from Ontap for replication %s: %v", dbReplication.UUID, err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrFailedToGetSnapmirrorDetailsFromOntapMountJob, err)
	}
	replication := addVsaModelReplicationDetailsToDatamodelReplication(ontapRep, dbReplication)
	return replication, nil
}

func (j *MountJobActivity) UpdateReplicationInDB(ctx context.Context, replication *datamodel.VolumeReplication) error {
	se := j.SE
	logger := util.GetLogger(ctx)

	err := se.UpdateVolumeReplicationTransferStats(ctx, replication)
	if err != nil {
		logger.Errorf("Failed to update replication %s in DB: %v", replication.UUID, err)
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	return nil
}

func (j *MountJobActivity) GetReplication(ctx context.Context, uuid string) (*datamodel.VolumeReplication, error) {
	se := j.SE

	replication, err := se.GetVolumeReplication(ctx, uuid)
	if err != nil {
		return nil, err
	}
	return replication, nil
}

func (j *MountJobActivity) GetLunDetailsFromOntap(ctx context.Context, replication *datamodel.VolumeReplication, node *models.Node) (*vsa.LunResponse, error) {
	logger := util.GetLogger(ctx)
	provider, err := activitiesGetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	lunParams := vsa.LunGetParams{
		SvmName:    replication.ReplicationAttributes.DestinationSvmName,
		VolumeName: replication.ReplicationAttributes.DestinationVolumeName,
		LunName:    "lun_" + replication.ReplicationAttributes.SourceVolumeName,
	}
	lunDetails, err := provider.LunGet(lunParams)
	if err != nil {
		logger.Errorf("Failed to get LUN details from Ontap %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrFailedToGetLunDetailsFromOntap, err)
	}
	return lunDetails, nil
}

func (j *MountJobActivity) UpdateVolumeLunDetailsInDB(ctx context.Context, replication *datamodel.VolumeReplication, lunDetails *vsa.LunResponse) error {
	se := j.SE
	updates := make(map[string]interface{})
	if replication.Volume.VolumeAttributes != nil && replication.Volume.VolumeAttributes.BlockDevices != nil {
		blockDevices := *replication.Volume.VolumeAttributes.BlockDevices

		lunPath := strings.Split(lunDetails.Name, "/")
		lunName := lunPath[len(lunPath)-1]
		blockDevices[0].Name = lunName
		blockDevices[0].Size = lunDetails.Size
		blockDevices[0].LunUUID = lunDetails.ExternalUUID
		blockDevices[0].Identifier = lunDetails.SerialNumber

		replication.Volume.VolumeAttributes.BlockDevices = &blockDevices
	}
	updates["volume_attributes"] = replication.Volume.VolumeAttributes
	err := se.UpdateVolumeFields(ctx, replication.ReplicationAttributes.DestinationVolumeUUID, updates)
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}
