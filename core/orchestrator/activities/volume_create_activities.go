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

const (
	VolumeTypeRW = "rw"
)

type VolumeCreateActivity struct {
	SE *database.Storage
}

func (a *VolumeCreateActivity) CreateVolume(ctx context.Context, volume *datamodel.Volume) (*datamodel.Volume, error) {
	se := *a.SE

	return se.CreateVolume(ctx, volume)
}

func (a *VolumeCreateActivity) CreateVolumeInONTAP(ctx context.Context, volume *datamodel.Volume, node *models.Node) (*vsa.VolumeResponse, error) {
	logger, err := util.GetLogger(ctx)
	if err != nil {
		return nil, err
	}
	provider := GetProviderByNode(node)
	res, err := provider.CreateVolume(vsa.CreateVolumeParams{
		VolumeName:    volume.Name,
		SvmName:       volume.Svm.Name,
		AggregateName: aggregateName,
		Size:          volume.SizeInBytes,
		VolumeType:    VolumeTypeRW,
	})
	if err != nil {
		return nil, err
	}
	logger.Debug("volume created successfully")

	return res, nil
}

func (a *VolumeCreateActivity) CreateIgroup(ctx context.Context, volume *datamodel.Volume, hostParams []*common.HostParams, node *models.Node) error {
	logger, err := util.GetLogger(ctx)
	if err != nil {
		return err
	}
	provider := GetProviderByNode(node)

	// FixMe: What if a new host is added to the host group?
	for _, host := range hostParams {
		igroupExists, err := provider.IgroupExists(host.HostName, volume.Svm.Name)
		if err != nil {
			return err
		}

		if !igroupExists {
			_, err := provider.IgroupCreate(vsa.IgroupCreateParams{
				IgroupName: host.HostName,
				SvmName:    volume.Svm.Name,
				OsType:     host.OsType,
				Initiator:  host.HostIQNs,
			})
			if err != nil {
				return err
			}
			logger.Debug("Igroup created successfully", "name", host.HostName)
		}
	}

	return nil
}

func (a *VolumeCreateActivity) CreateLun(ctx context.Context, volume *datamodel.Volume, node *models.Node, availableSpace int64) (string, error) {
	logger, err := util.GetLogger(ctx)
	if err != nil {
		return "", err
	}
	provider := GetProviderByNode(node)
	halfGiB, _ := utils.ConvertToBytes(0.5, utils.GiB)
	size := availableSpace - halfGiB
	lunName := "lun_" + volume.Name
	lun, err := provider.LunCreate(vsa.LunCreateParams{
		LunName:    lunName,
		VolumeName: volume.Name,
		SvmName:    volume.Svm.Name,
		OsType:     volume.VolumeAttributes.BlockProperties.OSType,
		Size:       size,
	})
	if err != nil {
		return "", err
	}
	logger.Debug("lun created successfully")

	return lun.Name, nil
}

func (a *VolumeCreateActivity) CreateLunMap(ctx context.Context, params *common.CreateLunMapParams, node *models.Node) error {
	logger, err := util.GetLogger(ctx)
	if err != nil {
		return err
	}
	provider := GetProviderByNode(node)
	err = provider.LunMapCreate(vsa.LunMapCreateParams{
		LunName:    params.LunName,
		SvmName:    params.SvmName,
		IGroupName: params.HostNames,
	})
	if err != nil {
		return err
	}
	logger.Debug("lun map created successfully")

	return nil
}

func (a *VolumeCreateActivity) UpdateVolumeDetails(ctx context.Context, volume *datamodel.Volume, volCreateResponse *vsa.ProviderResponse) error {
	se := *a.SE

	volume.VolumeAttributes.ExternalUUID = volCreateResponse.ExternalUUID
	volume.State = models.LifeCycleStateREADY
	volume.StateDetails = models.LifeCycleStateAvailableDetails

	if err := se.UpdateVolume(ctx, volume); err != nil {
		return err
	}

	return nil
}

func (a *VolumeCreateActivity) GetHosts(ctx context.Context, volume *datamodel.Volume) ([]*datamodel.HostGroup, error) {
	se := *a.SE

	if volume.VolumeAttributes.BlockProperties == nil {
		return nil, errors.New("block properties not found")
	}

	uuids := volume.VolumeAttributes.BlockProperties.HostGroupUUIDs

	dbHostGroups, err := se.GetMultipleHostGroups(ctx, uuids, volume.AccountID)
	if err != nil {
		return nil, err
	}

	if len(dbHostGroups) != len(uuids) {
		return nil, errors.New("all host groups could not be found")
	}

	return dbHostGroups, nil
}
