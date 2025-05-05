package orchestrator

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
)

var (
	createHostGroup = _createHostGroup
	getHostGroup    = _getHostGroup
)

type CreateHostGroupParams struct {
	Name          string
	Description   string
	HostGroupType string
	Hosts         []string
	OSType        string
	AccountID     string
}

// GetHostGroup retrieves the specified host group and returns it
func (o *Orchestrator) GetHostGroup(ctx context.Context, hostGroupUUID string, accountID string) (*models.HostGroup, error) {
	return getHostGroup(ctx, o.storage, hostGroupUUID, accountID)
}

func _getHostGroup(ctx context.Context, storage database.Storage, hostGroupUUID string, accountID string) (*models.HostGroup, error) {
	account, err := storage.GetAccount(ctx, accountID)
	if err != nil {
		return nil, err
	}

	hostGroup, err := storage.GetHostGroup(ctx, hostGroupUUID, account.ID)
	if err != nil {
		return nil, err
	}

	return convertDatastoreHostGroupToModel(hostGroup, account.Name), nil
}

// CreateHostGroup creates the specified host group and adds it to the list of host group belonging to the specified owner
func (o *Orchestrator) CreateHostGroup(ctx context.Context, params *CreateHostGroupParams) (*models.HostGroup, error) {
	return createHostGroup(ctx, o.storage, params)
}

func _createHostGroup(ctx context.Context, storage database.Storage, params *CreateHostGroupParams) (*models.HostGroup, error) {
	account, err := storage.GetAccount(ctx, params.AccountID)
	if err != nil {
		return nil, err
	}

	hostGroup := &datamodel.HostGroup{
		OSType:        params.OSType,
		Name:          params.Name,
		Description:   params.Description,
		HostGroupType: params.HostGroupType,
		Hosts: datamodel.Hosts{
			Hosts: params.Hosts,
		},
		AccountID: account.ID,
	}

	hostGroup, err = storage.CreateHostGroup(ctx, hostGroup)
	if err != nil {
		return nil, err
	}

	return convertDatastoreHostGroupToModel(hostGroup, account.Name), nil
}

func convertDatastoreHostGroupToModel(hostGroup *datamodel.HostGroup, accountName string) *models.HostGroup {
	return &models.HostGroup{
		BaseModel: models.BaseModel{
			UUID:      hostGroup.UUID,
			CreatedAt: hostGroup.CreatedAt,
			UpdatedAt: hostGroup.UpdatedAt,
			DeletedAt: DeletedAtOrNil(hostGroup.DeletedAt),
		},
		Name:          hostGroup.Name,
		Description:   hostGroup.Description,
		State:         hostGroup.State,
		StateDetails:  hostGroup.StateDetails,
		AccountName:   accountName,
		OSType:        hostGroup.OSType,
		Hosts:         hostGroup.Hosts.Hosts,
		HostGroupType: hostGroup.HostGroupType,
	}
}
