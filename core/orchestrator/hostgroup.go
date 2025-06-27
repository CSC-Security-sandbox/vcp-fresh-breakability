package orchestrator

import (
	"context"
	"database/sql"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

var (
	createHostGroup      = _createHostGroup
	getHostGroup         = _getHostGroup
	deleteHostGroup      = _deleteHostGroup
	getMultipleHostGroup = _getMultipleHostGroup
)

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
func (o *Orchestrator) CreateHostGroup(ctx context.Context, params *common.CreateHostGroupParams) (*models.HostGroup, error) {
	return createHostGroup(ctx, o.storage, params)
}

func _createHostGroup(ctx context.Context, storage database.Storage, params *common.CreateHostGroupParams) (*models.HostGroup, error) {
	account, err := getOrCreateAccount(ctx, storage, params.AccountName)
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

// DeleteHostGroup deletes the host group with the specified UUID
func (o *Orchestrator) DeleteHostGroup(ctx context.Context, accountName string, hostGroupUUID string) (*models.HostGroup, error) {
	return deleteHostGroup(ctx, o.storage, hostGroupUUID, accountName)
}

func _deleteHostGroup(ctx context.Context, storage database.Storage, hostGroupUUID string, accountID string) (*models.HostGroup, error) {
	account, err := storage.GetAccount(ctx, accountID)
	if err != nil {
		return nil, err
	}

	hostGroup, err := storage.DeleteHostGroup(ctx, hostGroupUUID, account.ID)
	if err != nil {
		return nil, err
	}

	return convertDatastoreHostGroupToModel(hostGroup, account.Name), nil
}

// GetMultipleHostGroups retrieves the specified host group UUID and returns it
func (o *Orchestrator) GetMultipleHostGroups(ctx context.Context, accountName string, hostGroupUUIDs []string) ([]*models.HostGroup, error) {
	return getMultipleHostGroup(ctx, o.storage, hostGroupUUIDs, accountName)
}

func _getMultipleHostGroup(ctx context.Context, storage database.Storage, hostGroupUUIDs []string, accountID string) ([]*models.HostGroup, error) {
	account, err := storage.GetAccount(ctx, accountID)
	if err != nil {
		return nil, err
	}

	hostGroups, err := storage.GetMultipleHostGroups(ctx, hostGroupUUIDs, account.ID)
	if err != nil {
		return nil, err
	}

	convHostGroups := make([]*models.HostGroup, 0)
	for _, hg := range hostGroups {
		convHostGroups = append(convHostGroups, convertDatastoreHostGroupToModel(hg, account.Name))
	}

	return convHostGroups, nil
}

func (o *Orchestrator) UpdateHostGroup(ctx context.Context, params *common.UpdateHostGroupParams) (*models.HostGroup, string, error) {
	logger := util.GetLogger(ctx)
	account, err := o.storage.GetAccount(ctx, params.AccountName)
	if err != nil {
		return nil, "", err
	}

	hg, err := o.storage.UpdateHostGroup(ctx, params.HostGroupUUID, account.ID, params.Description, &params.Hosts)
	if err != nil {
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeUpdateHostGroup),
		State:        string(models.JobsStateNEW),
		ResourceName: hg.Name,
		AccountID:    sql.NullInt64{Int64: hg.Account.ID, Valid: true},
	}
	createdJob, err := o.storage.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create hostgroup update job", "error", err)
		return nil, "", err
	}

	_, err = o.temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		workflows.UpdateHostGroupWorkflow,
		hg,
	)
	if err != nil {
		logger.Error("Failed to start update hostgroup workflow: ", "error", err)
		return nil, "", err
	}

	return convertDatastoreHostGroupToModel(hg, account.Name), createdJob.UUID, nil
}
