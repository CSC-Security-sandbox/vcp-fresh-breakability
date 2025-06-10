package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

var (
	minQuotaInBytesVolume      = env.GetUint64("MIN_QUOTA_IN_BYTES_VOLUME", 107374182400)    // 100GiB
	maxQuotaInBytesVolume      = env.GetUint64("MAX_QUOTA_IN_BYTES_VOLUME", 109951162777605) // 102,400 GiB
	createVolume               = _createVolume
	validateCreateVolumeParams = _validateCreateVolumeParams
	getIPAddressForVolume      = _getIPAddressForVolume
	updateVolume               = _updateVolume
	deleteVolume               = _deleteVolume
)

// CreateVolume creates the specified volume and adds it to the list of volume belonging to the specified owner
func (o *Orchestrator) CreateVolume(ctx context.Context, params *common.CreateVolumeParams) (*models.Volume, string, error) {
	return createVolume(ctx, o.storage, o.temporal, params)
}

func _createVolume(ctx context.Context, se database.Storage, temporal client.Client, params *common.CreateVolumeParams) (*models.Volume, string, error) {
	logger := util.GetLogger(ctx)

	account, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return nil, "", err
	}

	err = validateCreateVolumeParams(ctx, se, params, account.ID)
	if err != nil {
		return nil, "", err
	}

	pool, err := se.GetPool(ctx, params.PoolID, account.ID)
	if err != nil {
		return nil, "", err
	}

	svm, err := se.GetSvmForPoolID(ctx, pool.ID)
	if err != nil {
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeCreateVolume),
		State:        string(models.JobsStateNEW),
		ResourceName: params.Name,
		AccountID:    sql.NullInt64{Int64: account.ID, Valid: true},
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	volumeObj := &datamodel.Volume{
		Name:        params.Name,
		Account:     account,
		AccountID:   account.ID,
		SizeInBytes: int64(params.QuotaInBytes),
		Description: params.Description,
		PoolID:      pool.ID,
		SvmID:       svm.ID,
		Pool:        pool,
		VolumeAttributes: &datamodel.VolumeAttributes{
			CreationToken:    params.CreationToken,
			Protocols:        params.Protocols,
			VendorSubnetID:   params.Network,
			IsDataProtection: params.IsDataProtection,
		},
	}

	if params.BlockProperties != nil {
		volumeObj.VolumeAttributes.BlockProperties = &datamodel.BlockProperties{
			OSType:         params.BlockProperties.OSType,
			HostGroupUUIDs: params.BlockProperties.HostGroupUUIDs,
		}
	}

	if params.DataProtection != nil {
		volumeObj.DataProtection = &datamodel.DataProtection{
			BackupVaultID:          params.DataProtection.BackupVaultID,
			BackupPolicyID:         params.DataProtection.BackupPolicyId,
			BackupChainBytes:       params.DataProtection.BackupChainBytes,
			PolicyEnforced:         params.DataProtection.PolicyEnforced,
			ScheduledBackupEnabled: params.DataProtection.ScheduledBackupEnabled,
		}
	}

	dbVolume, err := se.CreateVolume(ctx, volumeObj)
	if err != nil {
		return nil, "", err
	}
	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		workflows.CreateVolumeWorkflow,
		params,
		dbVolume,
	)

	if err != nil {
		logger.Error("Failed to start create volume workflow: ", "error", err)
		return nil, "", err
	}
	return convertDatastoreVolumeToModel(dbVolume, nil), createdJob.UUID, nil
}

// GetVolume gets the specified volume
func (o *Orchestrator) GetVolume(ctx context.Context, volumeId string) (*models.Volume, error) {
	se := o.storage

	volume, err := se.GetVolume(ctx, volumeId)
	if err != nil {
		return nil, err
	}

	ipAddress, err := getIPAddressForVolume(ctx, se, volume)
	if err != nil {
		return nil, err
	}

	return convertDatastoreVolumeToModel(volume, &ipAddress), nil
}

func _getIPAddressForVolume(ctx context.Context, se database.Storage, volume *datamodel.Volume) (string, error) {
	nodes, err := se.GetNodesByPoolID(ctx, volume.PoolID)
	if err != nil {
		return "", err
	}

	lif, err := se.GetLifForNode(ctx, nodes[0].ID, volume.AccountID)
	if err != nil {
		return "", err
	}

	return lif.IPAddress, nil
}

func _validateCreateVolumeParams(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, accountID int64) error {
	if params.QuotaInBytes < minQuotaInBytesVolume || params.QuotaInBytes > maxQuotaInBytesVolume {
		return customerrors.NewUserInputValidationErr("volume size must be between 100 GiB and 102,400 GiB.")
	}

	pool, err := se.GetPool(ctx, params.PoolID, accountID)
	if err != nil {
		return err
	}

	if pool.State != models.LifeCycleStateREADY {
		return customerrors.NewUserInputValidationErr("pool is not ready")
	}

	if params.Network == "" {
		params.Network = pool.Network
	} else if params.Network != pool.Network {
		return customerrors.NewUserInputValidationErr("pool network and volume network should be same")
	}

	svm, err := se.GetSvmForPoolID(ctx, pool.ID)
	if err != nil {
		return err
	}

	if svm.State != models.LifeCycleStateREADY {
		return customerrors.NewUserInputValidationErr("svm is not ready")
	}

	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return err
	}

	if len(nodes) < 2 {
		return customerrors.NewUserInputValidationErr("required count of nodes not found")
	}

	for _, node := range nodes {
		if node.State != models.LifeCycleStateREADY {
			return customerrors.NewUserInputValidationErr("node is not ready")
		}
		lif, err := se.GetLifForNode(ctx, node.ID, node.AccountID)
		if err != nil {
			return err
		}
		if lif.Name == "" {
			return customerrors.NewUserInputValidationErr(fmt.Sprintf("lif for node %s is not available", node.Name))
		}
	}

	if params.BlockProperties != nil {
		hostGroupUUIDs := params.BlockProperties.HostGroupUUIDs
		if len(hostGroupUUIDs) > 0 {
			hostGroups, err := se.GetMultipleHostGroups(ctx, params.BlockProperties.HostGroupUUIDs, pool.Account.ID)
			if err != nil {
				return err
			}
			if len(params.BlockProperties.HostGroupUUIDs) != len(hostGroups) {
				return customerrors.NewUserInputValidationErr("could not find some of the host groups, please check the hostgroup details and try with valid host group names.")
			}
			for _, hostGroup := range hostGroups {
				if hostGroup.State != models.LifeCycleStateREADY {
					return customerrors.NewUserInputValidationErr(fmt.Sprintf("host group %s is not available", hostGroup.Name))
				}
			}
		}
	}
	if params.DataProtection != nil {
		var scheduledBackupEnabledStr *string
		if params.DataProtection.ScheduledBackupEnabled != nil && *params.DataProtection.ScheduledBackupEnabled {
			value := fmt.Sprintf("%v", params.DataProtection.ScheduledBackupEnabled)
			scheduledBackupEnabledStr = &value
		}

		if nillable.IsNilOrEmpty(&params.DataProtection.BackupPolicyId) && !nillable.IsNilOrEmpty(scheduledBackupEnabledStr) {
			return customerrors.NewUserInputValidationErr("BackupPolicyID to be provided to assign/unassign a backup policy to a volume")
		}
	}

	return nil
}

func convertDatastoreVolumeToModel(volume *datamodel.Volume, ipAddress *string) *models.Volume {
	res := &models.Volume{
		BaseModel: models.BaseModel{
			UUID:      volume.UUID,
			CreatedAt: volume.CreatedAt,
			UpdatedAt: volume.UpdatedAt,
			DeletedAt: DeletedAtOrNil(volume.DeletedAt),
		},
		PoolID:                volume.Pool.UUID,
		PoolName:              volume.Pool.Name,
		AccountName:           volume.Account.Name,
		DisplayName:           volume.Name,
		Description:           volume.Description,
		QuotaInBytes:          uint64(volume.SizeInBytes),
		LifeCycleState:        volume.State,
		LifeCycleStateDetails: volume.StateDetails,
		IsDataProtection:      volume.VolumeAttributes.IsDataProtection,
	}
	attributes := volume.VolumeAttributes
	res.VendorSubnetID = attributes.VendorSubnetID
	res.CreationToken = attributes.CreationToken
	res.ProtocolTypes = attributes.Protocols

	if attributes.BlockProperties != nil {
		res.BlockProperties = &models.BlockProperties{
			OSType:          attributes.BlockProperties.OSType,
			HostGroupUUIDs:  attributes.BlockProperties.HostGroupUUIDs,
			LunSerialNumber: attributes.BlockProperties.LunSerialNumber,
		}
	}

	if ipAddress != nil {
		res.IPAddress = *ipAddress
	}

	return res
}

func (o *Orchestrator) DeleteVolume(ctx context.Context, volumeId string) (*models.Volume, string, error) {
	return deleteVolume(ctx, o.storage, o.temporal, volumeId)
}

func _deleteVolume(ctx context.Context, se database.Storage, temporal client.Client, volumeId string) (*models.Volume, string, error) {
	logger := util.GetLogger(ctx)

	volume, err := se.GetVolume(ctx, volumeId)
	if err != nil {
		return nil, "", err
	}

	if volume != nil && volume.State == models.LifeCycleStateDeleting {
		return nil, "", errors.New("volume is already in deleting state")
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeDeleteVolume),
		State:        string(models.JobsStateNEW),
		ResourceName: volume.Name,
		AccountID:    sql.NullInt64{Int64: volume.Account.ID, Valid: true},
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create volume delete job in database", "error", err)
		return nil, "", err
	}

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		workflows.DeleteVolumeWorkflow,
		volume,
	)
	if err != nil {
		logger.Error("Failed to start delete volume workflow: ", "error", err)
		return nil, "", err
	}

	volume.State = models.LifeCycleStateDeleting
	volume.StateDetails = models.LifeCycleStateDeletingDetails
	return convertDatastoreVolumeToModel(volume, nil), createdJob.UUID, nil
}

func (o *Orchestrator) GetMultipleVolumes(ctx context.Context, volumeIds []string, accountName string) ([]*models.Volume, error) {
	se := o.storage

	account, err := getOrCreateAccount(ctx, se, accountName)
	if err != nil {
		return nil, err
	}

	conditions := [][]interface{}{{"account_id = ?", account.ID}}
	conditions = append(conditions, []interface{}{"uuid in ?", volumeIds})
	volumes, err := se.GetMultipleVolumes(ctx, conditions)
	if err != nil {
		return nil, err
	}

	var result []*models.Volume
	for _, volume := range volumes {
		ipAddress, err := getIPAddressForVolume(ctx, se, volume)
		if err != nil {
			return nil, err
		}
		result = append(result, convertDatastoreVolumeToModel(volume, &ipAddress))
	}
	return result, nil
}

// UpdateVolume updates the specified volume with the new parameters
func (o *Orchestrator) UpdateVolume(ctx context.Context, param *common.UpdateVolumeParams) (*models.Volume, string, error) {
	return updateVolume(ctx, o.storage, o.temporal, param)
}

func _updateVolume(ctx context.Context, se database.Storage, temporal client.Client, params *common.UpdateVolumeParams) (*models.Volume, string, error) {
	logger := util.GetLogger(ctx)

	dbVolume, err := se.GetVolume(ctx, params.VolumeId)
	if err != nil {
		return nil, "", err
	}

	if dbVolume.State == models.LifeCycleStateUpdating {
		job, err := se.GetJobByResourceUUID(ctx, dbVolume.UUID)
		if err != nil {
			return nil, "", err
		}
		return convertDatastoreVolumeToModel(dbVolume, nil), job.UUID, nil
	}

	err = validateUpdateVolumeRequest(dbVolume, params)
	if err != nil {
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeUpdateVolume),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.Name,
		AccountID:     sql.NullInt64{Int64: dbVolume.AccountID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: dbVolume.UUID},
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create volume update job in database", "error", err)
		return nil, "", err
	}

	dbVolume, err = updateVolumeStatus(ctx, se, dbVolume)
	if err != nil {
		logger.Error("Failed to update volume state in database", "error", err)
		return nil, "", err
	}

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		workflows.UpdateVolumeWorkflow,
		params,
		dbVolume,
	)

	if err != nil {
		logger.Error("Failed to start update volume workflow: ", "error", err)
		return nil, "", err
	}
	return convertDatastoreVolumeToModel(dbVolume, nil), createdJob.UUID, nil
}

func updateVolumeStatus(ctx context.Context, se database.Storage, dbVolume *datamodel.Volume) (*datamodel.Volume, error) {
	err := se.UpdateVolumeFields(ctx, dbVolume.UUID, map[string]interface{}{
		"state":         models.LifeCycleStateUpdating,
		"state_details": models.LifeCycleStateUpdatingDetails,
	})
	if err != nil {
		return nil, err
	}
	dbVolume.State = models.LifeCycleStateUpdating
	dbVolume.StateDetails = models.LifeCycleStateUpdatingDetails
	return dbVolume, err
}

func validateUpdateVolumeRequest(volume *datamodel.Volume, params *common.UpdateVolumeParams) error {
	if utils.IsTransitionalState(volume.State) {
		return customerrors.NewUserInputValidationErr("volume is not in a valid state for update")
	}

	if params.QuotaInBytes < volume.SizeInBytes {
		return customerrors.NewUserInputValidationErr("volume size cannot be reduced")
	}
	return nil
}
