package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/repository"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
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

const (
	minCoolnessPeriodDays = 2
	maxCoolnessPeriodDays = 183
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

	pool, err := se.GetPool(ctx, params.PoolID, account.ID)
	if err != nil {
		return nil, "", err
	}

	err = validateCreateVolumeParams(ctx, se, params, pool)
	if err != nil {
		return nil, "", err
	}

	svm, err := se.GetSvmForPoolID(ctx, pool.ID)
	if err != nil {
		return nil, "", err
	}

	if params.SnapshotID != "" {
		dbSnapshot, err := se.GetSnapshotByUUID(ctx, params.SnapshotID, account.ID, true)
		if err != nil {
			logger.Error("Failed to fetch parent snapshot for volume creation. Please use the correct snapshot and retry again.", "error", err)
			return nil, "", err
		}
		if dbSnapshot.State != models.LifeCycleStateREADY {
			logger.Error("Parent snapshot is not in a valid state for volume creation", "snapshot_state", dbSnapshot.State)
			return nil, "", customerrors.NewUserInputValidationErr("Parent snapshot is not in a valid state for volume creation. Please wait for the snapshot to be ready and retry again.")
		}
		params.Snapshot = dbSnapshot
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeCreateVolume),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	dbPool := repository.ConvertPoolViewToPool(pool)
	volumeObj := &datamodel.Volume{
		Name:        params.Name,
		Account:     account,
		AccountID:   account.ID,
		SizeInBytes: int64(params.QuotaInBytes),
		Description: params.Description,
		PoolID:      pool.ID,
		SvmID:       svm.ID,
		Pool:        dbPool,
		VolumeAttributes: &datamodel.VolumeAttributes{
			CreationToken:    params.CreationToken,
			Protocols:        params.Protocols,
			VendorSubnetID:   params.Network,
			IsDataProtection: params.IsDataProtection,
		},
	}

	if params.BlockProperties != nil {
		volumeObj.VolumeAttributes.BlockProperties = &datamodel.BlockProperties{
			OSType: params.BlockProperties.OSType,
		}
		hgs, err := getMultipleHostGroup(ctx, se, params.BlockProperties.HostGroupUUIDs, account.Name)
		if err != nil {
			return nil, "", err
		}
		for _, hg := range hgs {
			volumeObj.VolumeAttributes.BlockProperties.HostGroupDetails = append(
				volumeObj.VolumeAttributes.BlockProperties.HostGroupDetails, datamodel.HostGroupDetail{
					HostGroupUUID: hg.UUID,
					HostQNs:       hg.Hosts,
				})
		}
	}

	if params.DataProtection != nil {
		volumeObj.DataProtection = &datamodel.DataProtection{
			BackupVaultID:          params.DataProtection.BackupVaultID,
			BackupPolicyID:         params.DataProtection.BackupPolicyId,
			BackupChainBytes:       params.DataProtection.BackupChainBytes,
			ScheduledBackupEnabled: params.DataProtection.ScheduledBackupEnabled,
		}
	}

	if params.SnapshotPolicy != nil {
		volumeObj.SnapshotPolicy = &datamodel.SnapshotPolicy{
			Name:      volumeObj.Name,
			IsEnabled: params.SnapshotPolicy.IsEnabled,
			Schedules: convertToDBSnapshotPolicySchedule(params.SnapshotPolicy.Schedules),
		}
	}

	if params.TieringPolicy != nil && params.TieringPolicy.CoolAccess {
		volumeObj.CoolAccess = params.TieringPolicy.CoolAccess
		volumeObj.CoolAccessTieringPolicy = params.TieringPolicy.CoolAccessTieringPolicy
		volumeObj.CoolnessPeriod = params.TieringPolicy.CoolnessPeriod
		volumeObj.CoolAccessRetrievalPolicy = params.TieringPolicy.CoolAccessRetrievalPolicy
	}

	dbVolume, err := se.CreateVolume(ctx, volumeObj)
	if err != nil {
		return nil, "", err
	}

	location, err := getLocationFromVendorID(dbVolume.Pool.VendorID)
	if err != nil {
		logger.Error("Failed to get location from vendor ID: ", "error", err)
		return nil, "", err
	}

	// controlWorkflowID defines the workflow ID for the control workflow
	controlWorkflowID := fmt.Sprintf(workflows.VolumeCreateDeleteSnapshotDeleteSeq, dbVolume.Account.ID, location, dbVolume.Pool.Name)
	err = workflows.ExecuteWorkflowSequentially(
		temporal,
		ctx,
		client.StartWorkflowOptions{
			TaskQueue: workflowengine.CustomerTaskQueue,
			ID:        controlWorkflowID,
		},
		workflows.CreateVolumeWorkflow,
		workflow.ChildWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			WorkflowID:            createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
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

func (o *Orchestrator) GetVolumeCount(ctx context.Context, projectNumber string) (int64, error) {
	// Get the count of volume replications for the specified account
	count, err := o.storage.GetVolumeCount(ctx, projectNumber)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// ListVolumes returns list of volumes belonging to the specified owner
func (o *Orchestrator) ListVolumes(ctx context.Context, accountName string) ([]*models.Volume, error) {
	se := o.storage

	account, err := getAccountWithName(ctx, se, accountName)
	if err != nil {
		return nil, err
	}

	conditions := [][]interface{}{{"account_id = ?", account.ID}}
	volumes, err := se.ListVolumes(ctx, conditions)
	if err != nil {
		return nil, err
	}

	return convertDatastoreVolumesToModel(volumes), nil
}

func convertDatastoreVolumesToModel(volumes []*datamodel.Volume) []*models.Volume {
	var volumesList []*models.Volume
	for _, volume := range volumes {
		p := convertDatastoreVolumeToModel(volume, nil)
		volumesList = append(volumesList, p)
	}
	return volumesList
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

func _validateCreateVolumeParams(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
	if params.QuotaInBytes < minQuotaInBytesVolume || params.QuotaInBytes > maxQuotaInBytesVolume {
		return customerrors.NewUserInputValidationErr("volume size must be between 100 GiB and 102,400 GiB.")
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
		err = validateBlockProperties(ctx, se, hostGroupUUIDs, pool.Account.ID)
		if err != nil {
			return err
		}
	}

	if params.DataProtection != nil && params.DataProtection.BackupPolicyId != "" {
		// Validate assigning backup policy to the volume
		if params.DataProtection.BackupVaultID == "" {
			return customerrors.NewUserInputValidationErr("backup vault id is required to assign a backup policy to a volume")
		}
		if params.DataProtection.ScheduledBackupEnabled == nil {
			return customerrors.NewUserInputValidationErr("scheduled backups needs to be enabled/disabled when a backup policy is assigned to a volume")
		}
		if params.IsDataProtection {
			return customerrors.NewUserInputValidationErr("scheduled backups are not supported for cross region replication, only manual backups with existing snapshots are supported")
		}
	}

	if !pool.AllowAutoTiering && params.TieringPolicy != nil && params.TieringPolicy.CoolAccess {
		return customerrors.NewUserInputValidationErr("Auto Tiering is not allowed for this volume. Please enable Auto Tiering on the Pool and try again")
	} else if params.TieringPolicy != nil && params.TieringPolicy.CoolAccess {
		if params.TieringPolicy.CoolnessPeriod < minCoolnessPeriodDays || params.TieringPolicy.CoolnessPeriod > maxCoolnessPeriodDays {
			return customerrors.NewUserInputValidationErr("Auto Tiering Cooling Threshold days must be between 2 and 183 days")
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
		Zone:                  volume.Pool.PoolAttributes.PrimaryZone,
		UsedBytes:             volume.UsedBytes,
		EncryptionType:        utils.GetEncryptionType(nil), // pass volume.Pool.KmsConfigID when association is implemented
	}
	attributes := volume.VolumeAttributes
	res.VendorSubnetID = attributes.VendorSubnetID
	res.CreationToken = attributes.CreationToken
	res.ProtocolTypes = attributes.Protocols

	if attributes.BlockProperties != nil {
		res.BlockProperties = &models.BlockProperties{
			OSType:          attributes.BlockProperties.OSType,
			LunSerialNumber: attributes.BlockProperties.LunSerialNumber,
			HostGroupDetail: convertHostGroupDetails(attributes.BlockProperties.HostGroupDetails),
		}
	}
	if volume.DataProtection != nil {
		res.DataProtection = &models.DataProtection{
			BackupVaultID:          volume.DataProtection.BackupVaultID,
			BackupPolicyId:         volume.DataProtection.BackupPolicyID,
			BackupChainBytes:       volume.DataProtection.BackupChainBytes,
			ScheduledBackupEnabled: volume.DataProtection.ScheduledBackupEnabled,
		}
	}

	if ipAddress != nil {
		res.IPAddress = *ipAddress
	}

	if volume.SnapshotPolicy != nil {
		res.SnapshotPolicy = &models.SnapshotPolicy{
			Name:      volume.SnapshotPolicy.Name,
			IsEnabled: volume.SnapshotPolicy.IsEnabled,
			Comment:   volume.SnapshotPolicy.Comment,
			Schedules: convertToModelSnapshotPolicySchedule(volume.SnapshotPolicy.Schedules),
		}
	}

	if volume.CoolAccess {
		res.TieringPolicy = &models.TieringPolicy{
			CoolAccess:              volume.CoolAccess,
			CoolnessPeriod:          volume.CoolnessPeriod,
			CoolAccessTieringPolicy: volume.CoolAccessTieringPolicy,
		}
	}

	return res
}

func convertHostGroupDetails(hgs []datamodel.HostGroupDetail) []models.HostGroupDetails {
	resp := make([]models.HostGroupDetails, 0)
	for _, hg := range hgs {
		resp = append(resp, models.HostGroupDetails{
			Hosts:       hg.HostQNs,
			HostGroupID: hg.HostGroupUUID,
		})
	}
	return resp
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

	if utils.IsTransitionalState(volume.State) {
		logger.Errorf("Volume %s cannot be deleted, while in transitioning state: %s", volume.Name, volume.State)
		return nil, "", vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError, customerrors.NewConflictErr("volume is in transition state and cannot be deleted, state: "+volume.State))
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeDeleteVolume),
		State:         string(models.JobsStateNEW),
		ResourceName:  volume.Name,
		AccountID:     sql.NullInt64{Int64: volume.Account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create volume delete job in database", "error", err)
		return nil, "", err
	}

	location, err := getLocationFromVendorID(volume.Pool.VendorID)
	if err != nil {
		logger.Error("Failed to get location from vendor ID: ", "error", err)
		return nil, "", err
	}

	// controlWorkflowID defines the workflow ID for the control workflow
	controlWorkflowID := fmt.Sprintf(workflows.VolumeCreateDeleteSnapshotDeleteSeq, volume.Account.ID, location, volume.Pool.Name)
	err = workflows.ExecuteWorkflowSequentially(
		temporal,
		ctx,
		client.StartWorkflowOptions{
			TaskQueue: workflowengine.CustomerTaskQueue,
			ID:        controlWorkflowID,
		},
		workflows.DeleteVolumeWorkflow,
		workflow.ChildWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			WorkflowID:            createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
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

	if params.DataProtection != nil {
		if dbVolume.DataProtection == nil {
			dbVolume.DataProtection = &datamodel.DataProtection{
				BackupVaultID: params.DataProtection.BackupVaultID,
			}
		} else if dbVolume.DataProtection.BackupVaultID != "" && (params.DataProtection.BackupVaultID == "" || params.DataProtection.BackupVaultID != dbVolume.DataProtection.BackupVaultID) {
			backups, err := se.GetBackupsByBackupVaultOwnerIDAndFilter(ctx, dbVolume.DataProtection.BackupVaultID, dbVolume.Account.ID, nil)
			if err != nil {
				return nil, "", err
			}
			if len(backups) > 0 {
				return nil, "", customerrors.NewUserInputValidationErr("cannot remove backup vault as there are backups associated with it")
			}
			dbVolume.DataProtection.BackupVaultID = params.DataProtection.BackupVaultID
		} else {
			dbVolume.DataProtection.BackupVaultID = params.DataProtection.BackupVaultID
		}
	}

	pool, err := se.GetPool(ctx, params.PoolID, dbVolume.AccountID)
	if err != nil {
		return nil, "", err
	}

	err = validateUpdateVolumeRequest(ctx, se, dbVolume, params, pool)
	if err != nil {
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeUpdateVolume),
		State:         string(models.JobsStateNEW),
		ResourceName:  dbVolume.Name,
		AccountID:     sql.NullInt64{Int64: dbVolume.AccountID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: dbVolume.UUID},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create volume update job in database", "error", err)
		return nil, "", err
	}

	if params.SnapshotPolicy != nil {
		params.SnapshotPolicy.Name = dbVolume.Name
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

func validateUpdateVolumeRequest(ctx context.Context, se database.Storage, volume *datamodel.Volume, params *common.UpdateVolumeParams, pool *datamodel.PoolView) error {
	if utils.IsTransitionalState(volume.State) {
		return customerrors.NewUserInputValidationErr("volume is not in a valid state for update")
	}

	if params.QuotaInBytes < volume.SizeInBytes {
		return customerrors.NewUserInputValidationErr("volume size cannot be reduced")
	}

	if !pool.AllowAutoTiering && params.TieringPolicy != nil && params.TieringPolicy.CoolAccess {
		return customerrors.NewUserInputValidationErr("Auto Tiering is not allowed for this volume. Please enable Auto Tiering on the Pool and try again")
	} else if params.TieringPolicy != nil && params.TieringPolicy.CoolAccess {
		if params.TieringPolicy.CoolnessPeriod < minCoolnessPeriodDays || params.TieringPolicy.CoolnessPeriod > maxCoolnessPeriodDays {
			return customerrors.NewUserInputValidationErr("Auto Tiering Cooling Threshold days must be between 2 and 183 days")
		}
	}

	if params.BlockProperties != nil {
		hostGroupUUIDs := params.BlockProperties.HostGroupUUIDs
		err := validateBlockProperties(ctx, se, hostGroupUUIDs, volume.Account.ID)
		if err != nil {
			return err
		}
	}

	// When just enabling or disabling the snapshot policy, we need to check if there is an existing snapshot policy
	if params.SnapshotPolicy != nil && len(params.SnapshotPolicy.Schedules) == 0 && (volume.SnapshotPolicy == nil || volume.SnapshotPolicy.Name == "") {
		return customerrors.NewUserInputValidationErr("no existing snapshot policy found for the volume and no schedules provided in the update request. Cannot create a new snapshot policy without schedules")
	}

	return nil
}

func validateBlockProperties(ctx context.Context, se database.Storage, hostGroupUUIDs []string, accountID int64) error {
	if len(hostGroupUUIDs) > 0 {
		hostGroups, err := se.GetMultipleHostGroups(ctx, hostGroupUUIDs, accountID)
		if err != nil {
			return err
		}
		if len(hostGroupUUIDs) != len(hostGroups) {
			return customerrors.NewUserInputValidationErr("could not find some of the host groups, please check the hostgroup details and try with valid host group names.")
		}
		uniqueHostSet := make(map[string]bool)
		for _, hostGroup := range hostGroups {
			if hostGroup.State != models.LifeCycleStateREADY {
				return customerrors.NewUserInputValidationErr(fmt.Sprintf("host group %s is not available", hostGroup.Name))
			}
			for _, host := range hostGroup.Hosts.Hosts {
				if _, exists := uniqueHostSet[host]; exists {
					return customerrors.NewUserInputValidationErr(fmt.Sprintf("host : %s is present in multiple host groups", host))
				}
				uniqueHostSet[host] = true
			}
		}
	}

	return nil
}

func convertToDBSnapshotPolicySchedule(schedules []*models.SnapshotPolicySchedule) []*datamodel.SnapshotPolicySchedule {
	var dbSnapshotPolicySchedules []*datamodel.SnapshotPolicySchedule
	for _, schedule := range schedules {
		dbSnapshotPolicySchedules = append(dbSnapshotPolicySchedules, &datamodel.SnapshotPolicySchedule{
			Count:           schedule.Count,
			SnapmirrorLabel: schedule.SnapmirrorLabel,
			DaysOfMonth:     schedule.Schedule.DaysOfMonth,
			DaysOfWeek:      schedule.Schedule.DaysOfWeek,
			Hours:           schedule.Schedule.Hours,
			Minutes:         schedule.Schedule.Minutes,
		})
	}
	return dbSnapshotPolicySchedules
}

func convertToModelSnapshotPolicySchedule(schedules []*datamodel.SnapshotPolicySchedule) []*models.SnapshotPolicySchedule {
	var dbSnapshotPolicySchedules []*models.SnapshotPolicySchedule
	for _, schedule := range schedules {
		dbSnapshotPolicySchedules = append(dbSnapshotPolicySchedules, &models.SnapshotPolicySchedule{
			Count:           schedule.Count,
			SnapmirrorLabel: schedule.SnapmirrorLabel,
			Prefix:          schedule.SnapmirrorLabel,
			Schedule: &models.Schedule{
				DaysOfMonth: schedule.DaysOfMonth,
				DaysOfWeek:  schedule.DaysOfWeek,
				Hours:       schedule.Hours,
				Minutes:     schedule.Minutes,
			},
		})
	}
	return dbSnapshotPolicySchedules
}

func getLocationFromVendorID(vendorID string) (string, error) {
	// vendorID is in the format: "/projects/project123/locations/location123/pools/pool123"
	parts := strings.Split(vendorID, "/")

	if len(parts) != 7 {
		return "", customerrors.NewUserInputValidationErr("invalid vendor ID, expected format: /projects/{project}/locations/{location}/pools/{pool}, found: " + vendorID)
	}

	return parts[len(parts)-3], nil
}
