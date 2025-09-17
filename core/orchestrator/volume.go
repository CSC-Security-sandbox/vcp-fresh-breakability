package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/flexcache_workflows"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

const maxConstituentVolumesPerAggregate = 200

var (
	numOfLvHAPairs             = env.GetInt("NUMBER_OF_HA_PAIRS_LARGE_CAPACITY", 2)
	minQuotaInBytesVolume      = utils.MinQuotaInBytesVolumeForVolume
	maxQuotaInBytesVolume      = utils.MaxQuotaInBytesVolumeForVolume
	createVolume               = _createVolume
	revertVolume               = _revertVolume
	validateCreateVolumeParams = _validateCreateVolumeParams
	getIPAddressForVolume      = _getIPAddressForVolume
	updateVolume               = _updateVolume
	deleteVolume               = _deleteVolume
	validateDeleteVolumeParams = _validateDeleteVolumeParams
	updateVolumeStatus         = _updateVolumeStatus

	envIsLocalEnv = env.IsLocalEnv
)

const (
	minCoolingThresholdDays   = 2
	maxCoolingThresholdDays   = 183
	MaxBackupPathComponents   = 8          // The expected number of components in the backup path
	BackupNameIndex           = 7          // The index of the backup name in the components
	BackupVaultNameIndex      = 5          // The index of the backup vault name in the components
	bytesPerGB                = 1073741824 // 1024^3 bytes = 1 GB
	percentageBase            = 100.0
	ErrMsgSnapReserveIncrease = "Cannot increase SnapReserve to %.0f%% as we cannot decrease the available space (%.2f GB). " +
		"Please increase the volume size to at least %.0f GB with this SnapReserve or reduce the SnapReserve percentage to continue."
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

	poolPrimaryZone := pool.PoolAttributes.PrimaryZone
	// Validate that volume zone matches pool's primary zone
	if params.Zone != poolPrimaryZone {
		return nil, "", customerrors.NewConflictErr(fmt.Sprintf("Volume zone '%s' does not match pool's primary zone '%s'.", params.Zone, poolPrimaryZone))
	}

	// Check for existing volume with same name in the determined zone
	vol, volErr := se.GetVolumeByNameAccountIDAndZone(ctx, params.Name, pool.Account.ID, params.Zone)
	if volErr != nil {
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(volErr, &customErr) && !customerrors.IsNotFoundErr(customErr.Unwrap()) {
			// propagate the Non-NotFound errors
			return nil, "", volErr
		}
		logger.Debug("No existing volume found with the given name in the same zone, proceeding to create a new volume",
			"volume_name", params.Name, "zone", params.Zone)
	} else {
		if vol.State != models.LifeCycleStateCreating {
			return nil, "", customerrors.NewConflictErr(fmt.Sprintf("Volume with resource_id '%s' already exists in zone '%s'", params.Name, poolPrimaryZone))
		} else {
			job, jobErr := se.GetJobByResourceUUID(ctx, vol.UUID, string(models.JobTypeCreateVolume))
			if jobErr != nil {
				logger.Error("Failed to fetch existing create volume job for volume in CREATING state", "error", jobErr)
				return convertDatastoreVolumeToModel(vol, nil), "", nil
			}
			return convertDatastoreVolumeToModel(vol, nil), job.UUID, nil
		}
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
		dbSnapshot, err := se.GetSnapshotByPoolID(ctx, params.SnapshotID, account.ID, pool.ID, true)
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
	dbPool := database.ConvertPoolViewToPool(pool)
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
			SnapReserve:      params.SnapReserve,
			Labels:           params.Labels,
		},
	}

	// Check BlockDevices first, then fallback to BlockProperties
	if params.BlockDevices != nil && len(*params.BlockDevices) > 0 {
		// Process BlockDevices as primary source
		blockDevices := make([]datamodel.BlockDevice, 0, len(*params.BlockDevices))
		for _, blockDeviceReq := range *params.BlockDevices {
			blockDevice := datamodel.BlockDevice{
				Name:   blockDeviceReq.Name,
				OSType: blockDeviceReq.OSType,
			}
			if len(blockDeviceReq.HostGroups) > 0 {
				hgs, err := getMultipleHostGroup(ctx, se, blockDeviceReq.HostGroups, account.Name)
				if err != nil {
					return nil, "", err
				}
				for _, hg := range hgs {
					blockDevice.HostGroupDetails = append(blockDevice.HostGroupDetails, datamodel.HostGroupDetail{
						HostGroupUUID: hg.UUID,
						HostQNs:       hg.Hosts,
					})
				}
			}
			blockDevices = append(blockDevices, blockDevice)
		}
		volumeObj.VolumeAttributes.BlockDevices = &blockDevices
	} else if params.BlockProperties != nil {
		// Fallback: Process BlockProperties if BlockDevices are not provided
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

	if params.FileProperties != nil {
		junctionPath := common.CreateJunctionPath(params.CreationToken)
		volumeObj.VolumeAttributes.FileProperties = &datamodel.FileProperties{
			JunctionPath: junctionPath,
		}
		if params.FileProperties.ExportPolicy != nil {
			exportRules := make([]*datamodel.ExportRule, 0, len(params.FileProperties.ExportPolicy.ExportRules))
			for _, rule := range params.FileProperties.ExportPolicy.ExportRules {
				exportRules = append(exportRules, &datamodel.ExportRule{
					AllowedClients: rule.AllowedClients,
					AccessType:     rule.AccessType,
					CIFS:           rule.CIFS,
					NFSv3:          rule.NFSv3,
					NFSv4:          rule.NFSv4,
					Index:          rule.Index,
				})
			}
			volumeObj.VolumeAttributes.FileProperties = &datamodel.FileProperties{
				ExportPolicy: &datamodel.ExportPolicy{
					ExportPolicyName: params.FileProperties.ExportPolicy.ExportPolicyName,
					ExportRules:      exportRules,
				},
				JunctionPath: junctionPath,
			}
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

	if params.AutoTieringPolicy != nil && params.AutoTieringPolicy.AutoTieringEnabled {
		volumeObj.AutoTieringEnabled = params.AutoTieringPolicy.AutoTieringEnabled
		volumeObj.AutoTieringPolicy = &datamodel.AutoTieringPolicy{
			TieringPolicy:        params.AutoTieringPolicy.TieringPolicy,
			CoolingThresholdDays: params.AutoTieringPolicy.CoolingThresholdDays,
			RetrievalPolicy:      params.AutoTieringPolicy.RetrievalPolicy,
		}
	}

	var backupVault *datamodel.BackupVault
	var backup *datamodel.Backup

	if params.BackupPath != "" {
		if volumeObj.VolumeAttributes == nil {
			volumeObj.VolumeAttributes = &datamodel.VolumeAttributes{}
		}
		logger.Debug("params.BackupPath: %v", params.BackupPath)
		volumeObj.VolumeAttributes.RestoredBackupPath = params.BackupPath
		components := strings.Split(params.BackupPath, "/")

		// Ensure there are enough components to avoid out of range errors
		if len(components) < MaxBackupPathComponents {
			return nil, "", customerrors.NewUserInputValidationErr("Backup path is not in correct format")
		}
		backupVaultName := components[BackupVaultNameIndex]
		backupName := components[BackupNameIndex]
		// backupVault, err = se.GetBackupVaultByNameAndOwnerID(ctx, backupVaultName, strconv.FormatInt(account.ID, 10))
		backupVault, err = se.GetBackupVaultByNameAndOwnerID(ctx, backupVaultName, strconv.FormatInt(account.ID, 10))
		if err != nil {
			return nil, "", err
		}
		backup, err = se.GetBackupByNameAndBackupVaultID(ctx, backupName, backupVault.ID)
		if err != nil {
			return nil, "", err
		}
		volumeObj.VolumeAttributes.RestoredBackupID = backup.UUID // Set the restored backup ID from the backup object
	}

	if params.LargeCapacity {
		volumeObj.LargeVolumeAttributes = &datamodel.LargeVolumeAttributes{
			LargeCapacity: true,
		}
		if params.LargeVolumeConstituentCount > 0 {
			volumeObj.LargeVolumeAttributes.LargeVolumeConstituentCount = nillable.GetInt32Ptr(params.LargeVolumeConstituentCount)
		}
	}

	dbVolume, err := se.CreateVolume(ctx, volumeObj)
	if err != nil {
		return nil, "", err
	}

	defer func() {
		if err != nil {
			// Mark volume in error state
			volumeUpdateErr := se.UpdateVolumeFields(ctx, dbVolume.UUID, map[string]interface{}{
				"state":         models.LifeCycleStateError,
				"state_details": models.LifeCycleStateCreationErrorDetails,
			})
			if volumeUpdateErr != nil {
				logger.Error("Failed to update volume state to ERROR", "volume_id", dbVolume.UUID, "error", volumeUpdateErr)
			}
		}
	}()

	location, err := utils.GetLocationFromVendorID(dbVolume.Pool.VendorID)
	if err != nil {
		logger.Error("Failed to get location from vendor ID: ", "error", err)
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeCreateVolume),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: dbVolume.UUID,
		},
	}

	if params.LargeCapacity {
		job.Type = string(models.JobTypeCreateLargeVolume)
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	// Defer to mark job as error if workflow execution fails
	defer func() {
		if err != nil {
			updateErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error())
			if updateErr != nil {
				logger.Error("Failed to update job state to ERROR", "job_id", createdJob.UUID, "error", updateErr)
			}
		}
	}()

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
		backupVault,
		backup,
	)
	if err != nil {
		logger.Error("Failed to start create volume workflow: ", "error", err)
		return nil, "", err
	}
	return convertDatastoreVolumeToModel(dbVolume, nil), createdJob.UUID, nil
}

// RevertVolume creates the specified volume and adds it to the list of volume belonging to the specified owner
func (o *Orchestrator) RevertVolume(ctx context.Context, params *common.RevertVolumeParams) (*models.Volume, string, error) {
	return revertVolume(ctx, o.storage, o.temporal, params)
}

func _revertVolume(ctx context.Context, se database.Storage, temporal client.Client, params *common.RevertVolumeParams) (*models.Volume, string, error) {
	logger := util.GetLogger(ctx)

	account, err := getAccountWithName(ctx, se, params.AccountName)
	if err != nil {
		logger.Error("Failed to fetch account for the given projectNumber", "error", err)
		return nil, "", err
	}

	volume, err := se.GetVolumeWithAccountID(ctx, params.VolumeID, account.ID)
	if err != nil {
		logger.Error("Failed to fetch volume for the given account ID", "error", err)
		return nil, "", err
	}

	if utils.IsTransitionalState(volume.State) {
		logger.Errorf("Volume %s cannot be reverted, while in transitioning state: %s", volume.Name, volume.State)
		return nil, "", customerrors.NewConflictErr("volume is in transition state and cannot be reverted, state: " + volume.State)
	}

	if volume.State != models.LifeCycleStateREADY {
		return nil, "", customerrors.NewConflictErr("Volume is not in READY state, state: " + volume.State)
	}

	if volume.VolumeAttributes != nil && volume.VolumeAttributes.IsDataProtection {
		return nil, "", customerrors.NewUserInputValidationErr("Cannot revert a Data Protection Volume")
	}

	// Validate snapshot exists and is accessible
	snapshot, err := se.GetSnapshotByUUID(ctx, params.SnapshotID, volume.Account.ID, volume.ID)
	if err != nil {
		logger.Error("Failed to fetch snapshot for volume revert", "error", err)
		return nil, "", customerrors.NewUserInputValidationErr("Snapshot not found")
	}

	// Validate snapshot state
	if snapshot.State != models.LifeCycleStateREADY {
		logger.Error("Snapshot is not in a valid state for volume revert", "snapshot_state", snapshot.State)
		return nil, "", customerrors.NewConflictErr("Snapshot is not in a valid state for volume revert. Please wait for the snapshot to be ready and retry again.")
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeRevertVolume),
		State:         string(models.JobsStateNEW),
		ResourceName:  volume.Name,
		AccountID:     sql.NullInt64{Int64: volume.AccountID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: volume.UUID},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create volume revert job in database", "error", err)
		return nil, "", err
	}

	// Defer to mark job as error if workflow execution fails
	defer func() {
		if err != nil {
			updateErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error())
			if updateErr != nil {
				logger.Error("Failed to update job state to ERROR", "job_id", createdJob.UUID, "error", updateErr)
			}
		}
	}()

	previousState := volume.State
	previousStateDetails := volume.StateDetails
	volume, err = updateVolumeStatus(ctx, se, volume, models.LifeCycleStateReverting, models.LifeCycleStateRevertingDetails)
	if err != nil {
		logger.Error("Failed to update volume state in database", "error", err)
		return nil, "", err
	}
	// Defer to mark job as error if workflow execution fails
	defer func() {
		if err != nil {
			volumeUpdateErr := se.UpdateVolumeFields(ctx, volume.UUID, map[string]interface{}{
				"state":         previousState,
				"state_details": previousStateDetails,
			})
			if volumeUpdateErr != nil {
				logger.Error("Failed to update volume state back to READY", "volume_id", volume.UUID, "error", volumeUpdateErr)
			}
		}
	}()

	location, err := utils.GetLocationFromVendorID(volume.Pool.VendorID)
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
		workflows.RevertVolumeWorkflow,
		workflow.ChildWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			WorkflowID:            createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		params,
		volume,
		snapshot,
	)
	if err != nil {
		logger.Error("Failed to start revert volume workflow: ", "error", err)
		return nil, "", err
	}

	return convertDatastoreVolumeToModel(volume, nil), createdJob.UUID, nil
}

// GetVolume gets the specified volume
func (o *Orchestrator) GetVolume(ctx context.Context, volumeId string, refreshVolumeFields bool) (*models.Volume, error) {
	log := util.GetLogger(ctx)
	se := o.storage

	volume, err := se.DescribeVolume(ctx, volumeId)
	if err != nil {
		return nil, err
	}

	ipAddresses, err := getIPAddressForVolume(ctx, se, volume)
	if err != nil {
		return nil, err
	}

	// return early if we don't need to update volume metrics or if the volume is not in ready state
	if !refreshVolumeFields || volume.State != models.LifeCycleStateREADY {
		return convertDatastoreVolumeToModel(volume, &ipAddresses), nil
	}

	dbJobs, err := getExistingRefreshVolumeFieldsJob(ctx, volume, se)
	if err != nil {
		log.Error("Failed to get existing JobTypeRefreshVolumeFields for this volume", "error", err)
		return nil, err
	}

	if len(dbJobs) > 0 {
		log.Info("JobTypeRefreshVolumeFields already exists for this volume, skipping creation")
		return convertDatastoreVolumeToModel(volume, &ipAddresses), nil
	}

	job := &datamodel.Job{
		Type:         string(models.JobTypeRefreshVolumeFields),
		State:        string(models.JobsStateNEW),
		ResourceName: volume.Name,
		AccountID:    sql.NullInt64{Int64: volume.Account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: volume.UUID,
			PoolUUID:     volume.Pool.UUID,
		},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		log.Error("Failed to create JobTypeRefreshVolumeFields in database", "error", err)
		return nil, err
	}

	// Defer to mark job as error if workflow execution fails
	defer func() {
		if err != nil {
			updateErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error())
			if updateErr != nil {
				log.Error("Failed to update job state to ERROR", "job_id", createdJob.UUID, "error", updateErr)
			}
		}
	}()

	_, err = o.temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.BackgroundTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		workflows.VolumeRefreshWorkflow,
		volume,
	)

	if err != nil {
		log.Error("Failed to execute VolumeRefreshWorkflow", "error", err)
		return nil, err
	}

	return convertDatastoreVolumeToModel(volume, &ipAddresses), nil
}

func getExistingRefreshVolumeFieldsJob(ctx context.Context, volume *datamodel.Volume, se database.Storage) ([]*datamodel.Job, error) {
	filter := dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("account_id", "=", volume.Account.ID),
		dbutils.NewFilterCondition("type", "=", models.JobTypeRefreshVolumeFields),
		dbutils.NewFilterCondition("state", "!=", string(models.JobsStateDONE)),
		dbutils.NewFilterCondition("job_attributes->>'resource_uuid'", "=", volume.UUID),
		dbutils.NewFilterCondition("job_attributes->>'pool_uuid'", "=", volume.Pool.UUID),
	)

	dbJobs, err := se.GetJobsWithCondition(ctx, *filter)
	if err != nil {
		return nil, err
	}
	return dbJobs, nil
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

func _getIPAddressForVolume(ctx context.Context, se database.Storage, volume *datamodel.Volume) ([]string, error) {
	ipAddresses := make([]string, 0)
	nodes, err := se.GetNodesByPoolID(ctx, volume.PoolID)
	if err != nil {
		return ipAddresses, err
	}

	if volume.VolumeAttributes.FileProperties != nil {
		protocol := volume.VolumeAttributes.Protocols[0]
		pType := utils.GetProtocolType(protocol)
		var nodesId []int64
		for _, node := range nodes {
			nodesId = append(nodesId, node.ID)
		}
		lifs, err := se.GetLifsForNodesWithProtocol(ctx, nodesId, volume.AccountID, string(pType))
		if err != nil {
			return ipAddresses, err
		}
		for _, lif := range lifs {
			ipAddresses = append(ipAddresses, lif.IPAddress)
		}
	} else {
		for _, node := range nodes {
			lif, err := se.GetLifForNode(ctx, node.ID, volume.AccountID)
			if err != nil {
				return ipAddresses, err
			}
			ipAddresses = append(ipAddresses, lif.IPAddress)
		}
	}

	return ipAddresses, nil
}

// VolumeTypeProcessor defines protocol-specific validation for volume creation
type VolumeTypeProcessor interface {
	Validate(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, accountID int64) error
}

type BlockVolumeProcessor struct{}
type FileVolumeProcessor struct{}

func (v *BlockVolumeProcessor) Validate(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, accountID int64) error {
	// Block-specific validation: host group checks, block properties, etc.
	params.FileProperties = nil // Ensure FileProperties is nil for block volumes
	// Validate BlockDevices if provided
	if params.BlockDevices != nil && len(*params.BlockDevices) > 0 {
		blockDevice := (*params.BlockDevices)[0]
		hostGroupUUIDs := blockDevice.HostGroups
		err := validateBlockProperties(ctx, se, hostGroupUUIDs, accountID)
		if err != nil {
			return err
		}
	} else if params.BlockProperties != nil {
		hostGroupUUIDs := params.BlockProperties.HostGroupUUIDs
		err := validateBlockProperties(ctx, se, hostGroupUUIDs, accountID)
		if err != nil {
			return err
		}
	} else {
		return customerrors.NewUserInputValidationErr("Block Device/Block Properties is required")
	}
	return nil
}

func (v *FileVolumeProcessor) Validate(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, accountID int64) error {
	params.BlockProperties = nil // Ensure BlockProperties is nil for file volumes
	if params.FileProperties == nil {
		return customerrors.NewUserInputValidationErr("FileProperties cannot be nil for NAS volumes")
	}

	if params.FileProperties.ExportPolicy != nil {
		if len(params.FileProperties.ExportPolicy.ExportRules) == 0 {
			return customerrors.NewUserInputValidationErr("Export Rules cannot be empty in Export Policy")
		}
		for _, rule := range params.FileProperties.ExportPolicy.ExportRules {
			if rule.AllowedClients == "" {
				return customerrors.NewUserInputValidationErr("allowed clients cannot be nil in export rules")
			} else {
				// Validate allowed clients
				if err := validateAllowedClients(rule.AllowedClients); err != nil {
					return customerrors.NewUserInputValidationErr(fmt.Sprintf("allowed clients validation failed: %v", err))
				}
			}
		}
	}

	if params.CreationToken == "" {
		return customerrors.NewUserInputValidationErr("Creation Token cannot be empty")
	}
	return nil
}

func GetVolumeTypeValidator(protocols []string, accountName string) (VolumeTypeProcessor, error) {
	if utils.IsSanProtocols(protocols) {
		return &BlockVolumeProcessor{}, nil
	}
	if utils.IsNasProtocols(protocols) {
		if !utils.IsFileProtocolSupported(accountName) {
			return nil, customerrors.NewUserInputValidationErr("file protocols are not enabled")
		}
		return &FileVolumeProcessor{}, nil
	}
	return nil, customerrors.NewUserInputValidationErr("unsupported or unspecified protocol")
}

func _validateCreateVolumeParams(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
	if pool.LargeCapacity != params.LargeCapacity {
		return customerrors.NewUserInputValidationErr("pool large capacity setting does not match volume large capacity setting")
	}

	if params.LargeCapacity {
		if utils.IsSanProtocols(params.Protocols) {
			return customerrors.NewUserInputValidationErr("SAN protocols are not supported for large capacity volumes")
		}

		if params.BlockDevices != nil && len(*params.BlockDevices) > 0 {
			return customerrors.NewUserInputValidationErr("BlockDevices are not supported for large capacity volumes")
		}

		if params.LargeVolumeConstituentCount > int32(numOfLvHAPairs*maxConstituentVolumesPerAggregate) {
			return customerrors.NewUserInputValidationErr(fmt.Sprintf("Large Volume constituent count cannot be greater than %d", int32(numOfLvHAPairs*maxConstituentVolumesPerAggregate)))
		}

		if params.QuotaInBytes < utils.MinQuotaInBytesLargeVolume || params.QuotaInBytes > utils.MaxQuotaInBytesLargeVolume {
			return customerrors.NewUserInputValidationErr(fmt.Sprintf("Invalid volume capacity %s. Must be between %s and %s.",
				utils.FmtUint64Bytes(params.QuotaInBytes), utils.FmtUint64Bytes(utils.MinQuotaInBytesLargeVolume),
				utils.FmtUint64Bytes(utils.MaxQuotaInBytesLargeVolume)))
		}
	}

	if !params.LargeCapacity {
		if params.LargeVolumeConstituentCount > 0 {
			return customerrors.NewUserInputValidationErr("Large Volume constituent count is only supported for large capacity volumes")
		}

		if params.QuotaInBytes < minQuotaInBytesVolume || params.QuotaInBytes > maxQuotaInBytesVolume {
			return customerrors.NewUserInputValidationErr(fmt.Sprintf("Invalid volume capacity %s. Must be between %s and %s.",
				utils.FmtUint64Bytes(params.QuotaInBytes), utils.FmtUint64Bytes(minQuotaInBytesVolume),
				utils.FmtUint64Bytes(maxQuotaInBytesVolume)))
		}
	}

	if pool.QuotaInBytes+params.QuotaInBytes > uint64(pool.SizeInBytes) {
		return customerrors.NewUserInputValidationErr("volume size cannot be greater than pool size")
	}

	switch pool.State {
	case models.LifeCycleStateCreating, models.LifeCycleStateDeleting, models.LifeCycleStateDeleted:
		return customerrors.NewUserInputValidationErr(fmt.Sprintf("Specified pool is in %s state, hence volume cannot be created", pool.State))
	case models.LifeCycleStateError:
		return customerrors.NewUserInputValidationErr("Pool is currently unavailable for creating volume")
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

	minNodeCount := 2
	if envIsLocalEnv() {
		// VSIMs may have 1 node, VSA clusters should have at least 2 nodes
		minNodeCount = 1
	}

	if len(nodes) < minNodeCount {
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

	if params.DataProtection != nil && params.DataProtection.BackupVaultID != "" {
		bv, err := se.GetBackupVaultByUUIDndOwnerID(ctx, params.DataProtection.BackupVaultID, pool.Account.ID)
		if err != nil && !customerrors.IsNotFoundErr(err) {
			return err
		}
		if bv != nil {
			if bv.LifeCycleState == models.LifeCycleStateError {
				return customerrors.NewUserInputValidationErr("backup vault is in error state, please check the backup vault and try again")
			}
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

	if !pool.AllowAutoTiering && params.AutoTieringPolicy != nil && params.AutoTieringPolicy.AutoTieringEnabled {
		return customerrors.NewUserInputValidationErr("Auto Tiering is not allowed for this volume. Please enable Auto Tiering on the Pool and try again")
	} else if params.AutoTieringPolicy != nil && params.AutoTieringPolicy.AutoTieringEnabled {
		if params.AutoTieringPolicy.CoolingThresholdDays < minCoolingThresholdDays || params.AutoTieringPolicy.CoolingThresholdDays > maxCoolingThresholdDays {
			return customerrors.NewUserInputValidationErr("Auto Tiering Cooling Threshold days must be between 2 and 183 days")
		}
	}

	// Protocol validation based on FileProtocolSupported flag
	if len(params.Protocols) == 0 {
		return customerrors.NewUserInputValidationErr("at least one protocol must be specified")
	}

	// Protocol-specific validation
	validator, err := GetVolumeTypeValidator(params.Protocols, params.AccountName)
	if err != nil {
		return err
	}
	return validator.Validate(ctx, se, params, pool.AccountID)
}

func _validateDeleteVolumeParams(ctx context.Context, se database.Storage, volume *datamodel.Volume) error {
	// Check if backup is in transition state for this volume
	backupTransitionState, err := se.IsBackupInCreatingorDeletingStateByVolume(ctx, volume.UUID)
	if err != nil {
		return err
	}

	if backupTransitionState {
		return customerrors.NewUserInputValidationErr("A backup operation on volume is currently in progress. Please wait for it to complete before deleting the volume")
	}

	// Check if volume is in replication
	replicationCount, err := se.GetVolumeReplicationCountByVolumeID(ctx, volume.ID)
	if err != nil {
		return err
	}

	if replicationCount > 0 {
		return customerrors.NewUserInputValidationErr("Cannot delete volume that has active replication. Please delete the replication first.")
	}

	return nil
}

func convertDatastoreVolumeToModel(volume *datamodel.Volume, ipAddress *[]string) *models.Volume {
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
		Mounted:               volume.VolumeAttributes.Mounted,
		Zone:                  volume.Pool.PoolAttributes.PrimaryZone,
		UsedBytes:             volume.UsedBytes,
		SnapReserve:           volume.VolumeAttributes.SnapReserve,
	}
	attributes := volume.VolumeAttributes
	res.VendorSubnetID = attributes.VendorSubnetID
	res.CreationToken = attributes.CreationToken
	res.ProtocolTypes = attributes.Protocols

	if volume.Svm != nil {
		res.SvmName = volume.Svm.Name
	}
	var kmsConfigUUID string
	if volume.Pool.KmsConfig != nil {
		res.KmsConfig = &models.KmsConfig{
			BaseModel: models.BaseModel{
				UUID:      volume.Pool.KmsConfig.UUID,
				CreatedAt: volume.Pool.KmsConfig.CreatedAt,
				UpdatedAt: volume.Pool.KmsConfig.UpdatedAt,
				DeletedAt: DeletedAtOrNil(volume.Pool.KmsConfig.DeletedAt),
			},
			Name:              volume.Pool.KmsConfig.Name,
			Description:       volume.Pool.KmsConfig.Description,
			State:             volume.Pool.KmsConfig.State,
			StateDetails:      volume.Pool.KmsConfig.StateDetails,
			KeyRing:           volume.Pool.KmsConfig.KeyRing,
			KeyRingLocation:   volume.Pool.KmsConfig.KeyRingLocation,
			KeyName:           volume.Pool.KmsConfig.KeyName,
			AccountID:         volume.Pool.KmsConfig.AccountID,
			CustomerProjectID: volume.Pool.KmsConfig.CustomerProjectID,
			KeyProjectID:      volume.Pool.KmsConfig.KeyProjectID,
			ResourceID:        volume.Pool.KmsConfig.ResourceID,
		}
		kmsConfigUUID = volume.Pool.KmsConfig.UUID
	}
	res.EncryptionType = utils.GetEncryptionType(&kmsConfigUUID)
	if attributes.BlockProperties != nil {
		res.BlockProperties = &models.BlockProperties{
			OSType:          attributes.BlockProperties.OSType,
			LunName:         attributes.BlockProperties.LunName,
			LunSerialNumber: attributes.BlockProperties.LunSerialNumber,
			HostGroupDetail: convertHostGroupDetails(attributes.BlockProperties.HostGroupDetails),
		}
	}
	if attributes.BlockDevices != nil {
		blockDevices := make([]models.BlockDevice, 0, len(*attributes.BlockDevices))
		for _, blockDevice := range *attributes.BlockDevices {
			blockDeviceModel := &models.BlockDevice{
				Name:       blockDevice.Name,
				OSType:     blockDevice.OSType,
				Size:       uint64(blockDevice.Size),
				Identifier: blockDevice.Identifier,
			}
			if len(blockDevice.HostGroupDetails) > 0 {
				hostGroups := make([]models.HostGroupDetails, 0, len(blockDevice.HostGroupDetails))
				for _, hg := range blockDevice.HostGroupDetails {
					hostGroups = append(hostGroups, models.HostGroupDetails{
						Hosts:       hg.HostQNs,
						HostGroupID: hg.HostGroupUUID,
					})
				}
				blockDeviceModel.HostGroupDetail = hostGroups
			}
			blockDevices = append(blockDevices, *blockDeviceModel)
		}
		res.BlockDevices = &blockDevices
	}
	labels := make(map[string]string)
	if attributes.Labels != nil {
		labels = convertJSONBToMap(attributes.Labels)
	}
	res.Labels = labels
	if volume.DataProtection != nil {
		res.DataProtection = &models.DataProtection{
			BackupVaultID:          volume.DataProtection.BackupVaultID,
			BackupPolicyId:         volume.DataProtection.BackupPolicyID,
			BackupChainBytes:       volume.DataProtection.BackupChainBytes,
			ScheduledBackupEnabled: volume.DataProtection.ScheduledBackupEnabled,
		}
	}

	if ipAddress != nil {
		res.IPAddresses = *ipAddress
	}

	if volume.SnapshotPolicy != nil {
		res.SnapshotPolicy = &models.SnapshotPolicy{
			Name:      volume.SnapshotPolicy.Name,
			IsEnabled: volume.SnapshotPolicy.IsEnabled,
			Comment:   volume.SnapshotPolicy.Comment,
			Schedules: convertToModelSnapshotPolicySchedule(volume.SnapshotPolicy.Schedules),
		}
	}

	if attributes.FileProperties != nil {
		res.FileProperties = &models.FileProperties{
			JunctionPath: attributes.FileProperties.JunctionPath,
		}
		if attributes.FileProperties.ExportPolicy != nil {
			exportRules := make([]*models.ExportRule, 0, len(attributes.FileProperties.ExportPolicy.ExportRules))
			for _, rule := range attributes.FileProperties.ExportPolicy.ExportRules {
				exportRules = append(exportRules, &models.ExportRule{
					AllowedClients:      rule.AllowedClients,
					AccessType:          rule.AccessType,
					CIFS:                rule.CIFS,
					NFSv3:               rule.NFSv3,
					NFSv4:               rule.NFSv4,
					UnixReadOnly:        rule.UnixReadOnly,
					UnixReadWrite:       rule.UnixReadWrite,
					Index:               rule.Index,
					ChownMode:           rule.ChownMode,
					AnonymousUser:       rule.AnonymousUser,
					Kerberos5iReadOnly:  rule.Kerberos5iReadOnly,
					Kerberos5iReadWrite: rule.Kerberos5iReadWrite,
					Kerberos5pReadOnly:  rule.Kerberos5pReadOnly,
					Kerberos5pReadWrite: rule.Kerberos5pReadWrite,
					Kerberos5ReadOnly:   rule.Kerberos5ReadOnly,
					Kerberos5ReadWrite:  rule.Kerberos5ReadWrite,
					S3:                  rule.S3,
				})
			}
			res.FileProperties = &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: attributes.FileProperties.ExportPolicy.ExportPolicyName,
					ExportRules:      exportRules,
				},
				JunctionPath: attributes.FileProperties.JunctionPath,
			}
		}
	}

	if volume.AutoTieringEnabled && volume.AutoTieringPolicy != nil {
		res.AutoTieringPolicy = &models.AutoTieringPolicy{
			AutoTieringEnabled:   volume.AutoTieringEnabled,
			CoolingThresholdDays: volume.AutoTieringPolicy.CoolingThresholdDays,
			TieringPolicy:        volume.AutoTieringPolicy.TieringPolicy,
		}
	}

	if volume.LargeVolumeAttributes != nil {
		res.LargeCapacity = volume.LargeVolumeAttributes.LargeCapacity
		res.LargeVolumeConstituentCount = volume.LargeVolumeAttributes.LargeVolumeConstituentCount
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
		return nil, "", customerrors.NewUserInputValidationErr("volume is in transition state and cannot be deleted, state: " + volume.State)
	}

	// Validate delete volume parameters and preconditions
	err = validateDeleteVolumeParams(ctx, se, volume)
	if err != nil {
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeDeleteVolume),
		State:         string(models.JobsStateNEW),
		ResourceName:  volume.Name,
		AccountID:     sql.NullInt64{Int64: volume.Account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: volume.UUID,
		},
	}

	workflowFunc := workflows.DeleteVolumeWorkflow

	if volume.LargeVolumeAttributes != nil && volume.LargeVolumeAttributes.LargeCapacity {
		job.Type = string(models.JobTypeDeleteLargeVolume)
	}

	if volume.CacheParameters != nil {
		job.Type = string(models.JobTypeFlexCacheDeleteVolume)
		workflowFunc = flexcache_workflows.DeleteFlexCacheVolumeWorkflow
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create volume delete job in database", "error", err)
		return nil, "", err
	}

	// Defer to mark job as error if workflow execution fails
	defer func() {
		if err != nil {
			updateErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error())
			if updateErr != nil {
				logger.Error("Failed to update job state to ERROR", "job_id", createdJob.UUID, "error", updateErr)
			}
		}
	}()

	err = se.UpdateVolumeFields(ctx, volume.UUID, map[string]interface{}{
		"state":         models.LifeCycleStateDeleting,
		"state_details": models.LifeCycleStateDeletingDetails,
	})
	if err != nil {
		logger.Error("Failed to update volume state in database", "error", err)
		return nil, "", err
	}
	// Defer to mark volume as error if workflow execution fails
	defer func() {
		if err != nil {
			volumeUpdateErr := se.UpdateVolumeFields(ctx, volume.UUID, map[string]interface{}{
				"state":         models.LifeCycleStateError,
				"state_details": models.LifeCycleStateDeletionErrorDetails,
			})
			if volumeUpdateErr != nil {
				logger.Error("Failed to update volume state to ERROR", "volume_id", volume.UUID, "error", volumeUpdateErr)
			}
		}
	}()

	location, err := utils.GetLocationFromVendorID(volume.Pool.VendorID)
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
		workflowFunc,
		workflow.ChildWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			WorkflowID:            createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
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

	var result []*models.Volume
	if len(volumeIds) == 1 {
		// CCFE does GetMultipleVolumes call with single volumeId instead of calling GetVolume API
		// So, to update volume metrics, we call GetVolume here
		volume, getVolErr := o.GetVolume(ctx, volumeIds[0], true)
		if getVolErr != nil {
			if customerrors.IsNotFoundErr(getVolErr) {
				return result, nil
			}
			return nil, getVolErr
		}
		result = append(result, volume)
	} else {
		conditions := [][]interface{}{{"account_id = ?", account.ID}}
		conditions = append(conditions, []interface{}{"uuid in ?", volumeIds})
		volumes, err2 := se.GetMultipleVolumes(ctx, conditions)
		if err2 != nil {
			return nil, err2
		}
		for _, volume := range volumes {
			ipAddresses, ipErr := getIPAddressForVolume(ctx, se, volume)
			if ipErr != nil {
				return nil, ipErr
			}
			result = append(result, convertDatastoreVolumeToModel(volume, &ipAddresses))
		}
	}

	return result, nil
}

func (o *Orchestrator) UpdateVolumeV2(ctx context.Context, param *common.UpdateVolumeParams) (*models.Volume, string, error) {
	logger := util.GetLogger(ctx)
	se := o.storage
	dbVolume, err := se.GetVolume(ctx, param.VolumeId)
	if err != nil {
		return nil, "", err
	}

	isReplication := false
	count, err := se.GetVolumeReplicationCountByVolumeID(ctx, dbVolume.ID)
	if err != nil {
		logger.Error("Failed to get volume replication", "error", err)
		return nil, "", err
	}

	if count != 0 {
		isReplication = true
	}

	return updateVolume(ctx, se, o.temporal, param, isReplication)
}

// UpdateVolume updates the specified volume with the new parameters
func (o *Orchestrator) UpdateVolume(ctx context.Context, param *common.UpdateVolumeParams) (*models.Volume, string, error) {
	return updateVolume(ctx, o.storage, o.temporal, param, false)
}

func _updateVolume(ctx context.Context, se database.Storage, temporal client.Client, params *common.UpdateVolumeParams, isReplication bool) (*models.Volume, string, error) {
	logger := util.GetLogger(ctx)

	dbVolume, err := se.GetVolume(ctx, params.VolumeId)
	if err != nil {
		return nil, "", err
	}

	if params.DataProtection != nil {
		// If backup vault is already attached to the volume and the backup vault is changed or removed
		if dbVolume.DataProtection != nil && dbVolume.DataProtection.BackupVaultID != "" && params.DataProtection.BackupVaultID != nil && (*params.DataProtection.BackupVaultID == "" || *params.DataProtection.BackupVaultID != dbVolume.DataProtection.BackupVaultID) {
			// If backup policy is already assigned to the volume, we should not be able to remove the backup vault from the volume
			if dbVolume.DataProtection.BackupPolicyID != "" {
				return nil, "", customerrors.NewUserInputValidationErr("cannot remove backup vault as backup policy is associated to the volume")
			}
			filters := [][]interface{}{{"volume_uuid = ?", dbVolume.UUID}}
			backups, err := se.GetBackupsByBackupVaultOwnerIDAndFilter(ctx, dbVolume.DataProtection.BackupVaultID, dbVolume.Account.ID, filters)
			if err != nil {
				return nil, "", err
			}
			if len(backups) > 0 {
				return nil, "", customerrors.NewUserInputValidationErr("cannot remove backup vault as there are backups associated with it")
			}
			dbVolume.DataProtection.BackupVaultID = *params.DataProtection.BackupVaultID
		} else {
			if dbVolume.DataProtection == nil {
				dbVolume.DataProtection = &datamodel.DataProtection{}
			}
			dbVolume.DataProtection.BackupVaultID = nillable.GetString(params.DataProtection.BackupVaultID, dbVolume.DataProtection.BackupVaultID)
			dbVolume.DataProtection.BackupPolicyID = nillable.GetString(params.DataProtection.BackupPolicyId, dbVolume.DataProtection.BackupPolicyID)
			dbVolume.DataProtection.ScheduledBackupEnabled = params.DataProtection.ScheduledBackupEnabled

			if dbVolume.DataProtection.BackupVaultID == "" && !nillable.IsNilOrEmpty(params.DataProtection.BackupPolicyId) {
				return nil, "", customerrors.NewUserInputValidationErr("backup vault is required to assign a backup policy to a volume")
			}
			if dbVolume.DataProtection.BackupPolicyID != "" && params.DataProtection.ScheduledBackupEnabled == nil {
				return nil, "", customerrors.NewUserInputValidationErr("scheduled backups needs to be enabled/disabled when a backup policy is assigned to a volume")
			}
		}
	}

	if params.Labels != nil && dbVolume.VolumeAttributes != nil {
		dbVolume.VolumeAttributes.Labels = params.Labels
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

	wf := workflows.UpdateVolumeWorkflow
	if isReplication {
		job.Type = string(models.JobTypeUpdateVolumeInReplication)
		wf = workflows.UpdateVolumeInReplicationWorkflow
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create volume update job in database", "error", err)
		return nil, "", err
	}

	// Store the original dbVolume for use in defer function to avoid nil pointer issues
	originalDBVolume := dbVolume

	// Defer to mark job as error if workflow execution fails
	defer func() {
		if err != nil && createdJob != nil {
			updateErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error())
			if updateErr != nil {
				logger.Error("Failed to update job state to ERROR", "job_id", createdJob.UUID, "error", updateErr)
			}
		}
	}()

	if params.SnapshotPolicy != nil {
		params.SnapshotPolicy.Name = dbVolume.Name
	}

	dbVolume, err = updateVolumeStatus(ctx, se, dbVolume, models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails)
	if err != nil {
		logger.Error("Failed to update volume state in database", "error", err)
		return nil, "", err
	}

	defer func() {
		if err != nil && createdJob != nil {
			volumeUpdateErr := se.UpdateVolumeFields(ctx, originalDBVolume.UUID, map[string]interface{}{
				"state":         models.LifeCycleStateError,
				"state_details": models.LifeCycleStateUpdateErrorDetails,
			})
			if volumeUpdateErr != nil {
				logger.Error("Failed to update volume state to ERROR", "volume_id", originalDBVolume.UUID, "error", volumeUpdateErr)
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		wf,
		params,
		dbVolume,
	)

	if err != nil {
		logger.Error("Failed to start update volume workflow: ", "error", err)
		return nil, "", err
	}
	return convertDatastoreVolumeToModel(dbVolume, nil), createdJob.UUID, nil
}

func _updateVolumeStatus(ctx context.Context, se database.Storage, dbVolume *datamodel.Volume, state string, stateDetails string) (*datamodel.Volume, error) {
	err := se.UpdateVolumeFields(ctx, dbVolume.UUID, map[string]interface{}{
		"state":         state,
		"state_details": stateDetails,
	})
	if err != nil {
		return nil, err
	}
	dbVolume.State = state
	dbVolume.StateDetails = stateDetails
	return dbVolume, err
}

func validateUpdateVolumeRequest(ctx context.Context, se database.Storage, volume *datamodel.Volume, params *common.UpdateVolumeParams, pool *datamodel.PoolView) error {
	if volume.State == models.LifeCycleStateUpdating {
		return customerrors.NewConflictErr("An update operation is already in progress for this volume")
	} else if utils.IsTransitionalState(volume.State) {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf("Volume %s cannot be updated, while in transitioning state: %s", volume.Name, volume.State))
	}

	// Greater than 0 means the value was provided in the request
	if params.QuotaInBytes > 0 {
		if params.QuotaInBytes < volume.SizeInBytes {
			return customerrors.NewUserInputValidationErr("volume size cannot be reduced")
		}
		// Calculate the size increase
		sizeIncrease := params.QuotaInBytes - volume.SizeInBytes

		// Check if adding the increase to current pool usage exceeds pool size
		if sizeIncrease > 0 && pool.QuotaInBytes+uint64(sizeIncrease) > uint64(pool.SizeInBytes) {
			return customerrors.NewUserInputValidationErr("Total size of volumes in a pool cannot exceed the pool capacity.")
		}
	}

	if !pool.AllowAutoTiering && params.AutoTieringPolicy != nil && params.AutoTieringPolicy.AutoTieringEnabled {
		return customerrors.NewUserInputValidationErr("Auto Tiering is not allowed for this volume. Please enable Auto Tiering on the Pool and try again")
	} else if params.AutoTieringPolicy != nil && params.AutoTieringPolicy.AutoTieringEnabled {
		if params.AutoTieringPolicy.CoolingThresholdDays < minCoolingThresholdDays || params.AutoTieringPolicy.CoolingThresholdDays > maxCoolingThresholdDays {
			return customerrors.NewUserInputValidationErr("Auto Tiering Cooling Threshold days must be between 2 and 183 days")
		}
	}

	if len(params.BlockDevices) > 0 {
		// Find the corresponding BlockDevice in the volume by LUN name
		var matchingBlockDevice *common.BlockDevice

		// Check if volume has BlockDevices
		if volume.VolumeAttributes.BlockDevices != nil && len(*volume.VolumeAttributes.BlockDevices) > 0 {
			// Try to find matching BlockDevice by name
			for _, paramBlockDevice := range params.BlockDevices {
				if paramBlockDevice.Name != "" {
					for _, volBlockDevice := range *volume.VolumeAttributes.BlockDevices {
						if volBlockDevice.Name == paramBlockDevice.Name {
							matchingBlockDevice = paramBlockDevice
							// Validate that OSType cannot be changed
							if paramBlockDevice.OSType != "" && paramBlockDevice.OSType != volBlockDevice.OSType {
								return customerrors.NewUserInputValidationErr("Cannot update OSType for block device. OSType is immutable after creation")
							}

							// assign the read-only properties from the volume's BlockDevice
							matchingBlockDevice.SizeInBytes = volBlockDevice.Size
							matchingBlockDevice.OSType = volBlockDevice.OSType
							matchingBlockDevice.LunSerialNumber = volBlockDevice.Identifier
							matchingBlockDevice.LunUUID = volBlockDevice.LunUUID
							break
						}
					}
					if matchingBlockDevice != nil {
						break
					}
				}
			}
		}
		if matchingBlockDevice == nil {
			return customerrors.NewUserInputValidationErr("could not find matching BlockDevice.")
		}
		hostGroupUUIDs := matchingBlockDevice.HostGroups
		err := validateBlockProperties(ctx, se, hostGroupUUIDs, volume.Account.ID)
		if err != nil {
			return err
		}
	} else if params.BlockProperties != nil {
		hostGroupUUIDs := params.BlockProperties.HostGroupUUIDs
		err := validateBlockProperties(ctx, se, hostGroupUUIDs, volume.Account.ID)
		if err != nil {
			return err
		}
	}

	if params.DataProtection != nil && params.DataProtection.BackupVaultID != nil && *params.DataProtection.BackupVaultID != "" {
		bv, err := se.GetBackupVaultByUUIDndOwnerID(ctx, *params.DataProtection.BackupVaultID, pool.Account.ID)
		if err != nil && !customerrors.IsNotFoundErr(err) {
			return err
		}
		if bv != nil {
			if bv.LifeCycleState == models.LifeCycleStateError {
				return customerrors.NewUserInputValidationErr("backup vault is in error state, please check the backup vault and try again")
			}
		}
	}

	if params.DataProtection != nil && !nillable.IsNilOrEmpty(params.DataProtection.BackupPolicyId) {
		backupPolicy, err := se.GetBackupPolicyByUUIDAndOwnerID(ctx, *params.DataProtection.BackupPolicyId, pool.Account.ID)
		if err != nil && !customerrors.IsNotFoundErr(err) {
			return err
		}
		if backupPolicy != nil && backupPolicy.LifeCycleState != models.LifeCycleStateREADY {
			return customerrors.NewUserInputValidationErr("backup policy is not in ready state, please check the backup policy and try again")
		}
	}

	// When just enabling or disabling the snapshot policy, we need to check if there is an existing snapshot policy
	if params.SnapshotPolicy != nil && len(params.SnapshotPolicy.Schedules) == 0 && (volume.SnapshotPolicy == nil || volume.SnapshotPolicy.Name == "") {
		return customerrors.NewUserInputValidationErr("no existing snapshot policy found for the volume and no schedules provided in the update request. Cannot create a new snapshot policy without schedules")
	}

	if volume.VolumeAttributes != nil && volume.VolumeAttributes.IsDataProtection {
		if params.SnapReserve != nil && *params.SnapReserve != volume.VolumeAttributes.SnapReserve {
			return customerrors.NewUserInputValidationErr("Cannot update snapshotReserve on a Data Protection Volume")
		}
		if params.DataProtection != nil && !nillable.IsNilOrEmpty(params.DataProtection.BackupPolicyId) {
			return customerrors.NewUserInputValidationErr("Cannot update backup policy on a Data Protection Volume. Only manual backups are supported")
		}
		if params.SnapshotPolicy != nil && len(params.SnapshotPolicy.Schedules) > 0 {
			return customerrors.NewUserInputValidationErr("Cannot update snapshot policy on a Data Protection Volume.")
		}
	}

	// Validate snapReserve changes to ensure sufficient LUN space
	if params.SnapReserve != nil && volume.VolumeAttributes != nil && *params.SnapReserve != volume.VolumeAttributes.SnapReserve {
		if *params.SnapReserve > volume.VolumeAttributes.SnapReserve {
			var requiredQuotaInBytes int64
			// Calculate current available LUN space
			currentLunSpace := volume.SizeInBytes - int64(float64(volume.SizeInBytes)*float64(volume.VolumeAttributes.SnapReserve)/percentageBase)
			if params.QuotaInBytes == 0 {
				// Calculate required size with the given snapReserve to ensure sufficient LUN space
				requiredQuotaInBytes = int64(float64(currentLunSpace) / (1 - float64(*params.SnapReserve)/percentageBase))
				return customerrors.NewUserInputValidationErr(fmt.Sprintf(ErrMsgSnapReserveIncrease, float64(*params.SnapReserve), float64(currentLunSpace)/float64(bytesPerGB), math.Ceil(float64(requiredQuotaInBytes)/float64(bytesPerGB))))
			} else {
				// Calculate updated LUN space with the new given size
				updatedLunSpace := params.QuotaInBytes - int64(float64(params.QuotaInBytes)*float64(*params.SnapReserve)/percentageBase)
				if updatedLunSpace < currentLunSpace {
					// Calculate required size to ensure sufficient LUN space
					requiredQuotaInBytes = int64(float64(currentLunSpace) / (1 - float64(*params.SnapReserve)/percentageBase))
					return customerrors.NewUserInputValidationErr(fmt.Sprintf(ErrMsgSnapReserveIncrease, float64(*params.SnapReserve), float64(currentLunSpace)/float64(bytesPerGB), math.Ceil(float64(requiredQuotaInBytes)/float64(bytesPerGB))))
				}
			}
		}
		// Allow snapReserve decrease as it increases available LUN space
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

func validateAllowedClients(allowedClients string) error {
	clients := strings.Split(allowedClients, ",")
	clientsMap := make(map[string]bool)
	if allowedClients == models.AllowedAllClients {
		return nil
	}
	for _, cidr := range clients {
		// first check if it's a valid IP without CIDR
		if ip := net.ParseIP(cidr); ip == nil {
			// if nil, then check if it's a valid IP with CIDR
			ip, ipnet, err := net.ParseCIDR(cidr)
			if err != nil {
				return customerrors.NewUserInputValidationErr("allowedClients must include unique IPv4 or IPv4 CIDR values.")
			}
			if !ip.Equal(ipnet.IP) {
				return customerrors.NewUserInputValidationErr(fmt.Sprintf("Requested export policy CIDR (%s) is invalid. Please use a valid CIDR (e.g. %s)", cidr, ipnet.String()))
			}
			if ones, _ := ipnet.Mask.Size(); ip.IsUnspecified() && ones != 0 {
				return customerrors.NewUserInputValidationErr("0.0.0.0 address can only be used with a 0 bit subnet mask")
			}
		}
		clientsMap[cidr] = true
	}

	if len(clientsMap) != len(clients) {
		return customerrors.NewUserInputValidationErr("allowedClients must include unique IPv4 or IPv4 CIDR values.")
	}
	return nil
}
