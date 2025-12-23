package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_policy"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_vault"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/flexcache_workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/replicationWorkflows"
	dbUtils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

var (
	minCVSizeInBytes                     = env.GetUint64("MIN_CONSTITUENT_VOLUME_SIZE_BYTES", 100*bytesPerGB)
	numOfLvHAPairs                       = env.GetInt64("NUMBER_OF_HA_PAIRS_LARGE_CAPACITY", 6)
	volumeRefreshIntervalMinutes         = env.GetInt("VOLUME_REFRESH_INTERVAL_MINUTES", 5)
	maxThinClonesPerPool                 = env.GetInt64("MAX_THIN_CLONES_PER_POOL", 100)
	thinCloneGASupport                   = env.GetBool("THIN_CLONE_GA_SUPPORT", false)
	cmekBackupEnabled                    = env.GetBool("CMEK_BACKUP_ENABLED", false)
	minQuotaInBytesVolume                = utils.MinQuotaInBytesVolumeForVolume
	maxQuotaInBytesVolume                = utils.MaxQuotaInBytesVolumeForVolume
	createVolume                         = _createVolume
	revertVolume                         = _revertVolume
	splitCloneVolume                     = _splitCloneVolume
	validateCreateVolumeParams           = _validateCreateVolumeParams
	validateSplitCloneVolumeParams       = _validateSplitCloneVolumeParams
	getIPAddressForVolume                = _getIPAddressForVolume
	updateVolume                         = _updateVolume
	deleteVolume                         = _deleteVolume
	validateDeleteVolumeParams           = _validateDeleteVolumeParams
	updateVolumeStatus                   = _updateVolumeStatus
	convertDatastoreVolumeToModel        = _convertDatastoreVolumeToModel
	checkAndCancelCreateWorkflowIfNeeded = _checkAndCancelCreateWorkflowIfNeeded
	minPrimeNumberConfigAllowed          = 7

	envIsLocalEnv                              = env.IsLocalEnv
	cvpCreateClient                            = cvp.CreateClient
	GetBackupVaultFromCVP                      = getBackupVaultFromCVP
	enableAutoPoolScaling                      = env.GetBool("ENABLE_AUTO_POOL_SCALING", true)
	autoPoolScalingLimits                      = env.GetString("AUTO_POOL_SCALING_LIMITS", "{\"c3-standard-4-lssd\":{\"min_volume_count\":0,\"max_volume_count\":245},\"c3-standard-8-lssd\":{\"min_volume_count\":0,\"max_volume_count\":495},\"c3-standard-22-lssd\":{\"min_volume_count\":0,\"max_volume_count\":995}}")
	maxConstituentVolumesPerVolumePerAggregate = env.GetInt64("MAX_CONSTITUENT_VOLUMES_PER_VOLUME_PER_AGGREGATE", 200)
	checkIsValidImmutableBackupPolicyWithRetry = _checkIsValidImmutableBackupPolicyWithRetry
)

const (
	minCoolingThresholdDays   = 2
	maxCoolingThresholdDays   = 183
	MaxBackupPathComponents   = 8          // The expected number of components in the backup path
	BackupNameIndex           = 7          // The index of the backup name in the components
	BackupVaultNameIndex      = 5          // The index of the backup vault name in the components
	LocationIdIndex           = 3          // The index of the locationId in the backup path
	bytesPerGB                = 1073741824 // 1024^3 bytes = 1 GB
	ErrMsgSnapReserveIncrease = "Cannot increase SnapReserve to %.0f%% as we cannot decrease the available space (%.2f GB). " +
		"Please increase the volume size to at least %.0f GB with this SnapReserve or reduce the SnapReserve percentage to continue."
)

// convertExportRulesToDatamodel converts a slice of models.ExportRule to a slice of datamodel.ExportRule
func convertExportRulesToDatamodel(modelRules []*models.ExportRule) []*datamodel.ExportRule {
	exportRules := make([]*datamodel.ExportRule, 0, len(modelRules))
	for _, rule := range modelRules {
		exportRules = append(exportRules, &datamodel.ExportRule{
			AllowedClients:      rule.AllowedClients,
			AccessType:          rule.AccessType,
			CIFS:                rule.CIFS,
			NFSv3:               rule.NFSv3,
			NFSv4:               rule.NFSv4,
			Index:               rule.Index,
			Kerberos5iReadOnly:  rule.Kerberos5iReadOnly,
			Kerberos5iReadWrite: rule.Kerberos5iReadWrite,
			Kerberos5pReadWrite: rule.Kerberos5pReadWrite,
			Kerberos5ReadOnly:   rule.Kerberos5ReadOnly,
			Kerberos5ReadWrite:  rule.Kerberos5ReadWrite,
			Kerberos5pReadOnly:  rule.Kerberos5pReadOnly,
			AllSquash:           rule.AllSquash,
			AnonUID:             rule.AnonUID,
		})
	}
	return exportRules
}

// buildFilePropertiesFromParams creates a datamodel.FileProperties from params.FileProperties
func buildFilePropertiesFromParams(paramsFileProperties *models.FileProperties, creationToken string) *datamodel.FileProperties {
	if paramsFileProperties == nil {
		return nil
	}

	junctionPath := common.CreateJunctionPath(creationToken)
	fileProperties := &datamodel.FileProperties{
		JunctionPath: junctionPath,
	}

	if paramsFileProperties.ExportPolicy != nil {
		exportRules := convertExportRulesToDatamodel(paramsFileProperties.ExportPolicy.ExportRules)
		fileProperties.ExportPolicy = &datamodel.ExportPolicy{
			ExportPolicyName: paramsFileProperties.ExportPolicy.ExportPolicyName,
			ExportRules:      exportRules,
		}
		// SecurityStyle is only set when ExportPolicy exists (for regular volumes)
		if paramsFileProperties.SecurityStyle != "" {
			fileProperties.SecurityStyle = paramsFileProperties.SecurityStyle
		}
	}

	// SMBShareSettings are set separately if they exist (for regular volumes)
	if len(paramsFileProperties.SMBShareSettings) > 0 {
		fileProperties.SMBShareSettings = paramsFileProperties.SMBShareSettings
	}

	return fileProperties
}

// CreateVolume creates the specified volume and adds it to the list of volume belonging to the specified owner
func (o *Orchestrator) CreateVolume(ctx context.Context, params *common.CreateVolumeParams) (*models.Volume, string, error) {
	return createVolume(ctx, o.storage, o.temporal, params)
}

func _createVolume(ctx context.Context, se database.Storage, temporal client.Client, params *common.CreateVolumeParams) (*models.Volume, string, error) {
	logger := util.GetLogger(ctx)
	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)

	account, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return nil, "", err
	}

	pool, err := se.GetPool(ctx, params.PoolID, account.ID)
	if err != nil {
		return nil, "", err
	}

	if pool.APIAccessMode == workflows.ONTAPMode {
		return nil, "", customerrors.NewUserInputValidationErr("Cannot create Volumes in ONTAP mode pool using GCNV API")
	}

	poolPrimaryZone := pool.PoolAttributes.PrimaryZone
	isRegionalPool := pool.PoolAttributes.IsRegionalHA
	// Validate that volume zone matches pool's primary zone for zonal volume
	if !isRegionalPool && params.Zone != poolPrimaryZone {
		return nil, "", customerrors.NewConflictErr(fmt.Sprintf("Volume zone '%s' does not match pool's primary zone '%s'.", params.Zone, poolPrimaryZone))
	}

	// Check for existing volume with same name in the determined zone
	vol, volErr := se.GetVolumeByNameAccountIDAndZone(ctx, params.Name, pool.Account.ID, params.Zone, isRegionalPool)
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
			// Provide appropriate error message based on pool type
			var errorMsg string
			if isRegionalPool {
				errorMsg = fmt.Sprintf("Volume with resource_id '%s' already exists in region '%s'", params.Name, params.Region)
			} else {
				errorMsg = fmt.Sprintf("Volume with resource_id '%s' already exists in zone '%s'", params.Name, params.Zone)
			}
			return nil, "", customerrors.NewConflictErr(errorMsg)
		} else {
			// If volume is in CREATING state, check if it belongs to the same pool
			if vol.Pool.UUID != pool.UUID {
				return nil, "", customerrors.NewConflictErr(fmt.Sprintf("Volume with resource_id '%s' already exists in the '%s' pool, which is different from the requested pool '%s'", params.Name, vol.Pool.Name, pool.Name))
			}

			// Determine the correct job type based on whether it's a large capacity volume
			jobType := string(models.JobTypeCreateVolume)
			if params.LargeCapacity {
				jobType = string(models.JobTypeCreateLargeVolume)
			}

			job, jobErr := se.GetJobByResourceUUID(ctx, vol.UUID, jobType)
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

	clonesSharedBytes := uint64(0)
	parentVolumeUUID := ""
	if params.SnapshotID != "" {
		dbSnapshot, err := se.GetSnapshotByPoolID(ctx, params.SnapshotID, account.ID, pool.ID, true)
		if err != nil {
			logger.Error("Failed to fetch parent snapshot for volume creation. Please use the correct snapshot and retry again.", "error", err)
			return nil, "", err
		}
		// block volume creation from snapshot if snapshot is created for CRR or backup, or clone volume
		if dbSnapshot != nil {
			// Block if snapshot type is backup
			if dbSnapshot.Type == activities.SnapshotTypeBackup {
				logger.Error("Snapshot created for backup is not eligible for volume creation", "snapshot_id", dbSnapshot.UUID)
				return nil, "", customerrors.NewUserInputValidationErr("Snapshot is not eligible for volume creation. Snapshots created for backup, data protection, replication, or clone volumes are not supported.")
			}
			// Block if underlying volume itself is a clone (shares bytes with parent)
			if !thinCloneGASupport && dbSnapshot.Volume != nil && dbSnapshot.Volume.ClonesSharedBytes > 0 {
				logger.Error("Snapshot from a clone volume is not eligible for volume creation", "snapshot_id", dbSnapshot.UUID, "volume_id", dbSnapshot.Volume.UUID, "clones_shared_bytes", dbSnapshot.Volume.ClonesSharedBytes)
				return nil, "", customerrors.NewUserInputValidationErr("Snapshot is not eligible for volume creation. Snapshots created for backup, data protection, replication, or clone volumes are not supported.")
			}
			// Block if snapshot name has snapmirror prefix (CRR replication snapshot)
			if strings.HasPrefix(dbSnapshot.Name, "snapmirror.") {
				logger.Error("Replication (snapmirror) snapshot is not eligible for volume creation", "snapshot_id", dbSnapshot.UUID, "snapshot_name", dbSnapshot.Name)
				return nil, "", customerrors.NewUserInputValidationErr("Snapshot is not eligible for volume creation. Snapshots created for backup, data protection, replication, or clone volumes are not supported.")
			}
		}

		if params.Protocols != nil && dbSnapshot != nil && dbSnapshot.Volume != nil && dbSnapshot.Volume.VolumeAttributes != nil && dbSnapshot.Volume.VolumeAttributes.Protocols != nil {
			if (utils.IsSanProtocols(params.Protocols) && utils.IsNasProtocols(dbSnapshot.Volume.VolumeAttributes.Protocols)) || (utils.IsNasProtocols(params.Protocols) && utils.IsSanProtocols(dbSnapshot.Volume.VolumeAttributes.Protocols)) {
				logger.Error("Snapshot volume protocol type does not match requested volume protocol type", "snapshot_protocols", dbSnapshot.Volume.VolumeAttributes.Protocols, "requested_protocols", params.Protocols)
				return nil, "", customerrors.NewUserInputValidationErr("Snapshot volume protocol type does not match requested volume protocol type. Please use the correct snapshot and retry again.")
			}
		}
		if dbSnapshot.State != models.LifeCycleStateREADY {
			logger.Error("Parent snapshot is not in a valid state for volume creation", "snapshot_state", dbSnapshot.State)
			return nil, "", customerrors.NewUserInputValidationErr("Parent snapshot is not in a valid state for volume creation. Please wait for the snapshot to be ready and retry again.")
		}

		if dbSnapshot.Volume != nil {
			parentVolumeUUID = dbSnapshot.Volume.UUID
		}

		if dbSnapshot.Volume != nil && dbSnapshot.Volume.LargeVolumeAttributes != nil && dbSnapshot.Volume.LargeVolumeAttributes.LargeVolumeConstituentCount != nil {
			params.LargeVolumeConstituentCount = *dbSnapshot.Volume.LargeVolumeAttributes.LargeVolumeConstituentCount
		}
		params.Snapshot = dbSnapshot
		clonesSharedBytes = uint64(dbSnapshot.SnapshotAttributes.LogicalSizeUsedInBytes)
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
			CreationToken:     params.CreationToken,
			Protocols:         params.Protocols,
			VendorSubnetID:    params.Network,
			IsDataProtection:  params.IsDataProtection,
			SnapReserve:       params.SnapReserve,
			SnapshotDirectory: params.SnapshotDirectory,
			Labels:            params.Labels,
		},
		ClonesSharedBytes: clonesSharedBytes,
	}

	if utils.IsSanProtocols(params.Protocols) {
		volumeObj.VolumeAttributes.SnapshotDirectory = false
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
		volumeObj.VolumeAttributes.FileProperties = buildFilePropertiesFromParams(params.FileProperties, params.CreationToken)
	}

	if params.SnapshotID != "" {
		volumeObj.VolumeAttributes.CloneParentInfo = &datamodel.CloneParentInfo{
			ParentSnapshotUUID: params.SnapshotID,
			ParentVolumeUUID:   parentVolumeUUID,
		}
	}

	if params.DataProtection != nil {
		volumeObj.DataProtection = &datamodel.DataProtection{
			BackupVaultID:          params.DataProtection.BackupVaultID,
			BackupPolicyID:         params.DataProtection.BackupPolicyId,
			BackupChainBytes:       params.DataProtection.BackupChainBytes,
			ScheduledBackupEnabled: params.DataProtection.ScheduledBackupEnabled,
			KmsGrant:               params.DataProtection.KmsGrant,
		}
	}

	if params.SnapshotPolicy != nil {
		volumeObj.SnapshotPolicy = &datamodel.SnapshotPolicy{
			Name:      volumeObj.Name,
			IsEnabled: params.SnapshotPolicy.IsEnabled,
			Schedules: convertToDBSnapshotPolicySchedule(params.SnapshotPolicy.Schedules),
		}
	}

	if params.AutoTieringPolicy != nil {
		volumeObj.AutoTieringEnabled = params.AutoTieringPolicy.AutoTieringEnabled
		volumeObj.AutoTieringPolicy = &datamodel.AutoTieringPolicy{
			TieringPolicy:            params.AutoTieringPolicy.TieringPolicy,
			CoolingThresholdDays:     params.AutoTieringPolicy.CoolingThresholdDays,
			RetrievalPolicy:          params.AutoTieringPolicy.RetrievalPolicy,
			HotTierBypassModeEnabled: params.AutoTieringPolicy.HotTierBypassModeEnabled,
			CloudWriteModeEnabled:    params.AutoTieringPolicy.CloudWriteModeEnabled,
		}
	}

	// Handle backup restore path - validation only, actual backup fetching is done in workflow

	if params.BackupPath != "" {
		if volumeObj.VolumeAttributes == nil {
			volumeObj.VolumeAttributes = &datamodel.VolumeAttributes{}
		}
		logger.Debugf("params.BackupPath: %s", params.BackupPath)
		volumeObj.VolumeAttributes.RestoredBackupPath = params.BackupPath
		components := strings.Split(params.BackupPath, "/")

		// Ensure there are enough components to avoid out of range errors
		if len(components) < MaxBackupPathComponents {
			return nil, "", customerrors.NewUserInputValidationErr("Backup path is not in correct format")
		}
		// Note: Backup vault/backup fetching, size validation, and large volume compatibility
		// are all handled by FetchBackupMetadataForRestore activity in the workflow
		logger.Infof("Backup path validated, backup metadata will be fetched in workflow")
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
			// Mark volume in deleted state
			_, volumeDeleteErr := se.DeleteVolume(ctx, dbVolume.UUID)
			if volumeDeleteErr != nil {
				logger.Error("Failed to delete volume", "volume_id", dbVolume.UUID, "error", volumeDeleteErr)
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
			PoolUUID:     pool.UUID,
		},
	}

	if params.LargeCapacity {
		job.Type = string(models.JobTypeCreateLargeVolume)
	}

	wf := workflows.CreateVolumeWorkflow

	if params.HybridReplicationParameters != nil {
		job.Type = string(models.JobTypeCreateHybridReplication)
		job.ResourceName = fmt.Sprintf("projects/%s/locations/%s/volumes/%s/replications/%s",
			params.AccountName,
			location,
			params.Name,
			params.HybridReplicationParameters.ResourceID)
		wf = replicationWorkflows.CreateHybridReplicationWorkflow
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
	controlWorkflowID := workflows.GenerateControlWorkflowID(dbVolume.Account.ID, location, dbVolume.Pool.Name)
	workflowOptions := workflows.DefaultSequentialWorkflowOptions(controlWorkflowID, createdJob.WorkflowID)

	// Execute workflow using centralized executor
	err = workflowExecutor.ExecuteSequentialWorkflow(
		ctx,
		workflowOptions,
		wf,
		params,
		dbVolume,
	)
	if err != nil {
		logger.Error("Failed to start create volume workflow: ", "error", err)
		return nil, "", err
	}

	// Check if pool needs scaling based on volume count (async, non-blocking)
	// This happens after volume creation workflow is triggered successfully
	// Configuration variables
	if enableAutoPoolScaling {
		checkAndTriggerPoolScalingIfNeeded(ctx, se, temporal, dbPool, false)
	}

	return convertDatastoreVolumeToModel(dbVolume, nil), createdJob.UUID, nil
}

// RevertVolume creates the specified volume and adds it to the list of volume belonging to the specified owner
func (o *Orchestrator) RevertVolume(ctx context.Context, params *common.RevertVolumeParams) (*models.Volume, string, error) {
	return revertVolume(ctx, o.storage, o.temporal, params)
}

func _revertVolume(ctx context.Context, se database.Storage, temporal client.Client, params *common.RevertVolumeParams) (*models.Volume, string, error) {
	logger := util.GetLogger(ctx)
	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)

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

	// Check if volume is in REVERTING state - if so, check for existing revert jobs for idempotency
	if volume.State == models.LifeCycleStateReverting {
		filter := dbUtils.CreateFilterWithConditions(
			dbUtils.NewFilterCondition("resource_name", "=", volume.Name),
			dbUtils.NewFilterCondition("account_id", "=", volume.AccountID),
			dbUtils.NewFilterCondition("type", "=", string(models.JobTypeRevertVolume)),
			dbUtils.NewFilterCondition("state", "!=", string(models.JobsStateDONE)),
			dbUtils.NewFilterCondition("state", "!=", string(models.JobsStateERROR)),
			dbUtils.NewFilterCondition("job_attributes ->> 'resource_uuid'", "=", volume.UUID))

		jobs, err := se.GetJobsWithCondition(ctx, *filter)
		if err != nil {
			logger.Errorf("Failed to get jobs with conditions: %v. Error: %v", filter, err)
			return nil, "", err
		}
		if len(jobs) > 0 {
			logger.Infof("Found ongoing volume revert job for account %s with volume %s. Job UUID: %s", params.AccountName, volume.Name, jobs[0].UUID)
			return convertDatastoreVolumeToModel(volume, nil), jobs[0].UUID, nil
		}
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

	// Defer to mark job as deleted if any error happens
	defer func() {
		if err != nil {
			// Delete job if error occurred
			if createdJob != nil && createdJob.UUID != "" {
				logger.Warnf("Error occurred, marking job entry in DB as deleted. Job UUID: %s", createdJob.UUID)
				if delErr := se.DeleteJob(ctx, createdJob.UUID, err.Error()); delErr != nil {
					logger.Errorf("Failed to delete job: %v", delErr)
				}
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
	// Defer to revert the resource state
	defer func() {
		if err != nil {
			// Revert volume state back to previous state if it was set to REVERTING
			if volume.State == models.LifeCycleStateReverting {
				logger.Warnf("Error occurred during volume revert, reverting volume state to READY. Volume UUID: %s", volume.UUID)
				volumeUpdateErr := se.UpdateVolumeFields(ctx, volume.UUID, map[string]interface{}{
					"state":         previousState,
					"state_details": previousStateDetails,
				})
				if volumeUpdateErr != nil {
					logger.Errorf("Failed to revert volume state to previous volume state: %v", volumeUpdateErr)
				}
			}
		}
	}()

	location, err := utils.GetLocationFromVendorID(volume.Pool.VendorID)
	if err != nil {
		logger.Error("Failed to get location from vendor ID: ", "error", err)
		return nil, "", err
	}

	// controlWorkflowID defines the workflow ID for the control workflow
	controlWorkflowID := workflows.GenerateControlWorkflowID(volume.Account.ID, location, volume.Pool.Name)
	workflowOptions := workflows.DefaultSequentialWorkflowOptions(controlWorkflowID, createdJob.WorkflowID)
	err = workflowExecutor.ExecuteSequentialWorkflow(
		ctx,
		workflowOptions,
		workflows.RevertVolumeWorkflow,
		params,
		volume,
		snapshot,
	)
	if err != nil {
		logger.Error("Failed to start revert volume workflow after retries: ", "error", err)
		return nil, "", err
	}

	return convertDatastoreVolumeToModel(volume, nil), createdJob.UUID, nil
}

// GetVolume gets the specified volume
func (o *Orchestrator) GetVolume(ctx context.Context, volumeId string, refreshVolumeFields bool) (*models.Volume, error) {
	se := o.storage

	volume, err := se.DescribeVolume(ctx, volumeId)
	if err != nil {
		return nil, err
	}

	ipAddresses, err := getIPAddressForVolume(ctx, se, volume)
	if err != nil {
		return nil, err
	}

	return convertDatastoreVolumeToModel(volume, &ipAddresses), nil
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

	return convertDatastoreVolumesToModel(ctx, se, volumes), nil
}

func convertDatastoreVolumesToModel(ctx context.Context, se database.Storage, volumes []*datamodel.Volume) []*models.Volume {
	var volumesList []*models.Volume
	for _, volume := range volumes {
		ipAddresses, err := getIPAddressForVolume(ctx, se, volume)
		if err != nil {
			// If we can't get IP addresses, continue with nil (for backward compatibility)
			// Log the error but don't fail the entire list operation
			util.GetLogger(ctx).Warnf("Failed to get IP addresses for volume %s: %v", volume.UUID, err)
			p := convertDatastoreVolumeToModel(volume, nil)
			volumesList = append(volumesList, p)
		} else {
			p := convertDatastoreVolumeToModel(volume, &ipAddresses)
			volumesList = append(volumesList, p)
		}
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

type (
	BlockVolumeProcessor struct{}
	FileVolumeProcessor  struct{}
)

func (v *BlockVolumeProcessor) Validate(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, accountID int64) error {
	// Block-specific validation: host group checks, block properties, etc.
	params.FileProperties = nil // Ensure FileProperties is nil for block volumes

	// NOTE: we only bypass block device validation for hybrid replications. If additional checks are introduced in the future, we will need to evaluate each one individually
	if params.HybridReplicationParameters != nil {
		return nil
	}
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

	if params.FileProperties.ExportPolicy != nil && params.FileProperties.ExportPolicy.ExportRules != nil {
		if err := validateExportRulesAgainstProtocols(params.FileProperties.ExportPolicy.ExportRules, params.Protocols); err != nil {
			return err
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

// checkIsValidImmutableBackupPolicyWithRetry validates immutable backup policy compliance with retry logic
// to handle concurrent backup policy or backup vault update operations.
// It performs the following validations:
// 1. Fetches the backup policy and backup vault
// 2. Validates daily backup retention against immutable period
// 3. Validates weekly backup retention against immutable period
// 4. Validates monthly backup retention against immutable period
// Returns error if any validation fails, nil otherwise.
func _checkIsValidImmutableBackupPolicyWithRetry(ctx context.Context, se database.Storage, backupPolicyUUID string, backupVaultUUID string, accountID int64, region string, accountName string) error {
	logger := util.GetLogger(ctx)

	for attempt := 1; attempt <= common.MaxRetries; attempt++ {
		err := _checkIsValidImmutableBackupPolicyWithStateCheck(ctx, se, backupPolicyUUID, backupVaultUUID, accountID, region, accountName)
		if err == nil {
			return nil // Success
		}

		// Check if this is a retryable error (backup policy or backup vault in updating state)
		if isImmutableBackupPolicyRetryableError(err) {
			if attempt < common.MaxRetries {
				logger.Warn("Immutable backup policy validation failed due to concurrent update, retrying",
					"attempt", attempt,
					"maxRetries", common.MaxRetries,
					"retryAfter", common.RetryDelay,
					"error", err)
				common.SleepFn(common.RetryDelay)
				continue
			} else {
				logger.Error("Immutable backup policy validation failed after all retry attempts",
					"attempt", attempt,
					"maxRetries", common.MaxRetries,
					"error", err)
				return err
			}
		}

		// Non-retryable error, return immediately
		return err
	}

	return fmt.Errorf("immutable backup policy validation failed after %d attempts", common.MaxRetries)
}

// isImmutableBackupPolicyRetryableError checks if the error is related to backup policy or backup vault
// being in updating state, which is a retryable condition.
func isImmutableBackupPolicyRetryableError(err error) bool {
	if err == nil {
		return false
	}
	var customError *vsaerrors.CustomError
	if vsaerrors.As(err, &customError) {
		if customError.TrackingID == vsaerrors.ErrImmutableValidationWithUpdatingBackupPolicy || customError.TrackingID == vsaerrors.ErrImmutableValidationWithUpdatingBackupVault {
			return true
		}
	}
	return false
}

// getBackupVaultFromCVP fetches a specific backup vault from CVP by its ID
func getBackupVaultFromCVP(ctx context.Context, backupVaultID string, region string, accountName string) (*datamodel.BackupVault, error) {
	logger := util.GetLogger(ctx)

	// Get authentication token and create CVP client
	getSignedJwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := cvpCreateClient(logger, getSignedJwtToken)
	xCorrelationID := utils.GetCoRelationIDFromContext(ctx)

	// List all backup vaults from CVP
	vaults, err := cvpClient.BackupVault.V1betaListBackupVaults(&backup_vault.V1betaListBackupVaultsParams{
		LocationID:     region,
		ProjectNumber:  accountName,
		XCorrelationID: &xCorrelationID,
	})
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			return nil, customerrors.NewNotFoundErr("Backup vault", nil)
		}
		logger.Errorf("Error fetching backup vaults from CVP: %v", err)
		return nil, err
	}

	// Search for the specific backup vault
	for _, bv := range vaults.Payload.BackupVaults {
		if bv.BackupVaultID == backupVaultID {
			// Convert to data model
			bvModel, err := activities.ConvertToBackupVaultDataModel(bv, region)
			if err != nil {
				return nil, fmt.Errorf("failed to convert backup vault to data model: %w", err)
			}

			return bvModel, nil
		}
	}

	// Backup vault not found
	return nil, customerrors.NewNotFoundErr("Backup vault", &backupVaultID)
}

// GetBackupPolicyFromCVP fetches backup policy from CVP and converts it to the internal data model
func GetBackupPolicyFromCVP(ctx context.Context, backupPolicyUUID, region, accountName string) (*datamodel.BackupPolicy, error) {
	logger := util.GetLogger(ctx)
	GetSignedJwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := cvpCreateClient(logger, GetSignedJwtToken)
	xCorrelationID := utils.GetCoRelationIDFromContext(ctx)

	// Fetch backup policy from CVP
	cvpBackupPolicy, err := cvpClient.BackupPolicy.V1betaDescribeBackupPolicy(&backup_policy.V1betaDescribeBackupPolicyParams{
		BackupPolicyID: backupPolicyUUID,
		LocationID:     region,
		ProjectNumber:  accountName,
		XCorrelationID: &xCorrelationID,
	})
	if err != nil {
		logger.Errorf("Error fetching backup policy from CVP: %v", err)
		return nil, err
	}

	if cvpBackupPolicy == nil || cvpBackupPolicy.Payload == nil {
		logger.Error("No backup policy found in CVP")
		return nil, customerrors.NewNotFoundErr("Backup policy", &backupPolicyUUID)
	}

	// Convert CVP response to internal data model
	backupPolicy := activities.ConvertToBackupPolicyDataModel(cvpBackupPolicy.Payload)

	return backupPolicy, nil
}

// _checkIsValidImmutableBackupPolicyWithStateCheck validates immutable backup policy compliance
// and checks for backup policy/vault updating states before performing validation.
func _checkIsValidImmutableBackupPolicyWithStateCheck(ctx context.Context, se database.Storage, backupPolicyUUID string, backupVaultUUID string, accountID int64, region string, accountName string) error {
	// Add input validation
	if backupPolicyUUID == "" {
		return fmt.Errorf("backup policy UUID cannot be empty")
	}
	if backupVaultUUID == "" {
		return fmt.Errorf("backup vault UUID cannot be empty")
	}
	if accountID <= 0 {
		return fmt.Errorf("account ID must be positive")
	}

	// Get backup policy details
	backupPolicy, err := se.GetBackupPolicyByUUIDAndOwnerID(ctx, backupPolicyUUID, accountID)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			logger := util.GetLogger(ctx)
			logger.Warnf("Backup policy '%v' not found in local DB, attempting to fetch from CVP", backupPolicyUUID)
			// If not found in local DB, try fetching from CVP
			backupPolicy, err = GetBackupPolicyFromCVP(ctx, backupPolicyUUID, region, accountName)
			if err != nil {
				return fmt.Errorf("failed to get backup policy from CVP: %v", err)
			}
		} else {
			return fmt.Errorf("failed to get backup policy: %v", err)
		}
	}

	// Check if backup policy is in updating state
	if backupPolicy.LifeCycleState == models.LifeCycleStateUpdating {
		return vsaerrors.NewVCPError(vsaerrors.ErrImmutableValidationWithUpdatingBackupPolicy, fmt.Errorf("Cannot validate immutable backup policy: backup policy '%v' is currently being updated. Please wait for the policy update to complete.", backupPolicyUUID))
	}

	// Get backup vault details
	backupVault, err := se.GetBackupVaultByUUIDndOwnerID(ctx, backupVaultUUID, accountID)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			logger := util.GetLogger(ctx)
			logger.Warnf("Backup vault '%v' not found in local DB, attempting to fetch from CVP", backupVaultUUID)
			// If not found in local DB, try fetching from CVP
			backupVault, err = GetBackupVaultFromCVP(ctx, backupVaultUUID, region, accountName)
			if err != nil {
				return fmt.Errorf("failed to get backup vault from CVP: %v", err)
			}
		} else {
			return fmt.Errorf("failed to get backup vault: %v", err)
		}
	}

	// Check if backup vault is in updating state
	if backupVault.LifeCycleState == models.LifeCycleStateUpdating {
		return vsaerrors.NewVCPError(vsaerrors.ErrImmutableValidationWithUpdatingBackupVault, fmt.Errorf("Cannot validate immutable backup policy: backup vault '%s' is currently being updated. Please wait for the vault update to complete.", backupVaultUUID))
	}

	// Skip validation if backup vault doesn't have immutable attributes configured
	if backupVault.ImmutableAttributes == nil {
		return nil
	}
	if *backupVault.ImmutableAttributes.BackupMinimumEnforcedRetentionDuration == 0 {
		return nil
	}
	immutableAttrs := backupVault.ImmutableAttributes
	backupPolicyParams := &common.BackupPolicyParams{
		DailyBackupsToKeep:   backupPolicy.DailyBackupsToKeep,
		WeeklyBackupsToKeep:  backupPolicy.WeeklyBackupsToKeep,
		MonthlyBackupsToKeep: backupPolicy.MonthlyBackupsToKeep,
	}
	retentionPolicyParams := &common.BackupRetentionPolicyParams{
		BackupMinimumEnforcedRetentionDuration: immutableAttrs.BackupMinimumEnforcedRetentionDuration,
		IsDailyBackupImmutable:                 &immutableAttrs.IsDailyBackupImmutable,
		IsWeeklyBackupImmutable:                &immutableAttrs.IsWeeklyBackupImmutable,
		IsMonthlyBackupImmutable:               &immutableAttrs.IsMonthlyBackupImmutable,
		IsAdhocBackupImmutable:                 &immutableAttrs.IsAdhocBackupImmutable,
	}
	err = common.ValidateBackupPolicyRetentionLimits(backupPolicyParams, retentionPolicyParams)
	if err != nil {
		return fmt.Errorf("immutable backup policy validation failed: %w", err)
	}
	return nil
}

// isPrime return true if constituentVolumeCount is prime for constituentVolumeCount greater than and equal to seven
func isPrime(constituentVolumeCount int) bool {
	if constituentVolumeCount%2 == 0 || constituentVolumeCount%3 == 0 {
		return false
	}
	sq_root := int(math.Sqrt(float64(constituentVolumeCount)))

	for i := 5; i <= sq_root; i = i + 6 {
		if constituentVolumeCount%i == 0 || constituentVolumeCount%(i+2) == 0 {
			return false
		}
	}
	return true
}

func getMaxConstituentVolumesPerAggregate(logger log.Logger, config string) (int64, error) {
	// Get the VSA instance type detail from Pool table
	vlmConfig := &vlm.VLMConfig{}
	err := json.Unmarshal([]byte(config), vlmConfig)
	if err != nil {
		return 0, err
	}

	return activities.GetMaxConstituentsPerAggregate(logger, vlmConfig.Deployment.VSAInstanceType), nil
}

func _validateCreateVolumeParams(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
	logger := util.GetLogger(ctx)
	if pool.LargeCapacity != params.LargeCapacity {
		return customerrors.NewUserInputValidationErr("pool large capacity setting does not match volume large capacity setting")
	}

	if hp := params.HybridReplicationParameters; hp != nil {
		replicationType := hp.ReplicationType
		replicationSchedule := hp.ReplicationSchedule
		if replicationType == models.HybridReplicationParametersReplicationTypeREVERSE || replicationType == models.HybridReplicationParametersReplicationTypeCONTINUOUS {
			msg := "Hybrid replication is not allowed for replicationType: " + string(replicationType)
			return customerrors.NewUserInputValidationErr(msg)
		}

		if replicationType == models.HybridReplicationParametersReplicationTypeONPREM {
			if nillable.IsNilOrEmpty(&replicationSchedule) {
				msg := "Can't have empty replicationSchedule for " + string(replicationType)
				return customerrors.NewUserInputValidationErr(msg)
			}
		}

		if hp.PeerClusterName == "" || hp.PeerVolumeName == "" || hp.PeerSvmName == "" || len(hp.PeerIPAddresses) == 0 || hp.ResourceID == "" {
			msg := "PeerClusterName, PeerSvmName, PeerVolumeName, PeerIPAddresses and ResourceID are required for Hybrid Replication"
			return customerrors.NewUserInputValidationErr(msg)
		}

		replicationCount, err := se.GetVolumeReplicationCountByPeerDetails(ctx, params.AccountName, params.HybridReplicationParameters.PeerSvmName, params.HybridReplicationParameters.PeerVolumeName)
		if err != nil {
			return err
		}

		if replicationCount > 0 {
			return customerrors.NewUserInputValidationErr("Hybrid replication already exists for the given peer SVM and volume")
		}

		if params.SnapshotID != "" || params.BackupID != "" || params.AutoTieringPolicy != nil || params.SnapshotPolicy != nil || params.BackupPath != "" {
			msg := "Restoring volume from snapshot, backup, or enabling auto-tiering/snapshot policy is not supported for Hybrid Replication volumes"
			return customerrors.NewUserInputValidationErr(msg)
		}

		if params.DataProtection != nil && ((params.DataProtection.ScheduledBackupEnabled != nil && *params.DataProtection.ScheduledBackupEnabled) || (params.DataProtection.BackupPolicyId) != "") {
			msg := "Scheduled backups are not supported for Hybrid Replication, only manual backups are supported"
			return customerrors.NewUserInputValidationErr(msg)
		}

		for _, ipAddress := range params.HybridReplicationParameters.PeerIPAddresses {
			if !utils.ValidateIPv4Address(ipAddress) {
				msg := "Invalid IP Address provided in Hybrid Replication Parameters"
				return customerrors.NewUserInputValidationErr(msg)
			}
		}

		if params.HybridReplicationParameters.Labels != nil {
			err := replication.ValidateLabels(params.HybridReplicationParameters.Labels)
			if err != nil {
				return err
			}
		}

		err = replication.ValidateReplicationResourceId(ctx, params.AccountName, params.HybridReplicationParameters.ResourceID, params.Name, se)
		if err != nil {
			return vsaerrors.NewVCPError(vsaerrors.ErrValidateCreateResourceIdInUse, err)
		}
	}

	if params.LargeCapacity {
		if utils.IsSanProtocols(params.Protocols) {
			return customerrors.NewUserInputValidationErr("SAN protocols are not supported for large capacity volumes")
		}

		if params.BlockDevices != nil && len(*params.BlockDevices) > 0 {
			return customerrors.NewUserInputValidationErr("BlockDevices are not supported for large capacity volumes")
		}

		if params.LargeVolumeConstituentCount > 0 {
			// validate large volume constituent count is not prime
			if params.LargeVolumeConstituentCount >= int32(minPrimeNumberConfigAllowed) && isPrime(int(params.LargeVolumeConstituentCount)) {
				return customerrors.NewUserInputValidationErr(fmt.Sprintf("Constituent volume count with %d is not supported", params.LargeVolumeConstituentCount))
			}

			// validate large volume constituent count is within allowed limit
			maxConstituentVolumesPerAggregate, err := getMaxConstituentVolumesPerAggregate(logger, pool.VLMConfig)
			if err != nil {
				return customerrors.NewTransientErr(fmt.Sprintf("error unmarshalling VLM config from pool: %v", err))
			}

			maxCVsPerVolumeLimit := numOfLvHAPairs * maxConstituentVolumesPerVolumePerAggregate
			if int64(params.LargeVolumeConstituentCount) > maxCVsPerVolumeLimit {
				return customerrors.NewUserInputValidationErr(fmt.Sprintf("Large Volume constituent count cannot be greater than %d for the current per-aggregate limit", maxCVsPerVolumeLimit))
			}

			// Subtracting 1 because there will always be root vol in the aggregate at start
			finalMaxCVs := (numOfLvHAPairs * maxConstituentVolumesPerAggregate) - 1
			if int64(params.LargeVolumeConstituentCount) > finalMaxCVs {
				return customerrors.NewUserInputValidationErr(fmt.Sprintf("Large Volume constituent count cannot be greater than %d", int32(finalMaxCVs)))
			}

			// validate that each constituent volume size is at least 100 GB
			cvSizeInBytes := params.QuotaInBytes / uint64(params.LargeVolumeConstituentCount)
			if cvSizeInBytes < minCVSizeInBytes {
				return customerrors.NewUserInputValidationErr(fmt.Sprintf("Constituent volume size cannot be less than %s. Current CV size is %s with %d constituent volumes",
					utils.FmtUint64Bytes(minCVSizeInBytes), utils.FmtUint64Bytes(cvSizeInBytes), params.LargeVolumeConstituentCount))
			}
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

	cloneSharedBytes := uint64(0)
	if params.SnapshotID != "" {
		account, err := getOrCreateAccount(ctx, se, params.AccountName)
		if err != nil {
			return err
		}

		dbSnapshot, err := se.GetSnapshotByPoolID(ctx, params.SnapshotID, account.ID, pool.ID, true)
		if err != nil {
			if customerrors.IsNotFoundErr(err) {
				return customerrors.NewUserInputValidationErr("snapshot not found")
			}
			return err
		}

		if !thinCloneGASupport {
			if pool.ThinCloneVolumeCount+1 > maxThinClonesPerPool {
				return customerrors.NewUserInputValidationErr("pool has reached maximum clone volume limit")
			}
		}
		if dbSnapshot != nil && dbSnapshot.SnapshotAttributes != nil {
			cloneSharedBytes = uint64(dbSnapshot.SnapshotAttributes.LogicalSizeUsedInBytes)
		}
	}

	if pool.QuotaInBytes+params.QuotaInBytes-cloneSharedBytes > uint64(pool.SizeInBytes) {
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
			if err := validateCRBBackupVault(bv, params.Region); err != nil {
				return err
			}
		}

		if params.DataProtection.KmsGrant != nil && *params.DataProtection.KmsGrant != "" {
			if !cmekBackupEnabled {
				return customerrors.NewUserInputValidationErr("CMEK backup is not enabled")
			}
		}
	}

	if params.DataProtection != nil && params.DataProtection.BackupPolicyId != "" {
		// Validate assigning backup policy to the volume
		if params.DataProtection.BackupVaultID == "" {
			return customerrors.NewUserInputValidationErr("backup vault id is required to assign a backup policy to a volume")
		}
		if utils.IsImmutableBackupEnabled() {
			logger := util.GetLogger(ctx)
			logger.Debug("Validating immutable backup policy compliance",
				"backupPolicyId", params.DataProtection.BackupPolicyId,
				"backupVaultId", params.DataProtection.BackupVaultID)

			// Validate immutable backup policy compliance
			err = checkIsValidImmutableBackupPolicyWithRetry(ctx, se, params.DataProtection.BackupPolicyId, params.DataProtection.BackupVaultID, pool.Account.ID, params.Region, params.AccountName)
			if err != nil {
				logger.Errorf("Immutable backup policy validation failed %v", err)

				// Check if it's a service-related error (CVP down, network issues, etc.)
				if customerrors.IsUnavailableErr(err) || customerrors.IsNetworkError(err) {
					return customerrors.NewUnavailableErr(fmt.Sprintf("Service is temporarily unavailable, please try again later: %v", err.Error()))
				}

				// Check if it's a retryable error (backup policy/vault in updating state)
				var customErr *vsaerrors.CustomError
				if vsaerrors.As(err, &customErr) {
					if customErr.TrackingID == vsaerrors.ErrImmutableValidationWithUpdatingBackupPolicy ||
						customErr.TrackingID == vsaerrors.ErrImmutableValidationWithUpdatingBackupVault {
						return customerrors.NewUnavailableErr(fmt.Sprintf("Backup policy or vault is currently being updated, please try again later: %v", err.Error()))
					}
				}

				// For all other errors (actual validation failures), return 400
				return customerrors.NewUserInputValidationErr(fmt.Sprintf("Backup policy is not compliant with immutable backup vault settings: %v", err.Error()))
			}
		}

		if params.DataProtection.ScheduledBackupEnabled == nil {
			return customerrors.NewUserInputValidationErr("scheduled backups needs to be enabled/disabled when a backup policy is assigned to a volume")
		}
		if params.IsDataProtection {
			return customerrors.NewUserInputValidationErr("scheduled backups are not supported for cross region replication, only manual backups with existing snapshots are supported")
		}
	}

	if !pool.AllowAutoTiering && params.AutoTieringPolicy != nil && (params.AutoTieringPolicy.AutoTieringEnabled || params.AutoTieringPolicy.HotTierBypassModeEnabled) {
		return customerrors.NewUserInputValidationErr("Auto Tiering is not allowed for this volume. Please enable Auto Tiering on the Pool and try again")
	} else if params.AutoTieringPolicy != nil && params.AutoTieringPolicy.AutoTieringEnabled {
		if params.AutoTieringPolicy.CoolingThresholdDays < minCoolingThresholdDays || params.AutoTieringPolicy.CoolingThresholdDays > maxCoolingThresholdDays {
			return customerrors.NewUserInputValidationErr("Auto Tiering Cooling Threshold days must be between 2 and 183 days")
		}
	}

	// Validate HotTierBypassModeEnabled
	if params.AutoTieringPolicy != nil && params.AutoTieringPolicy.HotTierBypassModeEnabled {
		if !params.AutoTieringPolicy.AutoTieringEnabled {
			return customerrors.NewUserInputValidationErr("Hot Tier Bypass Mode can only be enabled when Auto Tiering is enabled on the Volume")
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

func _convertDatastoreVolumeToModel(volume *datamodel.Volume, ipAddress *[]string) *models.Volume {
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
		SnapshotDirectory:     volume.VolumeAttributes.SnapshotDirectory,
		CloneSharedBytes:      volume.ClonesSharedBytes,
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
					AllSquash:           rule.AllSquash,
					AnonUID:             rule.AnonUID,
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
		if attributes.FileProperties.Fqdn != "" {
			res.FileProperties.Fqdn = attributes.FileProperties.Fqdn
		}
		if attributes.FileProperties.SMBShareSettings != nil {
			res.FileProperties.SMBShareSettings = attributes.FileProperties.SMBShareSettings
		}
		if attributes.FileProperties.SecurityStyle != "" {
			res.FileProperties.SecurityStyle = attributes.FileProperties.SecurityStyle
		}
	}

	// Return AutoTieringPolicy if pool has auto tiering enabled.
	// This ensures volumes created with PAUSED tierAction still return their tieringPolicy
	// when the pool supports auto tiering, regardless of the volume's AutoTieringEnabled state.
	if volume.AutoTieringPolicy != nil && (volume.Pool != nil && volume.Pool.AllowAutoTiering) {
		res.AutoTieringPolicy = &models.AutoTieringPolicy{
			AutoTieringEnabled:       volume.AutoTieringEnabled,
			CoolingThresholdDays:     volume.AutoTieringPolicy.CoolingThresholdDays,
			TieringPolicy:            volume.AutoTieringPolicy.TieringPolicy,
			HotTierBypassModeEnabled: volume.AutoTieringPolicy.HotTierBypassModeEnabled,
		}
		res.HotTierSizeGib = volume.HotTierSizeGib
		res.ColdTierSizeGib = volume.ColdTierSizeGib
	}

	if volume.LargeVolumeAttributes != nil {
		res.LargeCapacity = volume.LargeVolumeAttributes.LargeCapacity
		res.LargeVolumeConstituentCount = volume.LargeVolumeAttributes.LargeVolumeConstituentCount
	}

	if volume.CacheParameters != nil {
		var cacheConfig *models.CacheConfig
		if volume.CacheParameters.CacheConfig != nil {
			cacheConfig = &models.CacheConfig{
				AtimeScrubEnabled:       volume.CacheParameters.CacheConfig.AtimeScrubEnabled,
				AtimeScrubDays:          volume.CacheParameters.CacheConfig.AtimeScrubDays,
				CifsChangeNotifyEnabled: volume.CacheParameters.CacheConfig.CifsChangeNotifyEnabled,
				WritebackEnabled:        volume.CacheParameters.CacheConfig.WritebackEnabled,
				CachePrePopulateState:   volume.CacheParameters.CacheConfig.CachePrePopulateState,
			}

			if volume.CacheParameters.CacheConfig.CachePrePopulate != nil {
				cacheConfig.CachePrePopulate = &models.CachePrePopulate{
					ExcludePathList: volume.CacheParameters.CacheConfig.CachePrePopulate.ExcludePathList,
					PathList:        volume.CacheParameters.CacheConfig.CachePrePopulate.PathList,
					Recursion:       volume.CacheParameters.CacheConfig.CachePrePopulate.Recursion,
				}
			}
		}

		res.CacheParameters = &models.CacheParameters{
			PeerClusterName:       volume.CacheParameters.PeerClusterName,
			PeerSvmName:           volume.CacheParameters.PeerSvmName,
			PeerVolumeName:        volume.CacheParameters.PeerVolumeName,
			PeerIPAddresses:       volume.CacheParameters.PeerIpAddresses,
			EnableGlobalFileLock:  volume.CacheParameters.EnableGlobalFileLock,
			CacheConfig:           cacheConfig,
			CacheState:            volume.CacheParameters.CacheState,
			PreviousCacheState:    volume.CacheParameters.PreviousCacheState,
			CacheStateDetails:     volume.CacheParameters.CacheStateDetails,
			CacheStateDetailsCode: volume.CacheParameters.CacheStateDetailsCode,
			PeerExpiryTime:        volume.CacheParameters.CommandExpiryTime,
			PeeringCommand:        nillable.GetString(volume.CacheParameters.Command, ""),
			Passphrase:            volume.CacheParameters.Passphrase,
		}
	}

	if attributes != nil && attributes.CloneParentInfo != nil {
		var parentVolumeId *string
		var parentSnapshotId *string

		if attributes.CloneParentInfo.ParentVolumeUUID != "" {
			parentVolumeId = &attributes.CloneParentInfo.ParentVolumeUUID
		}
		if attributes.CloneParentInfo.ParentSnapshotUUID != "" {
			parentSnapshotId = &attributes.CloneParentInfo.ParentSnapshotUUID
		}

		res.CloneParentInfo = &models.CloneParentInfo{
			ParentVolumeId:   parentVolumeId,
			ParentSnapshotId: parentSnapshotId,
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
	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)

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

		err = checkAndCancelCreateWorkflowIfNeeded(ctx, se, temporal, volume)
		if err != nil {
			logger.Error("Failed to check and cancel create flex cache volume workflow", "error", err)
			return nil, "", err
		}
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
	controlWorkflowID := workflows.GenerateControlWorkflowID(volume.Account.ID, location, volume.Pool.Name)
	workflowOptions := workflows.DefaultSequentialWorkflowOptions(controlWorkflowID, createdJob.WorkflowID)

	err = workflowExecutor.ExecuteSequentialWorkflow(
		ctx,
		workflowOptions,
		workflowFunc,
		volume,
	)
	if err != nil {
		logger.Error("Failed to start delete volume workflow: ", "error", err)
		return nil, "", err
	}

	// Check if pool needs scaling based on volume count (async, non-blocking)
	// This happens after volume deletion workflow is triggered successfully
	// Configuration variables
	if enableAutoPoolScaling {
		pool, err := se.GetPool(ctx, volume.Pool.UUID, volume.Account.ID)
		if err != nil {
			return nil, "", err
		}
		dbPool := database.ConvertPoolViewToPool(pool)
		checkAndTriggerPoolScalingIfNeeded(ctx, se, temporal, dbPool, true)
	}

	volume.State = models.LifeCycleStateDeleting
	volume.StateDetails = models.LifeCycleStateDeletingDetails
	return convertDatastoreVolumeToModel(volume, nil), createdJob.UUID, nil
}

func (o *Orchestrator) GetMultipleVolumes(ctx context.Context, volumeIds []string, accountName string) ([]*models.Volume, error) {
	log := util.GetLogger(ctx)
	se := o.storage

	account, err := getOrCreateAccount(ctx, se, accountName)
	if err != nil {
		return nil, err
	}

	var result []*models.Volume
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

	wfErr := o.TriggerRefreshWorkflow(ctx, account, volumes)
	if wfErr != nil {
		log.Error("Error occurred in TriggerRefreshWorkflow", "error", wfErr.Error())
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

func (o *Orchestrator) TriggerRefreshWorkflow(ctx context.Context, account *datamodel.Account, volumes []*datamodel.Volume) error {
	log := util.GetLogger(ctx)
	if len(volumes) == 0 {
		log.Info("No volumes provided for refresh workflow")
		return nil
	}

	if account.AccountMetadata != nil && !account.AccountMetadata.VolumeRefreshWorkflowLastCompletionAt.IsZero() {
		lastCompletionTime := account.AccountMetadata.VolumeRefreshWorkflowLastCompletionAt
		timeSinceCompletion := time.Now().Sub(lastCompletionTime)

		if timeSinceCompletion <= time.Duration(volumeRefreshIntervalMinutes)*time.Minute {
			log.Debugf("Skipping VolumeRefreshWorkflow execution for account %s due to recent completion at %v (%.1f minutes ago, interval: %d minutes)",
				account.Name, lastCompletionTime, timeSinceCompletion.Minutes(), volumeRefreshIntervalMinutes)
			return nil
		}

		log.Debugf("Last VolumeRefreshWorkflow completion for account %s was at %v (%.1f minutes ago), triggering new execution",
			account.Name, lastCompletionTime, timeSinceCompletion.Minutes())
	} else {
		log.Debugf("No previous VolumeRefreshWorkflow completion timestamp found for account %s, triggering new execution",
			account.Name)
	}

	workflowId := fmt.Sprintf("VolumeRefreshWorkflow_AccountId_%s", volumes[0].Account.UUID)
	_, err := o.temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.BackgroundTaskQueue,
			ID:                    workflowId,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		},
		workflows.VolumeRefreshWorkflow,
		volumes,
	)
	if err != nil {
		return err
	}

	return nil
}

func _updateVolume(ctx context.Context, se database.Storage, temporal client.Client, params *common.UpdateVolumeParams, isReplication bool) (*models.Volume, string, error) {
	logger := util.GetLogger(ctx)
	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)

	dbVolume, err := se.GetVolume(ctx, params.VolumeId)
	if err != nil {
		return nil, "", err
	}
	params.PoolID = dbVolume.Pool.UUID // In update volume we don't get Pool UUID from request, Set pool ID for backwards compatibility

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
			dbVolume.DataProtection.KmsGrant = params.DataProtection.KmsGrant

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

	if params.SnapshotDirectoryAccess != nil {
		dbVolume.VolumeAttributes.SnapshotDirectory = *params.SnapshotDirectoryAccess
	}

	// @TODO: Implement CIFSAccessBasedEnumeration check when implementing security style

	pool, err := se.GetPool(ctx, params.PoolID, dbVolume.AccountID)
	if err != nil {
		return nil, "", err
	}
	logger.Debugf("Pool details: UUID: %s, AccountID: %d, SizeBytes: %d, QuotaBytes: %d, VolumeCount: %d, Throughput: %f", pool.UUID, pool.AccountID, pool.SizeInBytes, pool.QuotaInBytes, pool.VolumeCount, pool.Throughput)
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

	if params.SMBShareSettings != nil {
		if dbVolume.VolumeAttributes.FileProperties == nil {
			dbVolume.VolumeAttributes.FileProperties = &datamodel.FileProperties{}
		}
		dbVolume.VolumeAttributes.FileProperties.SMBShareSettings = params.SMBShareSettings
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

	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		createdJob.WorkflowID,
		workflowengine.CustomerTaskQueue,
		wf,
		nil,
		params,
		dbVolume,
	)
	if err != nil {
		logger.Error("Failed to start update volume workflow: ", "error", err)
		return nil, "", err
	}
	dbVolume = updateLatestTieringInformationToVolumeResponse(dbVolume, params)

	return convertDatastoreVolumeToModel(dbVolume, nil), createdJob.UUID, nil
}

func updateLatestTieringInformationToVolumeResponse(dbVolume *datamodel.Volume, updateParams *common.UpdateVolumeParams) *datamodel.Volume {
	updatedDBVolume := dbVolume
	if updateParams != nil && updateParams.AutoTieringPolicy != nil {
		if updatedDBVolume.AutoTieringPolicy == nil {
			updatedDBVolume.AutoTieringPolicy = &datamodel.AutoTieringPolicy{}
		}
		updatedDBVolume.AutoTieringEnabled = updateParams.AutoTieringPolicy.AutoTieringEnabled
		updatedDBVolume.AutoTieringPolicy.TieringPolicy = updateParams.AutoTieringPolicy.TieringPolicy
		updatedDBVolume.AutoTieringPolicy.CoolingThresholdDays = updateParams.AutoTieringPolicy.CoolingThresholdDays
		updatedDBVolume.AutoTieringPolicy.HotTierBypassModeEnabled = updateParams.AutoTieringPolicy.HotTierBypassModeEnabled
	}

	return updatedDBVolume
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
	log := util.GetLogger(ctx)

	if params.LargeCapacity != nil && (pool.LargeCapacity != *params.LargeCapacity) {
		return customerrors.NewUserInputValidationErr("Given large capacity value is not supported. Large capacity cannot be changed for existing volume")
	}

	if params.LargeVolumeConstituentCount != nil {
		// Check if the volume has existing LargeVolumeConstituentCount
		if volume.LargeVolumeAttributes != nil && volume.LargeVolumeAttributes.LargeVolumeConstituentCount != nil {
			// Only return error if the provided value is different from the existing value
			if *params.LargeVolumeConstituentCount != *volume.LargeVolumeAttributes.LargeVolumeConstituentCount {
				return customerrors.NewUserInputValidationErr("Updating large volume constituent count is not supported")
			}
		} else {
			// If volume doesn't have a constituent count, we don't allow setting it during update
			return customerrors.NewUserInputValidationErr("Updating large volume constituent count is not supported")
		}
	}

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

		log.Debugf("Current Volume Size: %d, New Volume Size: %d, Size Increase: %d, Pool Size: %d, Pool Quota: %d, Clones Shared Bytes: %d", volume.SizeInBytes, params.QuotaInBytes, sizeIncrease, pool.SizeInBytes, pool.QuotaInBytes, volume.ClonesSharedBytes)

		// Check if adding the increase to current pool usage exceeds pool size
		if sizeIncrease > 0 && pool.QuotaInBytes+uint64(sizeIncrease)-volume.ClonesSharedBytes > uint64(pool.SizeInBytes) {
			return customerrors.NewUserInputValidationErr("Total size of volumes in a pool cannot exceed the pool capacity.")
		}

		// Large capacity quota validation
		if pool.LargeCapacity {
			if uint64(params.QuotaInBytes) < utils.MinQuotaInBytesLargeVolume || uint64(params.QuotaInBytes) > utils.MaxQuotaInBytesLargeVolume {
				return customerrors.NewUserInputValidationErr(fmt.Sprintf("Invalid volume capacity %s. Must be between %s and %s.",
					utils.FmtUint64Bytes(uint64(params.QuotaInBytes)), utils.FmtUint64Bytes(utils.MinQuotaInBytesLargeVolume),
					utils.FmtUint64Bytes(utils.MaxQuotaInBytesLargeVolume)))
			}
		} else {
			if uint64(params.QuotaInBytes) < minQuotaInBytesVolume || uint64(params.QuotaInBytes) > maxQuotaInBytesVolume {
				return customerrors.NewUserInputValidationErr(fmt.Sprintf("Invalid volume capacity %s. Must be between %s and %s.",
					utils.FmtUint64Bytes(uint64(params.QuotaInBytes)), utils.FmtUint64Bytes(minQuotaInBytesVolume),
					utils.FmtUint64Bytes(maxQuotaInBytesVolume)))
			}
		}
	}

	// Large capacity validations
	if pool.LargeCapacity {
		// BlockDevices are not supported for large capacity volumes
		if params.BlockProperties != nil {
			return customerrors.NewUserInputValidationErr("BlockProperties are not supported for large capacity volumes")
		}
		if len(params.BlockDevices) > 0 {
			return customerrors.NewUserInputValidationErr("BlockDevices are not supported for large capacity volumes")
		}
	}

	if !pool.AllowAutoTiering && params.AutoTieringPolicy != nil && (params.AutoTieringPolicy.AutoTieringEnabled || params.AutoTieringPolicy.HotTierBypassModeEnabled) {
		return customerrors.NewUserInputValidationErr("Auto Tiering is not allowed for this volume. Please enable Auto Tiering on the Pool and try again")
	} else if params.AutoTieringPolicy != nil && params.AutoTieringPolicy.AutoTieringEnabled {
		if params.AutoTieringPolicy.CoolingThresholdDays < minCoolingThresholdDays || params.AutoTieringPolicy.CoolingThresholdDays > maxCoolingThresholdDays {
			return customerrors.NewUserInputValidationErr("Auto Tiering Cooling Threshold days must be between 2 and 183 days")
		}
	}

	// Validate HotTierBypassModeEnabled for update
	if params.AutoTieringPolicy != nil && params.AutoTieringPolicy.HotTierBypassModeEnabled {
		if !params.AutoTieringPolicy.AutoTieringEnabled {
			return customerrors.NewUserInputValidationErr("Hot Tier Bypass Mode can only be enabled when Auto Tiering is enabled on the Volume")
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

	if params.FileProperties != nil {
		err := validateUpdateFileProperties(params, volume)
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
			if err := validateCRBBackupVault(bv, params.Region); err != nil {
				return err
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

	if params.DataProtection != nil && !nillable.IsNilOrEmpty(params.DataProtection.BackupPolicyId) {
		backupPolicy, err := se.GetBackupPolicyByUUIDAndOwnerID(ctx, *params.DataProtection.BackupPolicyId, pool.Account.ID)
		if err != nil && !customerrors.IsNotFoundErr(err) {
			return err
		}
		if backupPolicy != nil && backupPolicy.LifeCycleState != models.LifeCycleStateREADY {
			return customerrors.NewUserInputValidationErr("backup policy is not in ready state, please check the backup policy and try again")
		}
	}

	// Block CMEK for all VCP volumes (both SAN and NAS)
	// Since we got the volume from VCP database (line 1812), it exists in VCP and is a VCP volume
	var backupVaultID string
	if params.DataProtection != nil && params.DataProtection.BackupVaultID != nil && *params.DataProtection.BackupVaultID != "" {
		backupVaultID = *params.DataProtection.BackupVaultID
	} else if volume.DataProtection != nil && volume.DataProtection.BackupVaultID != "" {
		backupVaultID = volume.DataProtection.BackupVaultID
	}

	if backupVaultID != "" {
		if params.DataProtection != nil && params.DataProtection.KmsGrant != nil && *params.DataProtection.KmsGrant != "" {
			if !cmekBackupEnabled {
				return customerrors.NewUserInputValidationErr("CMEK backup is not enabled")
			}
		}

		if volume.DataProtection != nil && volume.DataProtection.BackupVaultID == backupVaultID && volume.DataProtection.KmsGrant != nil && *volume.DataProtection.KmsGrant != "" {
			if !cmekBackupEnabled {
				return customerrors.NewUserInputValidationErr("CMEK backup is not enabled")
			}
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
	if utils.IsImmutableBackupEnabled() {
		logger := util.GetLogger(ctx)
		if params.DataProtection != nil {
			// Validate immutable backup policy compliance when both BackupPolicyId and BackupVaultID are set
			if volume.DataProtection != nil && volume.DataProtection.BackupVaultID != "" && volume.DataProtection.BackupPolicyID != "" {
				err := checkIsValidImmutableBackupPolicyWithRetry(ctx, se, volume.DataProtection.BackupPolicyID, volume.DataProtection.BackupVaultID, volume.Account.ID, params.Region, params.AccountName)
				if err != nil {
					logger.Errorf("Immutable backup policy validation failed %v", err)

					// Check if it's a service-related error (CVP down, network issues, etc.)
					if customerrors.IsUnavailableErr(err) || customerrors.IsNetworkError(err) {
						return customerrors.NewUnavailableErr(fmt.Sprintf("Service is temporarily unavailable, please try again later: %v", err.Error()))
					}

					// Check if it's a retryable error (backup policy/vault in updating state)
					var customErr *vsaerrors.CustomError
					if vsaerrors.As(err, &customErr) {
						if customErr.TrackingID == vsaerrors.ErrImmutableValidationWithUpdatingBackupPolicy ||
							customErr.TrackingID == vsaerrors.ErrImmutableValidationWithUpdatingBackupVault {
							return customerrors.NewUnavailableErr(fmt.Sprintf("Backup policy or vault is currently being updated, please try again later: %v", err.Error()))
						}
					}

					// For all other errors (actual validation failures), return 400
					return customerrors.NewUserInputValidationErr(fmt.Sprintf("Backup policy is not compliant with immutable backup vault settings: %v", err.Error()))
				}
			}
		}
	}

	// Validate snapReserve changes to ensure sufficient LUN space
	if params.SnapReserve != nil && volume.VolumeAttributes != nil && utils.IsSanProtocols(volume.VolumeAttributes.Protocols) && *params.SnapReserve != volume.VolumeAttributes.SnapReserve {
		if *params.SnapReserve > volume.VolumeAttributes.SnapReserve {
			var requiredQuotaInBytes int64
			// Calculate current available LUN space
			currentLunSpace := volume.SizeInBytes - int64(float64(volume.SizeInBytes)*float64(volume.VolumeAttributes.SnapReserve)/utils.PercentageBase)
			if params.QuotaInBytes == 0 {
				// Calculate required size with the given snapReserve to ensure sufficient LUN space
				requiredQuotaInBytes = int64(float64(currentLunSpace) / (1 - float64(*params.SnapReserve)/utils.PercentageBase))
				return customerrors.NewUserInputValidationErr(fmt.Sprintf(ErrMsgSnapReserveIncrease, float64(*params.SnapReserve), float64(currentLunSpace)/float64(bytesPerGB), math.Ceil(float64(requiredQuotaInBytes)/float64(bytesPerGB))))
			} else {
				// Calculate updated LUN space with the new given size
				updatedLunSpace := params.QuotaInBytes - int64(float64(params.QuotaInBytes)*float64(*params.SnapReserve)/utils.PercentageBase)
				if updatedLunSpace < currentLunSpace {
					// Calculate required size to ensure sufficient LUN space
					requiredQuotaInBytes = int64(float64(currentLunSpace) / (1 - float64(*params.SnapReserve)/utils.PercentageBase))
					return customerrors.NewUserInputValidationErr(fmt.Sprintf(ErrMsgSnapReserveIncrease, float64(*params.SnapReserve), float64(currentLunSpace)/float64(bytesPerGB), math.Ceil(float64(requiredQuotaInBytes)/float64(bytesPerGB))))
				}
			}
		}
		// Allow snapReserve decrease as it increases available LUN space
	}

	return nil
}

// validateCRBBackupVault validates cross-region backup vault configuration
// region is the region where the volume is being created/updated
func validateCRBBackupVault(backupVault *datamodel.BackupVault, region string) error {
	if backupVault.BackupVaultType == activities.CrossRegionBackupType {
		if !utils.IsCrossRegionBackupEnabled() {
			return customerrors.NewBadRequestErr(activities.CrossRegionBackupVaultErrMsg)
		}
		if backupVault.SourceRegionName == nil || *backupVault.SourceRegionName == "" {
			return customerrors.NewBadRequestErr("Source region must be specified for cross-region backup vault")
		}
		if backupVault.BackupRegionName == nil || *backupVault.BackupRegionName == "" {
			return customerrors.NewBadRequestErr("Backup region must be specified for cross-region backup vault")
		}
		if *backupVault.SourceRegionName == *backupVault.BackupRegionName {
			return customerrors.NewBadRequestErr("Backup region must be different from source region for cross-region backup vault")
		}
		if backupVault.LifeCycleState != models.LifeCycleStateREADY {
			return customerrors.NewBadRequestErr("Cross-region backup vault must be in READY state")
		}
		if *backupVault.BackupRegionName == region {
			return customerrors.NewUserInputValidationErr("cannot assign a cross-region backup vault to a volume in the destination region")
		}
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

func validateUpdateFileProperties(params *common.UpdateVolumeParams, volume *datamodel.Volume) error {
	if volume.VolumeAttributes == nil || volume.VolumeAttributes.FileProperties == nil {
		return customerrors.NewUserInputValidationErr("File properties is mandatory to update file properties on the volume")
	}

	// Validate that the volume supports NFS protocols before allowing file property updates
	if !utils.IsFileProtocolSupported(volume.Account.Name) {
		return customerrors.NewUserInputValidationErr("file properties can only be supported for volumes with NAS protocols")
	}

	if params.FileProperties == nil {
		return customerrors.NewUserInputValidationErr("File properties cannot be nil")
	}

	if params.FileProperties.ExportPolicy != nil && params.FileProperties.ExportPolicy.ExportRules != nil {
		// Use update params protocols if provided, otherwise use existing volume protocols
		protocols := params.Protocols
		if len(protocols) == 0 && volume.VolumeAttributes != nil {
			protocols = volume.VolumeAttributes.Protocols
		}

		if err := validateExportRulesAgainstProtocols(params.FileProperties.ExportPolicy.ExportRules, protocols); err != nil {
			return err
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

// validateExportRulesAgainstProtocols validates export rules against the supported NFS protocols.
// It checks that allowed clients are valid and that NFSv3/NFSv4 export policy rules match volume protocols.
func validateExportRulesAgainstProtocols(rules []*models.ExportRule, protocols []string) error {
	// Determine if volume supports NFSv3 and/or NFSv4
	hasNFSv3 := false
	hasNFSv4 := false
	for _, protocol := range protocols {
		if protocol == utils.ProtocolNFSv3 {
			hasNFSv3 = true
		}
		if protocol == utils.ProtocolNFSv4 {
			hasNFSv4 = true
		}
	}

	for _, rule := range rules {
		if rule.AllowedClients == "" {
			return customerrors.NewUserInputValidationErr("allowed clients cannot be nil in export rules")
		}
		// Validate allowed clients
		if err := validateAllowedClients(rule.AllowedClients); err != nil {
			return customerrors.NewUserInputValidationErr(fmt.Sprintf("allowed clients validation failed: %v", err))
		}

		// Validate NFSv3/NFSv4 export policy rules match volume protocols
		// For NFSv3-only volumes: exportPolicy.NFSv4 should always be false
		if !hasNFSv4 && rule.NFSv4 {
			return customerrors.NewUserInputValidationErr("Cannot specify NFSv4 export policy rules for non-NFSv4 volume")
		}

		// For NFSv4-only volumes: exportPolicy.NFSv3 should always be false
		if !hasNFSv3 && rule.NFSv3 {
			return customerrors.NewUserInputValidationErr("Cannot specify NFSv3 export policy rules for non-NFSv3 volume")
		}
	}
	return nil
}

// checkIfPoolUpdateRequired determines if a pool update is needed based on volume count, current instance type, and operation type
// For volume create: Returns true if volumeCount exceeds the maxVolumeCount of current instance type
// For volume delete: Returns true if volumeCount falls below the maxVolumeCount of the previous (smaller) instance type
func checkIfPoolUpdateRequired(volumeCount int64, currentInstanceType string, volLimitPerInstanceMap map[string]common.VolumeCountRange, isDelete bool) bool {
	currentLimits, exists := volLimitPerInstanceMap[currentInstanceType]
	if !exists {
		// If instance type not found in map, assume no update needed
		return false
	}

	if !isDelete {
		// Volume Create scenario: Update required if volume count exceeds current instance type's max
		return volumeCount > currentLimits.MaxVolumeCount
	}

	// Volume Delete scenario: Check if we can scale down to a smaller instance type
	// Find the previous (smaller) instance type's max capacity
	var previousMaxVolumeCount int64 = -1

	// Iterate through the map to find the instance type with max capacity just below the current one
	for _, limits := range volLimitPerInstanceMap {
		if limits.MaxVolumeCount < currentLimits.MaxVolumeCount {
			if previousMaxVolumeCount == -1 || limits.MaxVolumeCount > previousMaxVolumeCount {
				previousMaxVolumeCount = limits.MaxVolumeCount
			}
		}
	}

	// If no previous instance type exists (we're on the smallest), no update needed
	if previousMaxVolumeCount == -1 {
		return false
	}

	// Update required if volume count is less than the previous instance type's max
	// This means we can scale down to a smaller instance type
	return volumeCount < previousMaxVolumeCount
}

// checkAndTriggerPoolScalingIfNeeded checks if the pool needs scaling and triggers it asynchronously
func checkAndTriggerPoolScalingIfNeeded(ctx context.Context, se database.Storage, temporal client.Client, pool *datamodel.Pool, isDelete bool) {
	logger := util.GetLogger(ctx)

	// Validate pool state - only scale pools in a stable state
	if pool.State != models.LifeCycleStateREADY {
		logger.Warnf("Pool not in ready state poolID: %s, state: %s", pool.UUID, pool.State)
		return
	}

	// Get current volume count for the pool
	currentVolumeCount, err := se.GetVolumeCountByPoolID(ctx, pool.ID)
	if err != nil {
		logger.Error("Failed to get volume count for pool", "poolID", pool.ID, "error", err)
		return
	}
	volLimitPerInstanceMap := make(map[string]common.VolumeCountRange)
	err = json.Unmarshal([]byte(autoPoolScalingLimits), &volLimitPerInstanceMap)
	if err != nil || len(volLimitPerInstanceMap) == 0 {
		logger.Error("Failed to parse auto pool scaling limits", "error", err)
		return
	}

	// Get current instance type from pool's nodes
	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil || len(nodes) == 0 {
		logger.Error("Failed to get nodes for pool", "poolID", pool.ID, "error", err)
		return
	}
	currentInstanceType := ""
	if nodes[0].NodeAttributes != nil {
		currentInstanceType = nodes[0].NodeAttributes.InstanceType
	}
	if currentInstanceType == "" {
		logger.Warn("Current instance type not found for pool", "poolID", pool.UUID)
		return
	}

	// Check if current instance type is appropriate for the volume count
	requiresUpdate := checkIfPoolUpdateRequired(currentVolumeCount, currentInstanceType, volLimitPerInstanceMap, isDelete)
	if !requiresUpdate {
		logger.Infof("Pool update not required. Current instance type %s is suitable for %d volumes", currentInstanceType, currentVolumeCount)
		return
	}

	logger.Infof("Pool update required. Current instance type: %s, Volume count: %d", currentInstanceType, currentVolumeCount)

	autoScalingParams := &common.AutoPoolScalingParams{
		VolLimitPerInstanceMap: volLimitPerInstanceMap,
		CurrentVolumeCount:     currentVolumeCount,
	}

	region := env.GetString("LOCAL_REGION", "")
	updateParams := &common.UpdatePoolParams{
		PoolId:               pool.UUID,
		AccountName:          pool.Account.Name,
		Region:               region,
		SizeInBytes:          uint64(pool.SizeInBytes),
		TotalThroughputMibps: pool.PoolAttributes.ThroughputMibps,
		TotalIops:            &pool.PoolAttributes.Iops,
		Description:          pool.Description,
		Labels:               pool.PoolAttributes.Labels,
		AllowAutoTiering:     pool.AllowAutoTiering,
		LargeCapacity:        &pool.LargeCapacity,
	}

	if pool.AllowAutoTiering && pool.AutoTieringConfig != nil {
		updateParams.HotTierSizeInBytes = uint64(pool.AutoTieringConfig.HotTierSizeInBytes)
		updateParams.EnableHotTierAutoResize = pool.AutoTieringConfig.EnableHotTierAutoResize
	}

	poolCategory := models.GetPoolCategory(pool.LargeCapacity)
	job := &datamodel.Job{
		Type:          string(models.GetResourceJobType(models.ResourceTypePool, models.ResourceOperationUpdate, poolCategory)),
		State:         string(models.JobsStateNEW),
		ResourceName:  pool.UUID,
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
		AccountID:     sql.NullInt64{Int64: pool.AccountID, Valid: true},
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return
	}

	poolCurrentState := pool.State
	previousStateDetails := pool.StateDetails

	// Put the pool in updating state before the operation
	if _, poolErr := se.UpdatePoolState(ctx, pool, models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails); poolErr != nil {
		logger.Error("Failed to update pool state to ERROR", "poolID", pool.UUID, "error", poolErr)
	}

	defer func() {
		if err != nil {
			// Revert the state in error
			if _, poolErr := se.UpdatePoolState(ctx, pool, poolCurrentState, previousStateDetails); poolErr != nil {
				logger.Error("Failed to update pool state to ERROR", "poolID", pool.UUID, "error", poolErr)
			}
		}
	}()

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.BackgroundTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		workflows.UpdatePoolWorkflow,
		updateParams,
		pool,
		autoScalingParams,
	)
	if err != nil {
		logger.Errorf("failed to start automatic pool scaling workflow: %v", err)
	}

	logger.Infof("Triggered instance upgrade for pool: %s", pool.Name)
	return
}

func (o *Orchestrator) RestoreFilesFromBackup(ctx context.Context, params *common.RestoreFilesFromBackupParams) (string, error) {
	return _restoreFilesFromBackup(ctx, o.storage, o.temporal, params)
}

func _restoreFilesFromBackup(ctx context.Context, se database.Storage, temporal client.Client, params *common.RestoreFilesFromBackupParams) (string, error) {
	logger := util.GetLogger(ctx)

	account, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return "", err
	}

	volume, err := se.GetVolume(ctx, params.VolumeUUID)
	if err != nil {
		return "", err
	}

	if volume.State != models.LifeCycleStateREADY {
		return "", customerrors.NewUserInputValidationErr("Volume is not ready")
	}

	// Get backup either by BackupID or BackupPath
	var backup *datamodel.Backup
	var backupVault *datamodel.BackupVault

	if params.BackupPath != "" {
		components := strings.Split(params.BackupPath, "/")

		// Ensure there are enough components to avoid out of range errors
		if len(components) < MaxBackupPathComponents {
			return "", customerrors.NewUserInputValidationErr("Backup path is not in correct format")
		}

		backupRegion := components[LocationIdIndex]
		// Get the volume's region from its pool
		location, err := utils.GetLocationFromVendorID(volume.Pool.VendorID)
		if err != nil {
			logger.Error("Failed to get location from vendor ID: ", "error", err)
			return "", err
		}
		volumeRegion, _, err := utils.ParseRegionAndZone(location)
		if err != nil {
			return "", err
		}
		backupVaultName := components[BackupVaultNameIndex]
		if backupRegion != volumeRegion {
			backupVault, err = se.GetBackupVaultByCrossRegionBackupVaultName(ctx, backupVaultName, account.ID)
			if err != nil {
				return "", err
			}
		} else {
			backupVault, err = se.GetBackupVaultByNameAndOwnerID(ctx, backupVaultName, strconv.FormatInt(account.ID, 10))
			if err != nil {
				return "", err
			}
		}

		backupName := components[BackupNameIndex]
		// TODO: restore SDE Backup to VCP - need to fetch the details from sde db and store it will bucket details in case if the record is not found in VCP DB
		backup, err = se.GetBackupByNameAndBackupVaultID(ctx, backupName, backupVault.ID)
		if err != nil {
			return "", err
		}
	} else {
		return "", customerrors.NewUserInputValidationErr("BackupPath must be provided")
	}

	// Validate that backup is in a valid state for restore
	if backup.State != models.LifeCycleStateAvailable {
		return "", customerrors.NewUserInputValidationErr("Cannot restore files from backup which is not available")
	}

	// check that volume is files not block
	if utils.IsSanProtocols(volume.VolumeAttributes.Protocols) {
		return "", customerrors.NewUserInputValidationErr("Single file restore is not supported for ISCSI Volumes")
	}

	if utils.IsSanProtocols(backup.Attributes.Protocols) {
		return "", customerrors.NewUserInputValidationErr("Single file restore is not supported from a backup of ISCSI Volumes")
	}

	originalState := volume.State
	originalStateDetails := volume.StateDetails
	stateUpdated := false

	// Update volume state to RESTORING
	volume.State = models.LifeCycleStateRestoring
	volume.StateDetails = models.LifeCycleStateRestoringDetails

	// Create a job for the restore files operation
	job := &datamodel.Job{
		Type:          string(models.JobTypeRestoreFilesBackup),
		State:         string(models.JobsStateNEW),
		ResourceName:  volume.UUID,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create restore files from backup job in database", "error", err)
		return "", err
	}

	defer func() {
		if err != nil {
			// Rollback occurs for any error after a successful state update.
			// WorkflowExecutor now handles retries and workflow error states internally.
			if stateUpdated {
				volume.State = originalState
				volume.StateDetails = originalStateDetails
				if _, rollbackErr := updateVolumeStatus(ctx, se, volume, originalState, originalStateDetails); rollbackErr != nil {
					logger.Error("Failed to rollback volume state", "error", rollbackErr, "originalState", originalState)
				}
			}

			// Mark job as error if it was created
			if createdJob != nil {
				if jobErr := se.UpdateJob(ctx, createdJob.UUID, string(models.JobsStateERROR), 0, err.Error()); jobErr != nil {
					logger.Error("Failed to update job state to ERROR", "error", jobErr, "jobUUID", createdJob.UUID)
				}
			}
		}
	}()

	// Update volume state in database
	volume, err = updateVolumeStatus(ctx, se, volume, models.LifeCycleStateRestoring, models.LifeCycleStateRestoringDetails)
	if err != nil {
		logger.Error("Failed to update volume state to restoring", "error", err)
		return "", err
	}
	stateUpdated = true

	// Execute the workflow
	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)
	err = workflowExecutor.ExecuteWorkflow(
		ctx,
		createdJob.WorkflowID,
		workflowengine.CustomerTaskQueue,
		workflows.RestoreFilesFromBackupWorkflow,
		workflowengine.GetSFRWorkflowTimeout(),
		params,
		backup,
		volume,
	)
	if err != nil {
		logger.Error("Failed to start restore files from backup workflow after retries: ", "error", err)
		return "", err
	}
	return createdJob.UUID, nil
}

func (o *Orchestrator) SplitCloneVolume(ctx context.Context, params *common.SplitCloneVolumeParams) (*models.Volume, string, error) {
	return splitCloneVolume(ctx, o.storage, o.temporal, params)
}

func _splitCloneVolume(ctx context.Context, se database.Storage, temporal client.Client, params *common.SplitCloneVolumeParams) (*models.Volume, string, error) {
	logger := util.GetLogger(ctx)
	workflowExecutor := workflows.NewWorkflowExecutor(temporal, logger)

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

	pool, err := se.GetPool(ctx, volume.Pool.UUID, account.ID)
	if err != nil {
		logger.Error("Failed to fetch pool for the given account ID", "error", err)
		return nil, "", err
	}

	if utils.IsTransitionalState(volume.State) {
		logger.Errorf("Volume %s cannot be split, while in transitioning state: %s", volume.Name, volume.State)
		return nil, "", customerrors.NewConflictErr("volume is in transition state and cannot be split, state: " + volume.State)
	}

	if volume.State != models.LifeCycleStateREADY {
		return nil, "", customerrors.NewConflictErr("Volume is not in READY state, state: " + volume.State)
	}

	err = validateSplitCloneVolumeParams(ctx, volume, pool)
	if err != nil {
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeSplitVolume),
		State:         string(models.JobsStateNEW),
		ResourceName:  volume.Name,
		AccountID:     sql.NullInt64{Int64: volume.AccountID, Valid: true},
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: volume.UUID},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create volume split job in database", "error", err)
		return nil, "", err
	}

	// Defer to mark job as deleted if any error happens
	defer func() {
		if err != nil {
			// Delete job if error occurred
			if createdJob != nil && createdJob.UUID != "" {
				logger.Warnf("Error occurred, marking job entry in DB as deleted. Job UUID: %s", createdJob.UUID)
				if delErr := se.DeleteJob(ctx, createdJob.UUID, err.Error()); delErr != nil {
					logger.Errorf("Failed to delete job: %v", delErr)
				}
			}
		}
	}()

	previousState := volume.State
	previousStateDetails := volume.StateDetails
	volume, err = updateVolumeStatus(ctx, se, volume, models.LifeCycleStateSplitting, models.LifeCycleStateSplittingDetails)
	if err != nil {
		logger.Error("Failed to update volume state in database", "error", err)
		return nil, "", err
	}
	// Defer to revert the resource state
	defer func() {
		if err != nil {
			// Revert volume state back to previous state if it was set to SPLITTING
			if volume.State == models.LifeCycleStateSplitting {
				logger.Warnf("Error occurred during volume split, reverting volume state to previous state '%s'. Volume UUID: %s", previousState, volume.UUID)
				volumeUpdateErr := se.UpdateVolumeFields(ctx, volume.UUID, map[string]interface{}{
					"state":         previousState,
					"state_details": previousStateDetails,
				})
				if volumeUpdateErr != nil {
					logger.Errorf("Failed to revert volume state to previous volume state: %v", volumeUpdateErr)
				}
			}
		}
	}()

	location, err := utils.GetLocationFromVendorID(volume.Pool.VendorID)
	if err != nil {
		logger.Error("Failed to get location from vendor ID: ", "error", err)
		return nil, "", err
	}

	// controlWorkflowID defines the workflow ID for the control workflow
	controlWorkflowID := workflows.GenerateControlWorkflowID(volume.Account.ID, location, volume.Pool.Name)
	workflowOptions := workflows.DefaultSequentialWorkflowOptions(controlWorkflowID, createdJob.WorkflowID)
	err = workflowExecutor.ExecuteSequentialWorkflow(
		ctx,
		workflowOptions,
		workflows.SplitVolumeWorkflow,
		volume,
	)
	if err != nil {
		logger.Error("Failed to start split clone workflow after retries: ", "error", err)
		return nil, "", err
	}

	return convertDatastoreVolumeToModel(volume, nil), createdJob.UUID, nil
}

func _validateSplitCloneVolumeParams(ctx context.Context, volume *datamodel.Volume, pool *datamodel.PoolView) error {
	logger := util.GetLogger(ctx)

	if volume.ClonesSharedBytes == 0 {
		logger.Errorf("Volume %s is not a thin clone volume, cannot perform split operation", volume.Name)
		return customerrors.NewUserInputValidationErr("volume is not a thin clone volume, cannot perform split operation")
	}

	if pool.QuotaInBytes+volume.ClonesSharedBytes > uint64(pool.SizeInBytes) {
		logger.Errorf("Insufficient space in pool %s to split the clone volume %s", pool.Name, volume.Name)
		return customerrors.NewUserInputValidationErr("insufficient space in pool to split the clone volume, please free up space and try again")
	}

	return nil
}
