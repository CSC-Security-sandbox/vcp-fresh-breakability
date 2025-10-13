package workflows

import (
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/flexcache_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type volumeUpdateWorkflow struct {
	// add fields needed for volume workflow
	BaseWorkflow
}

// Enforcing the WorkflowInterface on volumeUpdateWorkflow
var _ WorkflowInterface = &volumeUpdateWorkflow{}
var (
	convertCacheParameters    = _convertCacheParameters
	flexCacheEnabled          = env.GetBool("FLEXCACHE_ENABLED", false)
	isUpdateFlexCacheRequired = _isUpdateFlexCacheRequired
)

// UpdateVolumeWorkflow Update Volume Workflow process volume related requests from a customer.
func UpdateVolumeWorkflow(ctx workflow.Context, params *common.UpdateVolumeParams, volume *datamodel.Volume) error {
	log := util.GetLogger(ctx)
	volumeWf := new(volumeUpdateWorkflow)
	err := volumeWf.Setup(ctx, volume)
	if err != nil {
		log.Errorf("Volume update workflow setup executed with error: %v", err)
		return err
	}
	volumeWf.Status = WorkflowStatusRunning
	err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Processing for UpdateVolumeWorkflow: %v", err)
		return err
	}

	_, customErr := volumeWf.Run(ctx, params, volume)
	if customErr != nil {
		log.Errorf("UpdateVolumeWorkflow completed with error: %v", customErr)
		volumeWf.Status = WorkflowStatusFailed
		err2 := volumeWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if err2 != nil {
			log.Errorf("Failed to update job status to Done with err for UpdateVolumeWorkflow: %v", err)
			return err2
		}
		return customErr
	}

	volumeWf.Status = WorkflowStatusCompleted
	err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Done for UpdateVolumeWorkflow: %v", err)
	}
	return err
}

func (wf *volumeUpdateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	volume := input.(*datamodel.Volume)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = volume.Account.Name
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

func (wf *volumeUpdateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	log := util.GetLogger(ctx)
	params := args[0].(*common.UpdateVolumeParams)
	volume := args[1].(*datamodel.Volume)
	updateActivity := &activities.VolumeUpdateActivity{}
	deleteActivity := &activities.VolumeDeleteActivity{}
	flexCacheUpdateActivity := &flexcache_activities.FlexCacheVolumeUpdateActivity{}

	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	options := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, options)

	rollbackManager := common.NewRollbackManager()
	defer func() {
		if err != nil {
			err2 := workflow.ExecuteActivity(ctx, activities.VolumeCreateActivity.UpdateVolumeStateInDB, volume.UUID, models.LifeCycleStateREADY, models.LifeCycleStateAvailableDetails).Get(ctx, nil)
			if err2 != nil {
				log.Errorf("Failed to update volume state in DB to READY: %v", err2)
			}
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, volume.Pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{Nodes: dbNodes, Password: volume.Pool.PoolCredentials.Password, SecretID: volume.Pool.PoolCredentials.SecretID, DeploymentName: volume.Pool.DeploymentName, CertificateID: volume.Pool.PoolCredentials.CertificateID, AuthType: volume.Pool.PoolCredentials.AuthType})

	// Update the snapshot policy if it is provided in the params
	if params.SnapshotPolicy != nil && params.SnapshotPolicy.Name != "" && !volume.VolumeAttributes.IsDataProtection {
		updatingPolicy := populateSnapshotPolicyFromParams(params.SnapshotPolicy)

		// If the volume does not have an existing snapshot policy, we need to create a new one using the provided snapshot policy
		if volume.SnapshotPolicy == nil || volume.SnapshotPolicy.Name == "" {
			createActivity := &activities.VolumeCreateActivity{}
			volume.SnapshotPolicy = updatingPolicy

			err = workflow.ExecuteActivity(ctx, createActivity.CreateSnapshotPolicyInONTAP, &volume, &node).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
			rollbackManager.AddActivity(deleteActivity.DeleteSnapshotPolicyInONTAP, volume.SnapshotPolicy.Name, node)
		} else // If the volume has an existing snapshot policy, we need to update it with only the changes
		{
			if len(updatingPolicy.Schedules) == 0 {
				// If the schedules are not populated in update, we want to set them as the existing schedules
				// This is done because ONTAP cannot update the snapshot policy without any schedules
				// This will happen when the user is trying to enable/disable the snapshot policy, without any change to schedules
				updatingPolicy.Schedules = volume.SnapshotPolicy.Schedules
			}
			// Passing the current & new snapshot policy to the activity to find the delta & update the snapshot policy in ONTAP
			err = workflow.ExecuteActivity(ctx, updateActivity.UpdateSnapshotPolicyInOntap, &node, &volume.SnapshotPolicy, updatingPolicy).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
			rollbackManager.AddActivity(updateActivity.UpdateSnapshotPolicyInOntap, node, updatingPolicy, volume.SnapshotPolicy)
			volume.SnapshotPolicy = updatingPolicy
		}
	}

	volResponse := &vsa.VolumeResponse{}
	err = workflow.ExecuteActivity(ctx, updateActivity.GetVolumeFromONTAP, volume, &node).Get(ctx, &volResponse)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if isUpdateRequired(volResponse, params, volume) {
		rollbackManager.AddActivity(updateActivity.UpdateVolumeInONTAP, volume, getUpdateParamsForRollback(volResponse, volume), node)
		err = workflow.ExecuteActivity(ctx, updateActivity.UpdateVolumeInONTAP, volume, params, node).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	if isUpdateFlexCacheRequired(volume, params) {
		rollbackManager.AddActivity(flexCacheUpdateActivity.UpdateFlexCacheVolumeInONTAP, volume,
			getUpdateParamsForRollback(volResponse, volume), node)
		err = workflow.ExecuteActivity(ctx, flexCacheUpdateActivity.UpdateFlexCacheVolumeInONTAP, volume,
			params, node).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	// Avoid updating the lun if the size is not changed
	if volume.VolumeAttributes != nil && len(volume.VolumeAttributes.Protocols) > 0 && utils.IsSANProtocol(volume.VolumeAttributes.Protocols[0]) && (params.QuotaInBytes > volume.SizeInBytes || (params.SnapReserve != nil && volume.VolumeAttributes != nil && *params.SnapReserve != volume.VolumeAttributes.SnapReserve)) && !volume.VolumeAttributes.IsDataProtection {
		updatedLun := &vsa.LunResponse{}
		err = workflow.ExecuteActivity(ctx, updateActivity.GetVolumeFromONTAP, volume, &node).Get(ctx, &volResponse)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		err = workflow.ExecuteActivity(ctx, updateActivity.UpdateLun, volume, volResponse, node, params).Get(ctx, &updatedLun)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		splitLunName := strings.Split(updatedLun.Name, "/")
		lunName := splitLunName[len(splitLunName)-1]
		if volume.VolumeAttributes != nil && volume.VolumeAttributes.BlockDevices != nil && len(*volume.VolumeAttributes.BlockDevices) > 0 {
			blockDevices := *volume.VolumeAttributes.BlockDevices
			for i := range blockDevices {
				if blockDevices[i].Name == lunName {
					blockDevice := &common.BlockDevice{
						Name:            blockDevices[i].Name,
						SizeInBytes:     updatedLun.Size,
						OSType:          blockDevices[i].OSType,
						LunSerialNumber: blockDevices[i].Identifier,
						LunUUID:         blockDevices[i].LunUUID,
					}
					volumeAttachedHG := utils.GetHgUUIDs(blockDevices[i].HostGroupDetails)
					blockDevice.HostGroups = volumeAttachedHG
					params.BlockDevices = append(params.BlockDevices, blockDevice)
				}
			}
		}
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		// No rollback for LUN because we cannot decrease the size of a LUN in ONTAP.
	}

	// Check BlockDevices first, then fallback to BlockProperties for host group updates
	if len(params.BlockDevices) > 0 {
		// Use BlockDevices as primary source for host group updates
		// For now, we'll use the first BlockDevice's host groups for consistency
		// In the future, this could be extended to handle multiple BlockDevices
		primaryBlockDevice := params.BlockDevices[0]

		// Get current host groups from BlockDevices if available, otherwise fallback to BlockProperties
		var volumeAttachedHG []string
		if volume.VolumeAttributes.BlockDevices != nil && len(*volume.VolumeAttributes.BlockDevices) > 0 {
			primaryVolumeBlockDevice := (*volume.VolumeAttributes.BlockDevices)[0]
			volumeAttachedHG = utils.GetHgUUIDs(primaryVolumeBlockDevice.HostGroupDetails)
		}

		if !utils.IsSliceEqual(primaryBlockDevice.HostGroups, volumeAttachedHG) {
			toCreate, toDelete := activities.HostGroupsUpdateDiffForVolume(volumeAttachedHG, primaryBlockDevice.HostGroups)

			// Ensure the lun iGroup maps to delete created
			if len(toDelete) > 0 {
				err = workflow.ExecuteActivity(ctx, updateActivity.UnmapHostGroupFromDisk, &volume, toDelete, &node).Get(ctx, nil)
				if err != nil {
					return nil, ConvertToVSAError(err)
				}
			}

			if len(toCreate) > 0 {
				err = workflow.ExecuteActivity(ctx, updateActivity.EnsureHostGroupsExistsAndMapDisk, &volume, toCreate, &node).Get(ctx, nil)
				if err != nil {
					return nil, ConvertToVSAError(err)
				}
			}
		}
	} else if params.BlockProperties != nil {
		// Fallback: Use BlockProperties if BlockDevices are not provided
		volumeAttachedHG := utils.GetHgUUIDs(volume.VolumeAttributes.BlockProperties.HostGroupDetails)
		if !utils.IsSliceEqual(params.BlockProperties.HostGroupUUIDs, volumeAttachedHG) {
			toCreate, toDelete := activities.HostGroupsUpdateDiffForVolume(volumeAttachedHG, params.BlockProperties.HostGroupUUIDs)

			if len(toCreate) > 0 {
				err = workflow.ExecuteActivity(ctx, updateActivity.EnsureHostGroupsExistsAndMapDisk, &volume, toCreate, &node).Get(ctx, nil)
				if err != nil {
					return nil, ConvertToVSAError(err)
				}
			}

			// Ensure the lun iGroup maps to delete created
			if len(toDelete) > 0 {
				err = workflow.ExecuteActivity(ctx, updateActivity.UnmapHostGroupFromDisk, &volume, toDelete, &node).Get(ctx, nil)
				if err != nil {
					return nil, ConvertToVSAError(err)
				}
			}
		}
	}

	if volume.VolumeAttributes != nil && volume.VolumeAttributes.FileProperties != nil {
		// Update junction path only if it is provided in the params
		if params.FileProperties != nil && params.FileProperties.JunctionPath != "" && params.FileProperties.JunctionPath != volume.VolumeAttributes.FileProperties.JunctionPath {
			rollbackManager.AddActivity(updateActivity.UpdateJunctionPathInONTAP, &volume, volume.VolumeAttributes.FileProperties.JunctionPath, &node)
			err = workflow.ExecuteActivity(ctx, updateActivity.UpdateJunctionPathInONTAP, &volume, params.FileProperties.JunctionPath, &node).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
			volume.VolumeAttributes.FileProperties.JunctionPath = params.FileProperties.JunctionPath
		}
		// Update export policy rules only if it is provided in the params
		if params.FileProperties != nil && isExportPolicyRulesUpdateRequired(volume.VolumeAttributes.FileProperties.ExportPolicy, params.FileProperties.ExportPolicy) {
			params.FileProperties.ExportPolicy.ExportPolicyName = volume.VolumeAttributes.FileProperties.ExportPolicy.ExportPolicyName
			rollbackManager.AddActivity(updateActivity.UpdateExportPolicyRulesInONTAP, &volume, volume.VolumeAttributes.FileProperties.ExportPolicy, &node)
			err = workflow.ExecuteActivity(ctx, updateActivity.UpdateExportPolicyRulesInONTAP, &volume, params.FileProperties.ExportPolicy, &node).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
			volume.VolumeAttributes.FileProperties.ExportPolicy = getUpdatedExportPolicy(params.FileProperties.ExportPolicy)
		}
	}

	if volume.DataProtection != nil && volume.DataProtection.BackupVaultID != "" {
		if !env.IsLocalEnv() {
			var token string
			err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetAuthJWTToken, params.AccountName).Get(ctx, &token)
			if err != nil {
				log.Errorf("Failed to get token for account %s: %v", params.AccountName, err)
				return nil, ConvertToVSAError(err)
			}
			ctx = workflow.WithValue(ctx, middleware.AuthorizationToken, token)
		}

		tenancyDetails := &common.TenancyInfo{}
		err = workflow.ExecuteActivity(ctx, updateActivity.FindTenancyDetails, volume.VolumeAttributes.VendorSubnetID, volume.Account.Name, &params.Region).Get(ctx, &tenancyDetails)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		err = workflow.ExecuteActivity(ctx, updateActivity.CheckBackupVaultExistInVCP, &volume, &params.Region).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		bucketDetails := &common.BucketDetails{}
		err = workflow.ExecuteActivity(ctx, updateActivity.CheckBucketResourceName, &volume).Get(ctx, &bucketDetails)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		if bucketDetails.BucketName == "" && bucketDetails.ServiceAccountName == "" && bucketDetails.TenantProjectNumber == "" {
			resourceName := &common.ResourceNames{}
			err = workflow.ExecuteActivity(ctx, updateActivity.GenerateResourceNamesForBackupVault, &volume, &tenancyDetails, params.Region).Get(ctx, &resourceName)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}

			err = workflow.ExecuteActivity(ctx, updateActivity.CreateBucketForBackupVault, &resourceName, &tenancyDetails, params.Region).Get(ctx, &bucketDetails)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}

			// Setting the 'satisfiesPzi' and 'satisfiesPzs' fields in bucketDetails by fetching the latest info from GCP
			err = syncBucketDetailsWithGCP(ctx, bucketDetails)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}

			err = workflow.ExecuteActivity(ctx, updateActivity.UpdateBucketDetailsOfBackupVault, &volume, &bucketDetails).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
		}
	}

	if volume.DataProtection != nil && params.DataProtection != nil && !nillable.IsNilOrEmpty(params.DataProtection.BackupPolicyId) {
		// Assigning backup policy to the volume
		if !env.IsLocalEnv() {
			var token string
			err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetAuthJWTToken, params.AccountName).Get(ctx, &token)
			if err != nil {
				log.Errorf("Failed to get token for account %s: %v", params.AccountName, err)
				return nil, ConvertToVSAError(err)
			}
			ctx = workflow.WithValue(ctx, middleware.AuthorizationToken, token)
		}

		var backupPolicyExists bool
		err = workflow.ExecuteActivity(ctx, updateActivity.VerifyIfBackupPolicyExistsInVCP, *params.DataProtection.BackupPolicyId, volume.AccountID).Get(ctx, &backupPolicyExists)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		if !backupPolicyExists {
			backupPolicyActivity := &activities.BackupPolicyActivity{}
			var vcpBackupPolicy *datamodel.BackupPolicy
			err = workflow.ExecuteActivity(ctx, updateActivity.FetchAndCreateBackupPolicyFromSDE, volume, params.Region).Get(ctx, &vcpBackupPolicy)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
			rollbackManager.AddActivity(backupPolicyActivity.DeleteBackupPolicyInVCP, vcpBackupPolicy.UUID)
			err = workflow.ExecuteActivity(ctx, updateActivity.CreateScheduleForBackupPolicy, vcpBackupPolicy, params.BackupSchedule).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
			rollbackManager.AddActivity(backupPolicyActivity.DeleteBackupPolicySchedule, vcpBackupPolicy.UUID)

			if !vcpBackupPolicy.PolicyEnabled {
				err = workflow.ExecuteActivity(ctx, backupPolicyActivity.PauseBackupPolicySchedule, vcpBackupPolicy).Get(ctx, nil)
				if err != nil {
					return nil, ConvertToVSAError(err)
				}
			}
		}
	}

	err = workflow.ExecuteActivity(ctx, updateActivity.UpdateVolumeInDB, volume, &params).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Update BackupMetadata labels if an entry exists for this volume
	backupActivity := &activities.BackupActivity{}
	metadataErr := workflow.ExecuteActivity(ctx, backupActivity.UpdateBackupMetadataIfExistsActivity, volume).Get(ctx, nil)
	if metadataErr != nil {
		// Log the error but don't fail the entire volume update workflow
		log.Errorf("Failed to update BackupMetadata for volume %s: %v", volume.UUID, metadataErr)
	}

	return nil, ConvertToVSAError(err)
}

func isExportPolicyRulesUpdateRequired(currentPolicy *datamodel.ExportPolicy, updatePolicy *models.ExportPolicy) bool {
	if currentPolicy == nil && updatePolicy != nil {
		return true
	}
	if currentPolicy != nil && updatePolicy == nil {
		return true
	}
	if currentPolicy == nil && updatePolicy == nil {
		return false
	}
	if updatePolicy.ExportPolicyName != "" && currentPolicy.ExportPolicyName != updatePolicy.ExportPolicyName {
		return false
	}
	if len(currentPolicy.ExportRules) != len(updatePolicy.ExportRules) {
		return true
	}
	// For each rule in updatePolicy, find a matching rule in currentPolicy
	for _, updateRule := range updatePolicy.ExportRules {
		found := false
		for _, currentRule := range currentPolicy.ExportRules {
			if rulesEqual(currentRule, updateRule) {
				found = true
				break
			}
		}
		if !found {
			return true
		}
	}

	// Also check the reverse: for each rule in currentPolicy, find a matching rule in updatePolicy
	for _, currentRule := range currentPolicy.ExportRules {
		found := false
		for _, updateRule := range updatePolicy.ExportRules {
			if rulesEqual(currentRule, updateRule) {
				found = true
				break
			}
		}
		if !found {
			return true
		}
	}

	return false
}

// Helper function to compare two export rules
func rulesEqual(currentRule *datamodel.ExportRule, updateRule *models.ExportRule) bool {
	// Compare the fields that matter for equality
	// Adjust these field names to match your actual struct fields
	return currentRule.AllowedClients == updateRule.AllowedClients &&
		currentRule.AnonymousUser == updateRule.AnonymousUser &&
		currentRule.AccessType == updateRule.AccessType &&
		currentRule.CIFS == updateRule.CIFS &&
		currentRule.NFSv3 == updateRule.NFSv3 &&
		currentRule.NFSv4 == updateRule.NFSv4 &&
		currentRule.S3 == updateRule.S3 &&
		currentRule.UnixReadOnly == updateRule.UnixReadOnly &&
		currentRule.UnixReadWrite == updateRule.UnixReadWrite &&
		currentRule.Kerberos5ReadOnly == updateRule.Kerberos5ReadOnly &&
		currentRule.Kerberos5ReadWrite == updateRule.Kerberos5ReadWrite &&
		currentRule.Kerberos5iReadOnly == updateRule.Kerberos5iReadOnly &&
		currentRule.Kerberos5iReadWrite == updateRule.Kerberos5iReadWrite &&
		currentRule.Kerberos5pReadOnly == updateRule.Kerberos5pReadOnly &&
		currentRule.Kerberos5pReadWrite == updateRule.Kerberos5pReadWrite &&
		currentRule.Superuser == updateRule.Superuser
}

func populateSnapshotPolicyFromParams(params *models.SnapshotPolicy) *datamodel.SnapshotPolicy {
	snapshotPolicy := &datamodel.SnapshotPolicy{
		Name:      params.Name,
		IsEnabled: params.IsEnabled,
		Schedules: []*datamodel.SnapshotPolicySchedule{},
	}

	for _, schedule := range params.Schedules {
		snapshotPolicy.Schedules = append(snapshotPolicy.Schedules, &datamodel.SnapshotPolicySchedule{
			DaysOfMonth:     schedule.Schedule.DaysOfMonth,
			DaysOfWeek:      schedule.Schedule.DaysOfWeek,
			Hours:           schedule.Schedule.Hours,
			Minutes:         schedule.Schedule.Minutes,
			Count:           schedule.Count,
			SnapmirrorLabel: schedule.SnapmirrorLabel,
		})
	}

	return snapshotPolicy
}

func isUpdateRequired(response *vsa.VolumeResponse, params *common.UpdateVolumeParams, existingVolume *datamodel.Volume) bool {
	if response.Size < params.QuotaInBytes {
		return true
	}
	if params.SnapshotPolicy != nil && params.SnapshotPolicy.Name != response.SnapshotPolicyName {
		return true
	}
	if params.SnapReserve != nil && response.SnapReserve != *params.SnapReserve {
		return true
	}
	if params.SnapshotDirectoryAccess != nil && response.SnapshotDirectoryAccessEnabled != *params.SnapshotDirectoryAccess {
		return true
	}

	if response.Size == params.QuotaInBytes && params.AutoTieringPolicy != nil {
		if params.AutoTieringPolicy.AutoTieringEnabled != existingVolume.AutoTieringEnabled ||
			(params.AutoTieringPolicy.AutoTieringEnabled && existingVolume.AutoTieringPolicy != nil && params.AutoTieringPolicy.CoolingThresholdDays != existingVolume.AutoTieringPolicy.CoolingThresholdDays) {
			return true
		}
	}

	// Add checks for other fields as and when required
	return false
}

func _isUpdateFlexCacheRequired(existingVolume *datamodel.Volume, params *common.UpdateVolumeParams) bool {
	// TODO: Refactor this and _applyFlexCacheUpdateParams to a common location.
	// feature is disabled
	if !flexCacheEnabled {
		return false
	}
	// No incoming FlexCache intent
	if params == nil || params.CacheParameters == nil || params.CacheParameters.CacheConfig == nil {
		return false
	}
	inCfg := params.CacheParameters.CacheConfig

	// Not a FlexCache volume (cannot add via update)
	if existingVolume == nil || existingVolume.CacheParameters == nil {
		return false
	}

	// Existing FlexCache params but missing config: treat as add
	if existingVolume.CacheParameters.CacheConfig == nil {
		return true
	}
	exCfg := existingVolume.CacheParameters.CacheConfig

	// Compare fields, ignoring nils in input
	changedBool := func(in, ex *bool) bool {
		if in == nil {
			return false
		}
		return ex == nil || *in != *ex
	}
	changedInt := func(in, ex *int16) bool {
		if in == nil {
			return false
		}
		return ex == nil || *in != *ex
	}

	if changedBool(inCfg.WritebackEnabled, exCfg.WritebackEnabled) ||
		changedBool(inCfg.AtimeScrubEnabled, exCfg.AtimeScrubEnabled) ||
		changedInt(inCfg.AtimeScrubDays, exCfg.AtimeScrubDays) ||
		changedBool(inCfg.CifsChangeNotifyEnabled, exCfg.CifsChangeNotifyEnabled) {
		return true
	}

	// PrePopulate diff handling
	pp := inCfg.PrePopulate
	if pp == nil {
		return false
	}
	exPP := exCfg.PrePopulate

	// Adding new PrePopulate section
	if exPP == nil {
		if pp.Recursion != nil || pp.PathList != nil || pp.ExcludePathList != nil {
			return true
		}
		return false
	}

	if changedBool(pp.Recursion, exPP.Recursion) {
		return true
	}

	sliceChanged := func(in, ex []string) bool {
		if in == nil {
			return false
		}
		if len(in) == 0 {
			return len(ex) != 0
		}

		// Set comparison (order ignored, duplicates not distinguished).
		inSet := make(map[string]struct{}, len(in))
		for _, v := range in {
			inSet[v] = struct{}{}
		}
		exSet := make(map[string]struct{}, len(ex))
		for _, v := range ex {
			exSet[v] = struct{}{}
		}

		if len(inSet) != len(exSet) {
			return true
		}
		for v := range inSet {
			if _, ok := exSet[v]; !ok {
				return true
			}
		}
		return false
	}

	return sliceChanged(pp.PathList, exPP.PathList) ||
		sliceChanged(pp.ExcludePathList, exPP.ExcludePathList)
}

func getUpdateParamsForRollback(volResponse *vsa.VolumeResponse, existingVolume *datamodel.Volume) *common.UpdateVolumeParams {
	params := &common.UpdateVolumeParams{
		// Set the necessary parameters for rolling back the volume update
		QuotaInBytes: volResponse.Size,
	}

	// Set AutoTieringPolicy if it exists
	if existingVolume.AutoTieringPolicy != nil {
		params.AutoTieringPolicy = &common.AutoTieringPolicy{
			AutoTieringEnabled:   existingVolume.AutoTieringEnabled,
			CoolingThresholdDays: existingVolume.AutoTieringPolicy.CoolingThresholdDays,
			TieringPolicy:        existingVolume.AutoTieringPolicy.TieringPolicy,
			RetrievalPolicy:      existingVolume.AutoTieringPolicy.RetrievalPolicy,
		}
	}

	if volResponse.SnapshotPolicyName != "" {
		params.SnapshotPolicy = &models.SnapshotPolicy{
			Name: volResponse.SnapshotPolicyName,
		}
	}

	if existingVolume.CacheParameters != nil {
		params.CacheParameters = convertCacheParameters(existingVolume.CacheParameters)
	}

	return params
}

func _convertCacheParameters(src *datamodel.CacheParameters) *models.CacheParameters {
	if src == nil {
		return nil
	}

	dst := &models.CacheParameters{
		PeerVolumeName:        src.PeerVolumeName,
		PeerClusterName:       src.PeerClusterName,
		PeerSvmName:           src.PeerSvmName,
		PeerIPAddresses:       src.PeerIpAddresses, // ensure field name matches models
		CacheState:            src.CacheState,
		CacheStateDetails:     src.CacheStateDetails,
		CacheStateDetailsCode: src.CacheStateDetailsCode,
	}

	if src.CacheConfig != nil {
		cc := &models.CacheConfig{
			WritebackEnabled:        src.CacheConfig.WritebackEnabled,
			AtimeScrubEnabled:       src.CacheConfig.AtimeScrubEnabled,
			AtimeScrubDays:          src.CacheConfig.AtimeScrubDays,
			CifsChangeNotifyEnabled: src.CacheConfig.CifsChangeNotifyEnabled,
		}
		if src.CacheConfig.PrePopulate != nil {
			cc.PrePopulate = &models.CachePrePopulate{
				PathList:        src.CacheConfig.PrePopulate.PathList,
				ExcludePathList: src.CacheConfig.PrePopulate.ExcludePathList,
				Recursion:       src.CacheConfig.PrePopulate.Recursion,
			}
		}
		dst.CacheConfig = cc
	}

	return dst
}

// getUpdatedExportPolicy converts models.ExportPolicy to datamodel.ExportPolicy
func getUpdatedExportPolicy(updatePolicy *models.ExportPolicy) *datamodel.ExportPolicy {
	if updatePolicy == nil {
		return nil
	}

	exportPolicy := &datamodel.ExportPolicy{
		ExportPolicyName: updatePolicy.ExportPolicyName,
		ExportRules:      make([]*datamodel.ExportRule, 0, len(updatePolicy.ExportRules)),
	}

	for _, rule := range updatePolicy.ExportRules {
		dataRule := &datamodel.ExportRule{
			AllowedClients:      rule.AllowedClients,
			AnonymousUser:       rule.AnonymousUser,
			Index:               rule.Index,
			ChownMode:           rule.ChownMode,
			AccessType:          rule.AccessType,
			CIFS:                rule.CIFS,
			NFSv3:               rule.NFSv3,
			NFSv4:               rule.NFSv4,
			S3:                  rule.S3,
			UnixReadOnly:        rule.UnixReadOnly,
			UnixReadWrite:       rule.UnixReadWrite,
			Kerberos5ReadOnly:   rule.Kerberos5ReadOnly,
			Kerberos5ReadWrite:  rule.Kerberos5ReadWrite,
			Kerberos5iReadOnly:  rule.Kerberos5iReadOnly,
			Kerberos5iReadWrite: rule.Kerberos5iReadWrite,
			Kerberos5pReadOnly:  rule.Kerberos5pReadOnly,
			Kerberos5pReadWrite: rule.Kerberos5pReadWrite,
			Superuser:           rule.Superuser,
		}
		exportPolicy.ExportRules = append(exportPolicy.ExportRules, dataRule)
	}

	return exportPolicy
}
