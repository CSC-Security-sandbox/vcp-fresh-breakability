package workflows

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
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
	convertCacheParameters               = _convertCacheParameters
	isUpdateFlexCacheRequired            = _isUpdateFlexCacheRequired
	isUpdateFlexCachePrepopulateRequired = _isUpdateFlexCachePrepopulateRequired
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
	if err = volumeWf.EnsureJobState(ctx, models.JobsStateNEW); err != nil {
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
	// Use LV-specific timeout for large capacity volumes
	startToCloseTimeout := getVolumeStartToCloseTimeout(volume)
	options := workflow.ActivityOptions{
		StartToCloseTimeout: time.Duration(startToCloseTimeout) * time.Second,
		HeartbeatTimeout:    time.Duration(volumeHeartbeatTimeoutSec) * time.Second,
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

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
		Nodes:            dbNodes,
		DeploymentName:   volume.Pool.DeploymentName,
		OntapCredentials: volume.Pool.PoolCredentials,
	})

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

		var ontapAsyncResponse *vsa.OntapAsyncResponse
		err = workflow.ExecuteActivity(ctx, flexCacheUpdateActivity.UpdateFlexCacheVolumeInONTAP, volume,
			params, node).Get(ctx, &ontapAsyncResponse)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		if err = WaitForONTAPJob(ctx, ontapAsyncResponse, node, 30*time.Minute); err != nil {
			return nil, ConvertToVSAError(fmt.Errorf("failed to update FlexCache volume: %w", err))
		}
	}

	// Handle prepopulate separately if requested
	if isUpdateFlexCachePrepopulateRequired(volume, params) {
		err := workflow.ExecuteActivity(ctx, flexCacheUpdateActivity.UpdatePrepopulateState,
			volume.UUID, cvpModels.FlexCacheConfigV1betaCachePrePopulateStateCACHEPREPOPULATESTATEUNSPECIFIED).Get(ctx, nil)
		if err != nil {
			log.Errorf("Failed to update prepopulate state to UNSPECIFIED: %v", err)
		}

		// Start the ONTAP prepopulate job
		var ontapJobUUID string
		err = workflow.ExecuteActivity(ctx, flexCacheUpdateActivity.StartFlexCachePrepopulate,
			volume, params, node).Get(ctx, &ontapJobUUID)
		if err != nil {
			log.Errorf("Failed to start prepopulate in ONTAP: %v", err)
			stateErr := workflow.ExecuteActivity(ctx, flexCacheUpdateActivity.UpdatePrepopulateState,
				volume.UUID, cvpModels.FlexCacheConfigV1betaCachePrePopulateStateERROR).Get(ctx, nil)
			if stateErr != nil {
				log.Errorf("Failed to update prepopulate state to ERROR after ONTAP failure: %v", stateErr)
			}

			log.Warnf("Prepopulate failed to start but continuing with volume update - prepopulate is best-effort")
		} else if ontapJobUUID == "" {
			err = workflow.ExecuteActivity(ctx, flexCacheUpdateActivity.UpdatePrepopulateState,
				volume.UUID, cvpModels.FlexCacheConfigV1betaCachePrePopulateStateCOMPLETE).Get(ctx, nil)
			if err != nil {
				log.Errorf("Failed to update prepopulate state to COMPLETE: %v", err)
			}
		} else {
			// Create a Job record for tracking with ONTAP UUID in job attributes
			var createdJobUUID string
			err = workflow.ExecuteActivity(ctx, flexCacheUpdateActivity.CreatePrepopulateJob,
				volume, ontapJobUUID).Get(ctx, &createdJobUUID)
			if err != nil {
				log.Errorf("Failed to create job record: %v", err)
				stateErr := workflow.ExecuteActivity(ctx, flexCacheUpdateActivity.UpdatePrepopulateState,
					volume.UUID, cvpModels.FlexCacheConfigV1betaCachePrePopulateStateERROR).Get(ctx, nil)
				if stateErr != nil {
					log.Errorf("Failed to update prepopulate state to ERROR after job creation failure: %v", stateErr)
				}

				log.Warnf("ONTAP prepopulate job %s started but cannot be tracked - job will run to completion in ONTAP for volume %s",
					ontapJobUUID, volume.UUID)
			}
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
					updateOrAddBlockDevice(params, blockDevice)
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
		if enableSmb && volume.VolumeAttributes != nil && utils.IsSMBProtocols(volume.VolumeAttributes.Protocols) && len(params.SMBShareSettings) != 0 {
			err = workflow.ExecuteActivity(ctx, updateActivity.UpdateSMBShareSettings, &volume, &params, &node).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
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

		var backupVault *datamodel.BackupVault
		err = workflow.ExecuteActivity(ctx, updateActivity.CheckBackupVaultExistInVCP, &volume, &params.Region).Get(ctx, &backupVault)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		backupRegion := params.Region
		if backupVault.BackupVaultType == activities.CrossRegionBackupType && *backupVault.BackupRegionName != "" {
			backupRegion = *backupVault.BackupRegionName
		}

		tenancyDetails := &common.TenancyInfo{}
		err = workflow.ExecuteActivity(ctx, updateActivity.FindTenancyDetails, volume.VolumeAttributes.VendorSubnetID, volume.Account.Name, backupRegion).Get(ctx, &tenancyDetails)
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

			var kmsGrant *string
			if params.DataProtection != nil && !nillable.IsNilOrEmpty(params.DataProtection.KmsGrant) {
				kmsGrant = params.DataProtection.KmsGrant
			}

			err = workflow.ExecuteActivity(ctx, updateActivity.CreateBucketForBackupVault, &resourceName, &tenancyDetails, backupRegion, kmsGrant).Get(ctx, &bucketDetails)
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

			var RemoteBV *datamodel.BackupVault
			volumeActivity := &activities.VolumeCreateActivity{}
			err = workflow.ExecuteActivity(ctx, volumeActivity.CheckOrCreateRemoteBackupVaultInVCP, &volume, backupVault, &bucketDetails).Get(ctx, &RemoteBV)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}

			err = workflow.ExecuteActivity(ctx, volumeActivity.UpdateRemoteBackupVaultWithBucketDetails, &volume, backupVault, RemoteBV, &bucketDetails).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
		}

		// TODO: Optimize this to avoid running for each volume call.
		// This is currently unoptimized and runs for every volume operation.
		// Consider optimizing to run once per pool or caching the results.
		if backupVault.BackupVaultType == activities.CrossRegionBackupType && backupVault.BackupRegionName != nil && *backupVault.BackupRegionName != "" {
			volumeCreateActivity := &activities.VolumeCreateActivity{}
			err = workflow.ExecuteActivity(ctx, volumeCreateActivity.SetupCrossRegionBackupPermissionsActivity, backupVault, &volume.Pool, &bucketDetails).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}

			// Wait for service account to be ready
			err = workflowSleep(ctx, time.Second*90)
			if err != nil {
				log.Errorf("Failed to sleep after cross-region backup permissions are created: %v", err)
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

	// If the request is to update individual qos for a volume
	if params.ThroughputMibps != nil || params.Iops != nil {
		if !volume.VolumePerformanceGroupID.Valid || volume.VolumePerformanceGroup == nil {
			return nil, ConvertToVSAError(fmt.Errorf("volume %s has no VolumePerformanceGroupID", volume.UUID))
		}
		// Determine the new values (use current if param is nil)
		newThroughput := volume.VolumePerformanceGroup.ThroughputMibps
		if params.ThroughputMibps != nil {
			newThroughput = *params.ThroughputMibps
		}
		newIops := volume.VolumePerformanceGroup.Iops
		if params.Iops != nil {
			newIops = *params.Iops
		}

		// If the request is to update individual qos for a volume and it is already assigned to its autogenerated VPG, then we just need to update the existing qosPolicyGroup's parameters.
		if volume.VolumePerformanceGroup.IsAutoGen {
			// Validate that OntapQosPolicyID exists
			if volume.VolumePerformanceGroup.OntapQosPolicyID == "" {
				return nil, ConvertToVSAError(fmt.Errorf("volume %s has autogenerated VPG but missing OntapQosPolicyID", volume.UUID))
			}

			// Add rollback activity
			currentThroughput := volume.VolumePerformanceGroup.ThroughputMibps
			currentIops := volume.VolumePerformanceGroup.Iops
			rollbackManager.AddActivity(updateActivity.UpdateQoSPolicyGroupForVolume,
				volume, &currentThroughput, &currentIops, &node)

			// Update the QoS policy group in ONTAP
			err = workflow.ExecuteActivity(ctx, updateActivity.UpdateQoSPolicyGroupForVolume,
				volume, newThroughput, newIops, &node).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}

			// Update the VPG in the database with new values
			volume.VolumePerformanceGroup.ThroughputMibps = newThroughput
			volume.VolumePerformanceGroup.Iops = newIops
			err = workflow.ExecuteActivity(ctx, updateActivity.UpdateVolumePerformanceGroupInDB,
				volume.VolumePerformanceGroup).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
		} else {
			// If the request is to update individual qos for a volume and it is not assigned to an autogenerated VPG, then we need to remove the current qosPolicyGroup and VPG assignment and create a new qosPolicyGroup.

			// Store old VPG info for rollback (create a copy)
			oldVPG := &datamodel.VolumePerformanceGroup{
				BaseModel:        volume.VolumePerformanceGroup.BaseModel,
				Name:             volume.VolumePerformanceGroup.Name,
				PoolID:           volume.VolumePerformanceGroup.PoolID,
				IsShared:         volume.VolumePerformanceGroup.IsShared,
				IsAutoGen:        volume.VolumePerformanceGroup.IsAutoGen,
				ThroughputMibps:  volume.VolumePerformanceGroup.ThroughputMibps,
				Iops:             volume.VolumePerformanceGroup.Iops,
				OntapQosPolicyID: volume.VolumePerformanceGroup.OntapQosPolicyID,
			}

			// Remove the volume from the old qosPolicyGroup
			err = workflow.ExecuteActivity(ctx, updateActivity.UnassignQoSPolicyFromVolume,
				volume, &node).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}

			// Create a new qosPolicyGroup with params.ThroughputMibps and params.Iops using autogenerated naming
			var newQosPolicy *vsa.QoSGroupPolicyResponse
			err = workflow.ExecuteActivity(ctx, updateActivity.CreateAutoGeneratedQoSPolicyGroupForVolume,
				volume, newThroughput, newIops, &node).Get(ctx, &newQosPolicy)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}

			// Assign the volume to the new qosPolicyGroup
			err = workflow.ExecuteActivity(ctx, updateActivity.AssignQoSPolicyToVolume,
				volume, newQosPolicy.Name, &node).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}

			// Update the VPG in the database to point at the new qosPolicyGroup
			// Mark as autogenerated since we're creating a new autogenerated policy
			volume.VolumePerformanceGroup.OntapQosPolicyID = newQosPolicy.UUID
			volume.VolumePerformanceGroup.ThroughputMibps = newThroughput
			volume.VolumePerformanceGroup.Iops = newIops
			volume.VolumePerformanceGroup.IsAutoGen = true

			// Add rollback activities to restore old state
			rollbackManager.AddActivity(updateActivity.UpdateVolumePerformanceGroupInDB, oldVPG)
			if oldVPG.OntapQosPolicyID != "" {
				rollbackManager.AddActivity(updateActivity.AssignQoSPolicyToVolume, volume, oldVPG.OntapQosPolicyID, &node)
			}

			// Update VPG in database
			err = workflow.ExecuteActivity(ctx, updateActivity.UpdateVolumePerformanceGroupInDB,
				volume.VolumePerformanceGroup).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
		}
	}

	// If current VPG doesnt match the provided VPGId, clean up the old VPG
	if volume.VolumePerformanceGroupID.Valid &&
		volume.VolumePerformanceGroup != nil &&
		params.VolumePerformanceGroupId != nil &&
		*params.VolumePerformanceGroupId != volume.VolumePerformanceGroup.UUID {
		oldVPG := volume.VolumePerformanceGroup
		oldQosPolicyID := oldVPG.OntapQosPolicyID

		// Remove the volume from the old qosPolicyGroup
		err = workflow.ExecuteActivity(ctx, updateActivity.UnassignQoSPolicyFromVolume,
			volume, &node).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		// Find the VPG by the provided VPGId
		var newVPG *datamodel.VolumePerformanceGroup
		err = workflow.ExecuteActivity(ctx, updateActivity.GetVolumePerformanceGroupByUUID,
			*params.VolumePerformanceGroupId).Get(ctx, &newVPG)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		// Validate that the new VPG belongs to the same pool as the volume
		if newVPG.PoolID != volume.PoolID {
			return nil, ConvertToVSAError(fmt.Errorf("VolumePerformanceGroup %s does not belong to the same pool as volume %s", newVPG.UUID, volume.UUID))
		}

		// Get the new VPG's qosPolicyGroup
		if newVPG.OntapQosPolicyID == "" {
			return nil, ConvertToVSAError(fmt.Errorf("VolumePerformanceGroup %s has no OntapQosPolicyID", newVPG.UUID))
		}

		var newQosPolicy *vsa.QoSGroupPolicyResponse
		err = workflow.ExecuteActivity(ctx, updateActivity.FindQoSGroupPolicyForVolume,
			newVPG.OntapQosPolicyID, volume.Svm.Name, &node).Get(ctx, &newQosPolicy)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		// Assign the volume to the VPG's qosPolicyGroup
		err = workflow.ExecuteActivity(ctx, updateActivity.AssignQoSPolicyToVolume,
			volume, newQosPolicy.Name, &node).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		// Update the volume in the db to point at the new VPG
		err = workflow.ExecuteActivity(ctx, updateActivity.UpdateVolumePerformanceGroupInDBForVolume,
			volume.UUID, newVPG).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		// Update the volume object in memory to reflect the new VPG
		volume.VolumePerformanceGroup = newVPG
		volume.VolumePerformanceGroupID = sql.NullInt64{Int64: newVPG.ID, Valid: true}

		// Add rollback activities to restore old state
		rollbackManager.AddActivity(updateActivity.UpdateVolumePerformanceGroupInDBForVolume, volume.UUID, oldVPG)
		if oldQosPolicyID != "" {
			rollbackManager.AddActivity(updateActivity.AssignQoSPolicyToVolume, volume, oldQosPolicyID, &node)
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

// updateOrAddBlockDevice updates existing BlockDevice or adds new one to params
func updateOrAddBlockDevice(
	params *common.UpdateVolumeParams,
	blockDevice *common.BlockDevice,
) {
	// Nil checks for safety
	if params == nil || blockDevice == nil {
		return
	}

	// Check if BlockDevice with same name already exists
	for i, existingDevice := range params.BlockDevices {
		if existingDevice.Name == blockDevice.Name {
			// Replace existing BlockDevice
			paramsHostGroups := params.BlockDevices[i].HostGroups
			params.BlockDevices[i] = blockDevice
			params.BlockDevices[i].HostGroups = paramsHostGroups
			return
		}
	}
	// BlockDevice not found, append new one
	params.BlockDevices = append(params.BlockDevices, blockDevice)
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
		currentRule.Superuser == updateRule.Superuser &&
		ptrBoolEqual(currentRule.AllSquash, updateRule.AllSquash) &&
		ptrInt64Equal(currentRule.AnonUid, updateRule.AnonUid)
}

// ptrBoolEqual returns true if both pointers are nil or both point to the same bool value.
func ptrBoolEqual(currentRule, update *bool) bool {
	if currentRule == nil && update == nil {
		return true
	}
	if currentRule == nil || update == nil {
		return false
	}
	return *currentRule == *update
}

// ptrInt64Equal returns true if both pointers are nil or both point to the same int64 value.
func ptrInt64Equal(currentValue, updateValue *int64) bool {
	if currentValue == nil && updateValue == nil {
		return true
	}
	if currentValue == nil || updateValue == nil {
		return false
	}
	return *currentValue == *updateValue
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

	if params.AutoTieringPolicy != nil {
		if params.AutoTieringPolicy.AutoTieringEnabled != existingVolume.AutoTieringEnabled ||
			(params.AutoTieringPolicy.AutoTieringEnabled && existingVolume.AutoTieringPolicy != nil && (params.AutoTieringPolicy.CoolingThresholdDays != existingVolume.AutoTieringPolicy.CoolingThresholdDays ||
				params.AutoTieringPolicy.TieringPolicy != existingVolume.AutoTieringPolicy.TieringPolicy)) {
			return true
		}
	}

	if params.FileProperties != nil && params.FileProperties.UnixPermissions != "" && existingVolume.VolumeAttributes != nil &&
		existingVolume.VolumeAttributes.FileProperties != nil {
		return true
	}

	// Add checks for other fields as and when required
	return false
}

func _isUpdateFlexCacheRequired(existingVolume *datamodel.Volume, params *common.UpdateVolumeParams) bool {
	// TODO: Refactor this and _applyFlexCacheUpdateParams to a common location.
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

	return false
}

func _isUpdateFlexCachePrepopulateRequired(existingVolume *datamodel.Volume, params *common.UpdateVolumeParams) bool {
	if params == nil || params.CacheParameters == nil || params.CacheParameters.CacheConfig == nil {
		return false
	}
	if existingVolume == nil || existingVolume.CacheParameters == nil {
		return false
	}

	inCfg := params.CacheParameters.CacheConfig
	exCfg := existingVolume.CacheParameters.CacheConfig

	if inCfg.CachePrePopulate != nil {
		if exCfg == nil || exCfg.CachePrePopulate == nil {
			return true
		}

		changedBool := func(in, ex *bool) bool {
			if in == nil {
				return false
			}
			return ex == nil || *in != *ex
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

		return sliceChanged(inCfg.CachePrePopulate.PathList, exCfg.CachePrePopulate.PathList) ||
			sliceChanged(inCfg.CachePrePopulate.ExcludePathList, exCfg.CachePrePopulate.ExcludePathList) ||
			changedBool(inCfg.CachePrePopulate.Recursion, exCfg.CachePrePopulate.Recursion)
	}
	return false
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
		if src.CacheConfig.CachePrePopulate != nil {
			cc.CachePrePopulate = &models.CachePrePopulate{
				PathList:        src.CacheConfig.CachePrePopulate.PathList,
				ExcludePathList: src.CacheConfig.CachePrePopulate.ExcludePathList,
				Recursion:       src.CacheConfig.CachePrePopulate.Recursion,
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
			AllSquash:           rule.AllSquash,
			AnonUid:             rule.AnonUid,
		}
		exportPolicy.ExportRules = append(exportPolicy.ExportRules, dataRule)
	}

	return exportPolicy
}
