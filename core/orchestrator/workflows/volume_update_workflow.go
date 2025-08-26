package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
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

	// Avoid updating the lun if the size is not changed
	if params.QuotaInBytes > volume.SizeInBytes || (params.SnapReserve != nil && volume.VolumeAttributes != nil && *params.SnapReserve != volume.VolumeAttributes.SnapReserve) {
		updatedLun := &vsa.LunResponse{}
		err = workflow.ExecuteActivity(ctx, updateActivity.GetVolumeFromONTAP, volume, &node).Get(ctx, &volResponse)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		err = workflow.ExecuteActivity(ctx, updateActivity.UpdateLun, volume, volResponse.AvailableSpace, node).Get(ctx, &updatedLun)
		lunName := utils.GetLunName(volume.Name)
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

	if volume.DataProtection != nil && volume.DataProtection.BackupVaultID != "" {
		if runningEnv != common.LocalEnv {
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

			err = workflow.ExecuteActivity(ctx, updateActivity.UpdateBucketDetailsOfBackupVault, &volume, &bucketDetails).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
		}
	}

	if volume.DataProtection != nil && params.DataProtection != nil && !nillable.IsNilOrEmpty(params.DataProtection.BackupPolicyId) {
		// Assigning backup policy to the volume
		if runningEnv != common.LocalEnv {
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
			var vcpBackupPolicy *datamodel.BackupPolicy
			err = workflow.ExecuteActivity(ctx, updateActivity.FetchAndCreateBackupPolicyFromSDE, volume, params.Region).Get(ctx, &vcpBackupPolicy)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}

			err = workflow.ExecuteActivity(ctx, updateActivity.CreateScheduleForBackupPolicy, vcpBackupPolicy, params.BackupSchedule).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
		}
	}

	err = workflow.ExecuteActivity(ctx, updateActivity.UpdateVolumeInDB, volume, &params).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	return nil, ConvertToVSAError(err)
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

	if response.Size == params.QuotaInBytes && params.AutoTieringPolicy != nil {
		if params.AutoTieringPolicy.AutoTieringEnabled != existingVolume.AutoTieringEnabled ||
			(params.AutoTieringPolicy.AutoTieringEnabled && existingVolume.AutoTieringPolicy != nil && params.AutoTieringPolicy.CoolingThresholdDays != existingVolume.AutoTieringPolicy.CoolingThresholdDays) {
			return true
		}
	}

	// Add checks for other fields as and when required
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

	return params
}
