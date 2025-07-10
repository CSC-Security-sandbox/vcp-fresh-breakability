package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
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
func UpdateVolumeWorkflow(ctx workflow.Context, params *common.UpdateVolumeParams, volume *datamodel.Volume) (gcpgenserver.V1betaDescribeVolumeRes, error) {
	log := util.GetLogger(ctx)
	volumeWf := new(volumeUpdateWorkflow)
	err := volumeWf.Setup(ctx, volume)
	if err != nil {
		return nil, err
	}
	volumeWf.Status = WorkflowStatusRunning
	err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			volumeWf.Status = WorkflowStatusFailed
			err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		} else {
			volumeWf.Status = WorkflowStatusCompleted
			err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
		}
	}()

	_, err = volumeWf.Run(ctx, params, volume)
	if err != nil {
		log.Errorf("Volume update workflow completed with error: %v", err)
		return nil, err
	}
	log.Infof("Volume update workflow completed successfully")
	return nil, err
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

func (wf *volumeUpdateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	log := util.GetLogger(ctx)
	params := args[0].(*common.UpdateVolumeParams)
	volume := args[1].(*datamodel.Volume)
	updateActivity := &activities.VolumeUpdateActivity{}
	deleteActivity := &activities.VolumeDeleteActivity{}

	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	options := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, options)

	rollbackManager := common.NewRollbackManager()
	defer func() {
		if err != nil {
			err2 := workflow.ExecuteActivity(ctx, activities.VolumeCreateActivity.UpdateVolumeStateInDB, volume.UUID, models.LifeCycleStateError, models.LifeCycleStateUpdateErrorDetails).Get(ctx, nil)
			if err2 != nil {
				log.Errorf("Failed to update volume state in DB to error: %v", err2)
			}
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, volume.Pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, err
	}

	node := common.CreateNodeForProvider(common.NodeProviderInput{Nodes: dbNodes, Username: volume.Pool.Username, Password: volume.Pool.Password, SecretID: volume.Pool.SecretID})

	// Update the snapshot policy if it is provided in the params
	if params.SnapshotPolicy != nil && params.SnapshotPolicy.Name != "" {
		updatingPolicy := populateSnapshotPolicyFromParams(params.SnapshotPolicy)

		// If the volume does not have an existing snapshot policy, we need to create a new one using the provided snapshot policy
		if volume.SnapshotPolicy == nil || volume.SnapshotPolicy.Name == "" {
			createActivity := &activities.VolumeCreateActivity{}
			volume.SnapshotPolicy = updatingPolicy

			err = workflow.ExecuteActivity(ctx, createActivity.CreateSnapshotPolicyInONTAP, &volume, &node).Get(ctx, nil)
			if err != nil {
				return nil, err
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
				return nil, err
			}
			rollbackManager.AddActivity(updateActivity.UpdateSnapshotPolicyInOntap, node, updatingPolicy, &volume.SnapshotPolicy)
			volume.SnapshotPolicy = updatingPolicy
		}
	}

	volResponse := &vsa.VolumeResponse{}
	err = workflow.ExecuteActivity(ctx, updateActivity.GetVolumeFromONTAP, volume, &node).Get(ctx, &volResponse)
	if err != nil {
		return nil, err
	}

	if isUpdateRequired(volResponse, params, volume) {
		err = workflow.ExecuteActivity(ctx, updateActivity.UpdateVolumeInONTAP, volume, params, node).Get(ctx, nil)
		if err != nil {
			return nil, err
		}
		rollbackManager.AddActivity(updateActivity.UpdateVolumeInONTAP, volume, getUpdateParamsForRollback(volResponse, volume), node)
	}

	// Avoid updating the lun if the size is not changed
	if params.QuotaInBytes > volume.SizeInBytes {
		err = workflow.ExecuteActivity(ctx, updateActivity.UpdateLun, volume, params.QuotaInBytes, node).Get(ctx, nil)
		if err != nil {
			return nil, err
		}
		// No rollback for LUN because we cannot decrease the size of a LUN in ONTAP.
	}

	if params.BlockProperties != nil {
		volumeAttachedHG := utils.GetHgUUIDs(volume.VolumeAttributes.BlockProperties.HostGroupDetails)
		if !utils.IsSliceEqual(params.BlockProperties.HostGroupUUIDs, volumeAttachedHG) {
			toCreate, toDelete := activities.HostGroupsUpdateDiffForVolume(utils.GetHgUUIDs(volume.VolumeAttributes.BlockProperties.HostGroupDetails), params.BlockProperties.HostGroupUUIDs)

			if len(toCreate) > 0 {
				err = workflow.ExecuteActivity(ctx, updateActivity.EnsureHostGroupsExistsAndMapDisk, &volume, toCreate, &node).Get(ctx, nil)
				if err != nil {
					return nil, err
				}
			}

			// Ensure the lun iGroup maps to delete  created
			if len(toDelete) > 0 {
				err = workflow.ExecuteActivity(ctx, updateActivity.UnmapHostGroupFromDisk, &volume, toDelete, &node).Get(ctx, nil)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	if volume.DataProtection != nil && volume.DataProtection.BackupVaultID != "" {
		if runningEnv != "local" {
			var token string
			err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetAuthJWTToken, params.AccountName).Get(ctx, &token)
			if err != nil {
				log.Errorf("Failed to get token for account %s: %v", params.AccountName, err)
				return nil, err
			}
			ctx = workflow.WithValue(ctx, middleware.AuthorizationToken, token)
		}
		
		tenancyDetails := &common.TenancyInfo{}
		err = workflow.ExecuteActivity(ctx, updateActivity.FindTenancyDetails, volume.VolumeAttributes.VendorSubnetID, volume.Account.Name, &params.Region).Get(ctx, &tenancyDetails)
		if err != nil {
			return nil, err
		}

		err = workflow.ExecuteActivity(ctx, updateActivity.CheckBackupVaultExistInVCP, &volume, &params.Region).Get(ctx, nil)
		if err != nil {
			return nil, err
		}

		bucketDetails := &common.BucketDetails{}
		err = workflow.ExecuteActivity(ctx, updateActivity.CheckBucketResourceName, &volume).Get(ctx, &bucketDetails)
		if err != nil {
			return nil, err
		}
		if bucketDetails.BucketName == "" && bucketDetails.ServiceAccountName == "" && bucketDetails.TenantProjectNumber == "" {
			resourceName := &common.ResourceNames{}
			err = workflow.ExecuteActivity(ctx, updateActivity.GenerateResourceNamesForBackupVault, &volume, &tenancyDetails, params.Region).Get(ctx, &resourceName)
			if err != nil {
				return nil, err
			}

			err = workflow.ExecuteActivity(ctx, updateActivity.CreateBucketForBackupVault, &resourceName, &tenancyDetails, params.Region).Get(ctx, &bucketDetails)
			if err != nil {
				return nil, err
			}

			err = workflow.ExecuteActivity(ctx, updateActivity.UpdateBucketDetailsOfBackupVault, &volume, &bucketDetails).Get(ctx, nil)
			if err != nil {
				return nil, err
			}
		}
	}

	err = workflow.ExecuteActivity(ctx, updateActivity.UpdateVolumeInDB, volume, &params).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
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

	if response.Size == params.QuotaInBytes && params.TieringPolicy != nil {
		if params.TieringPolicy.CoolAccess != existingVolume.CoolAccess ||
			(params.TieringPolicy.CoolAccess && params.TieringPolicy.CoolnessPeriod != existingVolume.CoolnessPeriod) {
			return true
		}
	}

	// Add checks for other fields as and when required
	return false
}

func getUpdateParamsForRollback(volResponse *vsa.VolumeResponse, existingVolume *datamodel.Volume) *common.UpdateVolumeParams {
	return &common.UpdateVolumeParams{
		// Set the necessary parameters for rolling back the volume update
		QuotaInBytes: volResponse.Size,
		TieringPolicy: &common.TieringPolicy{
			CoolAccess:                existingVolume.CoolAccess,
			CoolnessPeriod:            existingVolume.CoolnessPeriod,
			CoolAccessTieringPolicy:   existingVolume.CoolAccessTieringPolicy,
			CoolAccessRetrievalPolicy: existingVolume.CoolAccessRetrievalPolicy,
		},
	}
}
