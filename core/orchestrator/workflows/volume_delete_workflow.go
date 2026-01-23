package workflows

import (
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	volumeDeleteJobsRetryMaxAttempts = env.GetInt("REPLICATION_JOBS_RETRY_MAX_ATTEMPTS", 10)
	enableSmb                        = env.GetBool("ENABLE_SMB", false)
	enableQuotaRule                  = env.GetBool("ENABLE_QUOTA_RULE", false)
)

type volumeDeleteWorkflow struct {
	// add fields needed for volume workflow
	BaseWorkflow
}

// Enforcing the WorkflowInterface on volumeDeleteWorkflow
var _ WorkflowInterface = &volumeDeleteWorkflow{}

// DeleteVolumeWorkflow Delete Volume Workflow process volume related requests from a customer.
func DeleteVolumeWorkflow(ctx workflow.Context, volume *datamodel.Volume) error {
	log := util.GetLogger(ctx)
	volumeWf := new(volumeDeleteWorkflow)
	err := volumeWf.Setup(ctx, volume)
	if err != nil {
		log.Errorf("Volume delete workflow setup executed with error: %v", err)
		return err
	}
	if err = volumeWf.EnsureJobState(ctx, models.JobsStateNEW); err != nil {
		return err
	}

	volumeWf.Status = WorkflowStatusRunning
	err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Processing for DeleteVolumeWorkflow: %v", err)
		return err
	}

	_, customErr := volumeWf.Run(ctx, volume)
	if customErr != nil {
		log.Errorf("DeleteVolumeWorkflow completed with error: %v", customErr)
		volumeWf.Status = WorkflowStatusFailed
		err2 := volumeWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if err2 != nil {
			log.Errorf("Failed to update job status to Done with err for DeleteVolumeWorkflow: %v", err)
			return err2
		}
		return customErr
	}

	volumeWf.Status = WorkflowStatusCompleted
	err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Done for DeleteVolumeWorkflow: %v", err)
	}
	return err
}

func (wf *volumeDeleteWorkflow) Setup(ctx workflow.Context, input interface{}) error {
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

// shouldUpdateVolumeStateToError checks if the error should trigger updating the volume state to error
// Some errors are legitimate business logic errors that should not mark the volume as having a deletion error
func shouldUpdateVolumeStateToError(err error) bool {
	// Check for specific VCP errors that should not update volume state to error
	convertedError := ConvertToVSAError(err)
	var customError *vsaerrors.CustomError
	if vsaerrors.As(convertedError, &customError) {
		// ErrDeleteVolumeWhenInSplitState (7005) - volume has clones/replication
		if customError.TrackingID == vsaerrors.ErrDeleteVolumeWhenInSplitState {
			return false
		}
	}

	// For all other errors, update the volume state to error
	return true
}

// SmbTeardownWorkflow consolidates all SMB teardown activities into a single workflow.
// It determines the SMB teardown context, deletes CIFS server and DNS records if unused,
// and unsets SVM Active Directory if needed.
func SmbTeardownWorkflow(ctx workflow.Context, volume *datamodel.Volume, node *models.Node) error {
	deleteActivity := &activities.VolumeDeleteActivity{}

	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return ConvertToVSAError(err)
	}
	options := workflow.ActivityOptions{
		StartToCloseTimeout: time.Duration(volumeStartToCloseTimeoutSec) * time.Second,
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

	var smbTeardownCtx *activities.SmbTeardownContext
	err = workflow.ExecuteActivity(ctx, deleteActivity.DetermineSmbTeardownContext, &volume, &node).Get(ctx, &smbTeardownCtx)
	if err != nil {
		return ConvertToVSAError(err)
	}
	if smbTeardownCtx == nil {
		smbTeardownCtx = &activities.SmbTeardownContext{}
	}

	err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteCifsServerIfUnused, smbTeardownCtx, &node).Get(ctx, nil)
	if err != nil {
		return ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteDnsRecordIfUnused, smbTeardownCtx, &node).Get(ctx, nil)
	if err != nil {
		return ConvertToVSAError(err)
	}

	if smbTeardownCtx.ShouldDelete {
		var dbSvm *datamodel.Svm
		err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetSVM, smbTeardownCtx.PoolID).Get(ctx, &dbSvm)
		if err != nil {
			return ConvertToVSAError(err)
		}
		if dbSvm != nil {
			err = workflow.ExecuteActivity(ctx, activities.CommonActivities.UnsetSvmActiveDirectory, dbSvm).Get(ctx, nil)
			if err != nil {
				return ConvertToVSAError(err)
			}
		}
	}

	return nil
}

func (wf *volumeDeleteWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	log := util.GetLogger(ctx)
	volume := args[0].(*datamodel.Volume)
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
	ao1 := options
	ao1.RetryPolicy.MaximumAttempts = int32(volumeDeleteJobsRetryMaxAttempts)
	ao1.RetryPolicy.NonRetryableErrorTypes = append(ao1.RetryPolicy.NonRetryableErrorTypes, vsaerrors.DeleteVolumeInONTAPError)
	ctx1 := workflow.WithActivityOptions(ctx, ao1)

	// Handle cancellation only if volume is in CREATING state
	cancellationActivity := &activities.CancellationActivity{}
	commonActivity := &activities.CommonActivities{}
	poolActivity := &activities.PoolActivity{}
	ackTimeout, forceTimeout := common.GetCancellationTimeouts("VOLUME")
	// Determine the correct create job type based on volume type
	createJobType := models.JobTypeCreateVolume
	if volume.LargeVolumeAttributes != nil && volume.LargeVolumeAttributes.LargeCapacity {
		createJobType = models.JobTypeCreateLargeVolume
	}
	if cancelErr := common.HandleCancellationForCreatingResource(ctx, wf.Logger,
		common.HandleCancellationForCreatingResourceParams{
			ResourceUUID:               volume.UUID,
			ResourceState:              volume.State,
			CreateJobType:              createJobType,
			SignalName:                 CancelVolumeSignalName,
			CancellationAckTimeout:     ackTimeout,
			ForceTerminationAckTimeout: forceTimeout,
		},
		poolActivity.GetCreateJobByResourceUUID,
		cancellationActivity,
		commonActivity,
	); cancelErr != nil {
		wf.Logger.Warnf("Error handling cancellation: %v, proceeding with deletion", cancelErr)
	}

	rollbackManager := common.NewRollbackManager()
	defer func() {
		if err != nil {
			if shouldUpdateVolumeStateToError(err) {
				err2 := workflow.ExecuteActivity(ctx, activities.VolumeCreateActivity.UpdateVolumeStateInDB, volume.UUID, models.LifeCycleStateError, models.LifeCycleStateDeletionErrorDetails).Get(ctx, nil)
				if err2 != nil {
					log.Errorf("Failed to update volume state in DB to error: %v", err2)
				}
			} else {
				// Updating volume state to previous state before deletion was initiated
				err2 := workflow.ExecuteActivity(ctx, activities.VolumeCreateActivity.UpdateVolumeStateInDB, volume.UUID, volume.State, volume.StateDetails).Get(ctx, nil)
				if err2 != nil {
					log.Errorf("Failed to restore volume state to previous state: %v", err2)
				}
			}
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	// For volumes in CREATING state, check for the existence of partially created resources before accessing them
	hasExternalUUID := volume.VolumeAttributes != nil && volume.VolumeAttributes.ExternalUUID != ""
	hasFileProperties := volume.VolumeAttributes != nil && volume.VolumeAttributes.FileProperties != nil
	hasBlockDevices := volume.VolumeAttributes != nil && volume.VolumeAttributes.BlockDevices != nil && len(*volume.VolumeAttributes.BlockDevices) > 0
	hasBlockProperties := volume.VolumeAttributes != nil && volume.VolumeAttributes.BlockProperties != nil && len(volume.VolumeAttributes.BlockProperties.HostGroupDetails) > 0

	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &volume.Pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
		Nodes:            dbNodes,
		DeploymentName:   volume.Pool.DeploymentName,
		OntapCredentials: volume.Pool.PoolCredentials,
	})

	// Only perform ONTAP operations if volume was at least partially created
	if hasExternalUUID {
		var ontapAsyncResponse *vsa.OntapAsyncResponse
		err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteSnapmirrorInONTAP, &volume, &node).Get(ctx, &ontapAsyncResponse)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		err = WaitForONTAPJob(ctx, ontapAsyncResponse, node, time.Minute*10)
		if err != nil {
			return nil, ConvertToVSAError(fmt.Errorf("failed to delete snapmirror in ontap: %w", err))
		}

		// DeleteVolume polls the job internally, so we just wait for the activity to complete
		err = workflow.ExecuteActivity(ctx1, deleteActivity.DeleteVolumeInONTAP, volume.VolumeAttributes.ExternalUUID, volume.Name, node).Get(ctx1, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		if hasFileProperties && volume.VolumeAttributes.FileProperties.ExportPolicy != nil &&
			len(volume.VolumeAttributes.FileProperties.ExportPolicy.ExportRules) > 0 {
			err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteExportPolicy, &volume, &node).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
		}

		if hasBlockDevices {
			err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteIgroups, volume, node).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
		} else if hasBlockProperties {
			err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteIgroupsFromBlockProperties, volume, node).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
		}

		SnapshotPolicyName := getSnapshotPolicyName(volume)
		err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteSnapshotPolicyInONTAP, SnapshotPolicyName, &node).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteVolumeAssociatedSnapshots, volume.ID).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Only perform LDAP and SMB cleanup if volume was at least partially created
	if hasExternalUUID {
		if enableLdap && volume.Pool.PoolAttributes != nil && volume.Pool.PoolAttributes.LdapEnabled {
			var isLastFilesVolume bool
			err = workflow.ExecuteActivity(ctx, deleteActivity.DetermineIfVolumeIsLastFilesVolume, volume, node).Get(ctx, &isLastFilesVolume)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
			if isLastFilesVolume {
				err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteLDAPConfiguration, volume, node).Get(ctx, nil)
				if err != nil {
					return nil, ConvertToVSAError(err)
				}
			}
		}

		if enableSmb {
			err = workflow.ExecuteChildWorkflow(ctx, SmbTeardownWorkflow, volume, node).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
		}
	}

	if enableQuotaRule && volume.VolumeAttributes.FileProperties != nil {
		err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteAssociatedQuotaRules, volume.ID).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	err = workflow.ExecuteActivity(ctx, deleteActivity.DeleteVolume, &volume).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	return nil, ConvertToVSAError(err)
}
