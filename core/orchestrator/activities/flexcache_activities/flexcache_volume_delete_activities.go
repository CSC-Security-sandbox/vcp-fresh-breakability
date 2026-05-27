package flexcache_activities

import (
	"context"
	stderrors "errors"
	"strings"

	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/flexcache"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
)

var fetchTemporalClientForFlexCacheDelete = _fetchTemporalClientForFlexCacheDelete

func _fetchTemporalClientForFlexCacheDelete(ctx context.Context) client.Client {
	return activity.GetClient(ctx)
}

type FlexCacheVolumeDeleteActivity struct {
	SE database.Storage
}

// UnmountVolumeInOntapActivity deletes a FlexCache volume in ONTAP
func (a FlexCacheVolumeDeleteActivity) UnmountVolumeInOntapActivity(ctx context.Context, result *flexcache.DeleteFlexCacheResult) (*flexcache.DeleteFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	volume := result.DBVolume
	node := result.Node

	provider, err := hyperscalerGetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if volume.VolumeAttributes.ExternalUUID == "" {
		logger.Debug("no external UUID found for the volume, skipping unmount")
		return result, nil // No volume in ONTAP to unmount
	}

	response, err := provider.UnmountVolume(volume.VolumeAttributes.ExternalUUID)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrUnmountingFlexCacheVolume, err)
	}

	result.UnmountJobResponse = response
	logger.Debugf("FlexCache volume unmount job for volume with UUID %s initiated successfully", volume.UUID)

	return result, nil
}

// DeleteFlexCacheVolumeInOntapActivity deletes a FlexCache volume in ONTAP
func (a FlexCacheVolumeDeleteActivity) DeleteFlexCacheVolumeInOntapActivity(ctx context.Context, result *flexcache.DeleteFlexCacheResult) (*flexcache.DeleteFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	volume := result.DBVolume
	node := result.Node

	provider, err := hyperscalerGetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if volume.VolumeAttributes.ExternalUUID == "" {
		logger.Debug("no external UUID found for the volume, skipping delete")
		return result, nil // No volume in ONTAP to delete
	}

	response, err := provider.DeleteFlexCacheVolume(volume.VolumeAttributes.ExternalUUID, volume.Name)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDeletingFlexCacheVolume, err)
	}

	result.DeleteJobResponse = response
	logger.Debugf("FlexCache volume delete job for volume with UUID %s initiated successfully", volume.UUID)

	return result, nil
}

func (a FlexCacheVolumeDeleteActivity) DeleteSVMPeeringInOntapActivity(ctx context.Context, result *flexcache.DeleteFlexCacheResult) (*flexcache.DeleteFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	volume := result.DBVolume
	node := result.Node

	provider, err := hyperscalerGetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	svmPeer, err := provider.GetSVMPeer(&volume.Svm.Name, &volume.CacheParameters.PeerSvmName)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			logger.Debugf("SVM peer not found for svm=%s peer_svm=%s, skipping delete", volume.Svm.Name, volume.CacheParameters.PeerSvmName)
			return result, nil
		}
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	svmPeerUUID := svmPeer.UUID
	err = provider.DeleteSVMPeer(svmPeerUUID, false)
	if err != nil {
		// Ignore FlexCache in-use condition (idempotent handling)
		if msg := err.Error(); strings.Contains(msg, "The peer relationship is in use by FlexCache") ||
			strings.Contains(msg, "Relationship is in use by SnapMirror in local cluster") {
			logger.Infof("Skipping SVM peer delete for %s: still in use (%s); leaving svm_peer_uuid unchanged", svmPeerUUID, msg)
			return result, nil
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDeletingSVMPeer, err)
	}

	logger.Debugf("SVM peering with UUID %s deleted successfully", svmPeerUUID)

	return result, nil
}

func (a FlexCacheVolumeDeleteActivity) DeleteClusterPeerInOntapActivity(ctx context.Context, result *flexcache.DeleteFlexCacheResult) (*flexcache.DeleteFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	clusterPeeringRow := result.ClusterPeeringRow
	node := result.Node

	provider, err := hyperscalerGetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if clusterPeeringRow.OntapPeerUUID != "" {
		err = provider.DeleteClusterPeer(clusterPeeringRow.OntapPeerUUID)
		if err != nil {
			if customerrors.IsNotFoundErr(err) {
				logger.Debugf("Cluster peer not found for UUID %s, skipping delete", clusterPeeringRow.OntapPeerUUID)
				return result, nil
			}
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDeletingClusterPeer, err)
		}
		logger.Debugf("Cluster peering with UUID %s deleted successfully", clusterPeeringRow.OntapPeerUUID)
	}

	return result, nil
}

func (a FlexCacheVolumeDeleteActivity) DeleteClusterPeeringRowInDBActivity(ctx context.Context, result *flexcache.DeleteFlexCacheResult) (*flexcache.DeleteFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	clusterPeeringRow := result.ClusterPeeringRow

	if err := a.SE.DeleteClusterPeeringRow(ctx, clusterPeeringRow); err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataDeleteError, err)
	}

	logger.Debugf("Cluster peering row with ID %d deleted successfully", clusterPeeringRow.ID)
	return result, nil
}

func (a FlexCacheVolumeDeleteActivity) GetClusterPeeringFromDBActivity(ctx context.Context,
	result *flexcache.DeleteFlexCacheResult) (*flexcache.DeleteFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	volume := result.DBVolume

	if volume.ClusterPeerID.Valid && volume.ClusterPeerID.Int64 != 0 {
		existingPeer, err := a.SE.GetClusterPeeringRowByID(ctx, volume.ClusterPeerID.Int64)
		if err != nil {
			if customerrors.IsNotFoundErr(err) {
				logger.Debugf("Cluster peering row not found for cluster_peer_id=%d", volume.ClusterPeerID.Int64)
				return result, nil
			}
			logger.Errorf("Failed to get cluster peering row from database: %v", err)
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}
		result.ClusterPeeringRow = existingPeer
		logger.Debugf("Found existing cluster peering row in database: %s", existingPeer.UUID)
		return result, nil
	}

	logger.Debugf("cluster_peer_id not set on volume; skipping cluster peering row lookup")
	return result, nil
}

func (a FlexCacheVolumeDeleteActivity) GetFlexCacheAndReplicationCountsOnClusterPeeringActivity(ctx context.Context, result *flexcache.DeleteFlexCacheResult) (*flexcache.DeleteFlexCacheResult, error) {
	clusterPeeringRow := result.ClusterPeeringRow

	if clusterPeeringRow == nil {
		result.VolumeReplicationCountOnClusterPeering = 0
		result.FlexCacheVolumeCountOnClusterPeering = 0
		return result, nil
	}

	replicationCount, err := a.SE.GetVolumeReplicationCountByClusterPeerID(ctx, clusterPeeringRow.ID)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	flexCacheCount, err := a.SE.GetFlexCacheVolumeCountByClusterPeerID(ctx, clusterPeeringRow.ID)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	result.VolumeReplicationCountOnClusterPeering = replicationCount
	result.FlexCacheVolumeCountOnClusterPeering = flexCacheCount
	return result, nil
}

func (a FlexCacheVolumeDeleteActivity) UpdateClusterPeeringRowStateDeletedInDBActivity(ctx context.Context,
	result *flexcache.DeleteFlexCacheResult) (*flexcache.DeleteFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	clusterPeeringRow := result.ClusterPeeringRow
	se := a.SE
	clusterPeeringRow.State = coremodels.CvpClusterPeeringStatusDELETED
	if err := se.UpdateClusterPeeringRow(ctx, clusterPeeringRow); err != nil {
		logger.Errorf("Failed to update cluster peering row in DB: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Cluster peering row with UUID %s updated to state %s", clusterPeeringRow.UUID, clusterPeeringRow.State)
	return result, nil
}

// CancelPrepopulateJobsForVolume cancels all active prepopulate jobs for a volume.
// This is called during volume deletion to prevent orphaned prepopulate jobs.
// Active jobs (NEW/PROCESSING) are moved to ERROR state, which effectively cancels them
// since jobs in a terminal state (DONE/ERROR) are no longer processed by the background sync workflow.
func (a FlexCacheVolumeDeleteActivity) CancelPrepopulateJobsForVolume(ctx context.Context, volumeUUID string) error {
	logger := utilGetLogger(ctx)
	logger.Infof("Cancelling prepopulate jobs for volume %s", volumeUUID)

	err := a.SE.CancelPrepopulateJobsForVolume(ctx, volumeUUID)
	if err != nil {
		logger.Errorf("Failed to cancel prepopulate jobs for volume %s: %v", volumeUUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Successfully cancelled prepopulate jobs for volume %s", volumeUUID)
	return nil
}

// CancelFlexCacheCreateWorkflowIfPreparingActivity uses DBVolume from result; if it is a FlexCache volume in
// PREPARING state, cancels the FlexCache create workflow and clears running jobs for the resource.
// Called at the end of DeleteFlexCacheVolumeWorkflow (before DeleteVolume) so cluster peering teardown completes first.
func (a FlexCacheVolumeDeleteActivity) CancelFlexCacheCreateWorkflowIfPreparingActivity(ctx context.Context, result *flexcache.DeleteFlexCacheResult) error {
	logger := utilGetLogger(ctx)
	dbVolume := result.DBVolume

	if dbVolume.CacheParameters == nil {
		logger.Debugf("skip cancel create workflow for volume %s: not a FlexCache volume", dbVolume.Name)
		return nil
	}
	if dbVolume.State != coremodels.LifeCycleStatePreparing {
		logger.Debugf("cannot cancel create workflow for volume %s as it is not in PREPARING state", dbVolume.Name)
		return nil
	}

	createJob, err := a.SE.GetJobByResourceUUID(ctx, dbVolume.UUID, string(coremodels.JobTypeFlexCacheCreateVolume))
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			logger.Debugf("no create job found for volume %s", dbVolume.Name)
			return nil
		}
		logger.Errorf("error retrieving create job for volume %s: %v", dbVolume.Name, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	temporalClient := fetchTemporalClientForFlexCacheDelete(ctx)
	err = temporalClient.CancelWorkflow(ctx, createJob.WorkflowID, "")
	workflowMissing := false
	if err != nil {
		var notFoundErr *serviceerror.NotFound
		if stderrors.As(err, &notFoundErr) {
			workflowMissing = true
			logger.Debugf("create workflow not found in Temporal (workflowID=%s volume=%s); clearing running jobs for resource",
				createJob.WorkflowID, dbVolume.Name)
		} else {
			logger.Errorf("failed to cancel create workflow %s for volume %s: %v", createJob.WorkflowID, dbVolume.Name, err)
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
	}

	err = a.SE.CancelRunningJobsForResource(ctx, dbVolume.UUID)
	if err != nil {
		logger.Errorf("failed to cancel running jobs for volume %s: %v", dbVolume.Name, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if workflowMissing {
		logger.Infof("cleared running jobs for volume %s (create workflow %s was not running in Temporal)",
			dbVolume.Name, createJob.WorkflowID)
	} else {
		logger.Infof("successfully cancelled create workflow %s for volume %s", createJob.WorkflowID, dbVolume.Name)
	}
	return nil
}
