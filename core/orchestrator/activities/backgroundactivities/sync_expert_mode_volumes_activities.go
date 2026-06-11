package backgroundactivities

import (
	"context"
	"fmt"

	orchcommon "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/activity"
)

// SyncExpertModeVolumesActivity handles reconciliation of expert mode volumes between ONTAP and DB.
type SyncExpertModeVolumesActivity struct {
	SE database.Storage
}

// expertModeVolumeReconcileHeartbeatInterval is how many volumes to process between heartbeats
// in reconcileExpertModeVolumes. The per-pool sync activity must report progress before the
// activity heartbeat timeout when reconciling pools with many volumes.
const expertModeVolumeReconcileHeartbeatInterval = 50

// ontapModePoolFilter is the shared filter for expert mode volume sync (ONTAP API mode pools
// in any lifecycle state except terminal delete).
func ontapModePoolFilter() *dbutils.Filter {
	return dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("state", "not in", []string{
			datamodel.LifeCycleStateDeleting,
			datamodel.LifeCycleStateDeleted,
		}),
		dbutils.NewFilterCondition("api_access_mode", "=", orchcommon.ONTAPMode),
	)
}

// ListOntapModePoolsPaginated returns a page of ONTAP-mode pool identifiers eligible for
// volume sync. The parent workflow pages through these (offset/limit) until a short page
// signals the end, so no separate count query is needed.
func (a *SyncExpertModeVolumesActivity) ListOntapModePoolsPaginated(ctx context.Context, offset, limit int) ([]*database.PoolIdentifier, error) {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "Listing ONTAP mode pools paginated")

	pools, err := a.SE.ListPoolUUIDsPaginated(ctx, ontapModePoolFilter(), offset, limit)
	if err != nil {
		logger.Errorf("Failed to list ONTAP mode pools: %v", err)
		return nil, fmt.Errorf("failed to list ONTAP mode pools: %w", err)
	}
	activity.RecordHeartbeat(ctx, "ONTAP mode pools listed")
	logger.Infof("Found %d ONTAP mode pools (offset: %d, limit: %d)", len(pools), offset, limit)
	return pools, nil
}

// SyncExpertModeVolumesBatchReturnValue represents the return value for batch processing.
type SyncExpertModeVolumesBatchReturnValue struct {
	TotalProcessed       int
	Successful           int
	Failed               int
	FailedResourceNames  []string
	FailedResourceErrors []ParentChildWorkflowError
}

// SyncExpertModeVolumesForPoolActivity reconciles expert mode volumes for a single pool.
func (a *SyncExpertModeVolumesActivity) SyncExpertModeVolumesForPoolActivity(ctx context.Context, poolIdentifier *database.PoolIdentifier) error {
	if poolIdentifier == nil {
		return fmt.Errorf("SyncExpertModeVolumesForPoolActivity: pool identifier is nil")
	}
	return a.syncExpertModeVolumesForPool(ctx, poolIdentifier)
}

// syncExpertModeVolumesForPool reconciles expert mode volumes for a single pool.
func (a *SyncExpertModeVolumesActivity) syncExpertModeVolumesForPool(ctx context.Context, poolIdentifier *database.PoolIdentifier) error {
	logger := util.GetLogger(ctx)
	logger.Infof("Starting expert mode volume sync for pool %s", poolIdentifier.Name)
	safeRecordHeartbeat(ctx, fmt.Sprintf("starting sync for pool %s", poolIdentifier.Name))

	pool, err := a.SE.GetPoolByUUID(ctx, poolIdentifier.UUID)
	if err != nil {
		return fmt.Errorf("failed to get pool %s: %w", poolIdentifier.UUID, err)
	}
	safeRecordHeartbeat(ctx, fmt.Sprintf("loaded pool %s", pool.Name))

	ontapVolumes, err := a.getOntapVolumesForPool(ctx, pool)
	if err != nil {
		return fmt.Errorf("failed to get ONTAP volumes for pool %s: %w", pool.Name, err)
	}
	safeRecordHeartbeat(ctx, fmt.Sprintf("fetched %d ONTAP volumes for pool %s", len(ontapVolumes), pool.Name))

	dbVolumes, err := a.SE.ListExpertModeVolumesByPoolID(ctx, pool.ID)
	if err != nil {
		return fmt.Errorf("failed to get DB expert mode volumes for pool %s: %w", pool.Name, err)
	}
	safeRecordHeartbeat(ctx, fmt.Sprintf("loaded %d DB expert mode volumes for pool %s", len(dbVolumes), pool.Name))

	svms, err := a.SE.GetSvmsByPoolID(ctx, pool.ID)
	if err != nil {
		return fmt.Errorf("failed to get SVMs for pool %s: %w", pool.Name, err)
	}
	safeRecordHeartbeat(ctx, fmt.Sprintf("loaded %d SVMs for pool %s", len(svms), pool.Name))

	svmNameToID := buildSvmNameToIDMap(svms)

	added, updated, deleted, err := reconcileExpertModeVolumes(ctx, a.SE, pool, ontapVolumes, dbVolumes, svmNameToID)
	if err != nil {
		return err
	}

	logger.Infof("Expert mode volume sync for pool %s completed: added=%d, updated=%d, deleted=%d", poolIdentifier.Name, added, updated, deleted)
	return nil
}

func (a *SyncExpertModeVolumesActivity) getOntapVolumesForPool(ctx context.Context, pool *datamodel.Pool) ([]*vsa.Volume, error) {
	safeRecordHeartbeat(ctx, fmt.Sprintf("getting ONTAP provider for pool %s", pool.Name))
	provider, err := GetOntapRestProviderForPool(ctx, a.SE, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to get ONTAP provider: %w", err)
	}

	safeRecordHeartbeat(ctx, fmt.Sprintf("listing volumes from ONTAP for pool %s", pool.Name))
	volumes, err := provider.GetVolumes()
	if err != nil {
		return nil, fmt.Errorf("failed to get volumes from ONTAP: %w", err)
	}

	safeRecordHeartbeat(ctx, fmt.Sprintf("filtering %d ONTAP volumes for pool %s", len(volumes), pool.Name))
	return filterReconcilableVolumes(volumes), nil
}

// filterReconcilableVolumes removes infrastructure-only volumes (SVM root volumes and FlexGroup
// constituents) but keeps both rw user volumes and dp data-protection volumes, since both are
// reconciled into the expert-mode volume table.
func filterReconcilableVolumes(volumes []*vsa.Volume) []*vsa.Volume {
	var result []*vsa.Volume
	for _, vol := range volumes {
		if vol.IsSvmRoot != nil && *vol.IsSvmRoot {
			continue
		}
		if vol.Style != nil && *vol.Style == FlexGroupConstituent {
			continue
		}
		result = append(result, vol)
	}
	return result
}

func buildSvmNameToIDMap(svms []*datamodel.Svm) map[string]int64 {
	m := make(map[string]int64, len(svms))
	for _, svm := range svms {
		m[svm.Name] = svm.ID
	}
	return m
}

// reconcileExpertModeVolumes compares ONTAP volumes with DB volumes and adds missing entries,
// removes stale entries, and reconciles size drift for entries present in both. It processes
// all volumes even when individual operations fail, and returns an aggregate error so the caller
// can mark the pool as failed and surface the failure count.
func reconcileExpertModeVolumes(
	ctx context.Context,
	se database.Storage,
	pool *datamodel.Pool,
	ontapVolumes []*vsa.Volume,
	dbVolumes []*datamodel.ExpertModeVolumes,
	svmNameToID map[string]int64,
) (added int, updated int, deleted int, err error) {
	logger := util.GetLogger(ctx)

	ontapMap := make(map[string]*vsa.Volume, len(ontapVolumes))
	for _, vol := range ontapVolumes {
		ontapMap[vol.ExternalUUID] = vol
	}

	dbMap := make(map[string]*datamodel.ExpertModeVolumes, len(dbVolumes))
	for _, vol := range dbVolumes {
		if vol.ExternalUUID == "" {
			logger.Warnf(
				"Skipping expert mode volume row with missing external_uuid for pool %s: id=%d name=%q svm_id=%d state=%q",
				pool.Name, vol.ID, vol.Name, vol.SvmID, vol.State,
			)
			continue
		}
		dbMap[vol.ExternalUUID] = vol
	}

	var createErrors, updateErrors, deleteErrors int
	processed := 0

	for extUUID, ontapVol := range ontapMap {
		if processed > 0 && processed%expertModeVolumeReconcileHeartbeatInterval == 0 {
			safeRecordHeartbeat(ctx, fmt.Sprintf("reconciling ONTAP volumes for pool %s: scanned %d/%d", pool.Name, processed, len(ontapMap)))
		}
		processed++
		if dbVol, exists := dbMap[extUUID]; exists {
			if didUpdate, updateErr := reconcileExpertModeVolumeSize(ctx, se, pool, dbVol, ontapVol); updateErr != nil {
				updateErrors++
			} else if didUpdate {
				updated++
			}
			continue
		}

		svmName := ""
		if ontapVol.Svm != nil && ontapVol.Svm.Name != nil {
			svmName = *ontapVol.Svm.Name
		}
		svmID, svmFound := svmNameToID[svmName]
		if !svmFound {
			logger.Warnf("Skipping ONTAP volume %s — SVM %q not found in DB for pool %s", extUUID, svmName, pool.Name)
			continue
		}

		volName := ""
		if ontapVol.Name != nil {
			volName = *ontapVol.Name
		}
		var sizeInBytes int64
		if ontapVol.Space != nil && ontapVol.Space.Size != nil {
			sizeInBytes = *ontapVol.Space.Size
		}
		volStyle := ""
		if ontapVol.Style != nil {
			volStyle = *ontapVol.Style
		}

		newVol := &datamodel.ExpertModeVolumes{
			Name:         volName,
			ExternalUUID: extUUID,
			SizeInBytes:  sizeInBytes,
			AccountID:    pool.AccountID,
			PoolID:       pool.ID,
			SvmID:        svmID,
			State:        datamodel.LifeCycleStateAvailable,
			Style:        volStyle,
		}

		if _, createErr := se.CreateExpertModeVolume(ctx, newVol); createErr != nil {
			logger.Errorf("Failed to create expert mode volume %s (ext UUID %s) for pool %s: %v", volName, extUUID, pool.Name, createErr)
			createErrors++
			continue
		}
		added++
		logger.Infof("Added expert mode volume %s (ext UUID %s) to DB for pool %s", volName, extUUID, pool.Name)
	}

	processed = 0
	for extUUID, dbVol := range dbMap {
		if processed > 0 && processed%expertModeVolumeReconcileHeartbeatInterval == 0 {
			safeRecordHeartbeat(ctx, fmt.Sprintf("reconciling DB volumes for pool %s: scanned %d/%d", pool.Name, processed, len(dbMap)))
		}
		processed++
		if _, exists := ontapMap[extUUID]; exists {
			continue
		}

		if shouldSkipExpertModeVolumeReconcile(dbVol.State) {
			logger.Debugf("Skipping expert mode volume %s (ext UUID %s) in state %s during sync for pool %s",
				dbVol.Name, extUUID, dbVol.State, pool.Name)
			continue
		}

		if deleteErr := se.DeleteExpertModeVolume(ctx, dbVol.UUID); deleteErr != nil {
			logger.Errorf("Failed to delete expert mode volume %s (ext UUID %s) from DB for pool %s: %v", dbVol.Name, extUUID, pool.Name, deleteErr)
			deleteErrors++
			continue
		}
		deleted++
		logger.Infof("Deleted expert mode volume %s (ext UUID %s) from DB for pool %s", dbVol.Name, extUUID, pool.Name)
	}

	if createErrors > 0 || updateErrors > 0 || deleteErrors > 0 {
		return added, updated, deleted, fmt.Errorf(
			"reconciliation incomplete for pool %s: %d create errors, %d update errors, %d delete errors",
			pool.Name, createErrors, updateErrors, deleteErrors,
		)
	}

	return added, updated, deleted, nil
}

// reconcileExpertModeVolumeSize updates the DB size from ONTAP when the two disagree and the DB
// row is in a quiescent state. It is a no-op when ONTAP did not report a size, when sizes already
// match, or when the volume is in an in-flight lifecycle state that another workflow owns.
func reconcileExpertModeVolumeSize(
	ctx context.Context,
	se database.Storage,
	pool *datamodel.Pool,
	dbVol *datamodel.ExpertModeVolumes,
	ontapVol *vsa.Volume,
) (updated bool, err error) {
	logger := util.GetLogger(ctx)

	if ontapVol.Space == nil || ontapVol.Space.Size == nil {
		return false, nil
	}
	ontapSize := *ontapVol.Space.Size
	if ontapSize == dbVol.SizeInBytes {
		return false, nil
	}
	if !shouldReconcileExpertModeVolumeSize(dbVol.State) {
		logger.Debugf(
			"Skipping size reconcile for expert mode volume %s (ext UUID %s) in state %s for pool %s: ontap=%d db=%d",
			dbVol.Name, dbVol.ExternalUUID, dbVol.State, pool.Name, ontapSize, dbVol.SizeInBytes,
		)
		return false, nil
	}

	if updateErr := se.UpdateExpertModeVolumeFields(ctx, dbVol.ExternalUUID, map[string]interface{}{
		"size_in_bytes": ontapSize,
	}); updateErr != nil {
		logger.Errorf(
			"Failed to update size for expert mode volume %s (ext UUID %s) for pool %s: ontap=%d db=%d: %v",
			dbVol.Name, dbVol.ExternalUUID, pool.Name, ontapSize, dbVol.SizeInBytes, updateErr,
		)
		return false, updateErr
	}

	logger.Infof(
		"Reconciled size for expert mode volume %s (ext UUID %s) for pool %s: %d -> %d",
		dbVol.Name, dbVol.ExternalUUID, pool.Name, dbVol.SizeInBytes, ontapSize,
	)
	return true, nil
}

// shouldSkipExpertModeVolumeReconcile returns true when the volume must not be
// removed by background sync (delete in progress or terminal deleted).
func shouldSkipExpertModeVolumeReconcile(state string) bool {
	return state == datamodel.LifeCycleStateDeleting || state == datamodel.LifeCycleStateDeleted
}

// shouldReconcileExpertModeVolumeSize returns true when the DB row is in a state that is safe
// for the background reconciler to overwrite from ONTAP. In-flight states (CREATING, UPDATING,
// DELETING, DELETED) are owned by their workflows and must not be touched here.
func shouldReconcileExpertModeVolumeSize(state string) bool {
	switch state {
	case datamodel.LifeCycleStateCreating,
		datamodel.LifeCycleStateUpdating,
		datamodel.LifeCycleStateDeleting,
		datamodel.LifeCycleStateDeleted:
		return false
	}
	return true
}
