package activities

import (
	"context"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/activity"
	"gorm.io/gorm"
)

var (
	volumeSyncChunkSize         = env.GetInt("VOLUME_SYNC_CHUNK_SIZE", 200)
	GetOntapRestProviderForPool = _getOntapRestProviderForPool
	enableCloneInfoRefresh      = env.GetBool("ENABLE_CLONE_INFO_REFRESH", false)
)

type VolumeRefreshActivity struct {
	SE database.Storage
}

type PoolDBVolumesMap struct {
	DBVolumesByExternalUUID map[string]*datamodel.Volume
}

func (a *VolumeRefreshActivity) GetDBVolumesForPool(ctx context.Context, pool *datamodel.Pool) (*PoolDBVolumesMap, error) {
	se := a.SE
	logger := util.GetLogger(ctx)
	dbVolumes, err := se.GetVolumesByPoolID(ctx, pool.ID)
	if err != nil {
		logger.Errorf(fmt.Sprintf("Failed to get volumes from database for pool %s: %v", pool.UUID, err))
		return nil, err
	}

	dbVolumeMap := make(map[string]*datamodel.Volume, len(dbVolumes))
	for _, v := range dbVolumes {
		dbVolumeMap[v.VolumeAttributes.ExternalUUID] = v
	}

	return &PoolDBVolumesMap{
		DBVolumesByExternalUUID: dbVolumeMap,
	}, nil
}

func (a *VolumeRefreshActivity) GetOntapVolumes(ctx context.Context, pool *datamodel.Pool) (*GetOntapVolumesReturnValue, error) {
	logger := util.GetLogger(ctx)
	se := a.SE
	activity.RecordHeartbeat(ctx, "Starting GetOntapVolumes activity")

	provider, err := GetOntapRestProviderForPool(ctx, se, pool)
	if err != nil || provider == nil {
		errMsg := fmt.Sprintf("Failed to get ONTAP rest provider for pool %v: %v", pool.UUID, err)
		logger.Errorf(errMsg)
		return nil, err
	}

	ontapVolumes, err := provider.GetVolumes()
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get ONTAP volumes for the pool: %s, %v", pool.UUID, err)
		logger.Errorf(errMsg)
		return nil, err
	}
	logger.Debugf("Successfully Fetched %d volumes from ONTAP for pool %s", len(ontapVolumes), pool.UUID)

	ontapVolumeMap := getFilteredOntapVolumesMapByUUID(ontapVolumes)

	activity.RecordHeartbeat(ctx, "Finished GetOntapVolumes activity")
	return &GetOntapVolumesReturnValue{
		OntapVolumeMap: ontapVolumeMap,
	}, nil
}

func getFilteredOntapVolumesMapByUUID(volumes []*vsa.Volume) map[string]*vsa.Volume {
	volumeMap := make(map[string]*vsa.Volume, len(volumes))
	for _, volume := range volumes {
		if volume == nil || volume.IsSvmRoot == nil || *volume.IsSvmRoot {
			continue
		}
		if volume.UUID == nil || volume.Svm == nil || volume.Svm.Name == nil || volume.Name == nil {
			continue
		}
		volumeMap[*volume.UUID] = volume
	}
	return volumeMap
}

type GetOntapVolumesReturnValue struct {
	OntapVolumeMap map[string]*vsa.Volume
}

func (a *VolumeRefreshActivity) SyncUpdatedVolumesToDatabase(ctx context.Context, dbVols map[string]*datamodel.Volume) error {
	return _syncUpdatedVolumesToDatabase(ctx, a.SE, dbVols)
}

// ProcessVolumePoolMappingInput represents input for pool mapping processing
type ProcessVolumePoolMappingInput struct {
	Volumes []*datamodel.Volume
}

// ProcessVolumePoolMappingResult represents the result of pool mapping processing
type ProcessVolumePoolMappingResult struct {
	PoolByUUID map[string]*datamodel.Pool
	PoolUUIDs  []string
}

// ProcessVolumePoolMapping builds pool UUID mappings from the provided volumes
// This activity extracts and organizes pool information for efficient processing
func (a *VolumeRefreshActivity) ProcessVolumePoolMapping(ctx context.Context, input *ProcessVolumePoolMappingInput) (*ProcessVolumePoolMappingResult, error) {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "Starting ProcessVolumePoolMapping activity")

	if input == nil || len(input.Volumes) == 0 {
		return &ProcessVolumePoolMappingResult{
			PoolByUUID: make(map[string]*datamodel.Pool),
			PoolUUIDs:  []string{},
		}, nil
	}

	// Build pool mappings with validation
	poolByUUID := make(map[string]*datamodel.Pool)
	for _, vol := range input.Volumes {
		if vol == nil {
			logger.Warn("Encountered nil volume in input, skipping")
			continue
		}

		if vol.Pool == nil {
			logger.Warnf("Volume %s has no associated pool, skipping", vol.UUID)
			continue
		}

		if vol.Pool.UUID == "" {
			logger.Warnf("Volume %s has pool with empty UUID, skipping", vol.UUID)
			continue
		}

		poolByUUID[vol.Pool.UUID] = vol.Pool
	}

	// Extract pool UUIDs for ordered processing
	poolUUIDs := make([]string, 0, len(poolByUUID))
	for poolUUID := range poolByUUID {
		poolUUIDs = append(poolUUIDs, poolUUID)
	}

	logger.Infof("Processed %d unique pools from %d volumes", len(poolByUUID), len(input.Volumes))

	activity.RecordHeartbeat(ctx, "Finished ProcessVolumePoolMapping activity")

	return &ProcessVolumePoolMappingResult{
		PoolByUUID: poolByUUID,
		PoolUUIDs:  poolUUIDs,
	}, nil
}

// ProcessOntapVolumeMatchingInput represents input for ONTAP volume matching
type ProcessOntapVolumeMatchingInput struct {
	DbVolumes           []*datamodel.Volume
	OntapVolumesResults map[string]*GetOntapVolumesReturnValue
}

// ProcessOntapVolumeMatchingResult represents the result of ONTAP volume matching
type ProcessOntapVolumeMatchingResult struct {
	UpdatedVolumeByUUID    map[string]*datamodel.Volume
	OntapVolResponse       map[string]*vsa.VolumeResponse
	VolumesNotFoundInONTAP []*datamodel.Volume
	VolumesNotCloneInONTAP []*datamodel.Volume // Volumes that are clones in DB but are regular volumes in ONTAP
	MatchedCount           int
	NotFoundCount          int
}

// ProcessOntapVolumeMatching matches database volumes with ONTAP volumes and prepares updates
// This activity handles the complex logic of correlating volumes and extracting update data
func (a *VolumeRefreshActivity) ProcessOntapVolumeMatching(ctx context.Context, input *ProcessOntapVolumeMatchingInput) (*ProcessOntapVolumeMatchingResult, error) {
	logger := util.GetLogger(ctx)

	activity.RecordHeartbeat(ctx, "Starting ProcessOntapVolumeMatching activity")

	if input == nil {
		return nil, fmt.Errorf("ProcessOntapVolumeMatching input cannot be nil")
	}

	result := &ProcessOntapVolumeMatchingResult{
		UpdatedVolumeByUUID:    make(map[string]*datamodel.Volume),
		OntapVolResponse:       make(map[string]*vsa.VolumeResponse),
		VolumesNotFoundInONTAP: make([]*datamodel.Volume, 0),
		VolumesNotCloneInONTAP: make([]*datamodel.Volume, 0),
	}

	// Process each database volume
	for _, dbVolume := range input.DbVolumes {
		if err := a.processIndividualVolume(ctx, dbVolume, input.OntapVolumesResults, result); err != nil {
			logger.Errorf("Failed to process volume %s: %v", dbVolume.UUID, err)
			return nil, fmt.Errorf("failed to process volume %s: %w", dbVolume.UUID, err)
		}
	}

	result.MatchedCount = len(result.UpdatedVolumeByUUID)
	result.NotFoundCount = len(result.VolumesNotFoundInONTAP)

	logger.Infof("Volume matching completed: %d matched, %d not found in ONTAP",
		result.MatchedCount, result.NotFoundCount)

	// Log volumes that are clones in DB but not clones in ONTAP
	if len(result.VolumesNotCloneInONTAP) > 0 {
		logger.Warnf("%d volumes are clones in database but are regular volumes in ONTAP (not clones): %v",
			len(result.VolumesNotCloneInONTAP),
			getVolumeUUIDs(result.VolumesNotCloneInONTAP))
	}

	activity.RecordHeartbeat(ctx, "Finished ProcessOntapVolumeMatching activity")
	return result, nil
}

// processIndividualVolume handles the matching logic for a single volume
func (a *VolumeRefreshActivity) processIndividualVolume(
	ctx context.Context,
	dbVolume *datamodel.Volume,
	ontapVolumesResults map[string]*GetOntapVolumesReturnValue,
	result *ProcessOntapVolumeMatchingResult,
) error {
	logger := util.GetLogger(ctx)

	// Validate volume prerequisites
	if dbVolume.Pool == nil {
		logger.Warnf("Volume %s has no associated pool, skipping", dbVolume.UUID)
		return nil
	}

	if dbVolume.VolumeAttributes == nil {
		logger.Warnf("Volume %s has no volume attributes, skipping", dbVolume.UUID)
		return nil
	}

	if dbVolume.VolumeAttributes.ExternalUUID == "" {
		logger.Warnf("Volume %s has no external UUID, skipping", dbVolume.UUID)
		return nil
	}

	// Find ONTAP volumes for this volume's pool
	ontapVolumes, poolExists := ontapVolumesResults[dbVolume.Pool.UUID]
	if !poolExists {
		logger.Warnf("No ONTAP volumes found for pool %s, volume %s cannot be updated",
			dbVolume.Pool.UUID, dbVolume.UUID)
		result.VolumesNotFoundInONTAP = append(result.VolumesNotFoundInONTAP, dbVolume)
		return nil
	}

	// Look for this volume in ONTAP results using ExternalUUID
	ontapVolume, volumeExists := ontapVolumes.OntapVolumeMap[dbVolume.VolumeAttributes.ExternalUUID]
	if !volumeExists {
		logger.Warnf("Volume %s (ExternalUUID: %s) not found in ONTAP for pool %s",
			dbVolume.UUID, dbVolume.VolumeAttributes.ExternalUUID, dbVolume.Pool.Name)
		result.VolumesNotFoundInONTAP = append(result.VolumesNotFoundInONTAP, dbVolume)
		return nil
	}

	// Validate ONTAP volume has required fields
	if err := a.validateOntapVolume(ontapVolume); err != nil {
		logger.Warnf("ONTAP volume validation failed for %s: %v", dbVolume.UUID, err)
		result.VolumesNotFoundInONTAP = append(result.VolumesNotFoundInONTAP, dbVolume)
		return nil
	}

	// If volume is a clone in database, validate ONTAP has complete clone info
	var skipCloneInfoUpdate bool
	if enableCloneInfoRefresh && dbVolume.ClonesSharedBytes > 0 && ontapVolume != nil {
		if ontapVolume.Clone == nil {
			logger.Warnf("Volume %s is a clone in database (ClonesSharedBytes: %d) but is not a clone in ONTAP (missing clone info), treating as regular volume",
				dbVolume.UUID, dbVolume.ClonesSharedBytes)
			result.VolumesNotCloneInONTAP = append(result.VolumesNotCloneInONTAP, dbVolume)
			skipCloneInfoUpdate = true
		} else if ontapVolume.Clone != nil && (ontapVolume.Clone.ParentVolume == nil || ontapVolume.Clone.ParentVolume.Name == nil) {
			logger.Warnf("Volume %s is a clone in database but ONTAP is missing parent volume name, treating as regular volume",
				dbVolume.UUID)
			result.VolumesNotCloneInONTAP = append(result.VolumesNotCloneInONTAP, dbVolume)
			skipCloneInfoUpdate = true
		} else if ontapVolume.Clone != nil && (ontapVolume.Clone.ParentSnapshot == nil || ontapVolume.Clone.ParentSnapshot.Name == nil) {
			logger.Warnf("Volume %s is a clone in database but ONTAP is missing parent snapshot name, treating as regular volume",
				dbVolume.UUID)
			result.VolumesNotCloneInONTAP = append(result.VolumesNotCloneInONTAP, dbVolume)
			skipCloneInfoUpdate = true
		}
	}

	// Extract new values from ONTAP
	newUsedBytes := uint64(*ontapVolume.Space.LogicalSpace.Used)

	// Check if there are any differences between database and ONTAP values
	hasChanges := false
	if dbVolume.UsedBytes != newUsedBytes {
		hasChanges = true
		logger.Debugf("Volume %s UsedBytes changed: %d -> %d", dbVolume.UUID, dbVolume.UsedBytes, newUsedBytes)
	}

	// Check if volume is a clone and clone details are missing
	needsCloneUpdate := false
	var parentVolumeUUID, parentSnapshotUUID string

	// Only process clone info if volume is a clone in database
	if enableCloneInfoRefresh && dbVolume.ClonesSharedBytes > 0 && !skipCloneInfoUpdate {
		if ontapVolume != nil && ontapVolume.Clone != nil &&
			ontapVolume.Clone.ParentVolume != nil && ontapVolume.Clone.ParentVolume.Name != nil &&
			ontapVolume.Clone.ParentSnapshot != nil && ontapVolume.Clone.ParentSnapshot.Name != nil {
			parentVolumeName := *ontapVolume.Clone.ParentVolume.Name
			parentSnapshotName := *ontapVolume.Clone.ParentSnapshot.Name

			// Look up parent volume UUID from database using parent volume name
			parentVolume, err := a.SE.GetVolumeByNameAndAccountID(ctx, parentVolumeName, dbVolume.AccountID)
			if err != nil {
				logger.Warnf("Failed to get parent volume %s for clone volume %s: %v", parentVolumeName, dbVolume.UUID, err)
				// Continue without clone info update if parent volume not found
			} else {
				parentVolumeUUID = parentVolume.UUID

				// Look up parent snapshot UUID from database using parent snapshot name and parent volume ID
				parentSnapshot, err := a.SE.GetSnapshotByNameAndVolumeId(ctx, parentSnapshotName, dbVolume.AccountID, parentVolume.ID)
				if err != nil {
					logger.Warnf("Failed to get parent snapshot %s for clone volume %s (parent volume: %s): %v",
						parentSnapshotName, dbVolume.UUID, parentVolumeName, err)
					// Continue without clone info update if parent snapshot not found
				} else {
					parentSnapshotUUID = parentSnapshot.UUID

					// Check if clone details are missing in database
					if dbVolume.VolumeAttributes.CloneParentInfo == nil ||
						dbVolume.VolumeAttributes.CloneParentInfo.ParentVolumeUUID == "" ||
						dbVolume.VolumeAttributes.CloneParentInfo.ParentSnapshotUUID == "" {
						needsCloneUpdate = true
						hasChanges = true
						logger.Debugf("Volume %s is a clone with missing clone details. Parent Volume: %s (UUID: %s), Parent Snapshot: %s (UUID: %s)",
							dbVolume.UUID, parentVolumeName, parentVolumeUUID, parentSnapshotName, parentSnapshotUUID)
					} else {
						// Check if the UUIDs don't match (update if different)
						if dbVolume.VolumeAttributes.CloneParentInfo.ParentVolumeUUID != parentVolumeUUID ||
							dbVolume.VolumeAttributes.CloneParentInfo.ParentSnapshotUUID != parentSnapshotUUID {
							needsCloneUpdate = true
							hasChanges = true
							logger.Infof("Volume %s clone details mismatch. Updating Parent Volume UUID: %s -> %s, Parent Snapshot UUID: %s -> %s",
								dbVolume.UUID,
								dbVolume.VolumeAttributes.CloneParentInfo.ParentVolumeUUID, parentVolumeUUID,
								dbVolume.VolumeAttributes.CloneParentInfo.ParentSnapshotUUID, parentSnapshotUUID)
						}
					}
				}
			}
		}
	}

	// Only add to update list if there are actual changes
	if hasChanges {
		// Create updated volume with ONTAP data
		updatedVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: dbVolume.UUID,
				ID:   dbVolume.ID,
			},
			UsedBytes: newUsedBytes,
		}

		// Add clone parent info if needed
		if needsCloneUpdate {
			updatedVolume.VolumeAttributes = &datamodel.VolumeAttributes{
				CloneParentInfo: &datamodel.CloneParentInfo{
					ParentVolumeUUID:   parentVolumeUUID,
					ParentSnapshotUUID: parentSnapshotUUID,
				},
			}
		}

		// Create volume response for additional processing
		volResponse := &vsa.VolumeResponse{
			UsedBytes: *ontapVolume.Space.LogicalSpace.Used,
		}

		// Store results
		result.UpdatedVolumeByUUID[dbVolume.UUID] = updatedVolume
		result.OntapVolResponse[dbVolume.UUID] = volResponse

		if needsCloneUpdate {
			logger.Debugf("Successfully matched volume %s with ONTAP data (UsedBytes: %d, Clone Parent Volume: %s, Clone Parent Snapshot: %s)",
				dbVolume.UUID, updatedVolume.UsedBytes, parentVolumeUUID, parentSnapshotUUID)
		} else {
			logger.Debugf("Successfully matched volume %s with ONTAP data (UsedBytes: %d)",
				dbVolume.UUID, updatedVolume.UsedBytes)
		}
	} else {
		logger.Debugf("Volume %s has no changes, skipping database update", dbVolume.UUID)
	}

	return nil
}

// validateOntapVolume validates that the ONTAP volume has all required fields
func (a *VolumeRefreshActivity) validateOntapVolume(ontapVolume *vsa.Volume) error {
	if ontapVolume == nil {
		return fmt.Errorf("ONTAP volume is nil")
	}

	if ontapVolume.Space == nil {
		return fmt.Errorf("ONTAP volume space information is nil")
	}

	if ontapVolume.Space.LogicalSpace == nil {
		return fmt.Errorf("ONTAP volume logical space information is nil")
	}

	if ontapVolume.Space.LogicalSpace.Used == nil {
		return fmt.Errorf("ONTAP volume used space information is nil")
	}

	return nil
}

func _syncUpdatedVolumesToDatabase(ctx context.Context, se database.Storage, dbVols map[string]*datamodel.Volume) error {
	log := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "Starting SyncUpdatedVolumesToDatabase activity")

	if len(dbVols) == 0 {
		log.Debugf("No volumes to update in the database")
		return nil
	}

	// Separate volumes into two groups:
	// 1. Volumes that only need used_bytes updates (can use efficient batch update)
	// 2. Volumes that need volume_attributes updates (clone info) - these will get BOTH
	//    used_bytes AND volume_attributes updated via individual UpdateVolumeFields calls
	volumesForBatchUpdate := make([]datamodel.VolumeFieldUpdate, 0, len(dbVols))
	volumesForCloneUpdate := make([]*datamodel.Volume, 0)

	for _, vol := range dbVols {
		// Check if volume needs clone info update
		if vol.VolumeAttributes != nil && vol.VolumeAttributes.CloneParentInfo != nil {
			// Volume needs clone info update - will get both used_bytes and volume_attributes updated
			volumesForCloneUpdate = append(volumesForCloneUpdate, vol)
		} else {
			// Only used_bytes update needed - can use efficient batch update
			fieldUpdate := datamodel.VolumeFieldUpdate{
				UUID: vol.UUID,
				Fields: map[string]interface{}{
					"used_bytes": vol.UsedBytes,
				},
			}
			volumesForBatchUpdate = append(volumesForBatchUpdate, fieldUpdate)
		}
	}

	// Batch update volumes that only need used_bytes
	if len(volumesForBatchUpdate) > 0 {
		for i := 0; i < len(volumesForBatchUpdate); i += volumeSyncChunkSize {
			end := i + volumeSyncChunkSize
			if end > len(volumesForBatchUpdate) {
				end = len(volumesForBatchUpdate)
			}

			fieldUpdateChunk := volumesForBatchUpdate[i:end]

			if err := se.BatchUpdateVolumeFields(ctx, fieldUpdateChunk); err != nil {
				return err
			}
		}
		log.Debugf("Batch updated used_bytes for %d volumes", len(volumesForBatchUpdate))
	}

	// Individual updates for volumes that need clone info
	for _, vol := range volumesForCloneUpdate {
		// Fetch existing volume to get current volume_attributes
		dbVolume, err := se.GetVolume(ctx, vol.UUID)
		if err != nil {
			log.Errorf("Failed to get volume %s for clone info update: %v", vol.UUID, err)
			continue
		}

		// Merge clone parent info into existing volume_attributes
		var updatedAttributes *datamodel.VolumeAttributes
		if dbVolume.VolumeAttributes != nil {
			// Deep copy existing attributes
			updatedAttributes = &datamodel.VolumeAttributes{
				CreationToken:      dbVolume.VolumeAttributes.CreationToken,
				Protocols:          dbVolume.VolumeAttributes.Protocols,
				VendorSubnetID:     dbVolume.VolumeAttributes.VendorSubnetID,
				ExternalUUID:       dbVolume.VolumeAttributes.ExternalUUID,
				BlockProperties:    dbVolume.VolumeAttributes.BlockProperties,
				BlockDevices:       dbVolume.VolumeAttributes.BlockDevices,
				FileProperties:     dbVolume.VolumeAttributes.FileProperties,
				IsDataProtection:   dbVolume.VolumeAttributes.IsDataProtection,
				Mounted:            dbVolume.VolumeAttributes.Mounted,
				SnapReserve:        dbVolume.VolumeAttributes.SnapReserve,
				SnapshotDirectory:  dbVolume.VolumeAttributes.SnapshotDirectory,
				Labels:             dbVolume.VolumeAttributes.Labels,
				RestoredBackupID:   dbVolume.VolumeAttributes.RestoredBackupID,
				RestoredBackupPath: dbVolume.VolumeAttributes.RestoredBackupPath,
				SecurityStyle:      dbVolume.VolumeAttributes.SecurityStyle,
				CloneParentInfo:    vol.VolumeAttributes.CloneParentInfo, // Use new clone info
			}
		} else {
			// No existing attributes, create new with clone info
			updatedAttributes = &datamodel.VolumeAttributes{
				CloneParentInfo: vol.VolumeAttributes.CloneParentInfo,
			}
		}

		// Update volume with merged attributes and used_bytes
		updateFields := map[string]interface{}{
			"used_bytes":        vol.UsedBytes,
			"volume_attributes": updatedAttributes,
		}

		if err := se.UpdateVolumeFields(ctx, vol.UUID, updateFields); err != nil {
			log.Errorf("Failed to update volume %s with clone info: %v", vol.UUID, err)
			return err
		}
		log.Debugf("Updated volume %s with clone parent info (Parent Volume: %s, Parent Snapshot: %s)",
			vol.UUID,
			vol.VolumeAttributes.CloneParentInfo.ParentVolumeUUID,
			vol.VolumeAttributes.CloneParentInfo.ParentSnapshotUUID)
	}

	if len(volumesForCloneUpdate) > 0 {
		log.Debugf("Updated clone info for %d volumes", len(volumesForCloneUpdate))
	}

	activity.RecordHeartbeat(ctx, "Finished SyncUpdatedVolumesToDatabase activity")
	log.Debugf("Successfully updated %d volumes using field updates", len(dbVols))
	return nil
}

func _getOntapRestProviderForPool(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		if vsaerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, err)
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	if len(nodes) == 0 {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, fmt.Errorf("no nodes found for pool %s", pool.UUID))
	}

	if pool.PoolCredentials == nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, fmt.Errorf("pool credentials not found for pool %s", pool.UUID))
	}

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
		Nodes:            nodes,
		DeploymentName:   pool.DeploymentName,
		OntapCredentials: pool.PoolCredentials,
	})

	// Node now contains CA fields from PoolCredentials, so we can use GetProviderByNode directly
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return provider, nil
}

// UpdateAccountVolumeRefreshTimestampInput represents input for updating account metadata
type UpdateAccountVolumeRefreshTimestampInput struct {
	AccountUUID string
	CompletedAt time.Time
}

// UpdateAccountVolumeRefreshTimestamp updates the VolumeRefreshWorkflowLastCompletionAt timestamp in AccountMetadata
// This activity should be called at the end of VolumeRefreshWorkflow to record the completion time
func (a *VolumeRefreshActivity) UpdateAccountVolumeRefreshTimestamp(ctx context.Context, input *UpdateAccountVolumeRefreshTimestampInput) error {
	logger := util.GetLogger(ctx)
	se := a.SE
	activity.RecordHeartbeat(ctx, "Starting UpdateAccountVolumeRefreshTimestamp activity")

	if input == nil {
		return fmt.Errorf("UpdateAccountVolumeRefreshTimestamp input cannot be nil")
	}

	if input.AccountUUID == "" {
		return fmt.Errorf("account UUID cannot be empty")
	}

	logger.Infof("Updating VolumeRefreshWorkflow completion timestamp for account %s to %v",
		input.AccountUUID, input.CompletedAt)

	err := se.UpdateAccountVolumeRefreshTimestamp(ctx, input.AccountUUID, input.CompletedAt)
	if err != nil {
		logger.Errorf("Failed to update account volume refresh timestamp for account %s: %v",
			input.AccountUUID, err)
		return fmt.Errorf("failed to update account volume refresh timestamp: %w", err)
	}

	logger.Infof("Successfully updated VolumeRefreshWorkflow completion timestamp for account %s",
		input.AccountUUID)

	activity.RecordHeartbeat(ctx, "Finished UpdateAccountVolumeRefreshTimestamp activity")
	return nil
}

// getVolumeUUIDs extracts UUIDs from a slice of volumes for logging
func getVolumeUUIDs(volumes []*datamodel.Volume) []string {
	uuids := make([]string, 0, len(volumes))
	for _, vol := range volumes {
		if vol != nil {
			uuids = append(uuids, vol.UUID)
		}
	}
	return uuids
}
