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

	// Extract new values from ONTAP
	newUsedBytes := uint64(*ontapVolume.Space.LogicalSpace.Used)

	// Check if there are any differences between database and ONTAP values
	hasChanges := false
	if dbVolume.UsedBytes != newUsedBytes {
		hasChanges = true
		logger.Debugf("Volume %s UsedBytes changed: %d -> %d", dbVolume.UUID, dbVolume.UsedBytes, newUsedBytes)
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

		// Create volume response for additional processing
		volResponse := &vsa.VolumeResponse{
			UsedBytes: *ontapVolume.Space.LogicalSpace.Used,
		}

		// Store results
		result.UpdatedVolumeByUUID[dbVolume.UUID] = updatedVolume
		result.OntapVolResponse[dbVolume.UUID] = volResponse

		logger.Debugf("Successfully matched volume %s with ONTAP data (UsedBytes: %d)",
			dbVolume.UUID, updatedVolume.UsedBytes)
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

	// Convert volumes to field updates for efficient batch processing
	volumeFieldUpdates := make([]datamodel.VolumeFieldUpdate, 0, len(dbVols))

	for _, vol := range dbVols {
		// Create targeted field update (typically UsedBytes and other metrics from ONTAP)
		fieldUpdate := datamodel.VolumeFieldUpdate{
			UUID: vol.UUID,
			Fields: map[string]interface{}{
				"used_bytes": vol.UsedBytes,
			},
		}
		volumeFieldUpdates = append(volumeFieldUpdates, fieldUpdate)
	}

	// Process updates in chunks using BatchUpdateVolumeFields
	for i := 0; i < len(volumeFieldUpdates); i += volumeSyncChunkSize {
		end := i + volumeSyncChunkSize
		if end > len(volumeFieldUpdates) {
			end = len(volumeFieldUpdates)
		}

		fieldUpdateChunk := volumeFieldUpdates[i:end]

		if err := se.BatchUpdateVolumeFields(ctx, fieldUpdateChunk); err != nil {
			return err
		}
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
