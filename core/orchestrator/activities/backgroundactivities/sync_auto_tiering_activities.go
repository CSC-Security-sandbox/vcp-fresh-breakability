package backgroundactivities

import (
	"context"
	"fmt"
	"sync"

	ontapModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

const (
	PoolConsumptionHotTier  = "hotTier"
	PoolConsumptionColdTier = "coldTier"
	PoolsToPauseKey         = "poolsToPause"
	PoolsToResumeKey        = "poolsToResume"
	PoolsToAutoResizeKey    = "poolsToAutoResize"
)

var (
	AutoTierHotTierAutoResizeThresholdPercent = env.GetInt64("AUTO_TIER_HOT_TIER_AUTO_RESIZE_THRESHOLD_PERCENT", 90)
	AutoTierHotTierAutoResizeIncreasePercent  = env.GetFloat64("AUTO_TIER_HOT_TIER_AUTO_RESIZE_INCREASE_PERCENT", 10)
)

type AutoTierSyncActivity struct {
	SE database.Storage
}

func (a *AutoTierSyncActivity) UpdateAggregateInOntap(ctx context.Context, node *models.Node, tieringFullnessThreshold int64) error {
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	err = getAndUpdateAggregate(provider, tieringFullnessThreshold)
	if err != nil {
		return err
	}

	return nil
}

func getAndUpdateAggregate(provider vsa.Provider, tieringFullnessThreshold int64) error {
	aggr, err := provider.GetAggregateByName(activities.AggregateName)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Update aggregate on provider
	updateParams := vsa.UpdateAggregateParams{
		UUID:                     aggr.UUID,
		TieringFullnessThreshold: tieringFullnessThreshold,
	}
	if err = provider.UpdateAggregate(updateParams); err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

// SegregatePools takes a list of pools and separates them into auto-tier-resume and auto-tier-paused pools
// based on the hot-tier provisioned & cold-tier consumption.
func (a *AutoTierSyncActivity) SegregatePools(ctx context.Context, pools []*database.PoolIdentifier, poolConsumptionsMap map[string]map[string]float64) (map[string][]*database.PoolIdentifier, error) {
	logger := util.GetLogger(ctx)
	se := a.SE
	poolsToPause := make([]*database.PoolIdentifier, 0)
	poolsToResume := make([]*database.PoolIdentifier, 0)
	poolsToAutoResize := make([]*database.PoolIdentifier, 0)

	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, poolIdentifier := range pools {
		wg.Add(1)
		go func(poolIdentifier *database.PoolIdentifier) {
			defer wg.Done()
			// Fetch the complete pool details using pool UUID and account ID
			pool, err := se.GetPool(ctx, poolIdentifier.UUID, poolIdentifier.AccountID)
			if err != nil {
				logger.Errorf("Failed to get pool, error: %v", err)
				return
			}

			// Skip pools that are not configured for auto-tiering or are not in a ready state
			if !pool.AllowAutoTiering || pool.State != models.LifeCycleStateREADY {
				return
			}

			poolConsumption, ok := poolConsumptionsMap[pool.UUID]
			if !ok {
				// If no consumption data is available for the pool, log it and skip it.
				logger.Errorf("Pool does not have consumption data in map, poolUUID: %s", pool.UUID)
				return
			}

			// Check if the pool is eligible for auto-tiering pause or resume
			// Conditions to pause auto-tiering:
			// 1. Hot tier size + cold tier consumption >= logical pool size.
			// 2. Auto-tiering is not yet paused.
			// Conditions to resume auto-tiering:
			// 1. Hot tier size + cold tier consumption < logical pool size.
			// 2. Auto-tiering is currently paused.
			if pool.AutoTieringConfig.HotTierSizeInBytes+int64(poolConsumption[PoolConsumptionColdTier]) >= pool.SizeInBytes {
				// Condition to check if the pool is not already paused
				if !pool.AutoTieringConfig.TieringPaused {
					mu.Lock()
					poolsToPause = append(poolsToPause, poolIdentifier)
					mu.Unlock()
				}
			} else {
				// Condition to check if the pool is already paused and needs to be resumed
				if pool.AutoTieringConfig.TieringPaused {
					mu.Lock()
					poolsToResume = append(poolsToResume, poolIdentifier)
					mu.Unlock()
				}

				// Check if the pool is eligible for auto-resizing of hot tier
				// Conditions:
				// 1. Auto-resize flag is enabled.
				// 2. Pool is not eligible for pausing AT, since resizing will definitely exceed the logical pool size.
				// 3. No volumes in the pool have bypass mode enabled.
				// 4. Hot tier usage exceeds the defined threshold percentage.
				// 5. New hot tier provisioned size + cold tier consumption < logical pool size.
				if pool.AutoTieringConfig.EnableHotTierAutoResize && pool.AutoTieringConfig.HotTierSizeInBytes != 0 {
					exists, err := checkPoolVolumesWithBypassModeEnabled(ctx, se, pool)
					if err != nil {
						logger.Errorf("Failed to check pool volumes for bypass mode, poolUUID: %s, error: %v", pool.UUID, err)
						return
					}

					if exists {
						logger.Infof("Skipping hot tier autoresize for pool with volumes having bypass mode enabled, poolUUID: %s", pool.UUID)
						return
					}

					usagePercent := (int64(poolConsumption[PoolConsumptionHotTier]) * 100) / pool.AutoTieringConfig.HotTierSizeInBytes
					// We are increasing the hot tier size by 10%. The result is in round off GiB.
					newHotTierSizeInBytes := int64(float64(pool.AutoTieringConfig.HotTierSizeInBytes)*(1+0.01*AutoTierHotTierAutoResizeIncreasePercent)/(1<<30)) * (1 << 30)

					if usagePercent >= AutoTierHotTierAutoResizeThresholdPercent && (newHotTierSizeInBytes+int64(poolConsumption[PoolConsumptionColdTier]) < pool.SizeInBytes) {
						mu.Lock()
						poolsToAutoResize = append(poolsToAutoResize, poolIdentifier)
						mu.Unlock()
					}
				}
			}
		}(poolIdentifier)
	}
	wg.Wait()

	return map[string][]*database.PoolIdentifier{
		PoolsToPauseKey:      poolsToPause,
		PoolsToResumeKey:     poolsToResume,
		PoolsToAutoResizeKey: poolsToAutoResize,
	}, nil
}

func checkPoolVolumesWithBypassModeEnabled(ctx context.Context, se database.Storage, pool *datamodel.PoolView) (bool, error) {
	logger := util.GetLogger(ctx)
	volumes, err := se.GetVolumesByPoolID(ctx, pool.ID)
	if err != nil {
		logger.Errorf("Failed to list volumes for pool: %s, error: %v", pool.UUID, err)
		return false, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	for _, vol := range volumes {
		if vol.AutoTieringEnabled && vol.AutoTieringPolicy != nil && vol.AutoTieringPolicy.HotTierBypassModeEnabled {
			return true, nil
		}
	}

	return false, nil
}

func (a *AutoTierSyncActivity) FetchAndSavePoolsTieringInfo(ctx context.Context, pools []*database.PoolIdentifier) (map[string]map[string]float64, error) {
	logger := util.GetLogger(ctx)
	se := a.SE
	poolsConsumptionsMap := make(map[string]map[string]float64)

	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, poolIdentifier := range pools {
		wg.Add(1)
		go func(poolIdentifier *database.PoolIdentifier) {
			defer wg.Done()
			// Fetch the complete pool details using pool UUID and account ID
			pool, err := se.GetPool(ctx, poolIdentifier.UUID, poolIdentifier.AccountID)
			if err != nil {
				logger.Errorf("Failed to get pool, error: %v", err)
				return
			}

			// Skip pools that are not configured for auto-tiering or are not in a ready state
			if !pool.AllowAutoTiering || pool.State != models.LifeCycleStateREADY {
				return
			}

			provider, err := GetOntapRestProviderForPool(ctx, se, database.ConvertPoolViewToPool(pool))
			if err != nil || provider == nil {
				logger.Errorf("Failed to get ONTAP rest provider for pool %v: %v", pool.UUID, err)
				return
			}

			// Check and collect pools which have a tiering fullness threshold set as 50 and not paused
			// These pools need to have their tiering fullness threshold set to 0
			if pool.AutoTieringConfig != nil && pool.AutoTieringConfig.TieringFullnessThreshold == 50 && !pool.AutoTieringConfig.TieringPaused {
				err = getAndUpdateAggregate(provider, 0)
				if err != nil {
					// Logging error and skipping. Will retry in next sync.
					logger.Warnf("Failed to set aggregate threshold to 0 in ontap for pool %s, Error: %v", pool.Name, err)
				} else {
					err = se.UpdatePoolTieringConfig(ctx, poolIdentifier.UUID, nil, nil, nillable.GetInt64Ptr(0))
					if err != nil {
						// Logging error and skipping. Will retry in next sync.
						logger.Warnf("Failed to set thresholdPercentage field to 0 in db for pool %s, Error: %v", pool.Name, err)
					}
				}
			}

			ontapVolumes, err := provider.GetVolumes()
			if err != nil {
				logger.Errorf("Failed to get ONTAP volumes for the pool: %s, %v", pool.UUID, err)
				return
			}

			// Get DB volumes for the pool to create a mapping from external UUID to database UUID
			dbVolumes, err := se.GetVolumesByPoolID(ctx, pool.ID)
			if err != nil {
				logger.Errorf("Failed to get volumes from database for pool %s: %v", pool.UUID, err)
				return
			}

			// Create a mapping from external UUID to database volume
			dbVolumeMap := make(map[string]*datamodel.Volume, len(dbVolumes))
			for _, v := range dbVolumes {
				if v.VolumeAttributes != nil {
					dbVolumeMap[v.VolumeAttributes.ExternalUUID] = v
				}
			}

			expectedVolCount := int64(len(dbVolumes))

			hotTierConsumption, coldTierConsumption, err := calculateAndUpdateHotColdTierConsumption(ctx, ontapVolumes, expectedVolCount, se, dbVolumeMap)
			if err != nil {
				logger.Errorf("Failed to calculate hot/cold tier consumption for the pool: %s, %v", pool.UUID, err)
				return
			}

			logger.Infof("Fetched pool tier consumption from ONTAP, poolUUID: %s, hotTierConsumption: %d, coldTierConsumption: %d", pool.UUID, hotTierConsumption, coldTierConsumption)

			mu.Lock()
			poolsConsumptionsMap[pool.UUID] = map[string]float64{
				PoolConsumptionHotTier:  float64(hotTierConsumption),
				PoolConsumptionColdTier: float64(coldTierConsumption),
			}
			mu.Unlock()
		}(poolIdentifier)
	}
	wg.Wait()

	return poolsConsumptionsMap, nil
}

func calculateAndUpdateHotColdTierConsumption(ctx context.Context, ontapVolumes []*vsa.Volume, expectedVolCount int64, se database.Storage, dbVolumeMap map[string]*datamodel.Volume) (int64, int64, error) {
	logger := util.GetLogger(ctx)
	hotTierConsumption := int64(0)
	coldTierConsumption := int64(0)
	volCount := 0

	// Collect tiering updates for bulk update
	tieringUpdates := make(map[string]datamodel.VolumeTieringUpdate)

	for _, volume := range ontapVolumes {
		if volume == nil || volume.IsSvmRoot == nil || *volume.IsSvmRoot {
			continue
		}
		if volume.Space == nil ||
			volume.Space.CapacityTierFootprint == nil || volume.Space.PerformanceTierFootprint == nil ||
			volume.Space.LogicalSpace == nil || volume.Space.LogicalSpace.Used == nil {
			continue
		}

		volCount++

		coldTierFootprint := float64(*volume.Space.CapacityTierFootprint)
		hotTierFootprint := float64(*volume.Space.PerformanceTierFootprint)
		logicalSpaceUsed := float64(*volume.Space.LogicalSpace.Used)

		denominator := coldTierFootprint + hotTierFootprint
		if denominator == 0 {
			continue
		}
		ratio := coldTierFootprint / denominator

		// Correcting the cold tier consumption based on logical space used
		// to avoid over counting where data reduction/compression is applied via ONTAP
		logicalColdTierConsumption := logicalSpaceUsed * ratio

		// Find the database volume UUID and add to bulk update map
		dbVolume, ok := dbVolumeMap[*volume.UUID]
		if !ok || dbVolume == nil {
			logger.Errorf("Volume with external UUID %s not found in database volume map", *volume.UUID)
			continue
		}

		// Convert bytes to GiB and add to a bulk update map
		tieringUpdates[dbVolume.UUID] = datamodel.VolumeTieringUpdate{
			HotTierSizeGib:  uint64(utils.ConvertBytesToGib(hotTierFootprint)),
			ColdTierSizeGib: uint64(utils.ConvertBytesToGib(logicalColdTierConsumption)),
		}

		coldTierConsumption += int64(logicalColdTierConsumption)
		hotTierConsumption += int64(hotTierFootprint)
	}

	if volCount != int(expectedVolCount) {
		return 0, 0, fmt.Errorf("mismatch in vol count fetched from db and ontap, expectedDBCount: %d, ontapCount: %d", expectedVolCount, volCount)
	}

	// Bulk update all volume tiering fields in a single database transaction
	if len(tieringUpdates) > 0 {
		err := se.BatchUpdateVolumeTieringFields(ctx, tieringUpdates)
		if err != nil {
			// Not returning error, this will be retried in the next sync cycle
			logger.Errorf("Failed to bulk update volume tiering footprints in DB: %v", err)
		} else {
			logger.Infof("Successfully bulk updated tiering fields for %d volumes", len(tieringUpdates))
		}
	}

	return hotTierConsumption, coldTierConsumption, nil
}

func (a *AutoTierSyncActivity) ToggleHotTierBypassModeForPoolVolumes(ctx context.Context, pool *datamodel.Pool) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	provider, err := GetOntapRestProviderForPool(ctx, se, pool)
	if err != nil || provider == nil {
		logger.Errorf("Failed to get ONTAP rest provider for pool %v: %v", pool.UUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	volumes, err := se.GetVolumesByPoolID(ctx, pool.ID)
	if err != nil {
		logger.Errorf("Failed to list volumes for pool: %s, error: %v", pool.UUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	for _, vol := range volumes {
		if vol.AutoTieringEnabled && vol.AutoTieringPolicy != nil && vol.AutoTieringPolicy.HotTierBypassModeEnabled {
			updateParams := vsa.UpdateVolumeParams{
				UUID: vol.VolumeAttributes.ExternalUUID,
				TieringPolicy: &vsa.TieringPolicy{
					CoolAccessTieringPolicy: ontapModels.VolumeInlineTieringPolicyNone,
				},
			}

			if !pool.AutoTieringConfig.TieringPaused {
				updateParams.TieringPolicy.CoolAccessTieringPolicy = ontapModels.VolumeInlineTieringPolicyAll
			}

			if err = provider.UpdateVolume(updateParams); err != nil {
				logger.Errorf("Failed to change tiering policy to: %s for volume: %s, error: %v", updateParams.TieringPolicy.CoolAccessTieringPolicy, vol.UUID, err)
				return vsaerrors.WrapAsTemporalApplicationError(err)
			}

			logger.Infof("Tiering policy changed to: %s for volume: %s", updateParams.TieringPolicy.CoolAccessTieringPolicy, vol.UUID)
		}
	}
	return nil
}

func (a *AutoTierSyncActivity) UpdatePoolTieringConsumptionInDB(ctx context.Context, poolsConsumptionsMap map[string]map[string]float64) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	for poolUUID, consumptionMap := range poolsConsumptionsMap {
		hotTierConsumption := int64(consumptionMap[PoolConsumptionHotTier])
		coldTierConsumption := int64(consumptionMap[PoolConsumptionColdTier])

		err := se.UpdatePoolTieringConfig(ctx, poolUUID, nillable.GetInt64Ptr(hotTierConsumption), nillable.GetInt64Ptr(coldTierConsumption), nil)
		if err != nil {
			return fmt.Errorf("failed to update pool tiering consumption in DB for poolUUID: %s, error: %v", poolUUID, err)
		}

		logger.Infof("Updated pool tiering consumption in DB, poolUUID: %s, hotTierConsumption: %d, coldTierConsumption: %d", poolUUID, hotTierConsumption, coldTierConsumption)
	}
	return nil
}
