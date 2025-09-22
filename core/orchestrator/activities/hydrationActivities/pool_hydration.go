package hydrationActivities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	HydrateUpdatedPoolToCCFE = _hydrateUpdatedPoolToCCFE
)

func _hydrateUpdatedPoolToCCFE(ctx context.Context, dbPool datamodel.Pool) error {
	logger := util.GetLogger(ctx)

	// Validate required fields
	if err := validateHydratePool(dbPool); err != nil {
		logger.Errorf("Validation failed for hydrate pool: %s, error: %v", dbPool.Name, err)
		return err
	}

	callbackToken, err := auth.GenerateCallbackToken(ctx)
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return err
	}

	hydratePool := models.PoolHydrateObject{
		OwnerID:        dbPool.Account.Name,
		PoolID:         dbPool.UUID,
		Name:           dbPool.Name,
		State:          dbPool.State,
		Region:         dbPool.PoolAttributes.PrimaryZone,
		HotTierSizeGib: dbPool.AutoTieringConfig.HotTierSizeInBytes / (1 << 30),
	}

	// Hydrate the pool to CCFE
	err = common.HydrateUpdatedPool(ctx, hydratePool, callbackToken)
	if err != nil {
		logger.Error("Failed to hydrate pool to CCFE", "poolID", hydratePool.PoolID, "error", err)
		return err
	}

	return nil
}

func validateHydratePool(pool datamodel.Pool) error {
	if pool.Account.Name == "" {
		return errors.New("OwnerID/AccountName missing for hydrate pool")
	}
	if pool.UUID == "" {
		return errors.New("PoolID missing for hydrate pool")
	}
	if pool.PoolAttributes.PrimaryZone == "" {
		return errors.New("Region missing for hydrate pool")
	}
	if pool.Name == "" {
		return errors.New("Name missing for hydrate pool")
	}
	if pool.AutoTieringConfig.HotTierSizeInBytes <= 0 {
		return errors.New("HotTierSizeInBytes missing for hydrate pool")
	}
	if pool.State == "" {
		return errors.New("State missing for hydrate pool")
	}
	return nil
}
